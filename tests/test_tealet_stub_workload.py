"""Stochastic in-place vs stub-clone tealet workload.

Mirrors libtealet ``tests/test_stochastic.c --compare``: a mixed recursion,
create, switch, and exit workload run twice under the same RNG seed, once
with tealets created at the caller's current stack depth and once with
tealets duplicated from a single stub template.

Pytest collects a short correctness check (small operation counts). Timing
and RSS comparison is opt-in::

    python tests/test_tealet_stub_workload.py --compare -n 20000
    python tests/test_tealet_stub_workload.py --compare -n 20000 --stub-depth 20

Python-to-Python calls are inlined on 3.11+, so recursion goes through a C
builtin (``map``) to occupy a C eval frame per level.  That is what moves
the C stack pointer libtealet uses as a tealet base.
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import random
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, replace

import pytest

import _tealet

MODE_INPLACE = "inplace"
MODE_STUB = "stub"

_EXIT = object()
_DEFAULT_OPERATIONS = 1000
_DEFAULT_MAX_DEPTH = 20
_DEFAULT_MAX_TEALETS = 100
_DEFAULT_SEED = 42
_PAD_SIZE = 256
# pytest/CI stays small: enough to create, switch, and exit, not a bench.
_CI_OPERATIONS = 80
_CI_MAX_DEPTH = 8
_CI_MAX_TEALETS = 12


def _from_c(fn):
    """Call *fn* from a C builtin so this level occupies a C eval frame."""
    return next(map(_call, (fn,)))


def _call(fn):
    return fn()


def _rss_bytes():
    try:
        with open("/proc/self/statm", encoding="ascii") as fh:
            resident_pages = int(fh.read().split()[1])
        return resident_pages * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        return 0


@dataclass(frozen=True)
class WorkloadParams:
    mode: str = MODE_INPLACE
    seed: int = _DEFAULT_SEED
    operations: int = _DEFAULT_OPERATIONS
    max_depth: int = _DEFAULT_MAX_DEPTH
    max_tealets: int = _DEFAULT_MAX_TEALETS
    stub_depth: int = 0


@dataclass
class WorkloadResult:
    name: str
    mode: str
    stub_depth: int
    operations: int
    creates: int
    switches: int
    exits: int
    tealets_created: int
    peak_live: int
    time_s: float
    peak_rss_bytes: int
    rss_delta_bytes: int


def make_stub_at_depth(depth):
    """Create a stub after *depth* C-bounced recursive frames."""
    pad = bytearray(_PAD_SIZE)
    for i in range(_PAD_SIZE):
        pad[i] = (depth + i) & 0xFF
    if depth > 0:
        stub = _from_c(lambda: make_stub_at_depth(depth - 1))
    else:
        stub = _tealet.tealet()
        stub.stub()
    # keep the pad live so the frame cannot be discarded as unused
    if pad[0] == 0:
        pad[0] = 1
    return stub


class _Trial:
    def __init__(self, params: WorkloadParams):
        if params.mode not in (MODE_INPLACE, MODE_STUB):
            raise ValueError(f"unknown mode {params.mode!r}")
        self.params = params
        self.rng = random.Random(params.seed)
        self.tealets = []
        self.stub = None
        self.main = None
        self.operations = 0
        self.creates = 0
        self.switches = 0
        self.exits = 0
        self.tealets_created = 0
        self.peak_live = 0
        self.peak_rss_bytes = 0
        self.shutdown = False

    def run(self) -> WorkloadResult:
        gc.collect()
        rss0 = _rss_bytes()
        self.peak_rss_bytes = rss0
        self.main = _tealet.current()
        assert self.main is _tealet.main()
        self._add(self.main)
        if self.params.mode == MODE_STUB:
            self.stub = make_stub_at_depth(self.params.stub_depth)
            assert self.stub.state == _tealet.STATE_STUB

        t0 = time.perf_counter()
        self._worker_recursive(self.main, 0)
        elapsed = time.perf_counter() - t0
        self._sample_rss()
        self._drain()
        self._sample_rss()

        name = f"stub@{self.params.stub_depth}" if self.params.mode == MODE_STUB else MODE_INPLACE
        return WorkloadResult(
            name=name,
            mode=self.params.mode,
            stub_depth=self.params.stub_depth,
            operations=self.operations,
            creates=self.creates,
            switches=self.switches,
            exits=self.exits,
            tealets_created=self.tealets_created,
            peak_live=self.peak_live,
            time_s=elapsed,
            peak_rss_bytes=self.peak_rss_bytes,
            rss_delta_bytes=max(0, self.peak_rss_bytes - rss0),
        )

    def _add(self, tealet):
        self.tealets.append(tealet)
        n = len(self.tealets)
        if n > self.peak_live:
            self.peak_live = n

    def _remove(self, tealet):
        for i, item in enumerate(self.tealets):
            if item is tealet:
                last = self.tealets.pop()
                if i < len(self.tealets):
                    self.tealets[i] = last
                return

    def _pick_random(self, exclude):
        n = len(self.tealets)
        if n <= 1:
            return None
        for _ in range(10):
            candidate = self.tealets[self.rng.randrange(n)]
            if candidate is not exclude:
                return candidate
        for candidate in self.tealets:
            if candidate is not exclude:
                return candidate
        return None

    def _sample_rss(self):
        rss = _rss_bytes()
        if rss > self.peak_rss_bytes:
            self.peak_rss_bytes = rss

    def _switch(self, target):
        self.switches += 1
        return target.switch()

    def _create(self):
        self.creates += 1
        if self.params.mode == MODE_STUB:
            clone = self.stub.duplicate()
            return clone.run(self._worker_entry, None)
        created = _tealet.tealet()
        return created.run(self._worker_entry, None)

    def _worker_entry(self, current, _arg):
        self._add(current)
        self.tealets_created += 1
        result = self._worker_recursive(current, 0)
        self._remove(current)
        self.exits += 1
        if result is _EXIT:
            target = self._pick_random(current)
            return target if target is not None else current.main()
        return current.main()

    def _worker_recursive(self, current, depth):
        pad = bytearray(_PAD_SIZE)
        for i in range(_PAD_SIZE):
            pad[i] = (depth + i) & 0xFF

        self.operations += 1
        if self.operations % 10 == 0:
            self._sample_rss()
        if self.operations >= self.params.operations:
            self.shutdown = True

        while True:
            if self.shutdown:
                if pad[0] == 0:
                    pad[0] = 1
                return None

            choice = self.rng.randrange(5)
            if choice == 0 and depth > 0:
                return None
            if choice == 1 and depth < self.params.max_depth:
                nested = _from_c(lambda: self._worker_recursive(current, depth + 1))
                if nested is _EXIT:
                    return _EXIT
                continue
            if choice == 2 and len(self.tealets) > 1:
                target = self._pick_random(current)
                if target is not None:
                    self._switch(target)
                continue
            if choice == 3 and len(self.tealets) < self.params.max_tealets:
                self._create()
                continue
            if choice == 4 and current is not self.main and len(self.tealets) >= self.params.max_tealets:
                return _EXIT

    def _drain(self):
        while True:
            target = None
            for item in self.tealets:
                if item is not self.main:
                    target = item
                    break
            if target is None:
                break
            self._switch(target)


def run_workload(params: WorkloadParams) -> WorkloadResult:
    return _Trial(params).run()


def run_workload_isolated(params: WorkloadParams) -> WorkloadResult:
    """Run one trial in a child process so RSS and timing are not shared."""
    cmd = [
        sys.executable,
        os.path.abspath(__file__),
        "--json",
        "--mode",
        params.mode,
        "-n",
        str(params.operations),
        "-d",
        str(params.max_depth),
        "--stub-depth",
        str(params.stub_depth),
        "--seed",
        str(params.seed),
        "--max-tealets",
        str(params.max_tealets),
    ]
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return WorkloadResult(**json.loads(proc.stdout))


def format_comparison(inplace: WorkloadResult, stub: WorkloadResult, params: WorkloadParams) -> str:
    def ratio(left, right):
        if left == 0:
            return "n/a"
        if isinstance(left, float) or isinstance(right, float):
            return f"{right / left:16.3f}"
        return f"{right / left:16.3f}"

    def line(name, left, right, fmt):
        return f"  {name:<26} {left:{fmt}} {right:{fmt}} {ratio(left, right)}"

    header = (
        f"inplace vs stub  (seed={params.seed}, ops={params.operations}, "
        f"depth={params.max_depth}, stub-depth={stub.stub_depth}, "
        f"max tealets={params.max_tealets})"
    )
    rows = [
        "=" * 64,
        header,
        "=" * 64,
        f"  {'Metric':<26} {inplace.name:>14} {stub.name:>14} {'ratio':>16}",
        f"  {'-' * 26} {'-' * 14} {'-' * 14} {'-' * 16}",
        line("Time (s)", inplace.time_s, stub.time_s, "14.3f"),
        line("Operations", inplace.operations, stub.operations, "14d"),
        line("Creates", inplace.creates, stub.creates, "14d"),
        line("Switches", inplace.switches, stub.switches, "14d"),
        line("Exits", inplace.exits, stub.exits, "14d"),
        line("Tealets created", inplace.tealets_created, stub.tealets_created, "14d"),
        line("Peak live tealets", inplace.peak_live, stub.peak_live, "14d"),
        line("RSS delta (bytes)", inplace.rss_delta_bytes, stub.rss_delta_bytes, "14d"),
        line("Peak RSS (bytes)", inplace.peak_rss_bytes, stub.peak_rss_bytes, "14d"),
        "",
        "Notes:",
        "  ratio > 1 means stub is larger/slower than in-place.",
        "  In-place tealets are based at the caller's stack depth.",
        "  Stub tealets duplicate one template, so they share a stack base",
        "  and overlap to different degrees as they recurse.",
        "  Switch counts exclude the switch inside tealet.run().",
        "  RSS delta is peak resident size minus the sample at trial start.",
        "  CLI --compare runs each mode in a child process.",
    ]
    return "\n".join(rows)


def _assert_drained():
    assert _tealet.current() is _tealet.main()
    active = [t for t in _tealet.thread_active() if not t.is_main() and t.state == _tealet.STATE_RUN]
    assert active == []


def _eventful(result: WorkloadResult):
    assert result.operations >= 1
    assert result.creates >= 1
    assert result.switches >= 1
    assert result.exits >= 1
    assert result.tealets_created >= 1


def _ci_params(**overrides):
    params = dict(
        seed=_DEFAULT_SEED,
        operations=_CI_OPERATIONS,
        max_depth=_CI_MAX_DEPTH,
        max_tealets=_CI_MAX_TEALETS,
        stub_depth=0,
    )
    params.update(overrides)
    return WorkloadParams(**params)


class TestStubWorkload:
    def test_inplace_completes_with_create_switch_exit(self):
        result = run_workload(_ci_params(mode=MODE_INPLACE))
        _eventful(result)
        _assert_drained()

    @pytest.mark.stub
    def test_stub_completes_with_create_switch_exit(self):
        result = run_workload(_ci_params(mode=MODE_STUB, stub_depth=4))
        _eventful(result)
        _assert_drained()
        assert result.name == "stub@4"

    @pytest.mark.stub
    def test_same_seed_same_event_counts(self):
        # creation mode and stub depth change the stack base, not the RNG
        # decisions, so event counts must match under one seed.
        inplace = run_workload(_ci_params(mode=MODE_INPLACE))
        stub = run_workload(_ci_params(mode=MODE_STUB))
        deep = run_workload(_ci_params(mode=MODE_STUB, stub_depth=8))
        _assert_drained()
        for other in (stub, deep):
            assert inplace.operations == other.operations
            assert inplace.creates == other.creates
            assert inplace.switches == other.switches
            assert inplace.exits == other.exits
            assert inplace.tealets_created == other.tealets_created
            assert inplace.peak_live == other.peak_live


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Compare in-place vs stub-clone tealet workloads")
    parser.add_argument("-n", "--operations", type=int, default=_DEFAULT_OPERATIONS)
    parser.add_argument("-d", "--depth", type=int, default=_DEFAULT_MAX_DEPTH)
    parser.add_argument("-m", "--mode", choices=(MODE_INPLACE, MODE_STUB, "compare"), default="compare")
    parser.add_argument("--compare", action="store_true", help="shorthand for --mode compare")
    parser.add_argument("-s", "--stub-depth", type=int, default=0)
    parser.add_argument("--seed", type=int, default=_DEFAULT_SEED)
    parser.add_argument("--max-tealets", type=int, default=_DEFAULT_MAX_TEALETS)
    parser.add_argument("--json", action="store_true", help="print one trial as JSON (used by isolated compare)")
    return parser.parse_args(argv)


def _print_result(result: WorkloadResult):
    print(f"{result.name} completed in {result.time_s:.3f} s")
    print(f"Operations: {result.operations}")
    print(f"Creates/switches/exits: {result.creates} / {result.switches} / {result.exits}")
    print(f"Tealets created: {result.tealets_created}")
    print(f"Peak live tealets: {result.peak_live}")
    print(f"RSS delta: {result.rss_delta_bytes} bytes")
    print(f"Peak RSS: {result.peak_rss_bytes} bytes")


def main(argv=None):
    args = _parse_args(argv)
    mode = "compare" if args.compare else args.mode
    base = WorkloadParams(
        seed=args.seed,
        operations=args.operations,
        max_depth=args.depth,
        max_tealets=args.max_tealets,
        stub_depth=args.stub_depth,
    )
    if args.json:
        if mode == "compare":
            raise SystemExit("--json requires --mode inplace or --mode stub")
        result = run_workload(replace(base, mode=mode))
        print(json.dumps(asdict(result)))
        return 0
    print("Stochastic tealet workload")
    print("==========================")
    print(f"Target operations: {base.operations}")
    print(f"Max recursion depth: {base.max_depth}")
    print(f"Max tealets: {base.max_tealets}")
    print(f"Seed: {base.seed}")
    if mode == "compare":
        print(f"Creation mode: compare (inplace vs stub at depth {base.stub_depth})")
        inplace = run_workload_isolated(replace(base, mode=MODE_INPLACE))
        stub = run_workload_isolated(replace(base, mode=MODE_STUB))
        print(format_comparison(inplace, stub, base))
        return 0
    print(f"Creation mode: {mode}")
    result = run_workload(replace(base, mode=mode))
    _print_result(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
