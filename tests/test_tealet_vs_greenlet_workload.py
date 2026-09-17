"""Stochastic tealet vs upstream greenlet workload.

Same mixed recursion, create, switch, and exit loop as
``tests/test_tealet_stub_workload.py``, run twice under one RNG seed: once
with in-place tealets and once with PyPI ``greenlet``.

Pytest collects a short correctness check. Timing and RSS comparison is
opt-in and needs the optional ``greenlet`` dependency group::

    uv sync --active --dev --group greenlet
    python tests/test_tealet_vs_greenlet_workload.py --compare -n 20000

If the workspace ``tealet-greenlet`` package is also installed, its
``greenlet`` shim shadows PyPI greenlet.  Prefer an isolated run::

    uv run --isolated --group greenlet python tests/test_tealet_vs_greenlet_workload.py --compare -n 20000

Python-to-Python calls are inlined on 3.11+, so recursion goes through a C
builtin (``map``) to occupy a C eval frame per level.
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

MODE_TEALET = "tealet"
MODE_GREENLET = "greenlet"

_EXIT = object()
_DEFAULT_OPERATIONS = 1000
_DEFAULT_MAX_DEPTH = 20
_DEFAULT_MAX_TASKS = 100
_DEFAULT_SEED = 42
_PAD_SIZE = 256
# pytest/CI stays small: enough to create, switch, and exit, not a bench.
_CI_OPERATIONS = 80
_CI_MAX_DEPTH = 8
_CI_MAX_TASKS = 12

_GREENLET_INSTALL = (
    "PyPI greenlet is not importable. Install the optional dependency group:\n"
    "  uv sync --active --dev --group greenlet\n"
    "If tealet-greenlet is installed, its greenlet shim shadows the C extension.\n"
    "Use an isolated env that does not include tealet-greenlet:\n"
    "  uv run --isolated --group greenlet python tests/test_tealet_vs_greenlet_workload.py --compare"
)


def load_upstream_greenlet():
    """Return PyPI greenlet, or None if missing or shadowed by tealet-greenlet."""
    try:
        import greenlet
        from greenlet import _greenlet as ext
    except ImportError:
        return None
    path = getattr(ext, "__file__", "") or ""
    # tealet-greenlet ships a pure-Python greenlet._greenlet; PyPI greenlet
    # is a C extension (.so/.pyd).  Do not match "tealet" as a substring —
    # this repo's path already contains "pytealet".
    if path.endswith((".py", ".pyc")):
        return None
    return greenlet


_GREENLET = load_upstream_greenlet()
_requires_greenlet = pytest.mark.skipif(
    _GREENLET is None,
    reason="PyPI greenlet is not installed (optional dependency group 'greenlet')",
)


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
    mode: str = MODE_TEALET
    seed: int = _DEFAULT_SEED
    operations: int = _DEFAULT_OPERATIONS
    max_depth: int = _DEFAULT_MAX_DEPTH
    max_tasks: int = _DEFAULT_MAX_TASKS


@dataclass
class WorkloadResult:
    name: str
    mode: str
    operations: int
    creates: int
    switches: int
    exits: int
    spawned: int
    peak_live: int
    time_s: float
    peak_rss_bytes: int
    rss_delta_bytes: int


class _Trial:
    def __init__(self, params: WorkloadParams):
        if params.mode not in (MODE_TEALET, MODE_GREENLET):
            raise ValueError(f"unknown mode {params.mode!r}")
        if params.mode == MODE_GREENLET and _GREENLET is None:
            raise RuntimeError(_GREENLET_INSTALL)
        self.params = params
        self.rng = random.Random(params.seed)
        self.tasks = []
        self.main = None
        self.operations = 0
        self.creates = 0
        self.switches = 0
        self.exits = 0
        self.spawned = 0
        self.peak_live = 0
        self.peak_rss_bytes = 0
        self.shutdown = False
        self._greenlet = _GREENLET if params.mode == MODE_GREENLET else None

    def run(self) -> WorkloadResult:
        gc.collect()
        rss0 = _rss_bytes()
        self.peak_rss_bytes = rss0
        if self.params.mode == MODE_GREENLET:
            self.main = self._greenlet.getcurrent()
        else:
            self.main = _tealet.current()
            assert self.main is _tealet.main()
        self._add(self.main)

        t0 = time.perf_counter()
        self._worker_recursive(self.main, 0)
        elapsed = time.perf_counter() - t0
        self._sample_rss()
        self._drain()
        self._sample_rss()
        assert self.tasks == [self.main]

        return WorkloadResult(
            name=self.params.mode,
            mode=self.params.mode,
            operations=self.operations,
            creates=self.creates,
            switches=self.switches,
            exits=self.exits,
            spawned=self.spawned,
            peak_live=self.peak_live,
            time_s=elapsed,
            peak_rss_bytes=self.peak_rss_bytes,
            rss_delta_bytes=max(0, self.peak_rss_bytes - rss0),
        )

    def _add(self, task):
        self.tasks.append(task)
        n = len(self.tasks)
        if n > self.peak_live:
            self.peak_live = n

    def _remove(self, task):
        for i, item in enumerate(self.tasks):
            if item is task:
                last = self.tasks.pop()
                if i < len(self.tasks):
                    self.tasks[i] = last
                return

    def _pick_random(self, exclude):
        n = len(self.tasks)
        if n <= 1:
            return None
        for _ in range(10):
            candidate = self.tasks[self.rng.randrange(n)]
            if candidate is not exclude:
                return candidate
        for candidate in self.tasks:
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
        if self.params.mode == MODE_GREENLET:
            # parent=main keeps siblings acyclic so exit can reparent to any
            # live target (the default parent is the creator, which cycles
            # when a creator dies into its child)
            spawned = self._greenlet.greenlet(self._greenlet_worker_entry, parent=self.main)
            return spawned.switch()
        created = _tealet.tealet()
        return created.run(self._tealet_worker_entry, None)

    def _finish_worker(self, current, result):
        self._remove(current)
        self.exits += 1
        if result is _EXIT:
            target = self._pick_random(current)
            return target if target is not None else self.main
        return self.main

    def _tealet_worker_entry(self, current, _arg):
        self._add(current)
        self.spawned += 1
        result = self._worker_recursive(current, 0)
        return self._finish_worker(current, result)

    def _greenlet_worker_entry(self):
        current = self._greenlet.getcurrent()
        self._add(current)
        self.spawned += 1
        result = self._worker_recursive(current, 0)
        target = self._finish_worker(current, result)
        # dying greenlets resume parent; point parent at the same target
        # tealet would return, so the RNG and switch topology stay aligned
        if current is not self.main and target is not None:
            current.parent = target
        return None

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
            if choice == 2 and len(self.tasks) > 1:
                target = self._pick_random(current)
                if target is not None:
                    self._switch(target)
                continue
            if choice == 3 and len(self.tasks) < self.params.max_tasks:
                self._create()
                continue
            if choice == 4 and current is not self.main and len(self.tasks) >= self.params.max_tasks:
                return _EXIT

    def _drain(self):
        while True:
            target = None
            for item in self.tasks:
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
        "--seed",
        str(params.seed),
        "--max-tasks",
        str(params.max_tasks),
    ]
    proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return WorkloadResult(**json.loads(proc.stdout))


def format_comparison(tealet: WorkloadResult, greenlet: WorkloadResult, params: WorkloadParams) -> str:
    def ratio(left, right):
        if left == 0:
            return "n/a"
        return f"{right / left:16.3f}"

    def line(name, left, right, fmt):
        return f"  {name:<26} {left:{fmt}} {right:{fmt}} {ratio(left, right)}"

    header = (
        f"tealet vs greenlet  (seed={params.seed}, ops={params.operations}, "
        f"depth={params.max_depth}, max tasks={params.max_tasks})"
    )
    rows = [
        "=" * 64,
        header,
        "=" * 64,
        f"  {'Metric':<26} {tealet.name:>14} {greenlet.name:>14} {'ratio':>16}",
        f"  {'-' * 26} {'-' * 14} {'-' * 14} {'-' * 16}",
        line("Time (s)", tealet.time_s, greenlet.time_s, "14.3f"),
        line("Operations", tealet.operations, greenlet.operations, "14d"),
        line("Creates", tealet.creates, greenlet.creates, "14d"),
        line("Switches", tealet.switches, greenlet.switches, "14d"),
        line("Exits", tealet.exits, greenlet.exits, "14d"),
        line("Spawned", tealet.spawned, greenlet.spawned, "14d"),
        line("Peak live", tealet.peak_live, greenlet.peak_live, "14d"),
        line("RSS delta (bytes)", tealet.rss_delta_bytes, greenlet.rss_delta_bytes, "14d"),
        line("Peak RSS (bytes)", tealet.peak_rss_bytes, greenlet.peak_rss_bytes, "14d"),
        "",
        "Notes:",
        "  ratio > 1 means greenlet is larger/slower than tealet.",
        "  Tealets are created with tealet() + run() at the caller's stack depth.",
        "  Greenlets are created with greenlet(func) + switch().",
        "  Switch counts exclude the initial run/switch that starts a worker.",
        "  A dying greenlet resumes its parent; the worker points parent at the",
        "  same target tealet would return, so event counts stay aligned.",
        "  RSS delta is peak resident size minus the sample at trial start.",
        "  CLI --compare runs each mode in a child process.",
        "  This compares core tealet with PyPI greenlet, not tealet-greenlet.",
    ]
    return "\n".join(rows)


def _assert_tealet_drained():
    # only current/main: earlier tests in the suite may leave parked tealets
    assert _tealet.current() is _tealet.main()


def _assert_greenlet_drained():
    current = _GREENLET.getcurrent()
    assert current.parent is None


def _eventful(result: WorkloadResult):
    assert result.operations >= 1
    assert result.creates >= 1
    assert result.switches >= 1
    assert result.exits >= 1
    assert result.spawned >= 1


def _ci_params(**overrides):
    params = dict(
        seed=_DEFAULT_SEED,
        operations=_CI_OPERATIONS,
        max_depth=_CI_MAX_DEPTH,
        max_tasks=_CI_MAX_TASKS,
    )
    params.update(overrides)
    return WorkloadParams(**params)


class TestTealetVsGreenletWorkload:
    def test_tealet_completes_with_create_switch_exit(self):
        result = run_workload(_ci_params(mode=MODE_TEALET))
        _eventful(result)
        _assert_tealet_drained()

    @_requires_greenlet
    @pytest.mark.greenlet
    def test_greenlet_completes_with_create_switch_exit(self):
        result = run_workload(_ci_params(mode=MODE_GREENLET))
        _eventful(result)
        _assert_greenlet_drained()

    @_requires_greenlet
    @pytest.mark.greenlet
    def test_same_seed_same_event_counts(self):
        # backend must not change RNG decisions, so event counts match
        tealet = run_workload(_ci_params(mode=MODE_TEALET))
        greenlet = run_workload(_ci_params(mode=MODE_GREENLET))
        _assert_tealet_drained()
        _assert_greenlet_drained()
        assert tealet.operations == greenlet.operations
        assert tealet.creates == greenlet.creates
        assert tealet.switches == greenlet.switches
        assert tealet.exits == greenlet.exits
        assert tealet.spawned == greenlet.spawned
        assert tealet.peak_live == greenlet.peak_live


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Compare tealet vs upstream greenlet workloads")
    parser.add_argument("-n", "--operations", type=int, default=_DEFAULT_OPERATIONS)
    parser.add_argument("-d", "--depth", type=int, default=_DEFAULT_MAX_DEPTH)
    parser.add_argument("-m", "--mode", choices=(MODE_TEALET, MODE_GREENLET, "compare"), default="compare")
    parser.add_argument("--compare", action="store_true", help="shorthand for --mode compare")
    parser.add_argument("--seed", type=int, default=_DEFAULT_SEED)
    parser.add_argument("--max-tasks", type=int, default=_DEFAULT_MAX_TASKS)
    parser.add_argument("--json", action="store_true", help="print one trial as JSON (used by isolated compare)")
    return parser.parse_args(argv)


def _print_result(result: WorkloadResult):
    print(f"{result.name} completed in {result.time_s:.3f} s")
    print(f"Operations: {result.operations}")
    print(f"Creates/switches/exits: {result.creates} / {result.switches} / {result.exits}")
    print(f"Spawned: {result.spawned}")
    print(f"Peak live: {result.peak_live}")
    print(f"RSS delta: {result.rss_delta_bytes} bytes")
    print(f"Peak RSS: {result.peak_rss_bytes} bytes")


def main(argv=None):
    args = _parse_args(argv)
    mode = "compare" if args.compare else args.mode
    base = WorkloadParams(
        seed=args.seed,
        operations=args.operations,
        max_depth=args.depth,
        max_tasks=args.max_tasks,
    )
    if args.json:
        if mode == "compare":
            raise SystemExit("--json requires --mode tealet or --mode greenlet")
        result = run_workload(replace(base, mode=mode))
        print(json.dumps(asdict(result)))
        return 0
    print("Stochastic tealet vs greenlet workload")
    print("=====================================")
    print(f"Target operations: {base.operations}")
    print(f"Max recursion depth: {base.max_depth}")
    print(f"Max tasks: {base.max_tasks}")
    print(f"Seed: {base.seed}")
    if mode == "compare":
        if _GREENLET is None:
            raise SystemExit(_GREENLET_INSTALL)
        print("Creation mode: compare (tealet vs greenlet)")
        tealet = run_workload_isolated(replace(base, mode=MODE_TEALET))
        greenlet = run_workload_isolated(replace(base, mode=MODE_GREENLET))
        print(format_comparison(tealet, greenlet, base))
        return 0
    if mode == MODE_GREENLET and _GREENLET is None:
        raise SystemExit(_GREENLET_INSTALL)
    print(f"Creation mode: {mode}")
    result = run_workload(replace(base, mode=mode))
    _print_result(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
