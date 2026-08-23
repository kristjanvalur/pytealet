"""C profiler for tealet on Python 3.12+ (sys.monitoring + set_trace)."""

from __future__ import annotations

import sys
from typing import Any

if sys.version_info < (3, 12):
    raise ImportError("tealet.cprofile requires Python 3.12 or newer")

import _tealet_profile
from tealet.profile import _MAIN_FAMILY, StackStats, _merge_timings, _snapshot_timings, _Utils

__all__ = ["Profile", "StackStats", "run", "runctx"]


def _label(code: object) -> tuple[str, int, str]:
    if isinstance(code, str):
        return ("~", 0, code)
    filename = getattr(code, "co_filename", "~")
    line = getattr(code, "co_firstlineno", 0)
    name = getattr(code, "co_name", str(code))
    return (filename, line, name)


def _entries_to_timings(entries: list) -> dict:
    timings: dict = {}
    for entry in entries:
        func = _label(entry["code"])
        nc = int(entry["callcount"])
        rec = int(entry["reccallcount"])
        cc = nc - rec
        if cc < 0:
            cc = nc
        timings[func] = (cc, 0, float(entry["inlinetime"]), float(entry["totaltime"]), {})
    for entry in entries:
        func = _label(entry["code"])
        calls = entry.get("calls") or {}
        for callee, n in calls.items():
            cfunc = _label(callee)
            if cfunc not in timings:
                continue
            callers = timings[cfunc][4]
            callers[func] = callers.get(func, 0) + int(n)
    return timings


class Profile:
    """3.12+ C profiler with per-stack timings and tealet switch support.

    ``fold_on_exit`` (default on) merges a finished tealet into its family and
    drops the individual stack. Pass ``False`` to keep every stack for
    :meth:`stacks`. ``sys.monitoring`` is interpreter-wide, so :meth:`enable`
    samples every thread; each thread has its own stack (TLS).
    """

    def __init__(self, builtins: bool = True, *, fold_on_exit: bool = True) -> None:
        self._p = _tealet_profile.Profiler(fold_on_exit=fold_on_exit)
        self.stats: dict = {}
        self._builtins = builtins
        self.fold_on_exit = fold_on_exit

    def enable(self, subcalls: bool = True, builtins: bool | None = None) -> None:
        if builtins is None:
            builtins = self._builtins
        self._p.enable(subcalls, builtins)

    def disable(self) -> None:
        self._p.disable()

    def run(self, cmd: str):
        import __main__

        return self.runctx(cmd, __main__.__dict__, __main__.__dict__)

    def runctx(self, cmd, globals, locals):
        self.enable()
        try:
            exec(cmd, globals, locals)  # noqa: S102
        finally:
            self.disable()
        return self

    def runcall(self, func, /, *args, **kw):
        self.enable()
        try:
            return func(*args, **kw)
        finally:
            self.disable()

    def create_stats(self) -> None:
        self.stats = self.combined().stats

    def print_stats(self, sort: Any = -1) -> None:
        import pstats

        self.create_stats()
        pstats.Stats(self).strip_dirs().sort_stats(sort).print_stats()

    def dump_stats(self, file: str) -> None:
        import marshal

        with open(file, "wb") as f:
            self.create_stats()
            marshal.dump(self.stats, f)

    def stacks(self) -> list[StackStats]:
        return [_stack_stats(raw, nstacks=1) for raw in self._p.dump_stacks()]

    def stack_families(self) -> list[StackStats]:
        groups = self._grouped_parts()
        out = [
            StackStats(
                _snapshot_timings(item["timings"]),
                family=key,
                nstacks=item["nstacks"],
                thread_ids=frozenset(item["tids"]),
            )
            for key, item in groups.items()
        ]
        out.sort(key=lambda view: (-view.nstacks, view.family or _MAIN_FAMILY))
        return out

    def combined(self) -> StackStats:
        merged: dict = {}
        nstacks = 0
        tids: set[int] = set()
        for item in self._grouped_parts().values():
            _merge_timings(merged, item["timings"])
            nstacks += item["nstacks"]
            tids.update(item["tids"])
        return StackStats(_snapshot_timings(merged), family=None, nstacks=nstacks, thread_ids=frozenset(tids))

    def _grouped_parts(self) -> dict[tuple[str, int, str], dict]:
        groups: dict[tuple[str, int, str], dict] = {}
        for raw in self._p.dump_stacks():
            _accumulate_raw(groups, raw, nstacks=1)
        for raw in self._p.dump_folded():
            _accumulate_raw(groups, raw, nstacks=int(raw.get("nstacks") or 1))
        return groups

    def __enter__(self) -> Profile:
        self.enable()
        return self

    def __exit__(self, *exc_info) -> None:
        self.disable()


def _family_of(raw: dict) -> tuple[str, int, str]:
    family = raw.get("family")
    if family is None or family is False:
        return _MAIN_FAMILY
    return (str(family[0]), int(family[1]), str(family[2]))


def _raw_tids(raw: dict) -> set[int]:
    ids = raw.get("thread_ids")
    if ids is not None:
        return {int(t) for t in ids}
    return {int(raw["thread_id"])}


def _accumulate_raw(groups: dict[tuple[str, int, str], dict], raw: dict, *, nstacks: int) -> None:
    key = _family_of(raw)
    item = groups.get(key)
    if item is None:
        item = {"timings": {}, "nstacks": 0, "tids": set()}
        groups[key] = item
    _merge_timings(item["timings"], _entries_to_timings(raw["entries"]))
    item["nstacks"] += nstacks
    item["tids"].update(_raw_tids(raw))


def _stack_stats(raw: dict, *, nstacks: int) -> StackStats:
    timings = _entries_to_timings(raw["entries"])
    return StackStats(
        _snapshot_timings(timings),
        family=_family_of(raw),
        nstacks=nstacks,
        thread_ids=frozenset({int(raw["thread_id"])}),
    )


def run(statement: str, filename: str | None = None, sort: Any = -1):
    return _Utils(Profile).run(statement, filename, sort)


def runctx(
    statement: str,
    globals: dict[str, Any],
    locals: dict[str, Any] | None,
    filename: str | None = None,
    sort: Any = -1,
):
    return _Utils(Profile).runctx(statement, globals, locals, filename, sort)
