import gc
import os
import socket
import sys
from collections import Counter
from collections.abc import Iterable
from types import TracebackType
from typing import Any

import pytest


os.environ.setdefault("PYTEALET_CHECK_STACK", "1")

# Opt-in residual / cycle probe (see tests/README.md):
#   PYTEALET_GC_LEAK_CHECK=1            → warn on unraisables + residual type deltas
#   PYTEALET_GC_LEAK_CHECK=unraisable   → warn on unraisables only (quieter)
#   PYTEALET_GC_LEAK_CHECK=fail         → fail the test on any finding
_GC_LEAK_CHECK = os.environ.get("PYTEALET_GC_LEAK_CHECK", "").strip().lower()

_created_proactor_schedulers: list[object] = []


def _install_proactor_scheduler_tracking() -> None:
    from tealetio.proactor import ProactorScheduler

    original_init = ProactorScheduler.__init__
    if getattr(original_init, "_tealetio_test_tracking", False):
        return

    def tracking_init(self, *args, **kwargs) -> None:
        original_init(self, *args, **kwargs)
        _created_proactor_schedulers.append(self)

    tracking_init._tealetio_test_tracking = True
    ProactorScheduler.__init__ = tracking_init  # type: ignore[method-assign]


_install_proactor_scheduler_tracking()


_NATIVE_URING_RECV_MULTISHOT: tuple[bool, str] | None = None


def _native_uring_recv_multishot_capability() -> tuple[bool, str]:
    global _NATIVE_URING_RECV_MULTISHOT
    if _NATIVE_URING_RECV_MULTISHOT is not None:
        return _NATIVE_URING_RECV_MULTISHOT

    try:
        from tealetio.continuous_callbacks import finish_continuous_delivery
        from tealetio.proactor import UringProactor

        proactor = UringProactor()
    except (OSError, RuntimeError) as exc:
        _NATIVE_URING_RECV_MULTISHOT = (False, f"native io_uring is not available: {exc}")
        return _NATIVE_URING_RECV_MULTISHOT

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        def _finish_terminal_delivery(delivery) -> None:
            if not delivery.more:
                finish_continuous_delivery(delivery)

        operation = proactor.recv_many(
            reader,
            _finish_terminal_delivery,
            buf_group=proactor.shared_recv_buffer_pool(),
        )
        proactor.cancel(operation)
        deadline = proactor.get_time() + 1.0
        while proactor.has_pending_operations() and proactor.get_time() < deadline:
            proactor.wait(min(deadline, proactor.get_time() + 0.05))
        if proactor.has_pending_operations() or not operation.done():
            _NATIVE_URING_RECV_MULTISHOT = (False, "native io_uring recv multishot cancellation did not settle")
        else:
            _NATIVE_URING_RECV_MULTISHOT = (True, "")
    except (OSError, RuntimeError, NotImplementedError) as exc:
        _NATIVE_URING_RECV_MULTISHOT = (False, f"native io_uring recv multishot is not available: {exc}")
    finally:
        reader.close()
        writer.close()
        proactor.close()

    return _NATIVE_URING_RECV_MULTISHOT


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "requires_native_uring_recv_multishot: requires native io_uring multishot receive support",
    )


def pytest_collection_modifyitems(config, items):
    supported, reason = _native_uring_recv_multishot_capability()
    if supported:
        return
    skip_marker = pytest.mark.skip(reason=reason)
    for item in items:
        if "requires_native_uring_recv_multishot" in item.keywords:
            item.add_marker(skip_marker)


def _gc_leak_check_type_names() -> tuple[str, ...]:
    """Type names (module.qualname) we treat as suspicious residuals after a test."""

    return (
        "tealetio.files.ProactorFile",
        "tealetio.proactor.UringProactor",
        "tealetio.proactor.SelectorProactor",
        "tealetio.proactor.ThreadedSelectorProactor",
        "tealetio.proactor.SyncProactorScheduler",
        "tealetio.proactor.AsyncProactorScheduler",
        "tealetio.proactor.UringOperation",
        "tealetio.proactor.UringContinuousOperation",
        "tealetio.proactor.SyntheticRecvBufferPool",
        "tealetio.proactor._LeasedChunk",
        "tealetio.operations.Operation",
        "tealetio.operations.ContinuousOperation",
        "tealetio.operations.MultishotDelivery",
        "tealetio.io_waiter.IOWaitGroup",
        "tealetio.io_buffers.RecvIterBuffer",
    )


def _type_key(obj: object) -> str | None:
    cls = type(obj)
    mod = getattr(cls, "__module__", "") or ""
    if not mod.startswith("tealetio"):
        return None
    name = getattr(cls, "__qualname__", cls.__name__)
    return f"{mod}.{name}"


def _type_label(obj: object) -> str:
    cls = type(obj)
    mod = getattr(cls, "__module__", "") or ""
    name = getattr(cls, "__qualname__", cls.__name__)
    return f"{mod}.{name}" if mod else name


def _count_tealetio_types(interesting: Iterable[str]) -> Counter[str]:
    want = set(interesting)
    counts: Counter[str] = Counter()
    for obj in gc.get_objects():
        key = _type_key(obj)
        if key is not None and key in want:
            counts[key] += 1
    return counts


def _objects_of_type(type_name: str) -> list[object]:
    return [obj for obj in gc.get_objects() if _type_key(obj) == type_name]


def _node_label(obj: object, *, via: object | None = None) -> str:
    """Compact label for one node in a referrer path."""

    label = _type_label(obj)
    if type(obj).__name__ == "frame":
        code = obj.f_code  # type: ignore[attr-defined]
        return f"frame {code.co_filename}:{obj.f_lineno} {code.co_name}()"  # type: ignore[attr-defined]
    if isinstance(obj, dict) and via is not None:
        keys = [repr(k) for k, v in obj.items() if v is via]
        if keys:
            return f"dict{keys}"
        return f"dict(len={len(obj)})"
    if isinstance(obj, list) and via is not None:
        idxs = [i for i, v in enumerate(obj) if v is via]
        return f"list{idxs[:6]}"
    if isinstance(obj, tuple) and via is not None:
        idxs = [i for i, v in enumerate(obj) if v is via]
        return f"tuple{idxs[:6]}"
    d = getattr(obj, "__dict__", None)
    if isinstance(d, dict) and via is not None:
        attrs = [k for k, v in d.items() if v is via]
        if attrs:
            return f"{label}.{attrs[0]}" if len(attrs) == 1 else f"{label}{attrs}"
    return f"{label}@{id(obj):x}"


def _is_walker_noise(ref: object) -> bool:
    """Skip referrers that are the cycle walk itself or pure type objects."""

    if isinstance(ref, type):
        return True
    if type(ref).__name__ == "frame":
        name = ref.f_code.co_name  # type: ignore[attr-defined]
        if name.startswith("_find_referrer") or name in (
            "_format_cycle",
            "_cycles_for_residuals",
            "_gc_leak_check",
            "_objects_of_type",
            "_node_label",
        ):
            return True
    return False


def find_referrer_cycle(
    obj: object,
    *,
    max_depth: int = 10,
    max_nodes: int = 256,
) -> list[object] | None:
    """Depth-first walk of ``gc.get_referrers`` looking for a cycle through ``obj``.

    Edge direction is *who keeps me alive*: from ``x`` to each referrer of ``x``.
    Returns the first cycle path ``[a, b, …, a]`` that **includes** ``obj``, or
    ``None`` if none is found within the depth/node budget. Real application
    cycles are short (bound methods, mutual attrs); depth 10 is enough. Unrelated
    cycles among containers are ignored. Stops at the first hit through ``obj``.
    """

    path: list[object] = []
    on_path: set[int] = set()
    done: set[int] = set()
    root_id = id(obj)
    visited = 0

    def dfs(node: object, depth: int) -> list[object] | None:
        nonlocal visited
        if depth > max_depth or visited >= max_nodes:
            return None
        nid = id(node)
        if nid in on_path:
            start = next(i for i, p in enumerate(path) if id(p) == nid)
            cycle = path[start:] + [node]
            if any(id(p) == root_id for p in cycle):
                return cycle
            return None
        if nid in done:
            return None

        visited += 1
        on_path.add(nid)
        path.append(node)
        try:
            refs = gc.get_referrers(node)
        except Exception:
            refs = ()
        for ref in refs:
            if _is_walker_noise(ref):
                continue
            if ref is refs:
                continue
            hit = dfs(ref, depth + 1)
            if hit is not None:
                return hit
        path.pop()
        on_path.remove(nid)
        done.add(nid)
        return None

    return dfs(obj, 0)


def _format_cycle(cycle: list[object]) -> str:
    """Pretty-print a cycle path from :func:`find_referrer_cycle`."""

    if len(cycle) < 2:
        return _node_label(cycle[0]) if cycle else "(empty)"
    parts: list[str] = []
    for i, node in enumerate(cycle):
        # node is a referrer of cycle[i-1]; show how node holds the previous
        prev = cycle[i - 1] if i > 0 else cycle[-2]
        parts.append(_node_label(node, via=prev if i > 0 else None))
    return " → ".join(parts)


def _top_referrers(obj: object, *, limit: int = 8) -> list[str]:
    """Labels for immediate ``gc.get_referrers`` parents (external-root clue)."""

    out: list[str] = []
    try:
        refs = gc.get_referrers(obj)
    except Exception as exc:
        return [f"? get_referrers failed: {exc}"]
    for ref in refs:
        if _is_walker_noise(ref) or ref is refs:
            continue
        out.append(_node_label(ref, via=obj))
        if len(out) >= limit:
            break
    return out


def _cycles_for_residuals(residual_names: Iterable[str], *, max_roots: int = 2) -> list[str]:
    """Find a referrer cycle (or top external parents) for residual instances."""

    priority = (
        "tealetio.proactor.SyncProactorScheduler",
        "tealetio.proactor.AsyncProactorScheduler",
        "tealetio.proactor.UringProactor",
        "tealetio.proactor.SelectorProactor",
        "tealetio.proactor.ThreadedSelectorProactor",
        "tealetio.files.ProactorFile",
    )
    names = [n for n in priority if n in residual_names]
    names.extend(sorted(n for n in residual_names if n not in names))

    lines: list[str] = []
    for type_name in names:
        objs = _objects_of_type(type_name)
        for obj in objs[:max_roots]:
            cycle = find_referrer_cycle(obj)
            if cycle is not None:
                lines.append(f"{type_name}@{id(obj):x} CYCLE:\n  {_format_cycle(cycle)}")
            else:
                parents = _top_referrers(obj)
                parent_txt = ", ".join(parents) if parents else "(none)"
                lines.append(
                    f"{type_name}@{id(obj):x}: no cycle (external root?); top referrers: {parent_txt}"
                )
        if len(objs) > max_roots:
            lines.append(f"({type_name}: {len(objs) - max_roots} more instance(s) not traced)")
    return lines


@pytest.fixture(autouse=True)
def _gc_leak_check(request: pytest.FixtureRequest):
    """Disable cyclic GC during the test; collect before/after and report leftovers.

    Defined before ``_reset_scheduler_tls`` so teardown order is: close schedulers
    first, then this probe (so residuals are not open drivers).
    """

    if not _GC_LEAK_CHECK:
        yield
        return

    interesting = _gc_leak_check_type_names()
    # baseline after previous test teardown
    gc.collect()
    gc.collect()
    before = _count_tealetio_types(interesting)

    was_enabled = gc.isenabled()
    gc.disable()
    unraisables: list[str] = []

    def _hook(args: Any) -> None:
        exc = args.exc_value
        obj = args.object
        tb: TracebackType | None = args.exc_traceback
        where = ""
        if tb is not None:
            while tb.tb_next is not None:
                tb = tb.tb_next
            where = f" at {tb.tb_frame.f_code.co_filename}:{tb.tb_lineno}"
        unraisables.append(f"{type(exc).__name__}: {exc!s} (object={obj!r}){where}")

    old_hook = sys.unraisablehook
    sys.unraisablehook = _hook
    try:
        yield
    finally:
        sys.unraisablehook = old_hook
        if was_enabled:
            gc.enable()
        # force finalizers now (including half-held files, deliveries, …)
        collected = gc.collect()
        collected += gc.collect()
        after = _count_tealetio_types(interesting)

        report_residuals = _GC_LEAK_CHECK not in ("unraisable", "unraisables")
        residual = {
            name: after[name] - before.get(name, 0)
            for name in interesting
            if after[name] > before.get(name, 0)
        }
        garbage_n = len(gc.garbage)

        messages: list[str] = []
        if unraisables:
            messages.append("unraisable during post-test collect:\n  - " + "\n  - ".join(unraisables))
        if report_residuals and residual:
            detail = ", ".join(f"{k} +{delta} (now {after[k]})" for k, delta in sorted(residual.items()))
            messages.append(f"tealetio residuals vs pre-test baseline: {detail}")
            cycles = _cycles_for_residuals(residual)
            if cycles:
                messages.append("referrer cycles (DFS gc.get_referrers):\n" + "\n".join(cycles))
        if garbage_n:
            messages.append(f"gc.garbage non-empty: {garbage_n} object(s)")

        if messages:
            header = f"GC leak check ({request.node.nodeid}; collected={collected})"
            body = header + "\n" + "\n".join(messages)
            if _GC_LEAK_CHECK in ("fail", "error", "1fail"):
                pytest.fail(body)
            else:
                import warnings

                warnings.warn(body, UserWarning, stacklevel=1)


@pytest.fixture(autouse=True)
def _reset_scheduler_tls():
    from tealetio import BasicScheduler
    from tealetio.proactor import ProactorScheduler
    from tealetio.scheduler import _current_scheduler, _scheduler

    _created_proactor_schedulers.clear()
    _scheduler.instance = BasicScheduler()
    try:
        yield
    finally:
        closed_ids: set[int] = set()
        current = _current_scheduler()
        if isinstance(current, ProactorScheduler):
            current.close()
            closed_ids.add(id(current))
        for scheduler in _created_proactor_schedulers:
            if id(scheduler) not in closed_ids:
                scheduler.close()
                closed_ids.add(id(scheduler))
        # drop tracking refs so GC leak probe (teardown after us) can collect
        _created_proactor_schedulers.clear()
        _scheduler.instance = BasicScheduler()


def _make_scheduler_task_factory(name):
    from tealetio import DefaultTaskFactory, StubTaskFactory

    if name == "default":
        return DefaultTaskFactory()
    if name == "eager":
        return DefaultTaskFactory(eager_start=True)
    if name == "stub":
        return StubTaskFactory()
    raise AssertionError(f"unknown task factory case: {name}")


@pytest.fixture(
    params=[
        pytest.param("default", id="default-factory"),
        pytest.param("eager", id="eager-factory"),
        pytest.param("stub", id="stub-factory"),
    ]
)
def scheduler_task_factory_maker(request):
    def make_factory():
        return _make_scheduler_task_factory(request.param)

    return make_factory


@pytest.fixture(
    params=[
        pytest.param("default", id="default-factory"),
        pytest.param("stub", id="stub-factory"),
    ]
)
def deferred_scheduler_task_factory_maker(request):
    def make_factory():
        return _make_scheduler_task_factory(request.param)

    return make_factory
