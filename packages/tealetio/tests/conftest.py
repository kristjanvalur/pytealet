import os
import socket

import pytest


os.environ.setdefault("PYTEALET_CHECK_STACK", "1")

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
