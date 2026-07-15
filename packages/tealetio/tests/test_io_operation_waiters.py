from __future__ import annotations

import errno
import socket
import threading

import pytest

from tealetio import set_scheduler
from tealetio.io_manager import ProactorIOManager
from tealetio.operations import ContinuousOperation, MultishotDelivery
from tealetio.proactor import SyncProactorScheduler
from io_fakes import StubScheduler


class _MockProactor:
    pass


def _manager() -> ProactorIOManager:
    return ProactorIOManager(StubScheduler(), _MockProactor())  # type: ignore[arg-type]


def test_wrap_continuous_delivery_marshals_on_scheduler_thread() -> None:
    scheduler = SyncProactorScheduler()
    io = ProactorIOManager(scheduler, _MockProactor())  # type: ignore[arg-type]
    operation = ContinuousOperation(kind="accept_many", fileobj=object())
    owner_threads: list[int] = []
    delivery_threads: list[int] = []

    def deliver(delivery: MultishotDelivery) -> None:
        owner_threads.append(threading.get_ident())

    wrapped = io._wrap_continuous_delivery(operation, deliver)

    def exercise() -> None:
        owner = threading.get_ident()

        def invoke_from_worker() -> None:
            delivery_threads.append(threading.get_ident())
            wrapped(MultishotDelivery(value=1, more=True))

        worker = threading.Thread(target=invoke_from_worker)
        worker.start()
        worker.join()

        deadline = scheduler.time() + 1.0
        while len(owner_threads) < 1 and scheduler.time() < deadline:
            scheduler.sleep(0)

        assert delivery_threads != [owner]
        assert owner_threads == [owner]

    set_scheduler(scheduler)
    try:
        scheduler.run_until_complete(scheduler.spawn(exercise))
    finally:
        scheduler.close()


def test_wrap_continuous_delivery_finishes_only_on_terminal() -> None:
    io = _manager()
    operation = ContinuousOperation(kind="accept_many", fileobj=object())
    chunks: list[MultishotDelivery] = []
    wakes: list[str] = []

    wrapped = io._wrap_continuous_delivery(operation, chunks.append)
    operation.add_done_callback(lambda _op: wakes.append("wake"))

    wrapped(MultishotDelivery(index=0, value=1, more=True))
    assert not operation.done()

    wrapped(MultishotDelivery(index=1, value=2, more=False))

    assert [delivery.value for delivery in chunks] == [1, 2]
    assert operation.done()
    assert wakes == ["wake"]


def test_reorder_buffer_delivers_none_index_immediately() -> None:
    from tealetio.continuous_callbacks import ReorderBuffer
    from tealetio.tasks import CancelledError

    order: list[int | None] = []

    def record(delivery: MultishotDelivery) -> None:
        order.append(delivery.index)

    reorder_buffer = ReorderBuffer(record)
    reorder_buffer.deliver(MultishotDelivery(index=1, value="b", more=True))
    reorder_buffer.deliver(MultishotDelivery(index=None, exception=CancelledError(), more=False))
    reorder_buffer.deliver(MultishotDelivery(index=0, value="a", more=True))

    assert order == [None, 0, 1]


def test_reorder_buffer_arm_next_index_reuses_leg_start_index() -> None:
    from tealetio.continuous_callbacks import ReorderBuffer

    order: list[int] = []

    def record(delivery: MultishotDelivery) -> None:
        order.append(delivery.index)
        if delivery.index == 2:
            reorder_buffer.arm_next_index(2)

    reorder_buffer = ReorderBuffer(record)
    reorder_buffer.deliver(MultishotDelivery(index=0, value="a", more=True))
    reorder_buffer.deliver(MultishotDelivery(index=1, value="b", more=True))
    reorder_buffer.deliver(MultishotDelivery(index=2, exception=OSError(errno.ENOBUFS, "x"), more=False))
    reorder_buffer.deliver(MultishotDelivery(index=2, value="", more=False))

    assert order == [0, 1, 2, 2]


def test_reorder_buffer_reset_clears_pending_heap() -> None:
    from tealetio.continuous_callbacks import ReorderBuffer

    order: list[int] = []

    def record(delivery: MultishotDelivery) -> None:
        order.append(delivery.index)

    reorder_buffer = ReorderBuffer(record, start=5)
    reorder_buffer.deliver(MultishotDelivery(index=7, value="c", more=True))
    assert reorder_buffer.pending
    reorder_buffer.reset()
    assert not reorder_buffer.pending
    reorder_buffer.deliver(MultishotDelivery(index=0, value="z", more=True))
    assert order == [0]


def test_reorder_buffer_delivers_callbacks_in_index_order() -> None:
    from tealetio.continuous_callbacks import ReorderBuffer

    order: list[int] = []

    def record(delivery: MultishotDelivery) -> None:
        order.append(delivery.index)

    reorder_buffer = ReorderBuffer(record)
    reorder_buffer.deliver(MultishotDelivery(index=2, value="c", more=True))
    reorder_buffer.deliver(MultishotDelivery(index=0, value="a", more=True))
    reorder_buffer.deliver(MultishotDelivery(index=1, value="b", more=True))

    assert order == [0, 1, 2]


def test_lenient_reorder_buffer_defers_terminal_until_stragglers() -> None:
    from tealetio.continuous_callbacks import LenientReorderBuffer, finish_continuous_delivery

    operation = ContinuousOperation(kind="accept_many", fileobj=object())
    reorder_buffer = LenientReorderBuffer(finish_continuous_delivery)

    reorder_buffer.deliver(MultishotDelivery(index=2, value="terminal", more=False, operation=operation))
    assert not operation.done()

    reorder_buffer.deliver(MultishotDelivery(index=0, value="a", more=True, operation=operation))
    assert not operation.done()

    reorder_buffer.deliver(MultishotDelivery(index=1, value="b", more=True, operation=operation))
    assert operation.done()


def test_lenient_reorder_buffer_honours_start_index() -> None:
    from tealetio.continuous_callbacks import LenientReorderBuffer, finish_continuous_delivery

    operation = ContinuousOperation(kind="accept_many", fileobj=object())
    reorder_buffer = LenientReorderBuffer(finish_continuous_delivery, start=10)

    reorder_buffer.deliver(MultishotDelivery(index=12, value="terminal", more=False, operation=operation))
    assert not operation.done()

    reorder_buffer.deliver(MultishotDelivery(index=10, value="a", more=True, operation=operation))
    assert not operation.done()

    reorder_buffer.deliver(MultishotDelivery(index=11, value="b", more=True, operation=operation))
    assert operation.done()


def test_finish_operation_is_idempotent_when_already_done() -> None:
    operation = ContinuousOperation(kind="accept_many", fileobj=object())
    wakes: list[str] = []

    operation.add_done_callback(lambda _op: wakes.append("wake"))
    operation._finish(result=None)

    operation.finish_operation(MultishotDelivery(more=False))

    assert wakes == ["wake"]


def test_wrap_continuous_delivery_wakes_on_error_terminal() -> None:
    io = _manager()
    operation = ContinuousOperation(kind="accept_many", fileobj=object())
    wakes: list[str] = []
    seen: list[BaseException | None] = []

    def deliver(delivery: MultishotDelivery) -> None:
        seen.append(delivery.exception)

    wrapped = io._wrap_continuous_delivery(operation, deliver)
    operation.add_done_callback(lambda _op: wakes.append("wake"))

    wrapped(MultishotDelivery(exception=OSError("accept failed"), more=False))

    assert isinstance(seen[0], OSError)
    assert wakes == ["wake"]


def test_emit_delivery_attaches_operation() -> None:
    operation = ContinuousOperation(kind="accept_many", fileobj=object())
    seen: list[ContinuousOperation[object] | None] = []

    operation._result_callback = lambda delivery: seen.append(delivery.operation)
    operation._emit_result(1, more=True)

    assert seen == [operation]


def test_poll_many_marshals_callback_and_sets_ready_on_terminal() -> None:
    delivered: list[int] = []

    class _PollProactor:
        def poll_many(self, fd, mask, callback=None):
            operation = ContinuousOperation(kind="poll_many", fileobj=fd, result_callback=callback)
            operation._emit_result(3, more=True, index=0)
            operation._finish_with_terminal_delivery(MultishotDelivery(index=1, value=0, more=False))
            return operation

    io = ProactorIOManager(StubScheduler(), _PollProactor())  # type: ignore[arg-type]
    waiter = io.poll_many(5, 1, lambda delivery: delivered.append(delivery.value))

    assert delivered == [3, 0]
    assert waiter.operation is not None
    assert waiter.operation.done()


def test_accept_many_terminal_error_finishes_operation() -> None:
    error = OSError("accept failed")
    handler_errors: list[BaseException] = []

    class _AcceptProactor:
        def accept_many(self, sock, callback=None):
            operation = ContinuousOperation(kind="accept_many", fileobj=sock, result_callback=callback)
            operation._finish_with_terminal_delivery(MultishotDelivery(exception=error, more=False))
            return operation

    scheduler = StubScheduler()
    scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
    io = ProactorIOManager(scheduler, _AcceptProactor())  # type: ignore[arg-type]
    server = socket.socket()
    try:
        waiter = io.accept_many(server, lambda _: None)
        operation = waiter.operation
        assert operation is not None
        assert handler_errors == [error]
        assert operation.done()
        assert operation.exception() is error
    finally:
        server.close()


def test_accept_many_callback_exception_finishes_terminal_leg() -> None:
    handler_errors: list[BaseException] = []

    class _AcceptProactor:
        def accept_many(self, sock, callback=None):
            conn, peer = socket.socketpair()
            peer.close()
            operation = ContinuousOperation(kind="accept_many", fileobj=sock, result_callback=callback)
            operation._emit_result(conn, more=False)
            return operation

    scheduler = StubScheduler()
    scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
    io = ProactorIOManager(scheduler, _AcceptProactor())  # type: ignore[arg-type]
    server = socket.socket()
    try:
        waiter = io.accept_many(server, lambda _: (_ for _ in ()).throw(ValueError("accept failed")))
        operation = waiter.operation
        assert operation is not None
        assert len(handler_errors) == 1
        assert str(handler_errors[0]) == "accept failed"
        assert operation.done()
        assert operation.exception() is None
    finally:
        server.close()


def test_accept_many_streams_terminal_error_finishes_operation() -> None:
    error = OSError("accept failed")
    handler_errors: list[BaseException] = []

    class _AcceptProactor:
        def accept_many(self, sock, callback=None):
            operation = ContinuousOperation(kind="accept_many", fileobj=sock, result_callback=callback)
            operation._finish_with_terminal_delivery(MultishotDelivery(exception=error, more=False))
            return operation

    scheduler = StubScheduler()
    scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
    io = ProactorIOManager(scheduler, _AcceptProactor())  # type: ignore[arg-type]
    server = socket.socket()
    try:
        waiter = io.accept_many_streams(server, lambda _: None)
        operation = waiter.operation
        assert operation is not None
        assert handler_errors == [error]
        assert operation.done()
        assert operation.exception() is error
    finally:
        server.close()


def test_poll_many_wait_raises_operation_exception_on_terminal_error() -> None:
    error = OSError("poll failed")

    class _PollProactor:
        def poll_many(self, fd, mask, callback=None):
            operation = ContinuousOperation(kind="poll_many", fileobj=fd, result_callback=callback)
            operation._finish_with_terminal_delivery(MultishotDelivery(exception=error, more=False))
            return operation

    io = ProactorIOManager(StubScheduler(), _PollProactor())  # type: ignore[arg-type]
    waiter = io.poll_many(5, 1, lambda _delivery: None)
    operation = waiter.operation
    assert operation is not None
    assert operation.exception() is error

    with pytest.raises(OSError, match="poll failed"):
        waiter.wait()


def test_marshal_continuous_delivery_uses_operation_from_eager_emit() -> None:
    delivered: list[socket.socket] = []

    class _EagerProactor:
        def accept_many(self, sock, callback=None):
            conn, peer = socket.socketpair()
            peer.close()
            operation = ContinuousOperation(kind="accept_many", fileobj=sock, result_callback=callback)
            operation._emit_result(conn, more=False)
            return operation

    io = ProactorIOManager(StubScheduler(), _EagerProactor())  # type: ignore[arg-type]
    server = socket.socket()
    try:
        io.accept_many(
            server,
            lambda delivery: delivered.append(delivery[0]),
        )
    finally:
        server.close()

    assert len(delivered) == 1


def test_poll_many_wait_completes_after_terminal_delivery() -> None:
    scheduler = SyncProactorScheduler()

    class _PollProactor:
        def poll_many(self, fd, mask, callback=None):
            operation = ContinuousOperation(kind="poll_many", fileobj=fd, result_callback=callback)
            operation._finish_with_terminal_delivery(MultishotDelivery(value=7, more=False))
            return operation

    io = ProactorIOManager(scheduler, _PollProactor())  # type: ignore[arg-type]

    def exercise() -> None:
        waiter = io.poll_many(5, 1, lambda _delivery: None)
        assert waiter.wait() is None

    set_scheduler(scheduler)
    try:
        scheduler.run_until_complete(scheduler.spawn(exercise))
    finally:
        scheduler.close()
