from __future__ import annotations

import errno
import socket
import threading

import pytest

from tealetio import set_scheduler
from tealetio.io_manager import ProactorIOManager
from tealetio.operations import ContinuousOperation, MultishotDelivery
from tealetio.proactor import SyncProactorScheduler
from io_fakes import StubProactor, StubScheduler


class _MockProactor(StubProactor):
    pass


def _manager() -> ProactorIOManager:
    return ProactorIOManager(StubScheduler(), _MockProactor())  # type: ignore[arg-type]


def _nonblocking_listener() -> socket.socket:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    listener.setblocking(False)
    return listener


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


def test_reorder_buffer_index_none_flushes_heap_before_terminal() -> None:
    """index=None cancel must not leave heaped OOO legs stranded."""
    from tealetio.continuous_callbacks import ReorderBuffer
    from tealetio.operations import io_cancellation_error

    order: list[int | None] = []

    def record(delivery: MultishotDelivery) -> None:
        order.append(delivery.index)

    reorder_buffer = ReorderBuffer(record)
    reorder_buffer.deliver(MultishotDelivery(index=1, value="b", more=True))
    reorder_buffer.deliver(MultishotDelivery(index=None, exception=io_cancellation_error(), more=False))
    # late straggler after cancel still reaches the callback (no done() gate here)
    reorder_buffer.deliver(MultishotDelivery(index=0, value="a", more=True))

    assert order == [1, None, 0]


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


def test_reorder_buffer_drain_yields_pending_without_callbacks() -> None:
    from tealetio.continuous_callbacks import ReorderBuffer

    seen: list[int] = []

    def record(delivery: MultishotDelivery) -> None:
        seen.append(delivery.index)  # type: ignore[arg-type]

    reorder_buffer = ReorderBuffer(record)
    reorder_buffer.deliver(MultishotDelivery(index=2, value="c", more=True))
    reorder_buffer.deliver(MultishotDelivery(index=1, value="b", more=True))
    drained = {delivery.index: delivery.value for delivery in reorder_buffer.drain()}
    assert drained == {1: "b", 2: "c"}
    assert not reorder_buffer.pending
    assert seen == []
    reorder_buffer.deliver(MultishotDelivery(index=0, value="a", more=True))
    assert seen == [0]


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


def test_reorder_buffer_defers_terminal_until_stragglers() -> None:
    from tealetio.continuous_callbacks import ReorderBuffer, finish_continuous_delivery

    operation = ContinuousOperation(kind="accept_many", fileobj=object())
    reorder_buffer = ReorderBuffer(finish_continuous_delivery)

    reorder_buffer.deliver(MultishotDelivery(index=2, value="terminal", more=False, operation=operation))
    assert not operation.done()

    reorder_buffer.deliver(MultishotDelivery(index=0, value="a", more=True, operation=operation))
    assert not operation.done()

    reorder_buffer.deliver(MultishotDelivery(index=1, value="b", more=True, operation=operation))
    assert operation.done()


def test_reorder_buffer_honours_start_index_for_finish() -> None:
    from tealetio.continuous_callbacks import ReorderBuffer, finish_continuous_delivery

    operation = ContinuousOperation(kind="accept_many", fileobj=object())
    reorder_buffer = ReorderBuffer(finish_continuous_delivery, start=10)

    reorder_buffer.deliver(MultishotDelivery(index=12, value="terminal", more=False, operation=operation))
    assert not operation.done()

    reorder_buffer.deliver(MultishotDelivery(index=10, value="a", more=True, operation=operation))
    assert not operation.done()

    reorder_buffer.deliver(MultishotDelivery(index=11, value="b", more=True, operation=operation))
    assert operation.done()


def test_strict_reorder_buffer_finishes_on_next_sequence_cancel() -> None:
    from tealetio.continuous_callbacks import ReorderBuffer, finish_continuous_delivery
    from tealetio.operations import io_cancellation_error

    operation = ContinuousOperation(kind="recv_many", fileobj=object())
    reorder_buffer = ReorderBuffer(finish_continuous_delivery, start=0)

    for i in range(3):
        reorder_buffer.deliver(
            MultishotDelivery(index=i, value=memoryview(b"x"), more=True, operation=operation),
        )
    assert not operation.done()

    reorder_buffer.deliver(
        MultishotDelivery(index=3, exception=io_cancellation_error(), more=False, operation=operation),
    )
    assert operation.done()
    assert operation.cancelled()


def test_reorder_buffer_flushes_terminal_after_out_of_order_legs() -> None:
    """Accept multishot can post unique indices that arrive OOO after open_streams.

    A terminal (cancel / multishot end) at a high index must flush once earlier
    legs are present, not stall with pending_io forever.
    """
    from tealetio.continuous_callbacks import ReorderBuffer

    delivered: list[tuple[int, bool]] = []
    buffer = ReorderBuffer(
        lambda delivery: delivered.append(
            (-1 if delivery.index is None else delivery.index, delivery.more),
        ),
    )

    # indices 0..4 and 6..9 as OOO non-terminals; gap 5 filled last before terminal 10
    for index in (3, 1, 4, 0, 2, 8, 6, 9, 7):
        buffer.deliver(MultishotDelivery(value=index, more=True, index=index))
    buffer.deliver(MultishotDelivery(value="term", more=False, index=10))
    assert (10, False) not in delivered

    buffer.deliver(MultishotDelivery(value=5, more=True, index=5))
    assert delivered[-1] == (10, False)
    assert [index for index, _more in delivered] == list(range(11))


def test_reorder_buffer_index_none_cancel_flushes_heaped_legs() -> None:
    """Local cancel uses index=None; heaped OOO accepts must not be stranded.

    Strict reorder holds non-terminals until their index is due. A sequenced
    terminal waits for the gap; an unsequenced cancel must flush the heap first
    so open_streams results still reach the user callback before finish.
    """
    from tealetio.continuous_callbacks import ReorderBuffer, finish_continuous_delivery
    from tealetio.operations import io_cancellation_error

    operation = ContinuousOperation(kind="accept_many", fileobj=object())
    delivered: list[tuple[int | None, bool]] = []

    def on_delivery(delivery: MultishotDelivery) -> None:
        delivered.append((delivery.index, delivery.more))
        finish_continuous_delivery(delivery)

    buffer = ReorderBuffer(on_delivery)
    for index in (2, 0, 3):
        buffer.deliver(MultishotDelivery(index=index, value=index, more=True, operation=operation))
    assert not operation.done()
    assert buffer.pending
    # gap at 1: 2 and 3 still heaped; 0 already delivered
    assert [index for index, _more in delivered] == [0]

    buffer.deliver(
        MultishotDelivery(index=None, exception=io_cancellation_error(), more=False, operation=operation),
    )
    assert operation.done()
    assert operation.cancelled()
    assert not buffer.pending
    assert delivered == [(0, True), (2, True), (3, True), (None, False)]


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

    class _PollProactor(StubProactor):
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

    class _AcceptProactor(StubProactor):
        def accept_many(self, sock, callback=None, *, base_sequence: int = 0):
            operation = ContinuousOperation(kind="accept_many", fileobj=sock, result_callback=callback)
            operation._finish_with_terminal_delivery(MultishotDelivery(exception=error, more=False))
            return operation

    scheduler = StubScheduler()
    scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
    io = ProactorIOManager(scheduler, _AcceptProactor())  # type: ignore[arg-type]
    server = _nonblocking_listener()
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

    class _AcceptProactor(StubProactor):
        def accept_many(self, sock, callback=None, *, base_sequence: int = 0):
            conn, peer = socket.socketpair()
            peer.close()
            operation = ContinuousOperation(kind="accept_many", fileobj=sock, result_callback=callback)
            operation._emit_result(conn, more=False)
            return operation

    scheduler = StubScheduler()
    scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
    io = ProactorIOManager(scheduler, _AcceptProactor())  # type: ignore[arg-type]
    server = _nonblocking_listener()
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

    class _AcceptProactor(StubProactor):
        def accept_many(self, sock, callback=None, *, base_sequence: int = 0):
            operation = ContinuousOperation(kind="accept_many", fileobj=sock, result_callback=callback)
            operation._finish_with_terminal_delivery(MultishotDelivery(exception=error, more=False))
            return operation

    scheduler = StubScheduler()
    scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
    io = ProactorIOManager(scheduler, _AcceptProactor())  # type: ignore[arg-type]
    server = _nonblocking_listener()
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

    class _PollProactor(StubProactor):
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

    class _EagerProactor(StubProactor):
        def accept_many(self, sock, callback=None, *, base_sequence: int = 0):
            conn, peer = socket.socketpair()
            peer.close()
            operation = ContinuousOperation(kind="accept_many", fileobj=sock, result_callback=callback)
            operation._emit_result(conn, more=False)
            return operation

    io = ProactorIOManager(StubScheduler(), _EagerProactor())  # type: ignore[arg-type]
    server = _nonblocking_listener()
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

    class _PollProactor(StubProactor):
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
