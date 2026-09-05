from __future__ import annotations

import errno
import socket

import pytest

from tealetio.io_manager import ProactorIOManager
from tealetio.operations import MultishotDelivery, SelectorCancelHandle
from io_fakes import StubProactor, StubScheduler


class _MockProactor(StubProactor):
    pass


def _nonblocking_listener() -> socket.socket:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    listener.setblocking(False)
    return listener


def test_reorder_buffer_next_index_tracks_delivered() -> None:
    from tealetio.continuous_callbacks import ReorderBuffer

    reorder_buffer = ReorderBuffer(lambda _d: None, start=4)
    assert reorder_buffer.next_index == 4
    reorder_buffer.deliver(MultishotDelivery(index=5, value="b", more=True))
    assert reorder_buffer.next_index == 4
    assert reorder_buffer.pending
    reorder_buffer.deliver(MultishotDelivery(index=4, value="a", more=True))
    assert reorder_buffer.next_index == 6
    assert not reorder_buffer.pending


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
        seen.append(delivery.index)

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


def test_reorder_buffer_flushes_terminal_after_out_of_order_legs() -> None:
    """Accept multishot can post unique indices that arrive OOO after open_streams.

    A terminal (cancel / multishot end) at a high index must flush once earlier
    legs are present, not stall with pending_io forever.
    """
    from tealetio.continuous_callbacks import ReorderBuffer

    delivered: list[tuple[int, bool]] = []
    buffer = ReorderBuffer(
        lambda delivery: delivered.append((delivery.index, delivery.more)),
    )

    # indices 0..4 and 6..9 as OOO non-terminals; gap 5 filled last before terminal 10
    for index in (3, 1, 4, 0, 2, 8, 6, 9, 7):
        buffer.deliver(MultishotDelivery(value=index, more=True, index=index))
    buffer.deliver(MultishotDelivery(value="term", more=False, index=10))
    assert (10, False) not in delivered

    buffer.deliver(MultishotDelivery(value=5, more=True, index=5))
    assert delivered[-1] == (10, False)
    assert [index for index, _more in delivered] == list(range(11))


def test_reorder_buffer_sequenced_close_at_next_index() -> None:
    """No-live-op close posts at next_index; prefix is complete so the terminal is not a gap."""
    from tealetio.continuous_callbacks import ReorderBuffer
    from tealetio.operations import io_cancellation_error

    order: list[int] = []

    def record(delivery: MultishotDelivery) -> None:
        order.append(delivery.index)

    buffer = ReorderBuffer(record)
    buffer.deliver(MultishotDelivery(index=0, value="a", more=True))
    buffer.deliver(MultishotDelivery(index=1, value="b", more=True))
    assert buffer.next_index == 2
    buffer.deliver(MultishotDelivery(index=buffer.next_index, exception=io_cancellation_error(), more=False))
    assert order == [0, 1, 2]


def test_count_finalizer_delivers_immediately_out_of_order() -> None:
    from tealetio.continuous_callbacks import CountFinalizer

    order: list[int] = []
    finalizer = CountFinalizer(lambda d: order.append(d.index))
    finalizer.deliver(MultishotDelivery(index=2, value="c", more=True))
    finalizer.deliver(MultishotDelivery(index=0, value="a", more=True))
    finalizer.deliver(MultishotDelivery(index=1, value="b", more=True))
    assert order == [2, 0, 1]


def test_count_finalizer_defers_finish_until_stragglers() -> None:
    from tealetio.continuous_callbacks import CountFinalizer

    seen: list[int] = []
    finished: list[MultishotDelivery] = []

    def on_delivery(delivery: MultishotDelivery) -> None:
        seen.append(delivery.index)

    finalizer = CountFinalizer(on_delivery, finish=finished.append)
    error = OSError("accept failed")
    finalizer.deliver(MultishotDelivery(index=2, exception=error, more=False))
    assert seen == [2]
    assert finished == []
    assert finalizer._final_delivery is not None

    finalizer.deliver(MultishotDelivery(index=0, value=None, more=True))
    assert finished == []
    finalizer.deliver(MultishotDelivery(index=1, value=None, more=True))
    assert finished[0].exception is error
    assert seen == [2, 0, 1]
    assert finalizer._final_delivery is None


def test_count_finalizer_honours_start_index_for_finish() -> None:
    from tealetio.continuous_callbacks import CountFinalizer

    finished: list[MultishotDelivery] = []
    finalizer = CountFinalizer(lambda _d: None, start=10, finish=finished.append)
    finalizer.deliver(MultishotDelivery(index=12, value="terminal", more=False))
    assert finished == []
    finalizer.deliver(MultishotDelivery(index=10, value="a", more=True))
    assert finished == []
    finalizer.deliver(MultishotDelivery(index=11, value="b", more=True))
    assert finished[0].value == "terminal"


def test_count_finalizer_oneshot_terminal_finishes() -> None:
    from tealetio.continuous_callbacks import CountFinalizer

    finished: list[MultishotDelivery] = []
    finalizer = CountFinalizer(lambda _d: None, finish=finished.append)
    finalizer.deliver(MultishotDelivery(index=0, value=None, more=False))
    assert len(finished) == 1
    assert finished[0].exception is None


def test_count_finalizer_raising_callback_still_counts_and_finishes() -> None:
    from tealetio.continuous_callbacks import CountFinalizer

    seen: list[int] = []
    finished: list[MultishotDelivery] = []

    def on_delivery(delivery: MultishotDelivery) -> None:
        seen.append(delivery.index)
        if delivery.index == 2:
            raise RuntimeError("boom")

    finalizer = CountFinalizer(on_delivery, finish=finished.append)
    try:
        finalizer.deliver(MultishotDelivery(index=2, value="t", more=False))
    except RuntimeError as exc:
        assert str(exc) == "boom"
    else:
        raise AssertionError("expected RuntimeError")
    assert seen == [2]
    assert finished == []

    finalizer.deliver(MultishotDelivery(index=0, value=None, more=True))
    finalizer.deliver(MultishotDelivery(index=1, value=None, more=True))
    assert len(finished) == 1
    assert seen == [2, 0, 1]


def test_count_finalizer_soft_none_value_terminal_finishes() -> None:
    from tealetio.continuous_callbacks import CountFinalizer

    finished: list[MultishotDelivery] = []
    finalizer = CountFinalizer(lambda _d: None, finish=finished.append)
    finalizer.deliver(MultishotDelivery(index=0, value=None, more=False))
    assert len(finished) == 1


def test_count_finalizer_late_straggler_after_done_invokes_callback() -> None:
    from tealetio.continuous_callbacks import CountFinalizer

    seen: list[int] = []
    finished: list[MultishotDelivery] = []
    finalizer = CountFinalizer(lambda d: seen.append(d.index), finish=finished.append)
    finalizer.deliver(MultishotDelivery(index=0, value="a", more=False))
    assert len(finished) == 1
    assert finalizer._final_delivery is None
    finalizer.deliver(MultishotDelivery(index=1, value="late", more=True))
    assert seen == [0, 1]
    assert len(finished) == 1


def test_count_finalizer_missing_index_does_not_finish() -> None:
    from tealetio.continuous_callbacks import CountFinalizer

    finished: list[MultishotDelivery] = []
    finalizer = CountFinalizer(lambda _d: None, finish=finished.append)
    finalizer.deliver(MultishotDelivery(index=2, value="t", more=False))
    finalizer.deliver(MultishotDelivery(index=0, value=None, more=True))
    assert finished == []


def test_poll_many_marshals_callback_and_sets_closed_on_terminal() -> None:
    delivered: list[int] = []

    class _PollProactor(StubProactor):
        def poll_many(self, fd, mask, callback=None):
            from tealetio.operations import SelectorCancelHandle

            handle = SelectorCancelHandle(callback)
            handle._emit_result(3, more=True, index=0)
            handle._finish_with_terminal_delivery(MultishotDelivery(index=1, value=0, more=False))
            return handle

        def stop_poll(self, handle, callback):
            callback(None, None)
            return None

    io = ProactorIOManager(StubScheduler(), _PollProactor())  # type: ignore[arg-type]
    handle = io.poll_many(5, 1, lambda delivery: delivered.append(delivery.value))

    assert delivered == [3, 0]
    assert handle.closed is True


def test_accept_many_terminal_error_finishes_operation() -> None:
    error = OSError("accept failed")
    handler_errors: list[BaseException] = []

    class _AcceptProactor(StubProactor):
        def accept_many(self, sock, callback=None, *, base_sequence: int = 0):
            handle = SelectorCancelHandle(callback)
            handle._finish_with_terminal_delivery(MultishotDelivery(exception=error, more=False))
            return handle

    scheduler = StubScheduler()
    scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
    io = ProactorIOManager(scheduler, _AcceptProactor())  # type: ignore[arg-type]
    server = _nonblocking_listener()
    try:
        waiter = io.accept_many(server, lambda _: None)
        assert handler_errors == []
        assert waiter.done()
        assert waiter.exception() is error
    finally:
        server.close()


def test_accept_many_callback_exception_finishes_terminal_leg() -> None:
    handler_errors: list[BaseException] = []

    class _AcceptProactor(StubProactor):
        def accept_many(self, sock, callback=None, *, base_sequence: int = 0):
            conn, peer = socket.socketpair()
            peer.close()
            handle = SelectorCancelHandle(callback)
            handle._emit_result(conn, more=False)
            return handle

    scheduler = StubScheduler()
    scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
    io = ProactorIOManager(scheduler, _AcceptProactor())  # type: ignore[arg-type]
    server = _nonblocking_listener()
    try:
        waiter = io.accept_many(server, lambda _: (_ for _ in ()).throw(ValueError("accept failed")))
        assert len(handler_errors) == 1
        assert str(handler_errors[0]) == "accept failed"
        assert waiter.done()
        assert waiter.exception() is None
    finally:
        server.close()


def test_accept_many_streams_terminal_error_finishes_operation() -> None:
    error = OSError("accept failed")
    handler_errors: list[BaseException] = []

    class _AcceptProactor(StubProactor):
        def accept_many(self, sock, callback=None, *, base_sequence: int = 0):
            handle = SelectorCancelHandle(callback)
            handle._finish_with_terminal_delivery(MultishotDelivery(exception=error, more=False))
            return handle

    scheduler = StubScheduler()
    scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
    io = ProactorIOManager(scheduler, _AcceptProactor())  # type: ignore[arg-type]
    server = _nonblocking_listener()
    try:
        waiter = io.accept_many_streams(server, lambda _: None)
        assert handler_errors == []
        assert waiter.done()
        assert waiter.exception() is error
    finally:
        server.close()


def test_accept_many_defers_finish_until_terminal_count() -> None:
    """CountFinalizer defers IOWaiter settle until every sequenced leg has run."""
    error = OSError("accept failed")
    handler_errors: list[BaseException] = []
    user_calls: list[object] = []

    class _AcceptProactor(StubProactor):
        def accept_many(self, sock, callback=None, *, base_sequence: int = 0):
            return SelectorCancelHandle(callback)

    scheduler = StubScheduler()
    scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
    io = ProactorIOManager(scheduler, _AcceptProactor())  # type: ignore[arg-type]
    server = _nonblocking_listener()
    try:
        waiter = io.accept_many(server, user_calls.append)
        handle = waiter._handle
        assert handle is not None
        handle._finish_with_terminal_delivery(MultishotDelivery(index=2, exception=error, more=False))
        assert handler_errors == []
        assert user_calls == []
        assert not waiter.done()

        handle._emit_result(None, index=0, more=True)
        assert not waiter.done()
        handle._emit_result(None, index=1, more=True)
        assert waiter.done()
        assert waiter.exception() is error
        assert user_calls == []
    finally:
        server.close()


def test_accept_many_streams_defers_finish_until_terminal_count() -> None:
    """Same deferred-finish contract as accept_many; stragglers must not open streams."""
    error = OSError("accept failed")
    handler_errors: list[BaseException] = []
    user_calls: list[object] = []

    class _AcceptProactor(StubProactor):
        def accept_many(self, sock, callback=None, *, base_sequence: int = 0):
            return SelectorCancelHandle(callback)

    scheduler = StubScheduler()
    scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
    io = ProactorIOManager(scheduler, _AcceptProactor())  # type: ignore[arg-type]
    server = _nonblocking_listener()
    try:
        waiter = io.accept_many_streams(server, user_calls.append)
        handle = waiter._handle
        assert handle is not None
        handle._finish_with_terminal_delivery(MultishotDelivery(index=2, exception=error, more=False))
        assert handler_errors == []
        assert user_calls == []
        assert not waiter.done()

        handle._emit_result(None, index=0, more=True)
        assert not waiter.done()
        handle._emit_result(None, index=1, more=True)
        assert waiter.done()
        assert waiter.exception() is error
        assert user_calls == []
    finally:
        server.close()


def test_poll_many_terminal_error_sets_handle_closed() -> None:
    error = OSError("poll failed")
    seen: list[BaseException | None] = []

    class _PollProactor(StubProactor):
        def poll_many(self, fd, mask, callback=None):
            from tealetio.operations import SelectorCancelHandle

            handle = SelectorCancelHandle(callback)
            handle._finish_with_terminal_delivery(MultishotDelivery(exception=error, more=False))
            return handle

        def stop_poll(self, handle, callback):
            callback(None, None)
            return None

    io = ProactorIOManager(StubScheduler(), _PollProactor())  # type: ignore[arg-type]
    handle = io.poll_many(5, 1, lambda d: seen.append(d.exception))
    assert handle.closed is True
    assert seen == [error]


def test_marshal_continuous_delivery_uses_operation_from_eager_emit() -> None:
    delivered: list[socket.socket] = []

    class _EagerProactor(StubProactor):
        def accept_many(self, sock, callback=None, *, base_sequence: int = 0):
            conn, peer = socket.socketpair()
            peer.close()
            handle = SelectorCancelHandle(callback)
            handle._emit_result(conn, more=False)
            return handle

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


def test_poll_many_handle_close_is_idempotent_after_terminal() -> None:
    class _PollProactor(StubProactor):
        def poll_many(self, fd, mask, callback=None):
            from tealetio.operations import SelectorCancelHandle

            handle = SelectorCancelHandle(callback)
            handle._finish_with_terminal_delivery(MultishotDelivery(value=7, more=False))
            return handle

        def stop_poll(self, handle, callback):
            raise AssertionError("close after terminal must not stop_poll")

    io = ProactorIOManager(StubScheduler(), _PollProactor())  # type: ignore[arg-type]
    handle = io.poll_many(5, 1, lambda _delivery: None)
    assert handle.closed is True
    handle.close()
    handle.close()
