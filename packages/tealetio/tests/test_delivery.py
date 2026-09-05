from __future__ import annotations

import errno

from tealetio.delivery import MultishotDelivery


def test_reorder_buffer_next_index_tracks_delivered() -> None:
    from tealetio.delivery import ReorderBuffer

    reorder_buffer = ReorderBuffer(lambda _d: None, start=4)
    assert reorder_buffer.next_index == 4
    reorder_buffer.deliver(MultishotDelivery(index=5, value="b", more=True))
    assert reorder_buffer.next_index == 4
    assert reorder_buffer.pending
    reorder_buffer.deliver(MultishotDelivery(index=4, value="a", more=True))
    assert reorder_buffer.next_index == 6
    assert not reorder_buffer.pending


def test_reorder_buffer_arm_next_index_reuses_leg_start_index() -> None:
    from tealetio.delivery import ReorderBuffer

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
    from tealetio.delivery import ReorderBuffer

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
    from tealetio.delivery import ReorderBuffer

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
    from tealetio.delivery import ReorderBuffer

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
    from tealetio.delivery import ReorderBuffer

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
    from tealetio.delivery import ReorderBuffer
    from tealetio.delivery import io_cancellation_error

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
    from tealetio.delivery import CountFinalizer

    order: list[int] = []
    finalizer = CountFinalizer(lambda d: order.append(d.index))
    finalizer.deliver(MultishotDelivery(index=2, value="c", more=True))
    finalizer.deliver(MultishotDelivery(index=0, value="a", more=True))
    finalizer.deliver(MultishotDelivery(index=1, value="b", more=True))
    assert order == [2, 0, 1]


def test_count_finalizer_defers_finish_until_stragglers() -> None:
    from tealetio.delivery import CountFinalizer

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
    from tealetio.delivery import CountFinalizer

    finished: list[MultishotDelivery] = []
    finalizer = CountFinalizer(lambda _d: None, start=10, finish=finished.append)
    finalizer.deliver(MultishotDelivery(index=12, value="terminal", more=False))
    assert finished == []
    finalizer.deliver(MultishotDelivery(index=10, value="a", more=True))
    assert finished == []
    finalizer.deliver(MultishotDelivery(index=11, value="b", more=True))
    assert finished[0].value == "terminal"


def test_count_finalizer_oneshot_terminal_finishes() -> None:
    from tealetio.delivery import CountFinalizer

    finished: list[MultishotDelivery] = []
    finalizer = CountFinalizer(lambda _d: None, finish=finished.append)
    finalizer.deliver(MultishotDelivery(index=0, value=None, more=False))
    assert len(finished) == 1
    assert finished[0].exception is None


def test_count_finalizer_raising_callback_still_counts_and_finishes() -> None:
    from tealetio.delivery import CountFinalizer

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
    from tealetio.delivery import CountFinalizer

    finished: list[MultishotDelivery] = []
    finalizer = CountFinalizer(lambda _d: None, finish=finished.append)
    finalizer.deliver(MultishotDelivery(index=0, value=None, more=False))
    assert len(finished) == 1


def test_count_finalizer_late_straggler_after_done_invokes_callback() -> None:
    from tealetio.delivery import CountFinalizer

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
    from tealetio.delivery import CountFinalizer

    finished: list[MultishotDelivery] = []
    finalizer = CountFinalizer(lambda _d: None, finish=finished.append)
    finalizer.deliver(MultishotDelivery(index=2, value="t", more=False))
    finalizer.deliver(MultishotDelivery(index=0, value=None, more=True))
    assert finished == []

