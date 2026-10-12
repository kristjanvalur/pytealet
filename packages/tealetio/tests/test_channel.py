import asyncio

import pytest

from tealetio import BasicScheduler, Channel, Event, Timeout, TimeoutError, set_scheduler
from tealetio.tasks import Task
from tealetio.locks import RawTimeoutError, timeout

from helpers import new_scheduler as _new_scheduler


def _basic_scheduler() -> BasicScheduler:
    scheduler = BasicScheduler()
    set_scheduler(scheduler)
    return scheduler


def _finish_now(coro):
    """Run a channel coroutine that must return without awaiting."""

    try:
        coro.send(None)
    except StopIteration as stop:
        return stop.value
    coro.close()
    raise AssertionError("coroutine awaited")


class TestChannelExamples:
    def test_channel_balance_tracks_waiting_senders(self):
        s = _new_scheduler()
        ch = Channel()
        seen: list[str] = []

        def sender() -> None:
            seen.append("sender:before")
            ch.send(7)
            seen.append("sender:after")

        s.spawn(sender)
        s.pump(1)

        assert ch.balance == 1

        def receiver() -> None:
            seen.append(f"receiver:{ch.receive()}")

        s.spawn(receiver)
        s.run()

        assert ch.balance == 0
        assert seen == ["sender:before", "receiver:7", "sender:after"]

    def test_channel_balance_tracks_waiting_receivers(self):
        s = _new_scheduler()
        ch = Channel()
        seen: list[str] = []

        def receiver() -> None:
            seen.append("receiver:before")
            seen.append(f"receiver:{ch.receive()}")

        s.spawn(receiver)
        s.pump(1)

        assert ch.balance == -1

        def sender() -> None:
            ch.send(11)
            seen.append("sender:after")

        s.spawn(sender)
        s.run()

        assert ch.balance == 0
        assert seen == ["receiver:before", "receiver:11", "sender:after"]

    def test_channel_preference_sender(self):
        s = _new_scheduler()
        ch = Channel(preference=1)
        seen: list[str] = []

        def receiver() -> None:
            seen.append("receiver:before")
            seen.append(f"receiver:{ch.receive()}")

        def sender() -> None:
            ch.send(3)
            seen.append("sender:after")

        s.spawn(receiver)
        s.spawn(sender)
        s.run()

        assert seen == ["receiver:before", "sender:after", "receiver:3"]

    def test_channel_preference_validation(self):
        with pytest.raises(ValueError, match="preference must be -1, 0, or 1"):
            Channel(preference=2)

    def test_channel_send_exception(self):
        s = _new_scheduler()
        ch = Channel()
        seen: list[str] = []

        def receiver() -> None:
            try:
                ch.receive()
            except ValueError as exc:
                seen.append(f"caught:{exc}")

        def sender() -> None:
            ch.send_exception(ValueError("boom"))

        s.spawn(receiver)
        s.spawn(sender)
        s.run()

        assert seen == ["caught:boom"]

    def test_channel_send_exception_requires_instance(self):
        ch = Channel()
        with pytest.raises(TypeError, match="BaseException instance"):
            ch.send_exception(ValueError)  # type: ignore[arg-type]

    def test_channel_async_send_wakes_tealet_non_immediate(self):
        s = _new_scheduler()
        ch = Channel(preference=-1)
        seen: list[str] = []

        def receiver() -> None:
            seen.append("receiver:before")
            seen.append(f"receiver:{ch.receive()}")

        s.spawn(receiver)
        s.pump(1)
        assert ch.balance == -1

        asyncio.run(asyncio.wait_for(ch.async_send(9), timeout=1.0))
        assert seen == ["receiver:before"]

        s.run()
        assert seen == ["receiver:before", "receiver:9"]

    def test_channel_async_receive_wakes_tealet_non_immediate(self):
        s = _new_scheduler()
        ch = Channel(preference=1)
        seen: list[str] = []

        def sender() -> None:
            seen.append("sender:before")
            ch.send(4)
            seen.append("sender:after")

        s.spawn(sender)
        s.pump(1)
        assert ch.balance == 1

        value = asyncio.run(asyncio.wait_for(ch.async_receive(), timeout=1.0))
        assert value == 4
        assert seen == ["sender:before"]

        s.run()
        assert seen == ["sender:before", "sender:after"]

    def test_channel_async_sender_and_receiver_pair(self):
        ch = Channel()

        async def run() -> None:
            recv_task = asyncio.create_task(ch.async_receive())
            await asyncio.sleep(0)
            await ch.async_send(12)
            got = await asyncio.wait_for(recv_task, timeout=1.0)
            assert got == 12

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))

    def test_channel_async_receive_cancelled_with_pending_packet_delivers(self):
        ch = Channel()

        async def run() -> None:
            recv_task = asyncio.create_task(ch.async_receive())
            await asyncio.sleep(0)

            # Queue payload first, then cancel before receiver resumes.
            await ch.async_send(None)
            recv_task.cancel()

            got = await asyncio.wait_for(recv_task, timeout=1.0)
            assert got is None

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))

    def test_channel_async_receive_cancelled_without_packet_propagates(self):
        ch = Channel()

        async def run() -> None:
            recv_task = asyncio.create_task(ch.async_receive())
            await asyncio.sleep(0)
            recv_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await recv_task

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))

    def test_channel_send_direct_raw_timeout_propagates_after_commit(self):
        # Task.throw bypasses timeout(). the channel does not turn that
        # into a successful send, even though receive already took the value.
        s = _new_scheduler()
        ch = Channel(preference=0)
        seen: list[object] = []
        got: list[object] = []

        def sender() -> None:
            try:
                ch.send(5)
                seen.append("send:ok")
            except BaseException as exc:
                seen.append(type(exc).__name__)

        sender_task = s.spawn(sender)
        s.pump(1)
        assert ch.balance == 1

        s.call_soon(lambda: got.append(ch.receive()))
        s.call_soon(sender_task.throw, RawTimeoutError())
        s.run()

        assert got == [5]
        assert seen == ["RawTimeoutError"]
        assert ch.balance == 0

    def test_channel_async_send_cancelled_with_consumed_packet_returns(self):
        ch = Channel()

        async def run() -> None:
            send_task = asyncio.create_task(ch.async_send(None))
            await asyncio.sleep(0)

            # Consume payload first, then race cancellation against sender wake.
            got = await ch.async_receive()
            assert got is None
            send_task.cancel()

            await asyncio.wait_for(send_task, timeout=1.0)

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))

    def test_channel_receive_external_exception_drops_pending_packet(self):
        s = _new_scheduler()
        ch = Channel(preference=0)
        seen: list[str] = []

        def receiver() -> None:
            try:
                ch.receive()
            except RuntimeError as exc:
                seen.append(f"receiver:exc:{exc}")

        receiver_task = s.spawn(receiver)
        s.pump(1)
        assert ch.balance == -1

        s.call_soon(ch.send, 42)
        s.call_soon(receiver_task.throw, RuntimeError("interrupt"))
        s.run()

        assert "receiver:exc:interrupt" in seen
        assert ch.balance == 0

        # The pending packet must have been discarded with the external wake.
        got: list[int] = []

        def receiver2() -> None:
            value = ch.receive()
            assert isinstance(value, int)
            got.append(value)

        s.spawn(receiver2)
        s.pump(1)
        assert ch.balance == -1

        s.spawn(lambda: ch.send(99))
        s.run()
        assert got == [99]

    def test_channel_receive_direct_raw_timeout_drops_delivered_packet(self):
        # Task.throw bypasses timeout(). receive does not swallow it and
        # return the packet the sender already stored.
        s = _new_scheduler()
        ch = Channel(preference=0)
        seen: list[object] = []

        def receiver() -> None:
            try:
                seen.append(ch.receive())
            except BaseException as exc:
                seen.append(type(exc).__name__)

        receiver_task = s.spawn(receiver)
        s.pump(1)
        assert ch.balance == -1

        s.call_soon(ch.send, None)
        s.call_soon(receiver_task.throw, RawTimeoutError())
        s.run()

        assert seen == ["RawTimeoutError"]
        assert ch.balance == 0


class TestAtomicSection:
    def test_atomic_restore_drops_levels_added_during_the_block(self):
        s = _basic_scheduler()
        tasks: list[Task] = []

        def worker() -> None:
            task = tasks[0]
            assert task._atomic == 0
            with task.atomic():
                assert task._atomic == 1
                with task.atomic():
                    assert task._atomic == 2
                assert task._atomic == 1
                with task.atomic_restore():
                    task.atomic_inc()
                    task.atomic_inc()
                    assert task._atomic == 3
                assert task._atomic == 1
                with task.atomic_restore():
                    pass
                assert task._atomic == 1
            assert task._atomic == 0
            with task.atomic_restore():
                task.atomic_inc()
                task.atomic_inc()
                assert task._atomic == 2
            assert task._atomic == 0

        tasks.append(s.spawn(worker))
        s.run()

    def test_atomic_section_holds_timeout_until_the_block_ends(self):
        s = _basic_scheduler()
        gate = Event()
        parked = Event()
        seen: list[object] = []
        tasks: list[Task] = []

        def worker() -> None:
            task = tasks[0]
            try:
                with timeout(0):
                    with task.atomic():
                        seen.append("inside")
                        gate.swait()
                    seen.append("after")
                    parked.swait()
            except TimeoutError:
                seen.append("timeout")

        def opener() -> None:
            gate.set()

        tasks.append(s.spawn(worker))
        s.spawn(opener)
        s.run()

        assert seen == ["inside", "after", "timeout"]
        assert tasks[0]._atomic == 0

    def test_fired_timeout_is_not_rescheduled(self):
        s = _basic_scheduler()
        gate = Event()
        parked = Event()
        seen: list[object] = []
        tasks: list[Task] = []

        def worker() -> None:
            task = tasks[0]
            deadline = timeout(0)
            try:
                with deadline:
                    with task.atomic():
                        gate.swait()
                    seen.append(deadline.expired())
                    armed = deadline._handle
                    assert armed is not None
                    when = armed.when
                    deadline.reschedule(s.time() + 1000)
                    assert deadline._handle is armed
                    assert deadline._handle.when == when
                    seen.append("rescheduled")
                    parked.swait()
            except TimeoutError:
                seen.append("timeout")

        def opener() -> None:
            gate.set()

        tasks.append(s.spawn(worker))
        s.spawn(opener)
        s.run()

        assert seen == [True, "rescheduled", "timeout"]


class TestChannelTimeoutHandoff:
    def test_channel_send_timeout_while_waiting_propagates(self):
        s = _basic_scheduler()
        ch = Channel()
        seen: list[object] = []

        def sender() -> None:
            try:
                with timeout(0):
                    ch.send(5)
                seen.append("send:ok")
            except TimeoutError:
                seen.append("TimeoutError")

        s.spawn(sender)
        s.run()

        assert seen == ["TimeoutError"]
        assert ch.balance == 0

    def test_channel_receive_timeout_while_waiting_propagates(self):
        s = _basic_scheduler()
        ch = Channel()
        seen: list[object] = []

        def receiver() -> None:
            try:
                with timeout(0):
                    ch.receive()
                seen.append("recv:ok")
            except TimeoutError:
                seen.append("TimeoutError")

        s.spawn(receiver)
        s.run()

        assert seen == ["TimeoutError"]
        assert ch.balance == 0

    @pytest.mark.parametrize("preference", [0, 1])
    def test_channel_send_outer_timeout_lands_after_handoff(self, preference: int):
        s = _basic_scheduler()
        ch = Channel(preference=preference)
        hold = Event()
        seen: list[object] = []
        deadlines: list[Timeout] = []

        def sender() -> None:
            deadline = timeout(0)
            deadlines.append(deadline)
            try:
                with deadline:
                    ch.send(5)
                    seen.append("send:ok")
                    hold.swait()
            except TimeoutError:
                seen.append("TimeoutError")

        def receiver() -> None:
            seen.append(ch.receive())

        sender_task = s.spawn(sender)
        s.spawn(receiver)
        s.run()

        assert 5 in seen
        assert seen.index("send:ok") < seen.index("TimeoutError")
        assert deadlines[0].expired() is True
        assert ch.balance == 0
        assert sender_task._atomic == 0

    @pytest.mark.parametrize("preference", [0, -1])
    def test_channel_receive_outer_timeout_lands_after_handoff(self, preference: int):
        s = _basic_scheduler()
        ch = Channel(preference=preference)
        hold = Event()
        seen: list[object] = []
        deadlines: list[Timeout] = []

        def receiver() -> None:
            deadline = timeout(0)
            deadlines.append(deadline)
            try:
                with deadline:
                    seen.append(ch.receive())
                    hold.swait()
            except TimeoutError:
                seen.append("TimeoutError")

        def sender() -> None:
            ch.send(5)
            seen.append("send:ok")

        receiver_task = s.spawn(receiver)
        s.spawn(sender)
        s.run()

        assert "send:ok" in seen
        assert seen.index(5) < seen.index("TimeoutError")
        assert deadlines[0].expired() is True
        assert ch.balance == 0
        assert receiver_task._atomic == 0

    @pytest.mark.parametrize("preference", [0, 1])
    def test_channel_send_timeout_scoped_to_handoff_succeeds(self, preference: int):
        s = _basic_scheduler()
        ch = Channel(preference=preference)
        seen: list[object] = []
        deadlines: list[Timeout] = []

        def sender() -> None:
            deadline = timeout(0)
            deadlines.append(deadline)
            with deadline:
                ch.send(5)
            seen.append("send:ok")

        def receiver() -> None:
            seen.append(ch.receive())

        sender_task = s.spawn(sender)
        s.spawn(receiver)
        s.run()

        assert set(seen) == {5, "send:ok"}
        # preference 0 parks across a timer drain, so the clock fires and
        # the block then drops the throw. an immediate resume finishes the
        # block before the timer runs, so the handle is cancelled unfired.
        assert deadlines[0].expired() is (preference == 0)
        assert ch.balance == 0
        assert sender_task._atomic == 0

    @pytest.mark.parametrize("preference", [0, -1])
    def test_channel_receive_timeout_scoped_to_handoff_succeeds(self, preference: int):
        s = _basic_scheduler()
        ch = Channel(preference=preference)
        seen: list[object] = []
        deadlines: list[Timeout] = []

        def receiver() -> None:
            deadline = timeout(0)
            deadlines.append(deadline)
            with deadline:
                seen.append(ch.receive())

        def sender() -> None:
            ch.send(5)
            seen.append("send:ok")

        receiver_task = s.spawn(receiver)
        s.spawn(sender)
        s.run()

        assert set(seen) == {5, "send:ok"}
        # preference 0 parks across a timer drain, so the clock fires and
        # the block then drops the throw. an immediate resume finishes the
        # block before the timer runs, so the handle is cancelled unfired.
        assert deadlines[0].expired() is (preference == 0)
        assert ch.balance == 0
        assert receiver_task._atomic == 0

    def test_immediate_send_timeout_lands_after_handoff(self):
        # receiver is already waiting. preference -1 suspends the sender
        # inside send while the receiver runs.
        s = _basic_scheduler()
        ch = Channel(preference=-1)
        hold = Event()
        seen: list[object] = []

        def receiver() -> None:
            seen.append(ch.receive())

        def sender() -> None:
            try:
                with timeout(0):
                    ch.send(5)
                    seen.append("send:ok")
                    hold.swait()
            except TimeoutError:
                seen.append("TimeoutError")

        s.spawn(receiver)
        sender_task = s.spawn(sender)
        s.run()

        assert 5 in seen
        assert seen.index("send:ok") < seen.index("TimeoutError")
        assert ch.balance == 0
        assert sender_task._atomic == 0

    def test_immediate_receive_timeout_lands_after_handoff(self):
        # sender is already waiting. preference 1 suspends the receiver
        # inside receive while the sender runs.
        s = _basic_scheduler()
        ch = Channel(preference=1)
        hold = Event()
        seen: list[object] = []

        def sender() -> None:
            ch.send(5)
            seen.append("send:ok")

        def receiver() -> None:
            try:
                with timeout(0):
                    seen.append(ch.receive())
                    hold.swait()
            except TimeoutError:
                seen.append("TimeoutError")

        s.spawn(sender)
        receiver_task = s.spawn(receiver)
        s.run()

        assert "send:ok" in seen
        assert seen.index(5) < seen.index("TimeoutError")
        assert ch.balance == 0
        assert receiver_task._atomic == 0

    def test_async_receive_timeout_lands_after_handoff(self):
        # the sender queues async_receive, then parks. the end drain
        # commits before the due timer, so the timer must wait.
        s = _basic_scheduler()
        ch = Channel()
        hold = Event()
        seen: list[object] = []

        def commit() -> None:
            seen.append(("recv", _finish_now(ch.async_receive())))

        def sender() -> None:
            try:
                with timeout(0):
                    s.call_soon(commit)
                    ch.send(5)
                    seen.append("send:ok")
                    hold.swait()
            except TimeoutError:
                seen.append("TimeoutError")

        sender_task = s.spawn(sender)
        s.run()

        assert seen == [("recv", 5), "send:ok", "TimeoutError"]
        assert ch.balance == 0
        assert sender_task._atomic == 0

    def test_async_send_timeout_lands_after_handoff(self):
        # the receiver queues async_send, then parks. same drain order.
        s = _basic_scheduler()
        ch = Channel()
        hold = Event()
        seen: list[object] = []

        def commit() -> None:
            _finish_now(ch.async_send(5))
            seen.append("send:ok")

        def receiver() -> None:
            try:
                with timeout(0):
                    s.call_soon(commit)
                    seen.append(("recv", ch.receive()))
                    hold.swait()
            except TimeoutError:
                seen.append("TimeoutError")

        receiver_task = s.spawn(receiver)
        s.run()

        assert seen == ["send:ok", ("recv", 5), "TimeoutError"]
        assert ch.balance == 0
        assert receiver_task._atomic == 0
