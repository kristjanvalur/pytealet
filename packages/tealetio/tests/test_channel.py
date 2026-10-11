import asyncio

import pytest

from collections import deque

from tealetio import BasicScheduler, Channel, Event, Timeout, TimeoutError, set_scheduler
from tealetio.examples import send_or_requeue
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
        s.turn()

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
        s.turn()

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
                assert not hasattr(exc, "operation_result")
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
        s.turn()
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
        s.turn()
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

    def test_channel_async_receive_cancelled_with_pending_packet_propagates(self):
        ch = Channel()

        async def run() -> None:
            recv_task = asyncio.create_task(ch.async_receive())
            await asyncio.sleep(0)
            # the packet under this waiter is the commit. skip the wake so
            # cancel lands in the await.
            waiter = ch._waiters[0]
            ch._packets[waiter] = (False, 7)
            recv_task.cancel()
            with pytest.raises(asyncio.CancelledError) as caught:
                await recv_task
            assert Channel.operation_result(caught.value) == (True, 7, None)
            assert ch.balance == 0
            assert ch._packets == {}

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))

    def test_channel_async_receive_cancelled_without_packet_propagates(self):
        ch = Channel()

        async def run() -> None:
            recv_task = asyncio.create_task(ch.async_receive())
            await asyncio.sleep(0)
            recv_task.cancel()
            with pytest.raises(asyncio.CancelledError) as caught:
                await recv_task
            assert Channel.operation_result(caught.value) == (False, None, None)
            assert ch.balance == 0

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
                seen.append((type(exc).__name__, exc.operation_result))

        sender_task = s.spawn(sender)
        s.turn()
        assert ch.balance == 1

        s.call_soon(lambda: got.append(ch.receive()))
        s.call_soon(sender_task.throw, RawTimeoutError())
        s.run()

        assert got == [5]
        assert seen == [("RawTimeoutError", (True, None, None))]
        assert ch.balance == 0

    def test_channel_send_direct_throw_before_commit(self):
        s = _new_scheduler()
        ch = Channel(preference=0)
        seen: list[object] = []

        def sender() -> None:
            try:
                ch.send(5)
            except RuntimeError as exc:
                seen.append(exc.operation_result)

        sender_task = s.spawn(sender)
        s.turn()
        assert ch.balance == 1

        s.call_soon(sender_task.throw, RuntimeError("interrupt"))
        s.run()

        assert seen == [(False, None, None)]
        assert ch.balance == 0
        assert ch._packets == {}

    def test_channel_async_send_cancelled_with_consumed_packet_propagates(self):
        ch = Channel()

        async def run() -> None:
            send_task = asyncio.create_task(ch.async_send(7))
            await asyncio.sleep(0)
            waiter = ch._waiters.popleft()
            ch._balance -= 1
            assert ch._packets.pop(waiter) == (False, 7)
            send_task.cancel()
            with pytest.raises(asyncio.CancelledError) as caught:
                await send_task
            assert Channel.operation_result(caught.value) == (True, None, None)
            assert ch.balance == 0

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))

    def test_channel_async_send_cancelled_with_pending_packet_propagates(self):
        ch = Channel()

        async def run() -> None:
            send_task = asyncio.create_task(ch.async_send(7))
            await asyncio.sleep(0)
            assert ch.balance == 1
            send_task.cancel()
            with pytest.raises(asyncio.CancelledError) as caught:
                await send_task
            assert Channel.operation_result(caught.value) == (False, None, None)
            assert ch.balance == 0
            assert ch._packets == {}

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))

    def test_channel_receive_external_exception_drops_pending_packet(self):
        s = _new_scheduler()
        ch = Channel(preference=0)
        seen: list[object] = []

        def receiver() -> None:
            try:
                ch.receive()
            except RuntimeError as exc:
                seen.append(exc.operation_result)

        receiver_task = s.spawn(receiver)
        s.turn()
        assert ch.balance == -1

        s.call_soon(ch.send, 42)
        s.call_soon(receiver_task.throw, RuntimeError("interrupt"))
        s.run()

        assert seen == [(True, 42, None)]
        assert ch.balance == 0

        # The pending packet must have been discarded with the external wake.
        got: list[int] = []

        def receiver2() -> None:
            value = ch.receive()
            assert isinstance(value, int)
            got.append(value)

        s.spawn(receiver2)
        s.turn()
        assert ch.balance == -1

        s.spawn(lambda: ch.send(99))
        s.run()
        assert got == [99]

    def test_channel_receive_external_exception_reports_sent_exception(self):
        s = _new_scheduler()
        ch = Channel(preference=0)
        seen: list[object] = []
        payload = ValueError("boom")

        def receiver() -> None:
            try:
                ch.receive()
            except RuntimeError as exc:
                seen.append(exc.operation_result)

        receiver_task = s.spawn(receiver)
        s.turn()
        assert ch.balance == -1

        s.call_soon(ch.send_exception, payload)
        s.call_soon(receiver_task.throw, RuntimeError("interrupt"))
        s.run()

        assert seen == [(True, None, payload)]
        assert not hasattr(payload, "operation_result")
        assert ch.balance == 0
        assert ch._packets == {}

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
                seen.append((type(exc).__name__, exc.operation_result))

        receiver_task = s.spawn(receiver)
        s.turn()
        assert ch.balance == -1

        s.call_soon(ch.send, None)
        s.call_soon(receiver_task.throw, RawTimeoutError())
        s.run()

        assert seen == [("RawTimeoutError", (True, None, None))]
        assert ch.balance == 0

    def test_send_or_requeue_returns_after_the_peer_receives(self):
        s = _new_scheduler()
        ch = Channel()
        unsent: deque[object] = deque()
        got: list[object] = []

        s.spawn(lambda: send_or_requeue(ch, unsent, 5))
        s.spawn(lambda: got.append(ch.receive()))
        s.run()

        assert got == [5]
        assert list(unsent) == []

    def test_send_or_requeue_puts_an_undelivered_value_back(self):
        s = _new_scheduler()
        ch = Channel(preference=0)
        unsent: deque[object] = deque()

        def sender() -> None:
            send_or_requeue(ch, unsent, 5)

        task = s.spawn(sender)
        s.turn()
        assert ch.balance == 1

        s.call_soon(task.throw, RuntimeError("interrupt"))
        s.run()

        assert list(unsent) == [5]
        assert ch.balance == 0
        assert isinstance(task.exception(), RuntimeError)

    def test_send_or_requeue_keeps_a_value_the_peer_already_took(self):
        s = _new_scheduler()
        ch = Channel(preference=0)
        unsent: deque[object] = deque()
        got: list[object] = []

        def sender() -> None:
            send_or_requeue(ch, unsent, 5)

        task = s.spawn(sender)
        s.turn()
        assert ch.balance == 1

        s.call_soon(lambda: got.append(ch.receive()))
        s.call_soon(task.throw, RuntimeError("interrupt"))
        s.run()

        assert got == [5]
        assert list(unsent) == []
        assert isinstance(task.exception(), RuntimeError)


class TestOperationResult:
    def test_reads_the_exception_then_cause_then_context(self):
        direct = RuntimeError("direct")
        direct.operation_result = (True, 1, None)
        assert Channel.operation_result(direct) == (True, 1, None)

        cause = RuntimeError("cause")
        cause.operation_result = (False, None, None)
        wrapped = TimeoutError("wrapped")
        wrapped.__cause__ = cause
        context = asyncio.CancelledError()
        context.operation_result = (True, 7, None)
        wrapped.__context__ = context
        assert Channel.operation_result(wrapped) == (False, None, None)

        # 3.10 asyncio replaces CancelledError and chains the original as context.
        outer = asyncio.CancelledError()
        assert Channel.operation_result(outer) is None
        outer.__context__ = context
        assert Channel.operation_result(outer) == (True, 7, None)

        assert Channel.operation_result(RuntimeError("plain")) is None


def _timeout_result(exc: BaseException) -> tuple[bool, object, BaseException | None]:
    # timeout() raises a new TimeoutError. the triple stays on __cause__.
    assert not hasattr(exc, "operation_result")
    result = Channel.operation_result(exc)
    assert result is not None
    return result


class TestTimeout:
    def test_fired_timeout_is_not_rescheduled(self):
        s = _basic_scheduler()
        seen: list[object] = []

        def worker() -> None:
            deadline = timeout(0)
            try:
                with deadline:
                    try:
                        Event().swait()
                    except RawTimeoutError:
                        assert deadline.expired()
                        armed = deadline._handle
                        assert armed is not None
                        when = armed.when
                        deadline.reschedule(s.time() + 1000)
                        assert deadline._handle is armed
                        assert deadline._handle.when == when
                        seen.append("rescheduled")
                        raise
            except TimeoutError:
                seen.append("timeout")

        s.spawn(worker)
        s.run()

        assert seen == ["rescheduled", "timeout"]


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
            except TimeoutError as exc:
                seen.append(_timeout_result(exc))

        s.spawn(sender)
        s.run()

        assert seen == [(False, None, None)]
        assert ch.balance == 0
        assert ch._packets == {}

    def test_channel_receive_timeout_while_waiting_propagates(self):
        s = _basic_scheduler()
        ch = Channel()
        seen: list[object] = []

        def receiver() -> None:
            try:
                with timeout(0):
                    ch.receive()
                seen.append("recv:ok")
            except TimeoutError as exc:
                seen.append(_timeout_result(exc))

        s.spawn(receiver)
        s.run()

        assert seen == [(False, None, None)]
        assert ch.balance == 0
        assert ch._packets == {}

    def test_channel_send_timeout_after_the_peer_took_the_packet(self):
        # preference 0 queues the sender. the callback runs before the due
        # timer, so receive takes the packet and the timeout lands in send.
        s = _basic_scheduler()
        ch = Channel(preference=0)
        got: list[object] = []
        seen: list[object] = []

        def sender() -> None:
            try:
                with timeout(0):
                    ch.send(5)
                seen.append("send:ok")
            except TimeoutError as exc:
                seen.append(_timeout_result(exc))

        s.spawn(sender)
        s.turn()
        assert ch.balance == 1

        s.call_soon(lambda: got.append(ch.receive()))
        s.run()

        assert got == [5]
        assert seen == [(True, None, None)]
        assert ch.balance == 0
        assert ch._packets == {}

    def test_channel_receive_timeout_after_the_peer_stored_the_packet(self):
        # preference 0 queues the receiver. the callback runs before the due
        # timer, so send stores the packet and the timeout lands in receive.
        s = _basic_scheduler()
        ch = Channel(preference=0)
        seen: list[object] = []

        def receiver() -> None:
            try:
                with timeout(0):
                    seen.append(ch.receive())
            except TimeoutError as exc:
                seen.append(_timeout_result(exc))

        s.spawn(receiver)
        s.turn()
        assert ch.balance == -1

        s.call_soon(ch.send, 5)
        s.run()

        assert seen == [(True, 5, None)]
        assert ch.balance == 0
        assert ch._packets == {}

    def test_immediate_send_timeout_reports_delivery(self):
        # the receiver is already waiting. preference -1 switches to it
        # inside send. that task then exits to the driver, and the next
        # drain delivers the due timeout while send is still in the switch.
        s = _basic_scheduler()
        ch = Channel()
        got: list[object] = []
        seen: list[object] = []
        deadlines: list[Timeout] = []

        def receiver() -> None:
            got.append(ch.receive())

        def sender() -> None:
            deadline = timeout(0)
            deadlines.append(deadline)
            try:
                with deadline:
                    ch.send(5)
                seen.append("send:ok")
            except TimeoutError as exc:
                seen.append(_timeout_result(exc))

        s.spawn(receiver)
        s.turn()
        assert ch.balance == -1

        s.spawn(sender)
        s.run()

        assert got == [5]
        assert seen == [(True, None, None)]
        assert deadlines[0].expired() is True
        assert ch.balance == 0
        assert ch._packets == {}

    def test_immediate_receive_timeout_reports_the_value(self):
        # the sender is already waiting. preference 1 switches to it inside
        # receive. that task then exits to the driver, and the next drain
        # delivers the due timeout while receive is still in the switch.
        s = _basic_scheduler()
        ch = Channel(preference=1)
        seen: list[object] = []
        deadlines: list[Timeout] = []

        def sender() -> None:
            ch.send(5)
            seen.append("send:ok")

        def receiver() -> None:
            deadline = timeout(0)
            deadlines.append(deadline)
            try:
                with deadline:
                    seen.append(ch.receive())
            except TimeoutError as exc:
                seen.append(_timeout_result(exc))

        s.spawn(sender)
        s.turn()
        assert ch.balance == 1

        s.spawn(receiver)
        s.run()

        assert seen == ["send:ok", (True, 5, None)]
        assert deadlines[0].expired() is True
        assert ch.balance == 0
        assert ch._packets == {}

    def test_async_receive_then_timeout_reports_delivery(self):
        # the callback runs before the due timer, so async_receive takes the
        # packet and the timeout then lands in send.
        s = _basic_scheduler()
        ch = Channel()
        seen: list[object] = []

        def commit() -> None:
            seen.append(("recv", _finish_now(ch.async_receive())))

        def sender() -> None:
            try:
                with timeout(0):
                    s.call_soon(commit)
                    ch.send(5)
                    seen.append("send:ok")
            except TimeoutError as exc:
                seen.append(_timeout_result(exc))

        s.spawn(sender)
        s.run()

        assert seen == [("recv", 5), (True, None, None)]
        assert ch.balance == 0
        assert ch._packets == {}

    def test_async_send_then_timeout_reports_the_value(self):
        # the callback runs before the due timer, so async_send stores the
        # packet and the timeout then lands in receive.
        s = _basic_scheduler()
        ch = Channel()
        seen: list[object] = []

        def commit() -> None:
            _finish_now(ch.async_send(5))
            seen.append("send:ok")

        def receiver() -> None:
            try:
                with timeout(0):
                    s.call_soon(commit)
                    seen.append(("recv", ch.receive()))
            except TimeoutError as exc:
                seen.append(_timeout_result(exc))

        s.spawn(receiver)
        s.run()

        assert seen == ["send:ok", (True, 5, None)]
        assert ch.balance == 0
        assert ch._packets == {}
