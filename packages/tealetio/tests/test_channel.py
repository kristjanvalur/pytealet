import asyncio

import pytest

from tealetio import Channel
from tealetio.locks import RawTimeoutError

from helpers import new_scheduler as _new_scheduler


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

    def test_channel_send_raw_timeout_suppressed_when_packet_already_consumed(self):
        s = _new_scheduler()
        ch = Channel(preference=0)
        seen: list[object] = []

        def sender() -> None:
            try:
                ch.send(5)
                seen.append("send:ok")
            except BaseException as exc:
                seen.append(type(exc).__name__)

        sender_task = s.spawn(sender)
        s.pump(1)
        assert ch.balance == 1

        # Receiver consumes the packet first; timeout throw races after.
        s.call_soon(ch.receive)
        s.call_soon(sender_task.throw, RawTimeoutError())
        s.run()

        assert seen == ["send:ok"]
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

    def test_channel_receive_raw_timeout_suppressed_when_packet_already_delivered(self):
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

        # Sender callback runs first and delivers packet; timeout throw races after.
        # Use None payload to ensure packet existence check does not treat None
        # as "missing".
        s.call_soon(ch.send, None)
        s.call_soon(receiver_task.throw, RawTimeoutError())
        s.run()

        assert seen == [None]
        assert ch.balance == 0
