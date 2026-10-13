"""User-level rendezvous: delivery is the receiver's accept or reject."""

from tealetio import BasicScheduler, CancelledError, TimeoutError, set_scheduler
from tealetio.locks import timeout
from tealetio.examples.misc.rendezvous import Rendezvous


def _basic_scheduler() -> BasicScheduler:
    scheduler = BasicScheduler()
    set_scheduler(scheduler)
    return scheduler


def _assert_idle(rz: Rendezvous) -> None:
    assert rz._sender is None
    assert rz._receiver is None
    assert rz._handoff is None


class TestRendezvous:
    def test_receiver_arrives_first(self):
        s = _basic_scheduler()
        rz = Rendezvous()
        seen: list[object] = []

        def receiver() -> None:
            seen.append(("recv", rz.receive()))

        def sender() -> None:
            rz.send(5)
            seen.append("send:ok")

        s.spawn(receiver)
        s.spawn(sender)
        s.run()

        # receive records acceptance and returns the object before send
        # resumes on the answer.
        assert seen == [("recv", 5), "send:ok"]
        _assert_idle(rz)

    def test_sender_arrives_first(self):
        s = _basic_scheduler()
        rz = Rendezvous()
        seen: list[object] = []

        def sender() -> None:
            rz.send(5)
            seen.append("send:ok")

        def receiver() -> None:
            seen.append(("recv", rz.receive()))

        s.spawn(sender)
        s.spawn(receiver)
        s.run()

        assert seen == [("recv", 5), "send:ok"]
        _assert_idle(rz)

    def test_second_exchange_on_the_same_rendezvous(self):
        s = _basic_scheduler()
        rz = Rendezvous()
        seen: list[object] = []

        def worker() -> None:
            rz.send("a")
            seen.append(rz.receive())

        def peer() -> None:
            seen.append(rz.receive())
            rz.send("b")

        s.spawn(worker)
        s.spawn(peer)
        s.run()

        assert seen == ["a", "b"]

    def test_send_timeout_while_waiting_propagates_and_rendezvous_stays_usable(self):
        s = _basic_scheduler()
        rz = Rendezvous()
        seen: list[object] = []

        def attempt() -> None:
            try:
                with timeout(0):
                    rz.send(1)
                seen.append("send:ok")
            except TimeoutError:
                seen.append("TimeoutError")

        s.spawn(attempt)
        s.run()

        assert seen == ["TimeoutError"]
        _assert_idle(rz)

        def sender() -> None:
            rz.send(2)
            seen.append("send:ok")

        def receiver() -> None:
            seen.append(rz.receive())

        s.spawn(receiver)
        s.spawn(sender)
        s.run()

        assert seen == ["TimeoutError", 2, "send:ok"]

    def test_receive_timeout_while_waiting_propagates(self):
        s = _basic_scheduler()
        rz = Rendezvous()
        seen: list[object] = []

        def attempt() -> None:
            try:
                with timeout(0):
                    seen.append(rz.receive())
            except TimeoutError:
                seen.append("TimeoutError")

        s.spawn(attempt)
        s.run()

        assert seen == ["TimeoutError"]
        _assert_idle(rz)

    def test_sender_timeout_during_ack_does_not_report_delivery(self):
        # the driver is ahead of the receiver that set() queued, so the
        # timeout drain throws into the answer wait before the receiver
        # runs. send must not report delivery. the receiver still takes
        # the object.
        s = _basic_scheduler()
        rz = Rendezvous()
        seen: list[object] = []

        def receiver() -> None:
            seen.append(("recv", rz.receive()))

        def sender() -> None:
            try:
                with timeout(0):
                    rz.send(5)
                seen.append("send:ok")
            except TimeoutError:
                seen.append("TimeoutError")

        s.spawn(receiver)
        s.spawn(sender)
        s.run()

        assert seen == ["TimeoutError", ("recv", 5)]
        _assert_idle(rz)

        def sender_again() -> None:
            rz.send("again")
            seen.append("send:again")

        def receiver_again() -> None:
            seen.append(("recv", rz.receive()))

        s.spawn(receiver_again)
        s.spawn(sender_again)
        s.run()

        assert seen[-2:] == [("recv", "again"), "send:again"]
        _assert_idle(rz)

    def test_receiver_cancel_after_publish_rejects(self):
        s = _basic_scheduler()
        rz = Rendezvous()
        seen: list[object] = []

        def receiver() -> None:
            try:
                seen.append(("recv", rz.receive()))
            except CancelledError:
                seen.append("recv:cancel")

        def sender() -> None:
            try:
                rz.send(5)
                seen.append("send:ok")
            except RuntimeError as exc:
                seen.append(str(exc))

        receiver_task = s.spawn(receiver)
        s.turn()
        assert rz._receiver is receiver_task
        assert seen == []
        s.spawn(sender)
        s.turn()
        assert seen == []
        assert rz._handoff is None

        def cancel_receiver() -> None:
            receiver_task.cancel()

        canceller = s.spawn(cancel_receiver)
        s.reschedule(canceller, position=0)
        s.run()

        assert seen == [
            "recv:cancel",
            "rendezvous peer rejected the transfer",
        ]
        assert "send:ok" not in seen
        _assert_idle(rz)

        def sender_again() -> None:
            rz.send(7)
            seen.append("send:again")

        def receiver_again() -> None:
            seen.append(("recv", rz.receive()))

        s.spawn(receiver_again)
        s.spawn(sender_again)
        s.run()

        assert seen[-2:] == [("recv", 7), "send:again"]

    def test_second_sender_while_one_is_waiting_raises(self):
        s = _basic_scheduler()
        rz = Rendezvous()
        seen: list[object] = []

        def holder() -> None:
            rz.send(1)

        def second() -> None:
            try:
                rz.send(2)
            except RuntimeError:
                seen.append("busy")

        s.spawn(holder)
        s.spawn(second)
        s.run()

        assert seen == ["busy"]
        assert rz._sender is not None
        handoff = rz._handoff
        assert handoff is not None
        assert handoff.value == 1
