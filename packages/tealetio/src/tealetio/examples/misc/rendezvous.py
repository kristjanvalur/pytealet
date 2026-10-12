"""Illustration: pass one object once the receiver accepts it.

This is not a tealetio primitive. ``Channel`` is the supported rendezvous.
``send`` returns only after the receiver has accepted the object. If the
receiver is resumed by an exception after the object was handed over, it
rejects the transfer and ``send`` raises. If ``send`` itself is resumed by
an exception before it sees that answer, it raises and does not report
delivery. The receiver may still have taken the object.

A timeout while one side is still the only waiter fails that call and
leaves the rendezvous empty. Nothing here postpones ``timeout()`` or
``cancel()``.
"""

from __future__ import annotations

from tealetio.locks import Event
from tealetio.tasks import Task, get_current

_MISSING = object()
_ACCEPTED = object()
_REJECTED = object()


def _current() -> Task:
    task = get_current()
    if task is None:
        raise RuntimeError("rendezvous must be called from a task")
    return task


class _Handoff:
    """One exchange, owned by the task that parked first."""

    __slots__ = ("value", "filled", "answer", "ready", "ack")

    def __init__(self) -> None:
        self.value: object = None
        self.filled = False
        self.answer: object = _MISSING
        self.ready = Event()
        self.ack = Event()


class Rendezvous:
    """Block a sender and a receiver until the receiver accepts one object.

    A second sender while one is already waiting raises `RuntimeError`,
    and the same for a second receiver. After a completed exchange, a
    rejection, or a timeout while still the only waiter, the rendezvous
    can be used again.

    ``send`` raises `RuntimeError` when the receiver rejects the transfer.
    An exception in ``send`` before that answer is the sender's own
    exception. Success means the receiver accepted the object.
    """

    def __init__(self) -> None:
        self._sender: Task | None = None
        self._receiver: Task | None = None
        self._handoff: _Handoff | None = None

    def send(self, value: object) -> None:
        """Give `value` to the receiver, returning once the receiver accepts."""

        if self._receiver is not None:
            handoff = self._handoff
            assert handoff is not None
            self._publish(value, handoff)
            return
        if self._sender is not None:
            raise RuntimeError("rendezvous already has a waiting sender")
        self._offer(value)

    def receive(self) -> object:
        """Take the sender's object, blocking until the sender arrives."""

        if self._sender is not None:
            handoff = self._handoff
            assert handoff is not None
            return self._take(handoff)
        if self._receiver is not None:
            raise RuntimeError("rendezvous already has a waiting receiver")
        return self._wait_for_send()

    def _offer(self, value: object) -> None:
        current = _current()
        handoff = _Handoff()
        handoff.value = value
        handoff.filled = True
        self._sender = current
        self._handoff = handoff
        try:
            handoff.ready.swait()
        except BaseException:
            # still our offer: the receiver never took it. an exception
            # after the receiver has taken the slot is our own, and it
            # is not an acceptance.
            if self._sender is current:
                self._sender = None
                self._handoff = None
            raise
        self._observe(handoff)

    def _wait_for_send(self) -> object:
        current = _current()
        handoff = _Handoff()
        self._receiver = current
        self._handoff = handoff
        try:
            handoff.ready.swait()
        except BaseException:
            if self._receiver is current:
                self._receiver = None
                self._handoff = None
                raise
            # the sender has published and is parked on the answer.
            # this exception means we did not accept the object.
            handoff.answer = _REJECTED
            handoff.ack.set()
            raise
        assert handoff.filled
        value = handoff.value
        handoff.answer = _ACCEPTED
        handoff.ack.set()
        return value

    def _take(self, handoff: _Handoff) -> object:
        assert handoff.filled
        value = handoff.value
        handoff.answer = _ACCEPTED
        try:
            handoff.ready.set()
        except BaseException:
            # the sender is still parked on its offer.
            handoff.answer = _MISSING
            raise
        self._sender = None
        self._handoff = None
        return value

    def _publish(self, value: object, handoff: _Handoff) -> None:
        handoff.value = value
        handoff.filled = True
        try:
            handoff.ready.set()
        except BaseException:
            handoff.filled = False
            handoff.value = None
            raise
        # set only queues the receiver. free the slot before this wait so
        # an exception here is not read as a still-owned offer, and so the
        # receiver's caller can start another exchange.
        self._receiver = None
        self._handoff = None
        handoff.ack.swait()
        self._observe(handoff)

    def _observe(self, handoff: _Handoff) -> None:
        if handoff.answer is _REJECTED:
            raise RuntimeError("rendezvous peer rejected the transfer")
        assert handoff.answer is _ACCEPTED
