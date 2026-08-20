"""Shared type aliases and IO hint enums for tealetio."""

from __future__ import annotations

from enum import Enum
from typing import NamedTuple, TypeAlias

SocketSendBuffer: TypeAlias = bytes | bytearray | memoryview


class IoExpect(Enum):
    """Whether the first attempt is expected to complete immediately.

    Proactor-agnostic. Uring maps this to ``IORING_RECVSEND_POLL_FIRST`` on
    the first SQE only: ``READY`` omits the bit (try the send/recv now),
    ``BLOCK`` sets it when the kernel probe allows (poll first). Selector
    backends ignore it today. Later legs of a multi-leg send always wait.

    Default on ``Proactor.send`` is ``READY`` (no prior non-blocking try).
    ``ProactorIOManager.sock_sendall`` passes ``BLOCK`` after an eager
    would-block or partial send.
    """

    READY = "ready"
    BLOCK = "block"


class IoMore(Enum):
    """Whether more socket data is already queued after a oneshot receive.

    Maps onto the next ``recv``'s ``IoExpect``: ``MORE`` (the default) means
    try now (``IoExpect.READY``); ``EMPTY`` means wait
    (``IoExpect.BLOCK`` / ``POLL_FIRST``). Uring ``SOCK_NONEMPTY`` sets
    ``MORE``; its absence on a completed recv is ``EMPTY``.

    This is for callers who submit the next recv themselves
    (``Proactor.recv`` / ``sock_recv``). It is not a multishot-delivery
    field: ``recv_many`` re-arm stays inside the proactor, and
    ``MultishotDelivery.more`` is the continuous-stream ``CQE_F_MORE`` bit.
    """

    MORE = "more"
    EMPTY = "empty"


class RecvResult(NamedTuple):
    """Oneshot receive payload plus ``IoMore`` for the next recv submit.

    ``more`` defaults to ``MORE`` so an unspecified hint matches
    ``IoExpect.READY``. ``sock_recv`` still waits to ``bytes``
    (``map_result``). Direct ``proactor.recv`` callers who will recv again
    should read ``more``. Compares equal to ``bytes`` on ``data`` so existing
    ``result() == b"..."`` checks keep working; use
    ``isinstance(..., RecvResult)`` when the hint matters.
    """

    data: bytes
    more: IoMore = IoMore.MORE

    def __eq__(self, other: object) -> bool:
        if isinstance(other, RecvResult):
            return tuple.__eq__(self, other)
        if isinstance(other, (bytes, bytearray)):
            return self.data == other
        return NotImplemented

    def __hash__(self) -> int:
        return tuple.__hash__(self)


__all__ = ["IoExpect", "IoMore", "RecvResult", "SocketSendBuffer"]
