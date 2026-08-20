"""Shared type aliases and IO hint enums for tealetio."""

from __future__ import annotations

from enum import Enum
from typing import TypeAlias

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
    """Whether more socket data is already queued after a receive.

    Distinct from ``MultishotDelivery.more``, which is the continuous-stream
    ``IORING_CQE_F_MORE`` bit (another leg of this op). ``IoMore.MORE`` is
    the portable form of ``IORING_CQE_F_SOCK_NONEMPTY``: the next recv can
    skip a poll-first wait. ``EMPTY`` means no such hint.

    Carried on ``MultishotDelivery.ready`` for ``recv_many``. Oneshot
    ``sock_recv`` will grow a similar result later; do not overload the
    ``bytes`` return yet.
    """

    EMPTY = "empty"
    MORE = "more"


__all__ = ["IoExpect", "IoMore", "SocketSendBuffer"]
