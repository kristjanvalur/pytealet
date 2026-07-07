"""Experimental accept-time ``recv_many`` sink protocols.

See ``packages/tealetio/docs/EXPERIMENT_ACCEPT_RECV_MANY.md``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias, TypeVar

from .operations import ContinuousOperation
from .recv_iter import RecvIterBuffer, _RecvManyResult

if TYPE_CHECKING:
    from .proactor import RecvBufferPool

_AcceptRecvSinkT = TypeVar("_AcceptRecvSinkT", covariant=True)


class AcceptRecvSink(Protocol):
    """User object returned from an accept callback with ``recv_many`` already armed."""

    def recv_many_callback(self) -> Callable[[_RecvManyResult], object]:
        """Callback wired into ``proactor.recv_many()`` for this connection."""


class AcceptRecvSinkAttachable(AcceptRecvSink, Protocol):
    def attach_recv_many(self, stream: ContinuousOperation[_RecvManyResult]) -> None:
        """Optional hook to bind the continuous ``recv_many`` operation."""


AcceptRecvSinkFactory: TypeAlias = Callable[[Any, "RecvBufferPool"], AcceptRecvSink | None]
"""``(accepted_socket, buf_group) -> sink`` or ``None`` when the backend cannot arm ``recv_many``.

Invoked on the proactor delivery worker thread (uring completion service or
selector worker) for each accepted connection, before the accept callback runs.
The proactor then arms ``recv_many`` using ``recv_many_callback()`` and only
delivers the sink to the client afterwards.
"""


def recv_iter_buffer_factory(
    *,
    buffer_pool: "RecvBufferPool | None" = None,
) -> AcceptRecvSinkFactory:
    """Build a factory that returns a ``RecvIterBuffer`` per accepted socket."""

    def factory(conn: Any, buf_group: "RecvBufferPool") -> RecvIterBuffer:
        del conn
        pool = buffer_pool if buffer_pool is not None else buf_group
        return RecvIterBuffer(buf_group=pool)

    return factory