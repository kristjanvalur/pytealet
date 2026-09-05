from __future__ import annotations

import errno
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, NamedTuple, TypeAlias

from .stream_diag import worker_completion_mark_emit_end, worker_completion_mark_emit_start


class InvalidStateError(Exception):
    """Raised when an operation result is requested before completion."""


def io_cancellation_error() -> OSError:
    """Return the standard exception for proactor-requested IO cancellation."""

    return OSError(
        errno.ECANCELED,
        errno.errorcode.get(errno.ECANCELED, "io_uring operation failed"),
    )


def is_io_cancellation(exc: BaseException | None) -> bool:
    """Return True when ``exc`` represents IO cancellation (not task cancellation)."""

    return isinstance(exc, OSError) and exc.errno == errno.ECANCELED


class MultishotDelivery(NamedTuple):
    """One multishot leg delivery to a continuous stream callback.

    ``(index, value, exception, more)``. For ``recv_many``, ``accept_many``,
    and ``poll_many``, ``index`` is the stream ordinal from the backend
    (``completion.sequence`` on uring, or ``_next_index`` on selector
    ``SelectorCancelHandle`` and uring one-shot poll fallbacks, including
    local cancel terminals).
    ``value`` carries successful chunk data
    when present. ``exception`` carries transport failures the consumer may
    interpret (for example ``errno.ENOBUFS`` or a negative io_uring CQE).
    Terminal failures are emitted through the result callback. ``more``
    mirrors ``IORING_CQE_F_MORE`` on uring backends. For ``recv_many``,
    ``more=False`` with empty data signals EOF; ``more=False`` with non-empty
    data means the leg stopped before EOF and consumers should start a fresh
    ``recv_many()``. ``accept_many`` terminals (``more=False``) are stream-end
    for that arm; oneshot backends finish after each accept. ``poll_many``
    terminals are stream-end (stop or error).
    """

    index: int = 0
    value: Any = None
    exception: BaseException | None = None
    more: bool = True


@dataclass
class ContinuousStepResult:
    progressed: bool = False
    done: bool = False


# Opaque handle to a submitted proactor operation (oneshot or stream).
# Concrete values:
# - uring: armed ``Completion``, or ``None`` when the callback already ran
# - selector oneshot: private token in ``proactor.py``
# - selector recv/accept/poll-many: ``SelectorCancelHandle``
# - emulated oneshot poll_many: reverse-link holder
# Cancel with ``proactor.cancel`` / ``cancel_nowait``; stop poll with
# ``proactor.stop_poll``. Do not call ``done()`` / ``result()`` on the handle.
OpHandle: TypeAlias = Any
RecvManyHandle: TypeAlias = OpHandle
AcceptManyHandle: TypeAlias = OpHandle
PollManyHandle: TypeAlias = OpHandle


class _DeliveryHandle:
    """Internal stream token: emit ``MultishotDelivery`` to a result callback.

    Selector streams and emulated oneshot poll_many inherit this. Not a
    waitable; not the public ``OpHandle`` alias.
    """

    __slots__ = ("_result_callback",)

    def __init__(
        self,
        result_callback: Callable[[MultishotDelivery], object] | None = None,
    ) -> None:
        self._result_callback = result_callback

    def _emit_delivery(self, delivery: MultishotDelivery) -> None:
        """Deliver one multishot chunk to the result callback."""

        callback = self._result_callback
        if callback is None:
            return
        worker_completion_mark_emit_start()
        try:
            callback(delivery)
        finally:
            worker_completion_mark_emit_end()

    def _emit_result(
        self,
        result: Any,
        *,
        index: int = 0,
        exception: BaseException | None = None,
        more: bool = True,
    ) -> None:
        """Deliver one successful chunk wrapped in ``MultishotDelivery``."""

        self._emit_delivery(MultishotDelivery(index, result, exception, more))

    def _finish_with_terminal_delivery(self, delivery: MultishotDelivery) -> None:
        """Emit one terminal ``MultishotDelivery`` for the result callback."""

        assert not delivery.more
        self._emit_delivery(delivery)


class SelectorCancelHandle(_DeliveryHandle):
    """Selector recv-many / accept-many / poll-many cancel token: next stream ordinal.

    Selector has no ``completion.sequence``. Local cancel and unexpected step
    errors emit ``ECANCELED`` at ``_next_index``. Uring handles do not carry
    this field; they use the reverse ``Completion`` sequence instead.
    """

    __slots__ = ("_next_index",)

    def __init__(
        self,
        result_callback: Callable[[MultishotDelivery], object] | None = None,
        *,
        base_sequence: int = 0,
    ) -> None:
        super().__init__(result_callback)
        self._next_index = base_sequence
