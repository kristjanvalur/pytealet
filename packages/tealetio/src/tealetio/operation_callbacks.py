"""Suboperation chaining for continuous proactor callbacks."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from .operations import Operation

T = TypeVar("T")


def chain_suboperation(
    parent: Operation[Any],
    spawn: Callable[[], Operation[T]],
    on_complete: Callable[[Operation[T]], object],
) -> bool:
    """Spawn a child under ``parent._lock`` and run ``on_complete`` on completion.

    Serialises against ``parent.cancel()`` so an in-flight backend submit
    cannot outrun child registration. ``spawn()`` runs while holding
    ``parent._lock``, which can defer another thread's ``cancel()`` until a
    synchronous backend path (for example ``AF_UNIX`` connect) returns from
    ``spawn()``.

    The done callback is registered after releasing ``parent._lock`` so a child
    that completes synchronously does not deadlock when ``on_complete`` finishes
    the parent.

    Returns ``False`` only when the parent is already ``_done``. Callers need not
    finish the parent on ``False``. If the child was spawned, a failed attach
    cancels it.

    If the child is already done when the callback is registered,
    ``add_done_callback`` runs ``on_complete`` immediately.
    """

    def complete(op: Operation[T]) -> None:
        try:
            on_complete(op)
        except BaseException as exc:
            parent.complete_error(exc)
        finally:
            parent.detach_suboperation(op)

    with parent._lock:
        if parent._done:
            return False
        child = spawn()
        if parent._done:
            child.cancel()
            return False
        parent._active_suboperations.add(child)

    child.add_done_callback(complete)
    return True