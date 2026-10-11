"""Qt slots that start a tealet and return."""

from __future__ import annotations

import functools
import inspect
import sys
from collections.abc import Callable

from tealetio import CancelledError, get_scheduler

from ._qt import Slot

# tasks a slot started and nobody else kept. dropped when they finish.
_slot_tasks: set = set()


def _bind_slot_args(fn: Callable[..., object], args: tuple, kwargs: dict) -> tuple:
    """Drop trailing positional arguments until ``fn`` accepts the call.

    Qt signal connections pass arguments the slot does not declare. A
    ``clicked(bool)`` signal connected to a zero-argument method is the usual
    case.
    """

    signature = inspect.signature(fn)
    positional = list(args)
    while True:
        try:
            signature.bind(*positional, **kwargs)
        except TypeError:
            if not positional:
                raise
            positional.pop()
            continue
        return tuple(positional)


def _finish_slot_task(task) -> None:
    _slot_tasks.discard(task)
    try:
        exc = task.exception()
    except CancelledError:
        return
    if exc is not None:
        sys.excepthook(type(exc), exc, exc.__traceback__)


def tealetSlot(*slot_types, **slot_kwargs):
    """Run the decorated function as a tealet when Qt calls it.

    The wrapper returns as soon as the task is spawned. The body runs on the
    next ``turn``, so it may ``sleep``, ``wait_signal``, and ``wait_dialog``.
    Qt ignores the wrapper's return value; it is the spawned task, for the
    caller that invokes the wrapper directly.

    ``slot_types`` are passed to the binding's ``Slot`` decorator, the same
    way ``qasync.asyncSlot`` selects an overload. An exception from the body
    is reported with ``sys.excepthook`` when the task finishes.
    """

    def decorator(fn):
        @Slot(*slot_types, **slot_kwargs)
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            positional = _bind_slot_args(fn, args, kwargs)
            # snapshot: the lambda runs on a later turn, after this frame is gone
            call_kwargs = dict(kwargs)
            task = get_scheduler().spawn(lambda: fn(*positional, **call_kwargs))
            _slot_tasks.add(task)
            task.add_done_callback(_finish_slot_task)
            return task

        return wrapper

    return decorator
