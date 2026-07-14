"""Scheduler resolution and proactor IO access for stream entry points."""

from __future__ import annotations

from ..io_manager import IO_UNSUPPORTED_ERROR, SELECTOR_IO_UNSUPPORTED_ERROR, ProactorIOManager, SupportsProactorIO
from ..scheduler import BaseScheduler

__all__ = [
    "require_proactor_io",
    "resolve_scheduler",
]


def resolve_scheduler(scheduler: BaseScheduler | None) -> BaseScheduler:
    if scheduler is not None:
        return scheduler
    from ..scheduler import get_running_scheduler

    return get_running_scheduler()


def require_proactor_io(scheduler: BaseScheduler) -> ProactorIOManager:
    """Return ``scheduler.io`` for proactor schedulers or raise with a targeted message."""

    from ..proactor import ProactorScheduler
    from ..selector import SelectorScheduler

    if isinstance(scheduler, ProactorScheduler):
        proactor_scheduler: SupportsProactorIO = scheduler
        return proactor_scheduler.io
    if isinstance(scheduler, SelectorScheduler):
        raise RuntimeError(SELECTOR_IO_UNSUPPORTED_ERROR)
    raise RuntimeError(IO_UNSUPPORTED_ERROR)
