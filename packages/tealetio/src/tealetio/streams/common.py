"""Shared stream helpers and scheduler IO resolution."""

from __future__ import annotations

from typing import TypeVar

from ..io_manager import IO_UNSUPPORTED_ERROR, SELECTOR_IO_UNSUPPORTED_ERROR, ProactorIOManager, SupportsProactorIO
from ..scheduler import BaseScheduler
from ..stream_util import run_coro, writer_extra_info
from .constants import DEFAULT_LIMIT

T = TypeVar("T")

__all__ = [
    "DEFAULT_LIMIT",
    "require_proactor_io",
    "resolve_scheduler",
    "run_coro",
    "writer_extra_info",
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
