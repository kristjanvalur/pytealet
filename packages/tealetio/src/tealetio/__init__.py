"""Scheduler and asyncio compatibility layer for tealet."""

# Import order matters: scheduler must finish before proactor/asyncio so the
# package graph does not re-enter a partially initialised proactor.
# ruff: isort: off
from . import locks as locks
from . import runner as runner
from . import scheduler as scheduler
from . import selector as selector
from . import tasks as tasks
from . import proactor as proactor
from . import asyncio as asyncio
from . import streams as streams
from .locks import *
from .tasks import *
from .scheduler import *
from .runner import *
from .selector import *
from .proactor import *
from .asyncio import *
from .streams import *
# ruff: isort: on

__all__ = (
    locks.__all__
    + tasks.__all__
    + scheduler.__all__
    + runner.__all__
    + selector.__all__
    + proactor.__all__
    + asyncio.__all__
    + streams.__all__
)
