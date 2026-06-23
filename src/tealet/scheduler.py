"""Compatibility shim for :mod:`tealetio.scheduler`."""

import sys

from tealetio import scheduler as _scheduler

sys.modules[__name__] = _scheduler
