"""Compatibility shim for :mod:`tealetio.tasks`."""

import sys

from tealetio import tasks as _tasks

sys.modules[__name__] = _tasks
