"""Compatibility shim for :mod:`tealetio.locks`."""

import sys

from tealetio import locks as _locks

sys.modules[__name__] = _locks
