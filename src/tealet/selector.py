"""Compatibility shim for :mod:`tealetio.selector`."""

import sys

from tealetio import selector as _selector

sys.modules[__name__] = _selector
