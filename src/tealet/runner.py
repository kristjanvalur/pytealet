"""Compatibility shim for :mod:`tealetio.runner`."""

import sys

from tealetio import runner as _runner

sys.modules[__name__] = _runner
