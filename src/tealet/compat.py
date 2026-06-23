"""Compatibility shim for :mod:`tealetio.compat`."""

import sys

from tealetio import compat as _compat

sys.modules[__name__] = _compat
