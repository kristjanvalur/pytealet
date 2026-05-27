"""Compatibility re-export for greenlet._greenlet."""

from tealet.greenlet import _greenlet as _impl
from tealet.greenlet._greenlet import *

if hasattr(_impl, "UnswitchableGreenlet"):
    UnswitchableGreenlet = _impl.UnswitchableGreenlet
