"""Compatibility package that exposes tealet.greenlet as greenlet."""

from tealet.greenlet import GreenletExit
from tealet.greenlet import error
from tealet.greenlet import getcurrent
from tealet.greenlet import greenlet

# Re-export the local compatibility submodule as greenlet._greenlet.
from . import _greenlet

__all__ = [
    "greenlet",
    "getcurrent",
    "error",
    "GreenletExit",
    "_greenlet",
]
