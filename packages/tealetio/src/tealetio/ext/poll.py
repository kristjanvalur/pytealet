"""Tealet stand-in for the stdlib ``select`` module.

``import select as real_select`` captures the stdlib module. ``patch``
installs this module as ``sys.modules["select"]``. ``poll``, ``select``,
and, where the platform has it, ``epoll`` are the parking versions. Other
names are read from ``real_select``. ``patch``, ``unpatch``, and
``real_select`` stay on the module. ``kqueue`` is one of those forwarded
names.

Call ``patch`` before any other module imports ``select``. It does not
edit the stdlib module. A module that already imported ``select`` keeps
that module. ``selectors`` reads ``select.poll`` and ``select.epoll``
when it is imported, so import ``selectors`` after ``patch``.
``import tealetio`` does not call ``patch``.
"""

from __future__ import annotations

import select as real_select
import sys

from ..readiness import Epoll, poll, select

# Names defined here. ``__all__`` then gains the stdlib module's public
# names. The select module has no ``__all__``, so ``from select import *``
# would otherwise miss constants such as ``POLLIN``.
__all__ = [
    "poll",
    "select",
]
_OWN = ["poll", "select"]
if hasattr(real_select, "epoll"):
    epoll = Epoll
    __all__.append("epoll")
    _OWN.append("epoll")
for _name in dir(real_select):
    if not _name.startswith("_") and _name not in __all__:
        __all__.append(_name)


def patch() -> None:
    """Install this module as ``sys.modules["select"]``. Idempotent."""

    if sys.modules.get("select") is sys.modules[__name__]:
        return
    sys.modules["select"] = sys.modules[__name__]


def unpatch() -> None:
    """Put the stdlib module back into ``sys.modules["select"]``."""

    if sys.modules.get("select") is sys.modules[__name__]:
        sys.modules["select"] = real_select


def __getattr__(name: str) -> object:
    return getattr(real_select, name)


def __dir__() -> list[str]:
    return sorted(set(dir(real_select)) | set(_OWN) | {"patch", "real_select", "unpatch"})
