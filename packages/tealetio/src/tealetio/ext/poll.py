"""Install tealet ``poll``, ``select``, and ``epoll`` on the stdlib ``select`` module.

``selectors.PollSelector`` and ``EpollSelector`` capture ``select.poll`` and
``select.epoll`` in the class body. ``patch`` points those attributes at the
replacements. An existing selector instance keeps the object it already built.

``kqueue`` is left unchanged. ``import tealetio`` does not call ``patch``.
Probes keep using ``select._real_select`` via ``real_select_module()``.
"""

from __future__ import annotations

import select as select_mod
import selectors
import types
from typing import Any

from .. import readiness

_NAMES = ("poll", "select", "epoll")


def _snapshot(module: types.ModuleType) -> types.ModuleType:
    snap = types.ModuleType(module.__name__)
    snap.__dict__.update(module.__dict__)
    return snap


def _retarget_selectors() -> None:
    # A Python function stored on the class binds as a method. ``staticmethod``
    # keeps ``PollSelector()`` calling ``poll()`` with no selector argument.
    # The epoll replacement is a class, so it does not bind.
    poll_cls = getattr(selectors, "PollSelector", None)
    if poll_cls is not None:
        poll_factory = select_mod.poll
        if isinstance(poll_factory, types.FunctionType):
            poll_cls._selector_cls = staticmethod(poll_factory)
        else:
            poll_cls._selector_cls = poll_factory
    epoll_cls = getattr(selectors, "EpollSelector", None)
    if epoll_cls is not None and hasattr(select_mod, "epoll"):
        epoll_cls._selector_cls = select_mod.epoll


def patch() -> None:
    """Replace ``poll``, ``select``, and ``epoll``. Idempotent."""

    target: Any = select_mod
    if not hasattr(target, "_real_select"):
        target._real_select = _snapshot(select_mod)
    target.poll = readiness.poll
    target.select = readiness.select
    if hasattr(target, "epoll"):
        target.epoll = readiness.Epoll
    _retarget_selectors()


def unpatch() -> None:
    """Restore ``poll``, ``select``, and ``epoll``, then retarget selectors."""

    real = getattr(select_mod, "_real_select", None)
    if real is None:
        return
    target: Any = select_mod
    for name in _NAMES:
        if hasattr(real, name):
            setattr(target, name, getattr(real, name))
    delattr(target, "_real_select")
    _retarget_selectors()
