"""Install the tealet socket module.

Call ``patch`` before any other module imports ``socket``. It replaces
``sys.modules["socket"]``. It does not edit the stdlib module. A module
that already imported ``socket`` keeps that module.

``import tealetio`` does not call ``patch``.
"""

from __future__ import annotations

import sys


def patch() -> None:
    """Install the tealet socket module. Idempotent.

    Importing the stand-in loads tealetio's socket stack while
    ``sys.modules["socket"]`` is still the stdlib module, so those modules
    keep the real one.
    """

    from . import _socket

    if sys.modules.get("socket") is _socket:
        return
    sys.modules["socket"] = _socket


def unpatch() -> None:
    """Put the stdlib module back into ``sys.modules["socket"]``."""

    from . import _socket

    if sys.modules.get("socket") is _socket:
        sys.modules["socket"] = _socket.real_socket
