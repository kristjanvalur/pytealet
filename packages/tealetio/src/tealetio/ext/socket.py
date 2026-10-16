"""Tealet stand-in for the stdlib ``socket`` module.

``import socket as real_socket`` captures the stdlib module. ``patch``
installs this module as ``sys.modules["socket"]``. Names this module does
not define are read from ``real_socket``. ``patch``, ``unpatch``, and
``real_socket`` stay on the module.

Call ``patch`` before any other module imports ``socket``. It does not
edit the stdlib module. A module that already imported ``socket`` keeps
that module. ``import tealetio`` does not call ``patch``.
"""

from __future__ import annotations

import socket as real_socket
import sys

from ..sockets import TealetSocket as socket
from ..sockets import (
    connect_tcp,
    create_connection,
    create_server,
    fromfd,
    getaddrinfo,
    gethostbyaddr,
    gethostbyname,
    getnameinfo,
    socketpair,
)

SocketType = socket

# Names defined here. ``__all__`` then gains the stdlib module's names so
# ``from socket import *`` still exports constants such as ``AF_INET``.
__all__ = [
    "SocketType",
    "connect_tcp",
    "create_connection",
    "create_server",
    "fromfd",
    "getaddrinfo",
    "gethostbyaddr",
    "gethostbyname",
    "getnameinfo",
    "socket",
    "socketpair",
]
_OWN = tuple(__all__)
for _name in real_socket.__all__:
    if _name not in __all__:
        __all__.append(_name)


def patch() -> None:
    """Install this module as ``sys.modules["socket"]``. Idempotent."""

    if sys.modules.get("socket") is sys.modules[__name__]:
        return
    sys.modules["socket"] = sys.modules[__name__]


def unpatch() -> None:
    """Put the stdlib module back into ``sys.modules["socket"]``."""

    if sys.modules.get("socket") is sys.modules[__name__]:
        sys.modules["socket"] = real_socket


def __getattr__(name: str) -> object:
    return getattr(real_socket, name)


def __dir__() -> list[str]:
    return sorted(set(dir(real_socket)) | set(_OWN) | {"patch", "real_socket", "unpatch"})
