"""Tealet stand-in for the stdlib ``socket`` module.

``import socket as real_socket`` captures the stdlib module. ``patch`` then
installs this module as ``sys.modules["socket"]``. Names this module does not
define are read from ``real_socket``.
"""

from __future__ import annotations

import socket as real_socket

from ..sockets import TealetSocket as socket
from ..sockets import (
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


def __getattr__(name: str) -> object:
    return getattr(real_socket, name)


def __dir__() -> list[str]:
    return sorted(set(dir(real_socket)) | set(_OWN))
