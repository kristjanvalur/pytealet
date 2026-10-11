"""Install ``TealetSocket`` on the stdlib ``socket`` module.

``patch`` must run before ``from socket import socket``. Afterwards
``socket._real_socket`` is a snapshot taken before the replacements.
``real_socket_module()`` reads that snapshot so wakeup sockets and DNS stay
on the real constructors.

``import tealetio`` does not call ``patch``.
"""

from __future__ import annotations

import socket as socket_mod
import types
from typing import Any

from ..sockets import (
    TealetSocket,
    create_connection,
    create_server,
    fromfd,
    getaddrinfo,
    gethostbyaddr,
    gethostbyname,
    getnameinfo,
    socketpair,
)

_REPLACED = (
    "socket",
    "socketpair",
    "fromfd",
    "create_connection",
    "create_server",
    "getaddrinfo",
    "getnameinfo",
    "gethostbyname",
    "gethostbyaddr",
)

_VALUES = {
    "socket": TealetSocket,
    "socketpair": socketpair,
    "fromfd": fromfd,
    "create_connection": create_connection,
    "create_server": create_server,
    "getaddrinfo": getaddrinfo,
    "getnameinfo": getnameinfo,
    "gethostbyname": gethostbyname,
    "gethostbyaddr": gethostbyaddr,
}


def _snapshot(module: types.ModuleType) -> types.ModuleType:
    snap = types.ModuleType(module.__name__)
    snap.__dict__.update(module.__dict__)
    return snap


def patch() -> None:
    """Replace socket constructors and DNS helpers. Idempotent."""

    target: Any = socket_mod
    if not hasattr(target, "_real_socket"):
        target._real_socket = _snapshot(socket_mod)
    for name, value in _VALUES.items():
        setattr(target, name, value)
    if hasattr(target, "SocketType"):
        target.SocketType = TealetSocket


def unpatch() -> None:
    """Restore names replaced by ``patch`` and drop ``_real_socket``."""

    real = getattr(socket_mod, "_real_socket", None)
    if real is None:
        return
    target: Any = socket_mod
    for name in _REPLACED:
        if hasattr(real, name):
            setattr(target, name, getattr(real, name))
    if hasattr(target, "SocketType"):
        if hasattr(real, "SocketType"):
            target.SocketType = real.SocketType
        else:
            delattr(target, "SocketType")
    delattr(target, "_real_socket")
