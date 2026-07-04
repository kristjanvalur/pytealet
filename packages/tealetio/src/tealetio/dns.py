"""Scheduler-backed name resolution helpers modelled after asyncio."""

from __future__ import annotations

import socket
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .scheduler import BaseScheduler

_HAS_IPV6 = hasattr(socket, "AF_INET6")


def ipaddr_info(
    host: str | bytes | None,
    port: str | int | bytes | None,
    family: int,
    type: int,
    proto: int,
    flowinfo: int = 0,
    scopeid: int = 0,
) -> tuple[int, int, int, str, tuple[Any, ...]] | None:
    """Return a single addrinfo tuple when ``host`` is already a literal IP."""

    if not hasattr(socket, "inet_pton"):
        return None

    if proto not in {0, socket.IPPROTO_TCP, socket.IPPROTO_UDP} or host is None:
        return None

    if type == socket.SOCK_STREAM:
        proto = socket.IPPROTO_TCP
    elif type == socket.SOCK_DGRAM:
        proto = socket.IPPROTO_UDP
    else:
        return None

    if port is None:
        port = 0
    elif isinstance(port, bytes) and port == b"":
        port = 0
    elif isinstance(port, str) and port == "":
        port = 0
    else:
        try:
            port = int(port)
        except (TypeError, ValueError):
            return None

    if family == socket.AF_UNSPEC:
        afs = [socket.AF_INET]
        if _HAS_IPV6:
            afs.append(socket.AF_INET6)
    else:
        afs = [family]

    if isinstance(host, bytes):
        try:
            host = host.decode("idna")
        except UnicodeDecodeError:
            return None
    if "%" in host:
        return None

    for af in afs:
        try:
            socket.inet_pton(af, host)
            if _HAS_IPV6 and af == socket.AF_INET6:
                return af, type, proto, "", (host, port, flowinfo, scopeid)
            return af, type, proto, "", (host, port)
        except OSError:
            pass

    return None


def ensure_resolved(
    scheduler: BaseScheduler,
    address: tuple[Any, ...],
    *,
    family: int = 0,
    type: int = socket.SOCK_STREAM,
    proto: int = 0,
    flags: int = 0,
) -> list[tuple[int, int, int, str, tuple[Any, ...]]]:
    """Resolve ``address`` through the scheduler without blocking other tealets."""

    host = address[0]
    port = address[1]
    flowinfo = address[2] if len(address) > 2 else 0
    scopeid = address[3] if len(address) > 3 else 0
    info = ipaddr_info(host, port, family, type, proto, flowinfo, scopeid)
    if info is not None:
        return [info]
    return scheduler.getaddrinfo(host, port, family=family, type=type, proto=proto, flags=flags)
