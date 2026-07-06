"""Socket configuration helpers shared across IO backends."""

from __future__ import annotations

import os
import socket

__all__ = ["configure_scheduler_socket", "socket_from_uring_fd"]


def socket_from_uring_fd(fd: int) -> socket.socket:
    """Wrap an io_uring-returned socket fd for scheduler use.

    The fd is expected to already be non-blocking and close-on-exec from
    ``SOCK_NONBLOCK | SOCK_CLOEXEC`` on the uring submission.
    ``socket.socket(fileno=...)`` does not import those flags into
    ``getblocking()``; ``setblocking(False)`` syncs the wrapper without
    changing fd flags when they are already set.
    """

    sock = socket.socket(fileno=fd)
    sock.setblocking(False)
    return sock


def configure_scheduler_socket(sock: socket.socket) -> socket.socket:
    """Apply the scheduler socket contract: non-blocking and close-on-exec."""

    sock.setblocking(False)
    os.set_inheritable(sock.fileno(), False)
    return sock
