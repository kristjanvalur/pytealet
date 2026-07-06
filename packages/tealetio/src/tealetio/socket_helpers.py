"""Socket configuration helpers shared across IO backends."""

from __future__ import annotations

import os
import socket

__all__ = ["configure_scheduler_socket", "socket_from_uring_fd"]


def socket_from_uring_fd(fd: int) -> socket.socket:
    """Wrap an io_uring-returned socket fd for scheduler use.

    ``IORING_OP_SOCKET`` submissions pass ``flags=0``; the scheduler contract
    (non-blocking and close-on-exec) is applied here after the fd is returned.
    """

    return configure_scheduler_socket(socket.socket(fileno=fd))


def configure_scheduler_socket(sock: socket.socket) -> socket.socket:
    """Apply the scheduler socket contract: non-blocking and close-on-exec."""

    sock.setblocking(False)
    os.set_inheritable(sock.fileno(), False)
    return sock
