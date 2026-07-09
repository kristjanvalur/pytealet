"""Socket configuration helpers shared across IO backends."""

from __future__ import annotations

import os
import socket
import struct

__all__ = ["abortive_close", "configure_scheduler_socket", "socket_from_uring_fd"]

_LINGER_ABORT = struct.pack("ii", 1, 0)


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


def abortive_close(sock: socket.socket) -> None:
    """Abortively close an accepted connection we are dropping.

    Uses ``SO_LINGER`` with zero timeout so ``close()`` does not wait on unsent
    data. Safe to call on an already-closed socket.
    """

    try:
        if sock.fileno() != -1:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, _LINGER_ABORT)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass
