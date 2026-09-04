"""Socket configuration helpers shared across IO backends."""

from __future__ import annotations

import errno
import os
import socket
import struct

__all__ = [
    "ACCEPT_RETRY_DELAY",
    "abortive_close",
    "configure_scheduler_socket",
    "is_accept_resource_error",
    "is_soft_accept_errno",
    "is_soft_accept_error",
    "socket_from_uring_fd",
]

_LINGER_ABORT = struct.pack("ii", 1, 0)

# Transient accept failures. The proactor delivers them as ordinary terminal
# OSError on the IOWaiter. StreamServer (or any accept loop) decides whether
# to ignore, pause, or die. Same split as asyncio start_serving:
# ECONNABORTED/EPROTO skip that client; EMFILE/ENFILE/ENOBUFS/ENOMEM pause
# because the listen fd often stays readable. Hard errors (EBADF, EINVAL, …)
# are not in this set.
ACCEPT_RETRY_DELAY = 1.0
_SOFT_ACCEPT_ERRNOS: frozenset[int] = frozenset(
    {
        errno.EMFILE,
        errno.ENFILE,
        errno.ECONNABORTED,
        getattr(errno, "EPROTO", -1),
        getattr(errno, "ENOBUFS", -1),
        getattr(errno, "ENOMEM", -1),
    }
    - {-1}
)
_ACCEPT_RESOURCE_ERRNOS: frozenset[int] = frozenset(
    {
        errno.EMFILE,
        errno.ENFILE,
        getattr(errno, "ENOBUFS", -1),
        getattr(errno, "ENOMEM", -1),
    }
    - {-1}
)


def is_soft_accept_errno(err: int) -> bool:
    """Return True when ``err`` is a transient accept failure (re-arm friendly)."""

    return err in _SOFT_ACCEPT_ERRNOS


def is_soft_accept_error(exc: BaseException) -> bool:
    """Return True when ``exc`` is a transient accept ``OSError`` (loop may retry)."""

    return isinstance(exc, OSError) and exc.errno is not None and is_soft_accept_errno(exc.errno)


def is_accept_resource_error(exc: BaseException) -> bool:
    """Return True when ``exc`` is fd/memory pressure (pause before re-arm)."""

    return (
        isinstance(exc, OSError) and exc.errno is not None and exc.errno in _ACCEPT_RESOURCE_ERRNOS
    )


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
