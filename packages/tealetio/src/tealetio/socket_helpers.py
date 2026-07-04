"""Socket configuration helpers shared across IO backends."""

from __future__ import annotations

import os
import socket

__all__ = ["configure_scheduler_socket"]


def configure_scheduler_socket(sock: socket.socket) -> socket.socket:
    """Apply the scheduler socket contract: non-blocking and close-on-exec."""

    sock.setblocking(False)
    os.set_inheritable(sock.fileno(), False)
    return sock
