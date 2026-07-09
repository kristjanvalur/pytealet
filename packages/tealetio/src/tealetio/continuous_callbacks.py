"""Result pipelines for continuous proactor operations."""

from __future__ import annotations

import socket
from collections.abc import Callable

from .recv_iter import RECV_MANY_BUFFER_PRESSURE, _RecvManyResult


def recv_many_echo_pipeline(sock: socket.socket) -> Callable[[_RecvManyResult], _RecvManyResult]:
    """Echo each received data chunk back on ``sock`` before client delivery."""

    def pipeline(result: _RecvManyResult) -> _RecvManyResult:
        index, payload = result
        if index == RECV_MANY_BUFFER_PRESSURE:
            return result
        if isinstance(payload, memoryview) and payload:
            sock.send(payload.tobytes())
        return result

    return pipeline
