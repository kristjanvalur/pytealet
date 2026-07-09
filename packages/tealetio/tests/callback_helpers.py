"""Test-only continuous callback composition helpers."""

from __future__ import annotations

import socket
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from tealetio.continuous_callbacks import chain_suboperation
from tealetio.operations import ContinuousOperation, Operation
from tealetio.recv_iter import RECV_MANY_BUFFER_PRESSURE, _RecvManyResult

if TYPE_CHECKING:
    from tealetio.proactor import Proactor


def recv_many_echo_delivery(
    proactor: Proactor,
    parent: ContinuousOperation[_RecvManyResult],
    deliver: Callable[[_RecvManyResult], object],
    *,
    fire_and_forget: bool = False,
) -> Callable[[_RecvManyResult], None]:
    """Echo each ``recv_many`` chunk via a nested send operation (tests only).

    By default ``deliver`` runs only after the nested send succeeds. When
    ``fire_and_forget`` is true, the send is submitted and tracked for
    cancellation, but ``deliver`` runs immediately without waiting for echo
    completion. Send failures are always swallowed; ``recv_many`` keeps running.
    """

    sock = cast(socket.socket, parent.fileobj)

    def on_result(result: _RecvManyResult) -> None:
        index, payload = result
        if index == RECV_MANY_BUFFER_PRESSURE:
            deliver(result)
            return
        if isinstance(payload, memoryview) and payload:
            send_op = proactor.send(sock, payload.tobytes())
            if fire_and_forget:

                def on_send_complete(_op: Operation[Any]) -> None:
                    return

                chain_suboperation(parent, send_op, on_send_complete)
                deliver(result)
                return

            def on_send_complete(op: Operation[Any]) -> None:
                if op.exception() is not None:
                    return
                deliver(result)

            chain_suboperation(parent, send_op, on_send_complete)
            return
        deliver(result)

    return on_result