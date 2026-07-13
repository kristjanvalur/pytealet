from __future__ import annotations

import socket
import threading
from collections import deque
from typing import TYPE_CHECKING, Any

from .io_waiter import IOWaiter
from .locks import ThreadsafeEvent
from .types import SocketSendBuffer

if TYPE_CHECKING:
    from .io_manager import ProactorIOManager


class SendBuffer:
    """Ordered outbound queue bridging ``sock_sendall`` callbacks and ``drain()``.

    At most one send operation is active per buffer. Completions may arrive on a
    proactor worker thread; ``drain()`` blocks on the scheduler thread via
    ``ThreadsafeEvent`` until the queue is empty and the active leg has finished.
    """

    def __init__(
        self,
        *,
        sock: socket.socket,
        io: ProactorIOManager,
        scheduler: Any = None,
    ) -> None:
        self._sock = sock
        self._io = io
        self._lock = threading.Lock()
        self._event = ThreadsafeEvent(scheduler)
        self._pending: deque[bytes] = deque()
        self._active = False
        self._active_waiter: IOWaiter[None] | None = None
        self._send_error: BaseException | None = None
        self._closed = False

    def write(self, data: SocketSendBuffer) -> None:
        """Queue one buffer for transmission in FIFO order."""

        if not data:
            return
        chunk = bytes(data)
        with self._lock:
            if self._closed:
                raise RuntimeError("SendBuffer is closed")
            if self._send_error is not None:
                raise self._send_error
            self._pending.append(chunk)
            if self._active:
                return
            self._active = True
            chunk_to_send = self._pending.popleft()
        try:
            self._submit(chunk_to_send)
        except BaseException as exc:
            with self._lock:
                self._active = False
                self._send_error = exc
            raise

    def drain(self) -> None:
        """Block until all queued data has been sent."""

        while True:
            with self._lock:
                if self._send_error is not None:
                    raise self._send_error
                if not self._active and not self._pending:
                    return
                self._event.clear()
            self._event.swait()

    def close(self) -> None:
        """Reject further ``write()`` calls; queued data may still be drained."""

        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._event.set()

    @property
    def closed(self) -> bool:
        return self._closed

    def _submit(self, chunk: bytes) -> None:
        waiter = self._io.sock_sendall(self._sock, chunk)
        self._active_waiter = waiter
        waiter.add_done_callback(self._on_leg_complete)

    def _on_leg_complete(self) -> None:
        notify = False
        next_chunk: bytes | None = None
        waiter = self._active_waiter
        assert waiter is not None
        self._active_waiter = None
        leg_error: BaseException | None = None
        try:
            waiter.wait()
        except BaseException as exc:
            leg_error = exc
        with self._lock:
            if leg_error is not None:
                self._send_error = leg_error
            if self._send_error is None and self._pending and not self._closed:
                next_chunk = self._pending.popleft()
            else:
                self._active = False
                if not self._pending:
                    notify = True
        if next_chunk is not None:
            self._submit(next_chunk)
        if notify:
            self._event.set()