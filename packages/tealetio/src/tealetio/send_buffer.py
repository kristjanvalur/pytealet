from __future__ import annotations

import socket
from collections import deque
from typing import TYPE_CHECKING, Any

from .io_waiter import IOWaiter
from .locks import ThreadsafeCondition
from .types import SocketSendBuffer

if TYPE_CHECKING:
    from .io_manager import ProactorIOManager

_DEFAULT_HIGH_WATER = 64 * 1024


class SendBuffer:
    """Ordered outbound queue bridging ``sock_sendall`` callbacks and ``drain()``.

    At most one send operation is active per buffer. Completions may arrive on a
    proactor worker thread; ``drain()`` and ``flush()`` block on the scheduler
    thread via ``ThreadsafeCondition``.

    ``drain()`` follows asyncio transport semantics: return immediately while
    ``pending_bytes <= high_water``; otherwise block until ``pending_bytes <=
    low_water``. ``flush()`` blocks until the queue is completely empty.
    """

    def __init__(
        self,
        *,
        sock: socket.socket,
        io: ProactorIOManager,
        scheduler: Any = None,
        high_water: int | None = None,
        low_water: int | None = None,
    ) -> None:
        self._sock = sock
        self._io = io
        self._cond = ThreadsafeCondition(scheduler=scheduler)
        self._pending: deque[bytes] = deque()
        self._pending_bytes = 0
        self._in_flight_bytes = 0
        self._active = False
        self._active_waiter: IOWaiter[None] | None = None
        self._send_error: BaseException | None = None
        self._closed = False
        self._eof_pending = False
        self._write_eof_done = False
        self._set_write_buffer_limits(high=high_water, low=low_water)

    @property
    def pending_bytes(self) -> int:
        """Approximate bytes queued or in the active ``sock_sendall`` leg."""

        return self._pending_bytes + self._in_flight_bytes

    def get_write_buffer_limits(self) -> tuple[int, int]:
        """Return ``(low_water, high_water)``."""

        return (self._low_water, self._high_water)

    def set_write_buffer_limits(self, high: int | None = None, low: int | None = None) -> None:
        """Configure asyncio-style drain watermarks."""

        with self._cond:
            self._set_write_buffer_limits(high=high, low=low)
            self._cond.notify_all()

    def write(self, data: SocketSendBuffer) -> None:
        """Queue one buffer for transmission in FIFO order."""

        if not data:
            return
        chunk = bytes(data)
        chunk_len = len(chunk)
        with self._cond:
            if self._closed:
                raise RuntimeError("SendBuffer is closed")
            if self._eof_pending:
                raise RuntimeError("cannot write() after write_eof()")
            if self._send_error is not None:
                raise self._send_error
            self._pending.append(chunk)
            self._pending_bytes += chunk_len
            if self._active:
                return
            self._active = True
            chunk_to_send = self._pending.popleft()
            self._pending_bytes -= len(chunk_to_send)
            self._in_flight_bytes = len(chunk_to_send)
        try:
            self._submit(chunk_to_send)
        except BaseException as exc:
            with self._cond:
                self._active = False
                self._in_flight_bytes = 0
                self._send_error = exc
                self._cond.notify_all()
            raise

    def drain(self) -> None:
        """Block only when ``pending_bytes`` exceeds ``high_water``.

        When blocked, wait until ``pending_bytes <= low_water``. Unlike
        ``flush()``, some data may remain queued after ``drain()`` returns.
        """

        with self._cond:
            if self._send_error is not None:
                raise self._send_error
            if self._pending_bytes + self._in_flight_bytes <= self._high_water:
                return
            self._cond.swait_for(self._drain_ready)

    def flush(self) -> None:
        """Block until all queued data has been sent."""

        with self._cond:
            if self._send_error is not None:
                raise self._send_error
            self._cond.swait_for(self._flush_ready)

    def write_eof(self) -> None:
        """Mark end-of-write; ``SHUT_WR`` is deferred until queued data is sent."""

        with self._cond:
            if self._closed:
                raise RuntimeError("SendBuffer is closed")
            if self._send_error is not None:
                raise self._send_error
            if self._eof_pending:
                return
            self._eof_pending = True
            self._maybe_shutdown()
            self._cond.notify_all()

    def close(self) -> None:
        """Reject further ``write()`` calls; queued data may still be flushed."""

        with self._cond:
            if self._closed:
                return
            self._closed = True
            self._cond.notify_all()

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def eof_pending(self) -> bool:
        return self._eof_pending

    @property
    def write_eof_done(self) -> bool:
        return self._write_eof_done

    def _drain_ready(self) -> bool:
        if self._send_error is not None:
            raise self._send_error
        return self._pending_bytes + self._in_flight_bytes <= self._low_water

    def _flush_ready(self) -> bool:
        if self._send_error is not None:
            raise self._send_error
        return not self._active and self._pending_bytes == 0 and self._in_flight_bytes == 0

    def _set_write_buffer_limits(self, *, high: int | None, low: int | None) -> None:
        if high is None:
            if low is None:
                high = _DEFAULT_HIGH_WATER
            else:
                high = 4 * low
        if low is None:
            low = high // 4
        if high < low or low < 0:
            raise ValueError(f"high ({high!r}) must be >= low ({low!r}) must be >= 0")
        self._high_water = high
        self._low_water = low

    def _maybe_shutdown(self) -> None:
        """Shut down the write side when EOF is pending and the queue is idle.

        Caller must hold ``self._cond``.
        """

        if self._write_eof_done or not self._eof_pending:
            return
        if self._active or self._pending_bytes or self._in_flight_bytes:
            return
        self._write_eof_done = True
        self._io.sock_shutdown(self._sock, socket.SHUT_WR).forget()

    def _submit(self, chunk: bytes) -> None:
        waiter = self._io.sock_sendall(self._sock, chunk)
        self._active_waiter = waiter
        waiter.add_done_callback(self._on_leg_complete)

    def _on_leg_complete(self) -> None:
        next_chunk: bytes | None = None
        waiter = self._active_waiter
        assert waiter is not None
        self._active_waiter = None
        leg_error: BaseException | None = None
        try:
            waiter.wait()
        except BaseException as exc:
            leg_error = exc
        with self._cond:
            self._in_flight_bytes = 0
            if leg_error is not None:
                self._send_error = leg_error
            if self._send_error is None and self._pending and not self._closed:
                next_chunk = self._pending.popleft()
                self._pending_bytes -= len(next_chunk)
                self._in_flight_bytes = len(next_chunk)
            else:
                self._active = False
                self._maybe_shutdown()
            self._cond.notify_all()
        if next_chunk is not None:
            self._submit(next_chunk)
