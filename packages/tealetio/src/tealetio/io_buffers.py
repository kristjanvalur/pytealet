"""Blocking producer/consumer bridges over proactor callback IO.

``RecvIterBuffer`` and ``SendBuffer`` sit under ``scheduler.io``: they turn
continuous ``recv_many`` delivery and chained ``sock_sendall`` legs into
tealet-blocking ``take_next()`` / ``write()`` / ``drain()`` APIs. ``streams``
and other callers open them through ``ProactorIOManager`` factories.
"""

from __future__ import annotations

import errno
import socket
from collections import deque
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias, cast

from .continuous_callbacks import ReorderBuffer, marshal_to_scheduler
from .io_waiter import IOWaiter
from .locks import Condition, CrossThreadCondition
from .operations import ContinuousOperation, MultishotDelivery, Operation
from .scheduler import get_running_scheduler
from .operations import io_cancellation_error
from .types import SocketSendBuffer

if TYPE_CHECKING:
    from .io_manager import ProactorIOManager
    from .scheduler import BaseScheduler

__all__ = [
    "RECV_MANY_BUFFER_PRESSURE",
    "RecvIterBuffer",
    "SendBuffer",
    "open_recv_iter_buffer",
    "open_send_buffer",
]

# ``sock_recv_iter`` still yields this index for provided-buffer pool pressure.
RECV_MANY_BUFFER_PRESSURE = -1
_RecvManyValue = memoryview
_RecvIterYield: TypeAlias = tuple[int, memoryview]
_RecvIterReady: TypeAlias = tuple[None] | tuple[_RecvIterYield]

_DEFAULT_HIGH_WATER = 64 * 1024


class _BufGroupLike(Protocol):
    @property
    def buffer_count(self) -> int: ...

    @property
    def leased_count(self) -> int: ...


class _RecvIterProactor(Protocol):
    def recv_many(
        self,
        sock: socket.socket,
        callback: Any,
        *,
        buf_group: _BufGroupLike,
        base_sequence: int = 0,
    ) -> ContinuousOperation[_RecvManyValue]: ...

    def cancel(self, operation: Operation[Any]) -> Operation[None]: ...


def _is_enobufs_delivery(delivery: MultishotDelivery) -> bool:
    exc = delivery.exception
    return isinstance(exc, OSError) and exc.errno == errno.ENOBUFS


class RecvIterBuffer:
    """Ordered receive buffer bridging ``recv_many`` callbacks and ``sock_recv_iter``.

    Worker-thread ``recv_many`` deliveries are marshalled onto the scheduler
    thread, reordered there, and exposed to ``take_next()`` via a tealet
    ``Condition``.

    Resubmit gating uses ``buf_group.leased_count`` against the pool low-water mark
    (``leased_count < buffer_count / 2``). That tracks real uring ``BufGroup`` slots
    via ``BufView`` release and synthetic-pool leases on Python 3.12+.

    On older Python, synthetic pools skip view leases (no PEP 688), so
    ``leased_count`` does not reflect consumer-held chunks and backpressure via the
    buffer mechanism is effectively dropped there.

    After copying chunk data, call ``view.release()`` or drop the view; on Python
    3.12+ that returns leased pool slots via PEP 688.
    """

    def __init__(
        self,
        *,
        sock: socket.socket,
        buf_group: _BufGroupLike,
        proactor: _RecvIterProactor,
        scheduler: BaseScheduler | None = None,
    ) -> None:
        if scheduler is None:
            scheduler = get_running_scheduler()
        self._sock = sock
        self._buf_group = buf_group
        self._proactor = proactor
        self._scheduler = scheduler
        self._cond = Condition()
        self._reorder_buffer = ReorderBuffer(self._on_ordered_delivery, start=0)
        self._ready: deque[tuple[int, memoryview]] = deque()
        self._pressure_pending = False
        self._next_base = 0
        self._stream_done = False
        self._stream_error: BaseException | None = None
        self._current_operation: ContinuousOperation[_RecvManyValue] | None = None
        self._closed = False
        self.on_result = marshal_to_scheduler(scheduler, self._deliver)
        self._start_recv_many(base_sequence=0)

    def _start_recv_many(self, *, base_sequence: int) -> None:
        if self._closed:
            return
        operation = self._proactor.recv_many(
            self._sock,
            self.on_result,
            buf_group=self._buf_group,
            base_sequence=base_sequence,
        )
        with self._cond:
            if self._closed:
                if not operation.done():
                    self._proactor.cancel(operation)
                return
            self._current_operation = operation

    def _schedule_resubmit(self, *, base_sequence: int) -> None:
        self._next_base = base_sequence
        # leg ended; hold off on a new ``recv_many`` until queues drain and the pool is low.
        self._current_operation = None
        self._reorder_buffer.arm_next_index(base_sequence)

    def _pool_at_low_water(self) -> bool:
        """Return True when ``leased_count < buffer_count / 2`` (safe to re-submit ``recv_many``)."""

        buf_group = self._buf_group
        return buf_group.leased_count * 2 < buf_group.buffer_count

    def _signal_pressure_if_pending(self) -> bool:
        if self._pressure_pending:
            return False
        self._pressure_pending = True
        return True

    def _deliver(self, delivery: MultishotDelivery) -> None:
        with self._cond:
            closed = self._closed
        if closed:
            if not delivery.more:
                operation = delivery.operation
                if operation is not None and not operation.done():
                    operation.finish_operation(delivery)
            return
        self._reorder_buffer.deliver(delivery)

    def _on_ordered_delivery(self, delivery: MultishotDelivery) -> None:
        index = delivery.index
        finish_leg = False
        with self._cond:
            notify = False
            if _is_enobufs_delivery(delivery):
                assert index is not None
                self._schedule_resubmit(base_sequence=index)
                if self._signal_pressure_if_pending():
                    notify = True
                finish_leg = True
            elif delivery.exception is not None:
                self._stream_error = delivery.exception
                self._stream_done = True
                notify = True
                finish_leg = not delivery.more
            elif delivery.value is not None:
                assert index is not None
                data = delivery.value
                self._ready.append((index, data))
                notify = bool(self._ready) or self._reorder_buffer.pending
                if not delivery.more:
                    if data:
                        self._schedule_resubmit(base_sequence=index + 1)
                    else:
                        self._stream_done = True
                        self._stream_error = None
                    finish_leg = True
            if notify:
                self._cond.notify_all()

        if finish_leg:
            operation = delivery.operation
            if operation is not None:
                operation.finish_operation(delivery)

    def _should_resubmit(self) -> bool:
        if self._ready or self._reorder_buffer.pending:
            return False
        return self._pool_at_low_water()

    def consume_pressure_resume(self) -> None:
        """Start a fresh ``recv_many`` once the pool has drained below the low-water mark."""

        with self._cond:
            if self._closed or self._current_operation is not None or self._stream_done or not self._should_resubmit():
                return
            base_sequence = self._next_base
        self._start_recv_many(base_sequence=base_sequence)

    def _take_next_locked(self) -> _RecvIterReady | None:
        if self._pressure_pending:
            self._pressure_pending = False
            return ((RECV_MANY_BUFFER_PRESSURE, memoryview(b"")),)
        ready_item: tuple[int, memoryview] | None = None
        if self._ready:
            ready_item = self._ready.popleft()
        if ready_item is not None:
            index, chunk = ready_item
            if not chunk:
                self._stream_done = True
                self._stream_error = None
                return (None,)
            return ((index, chunk),)
        if self._stream_done:
            if self._stream_error is not None:
                raise self._stream_error
            return (None,)
        return None

    def take_next(self) -> _RecvIterYield | None:
        # resume before parking (consumer may have released a pool slot) and
        # after dispatch (leg may have ended while we held the chunk).
        self.consume_pressure_resume()
        with self._cond:
            ready = cast(_RecvIterReady, self._cond.swait_for(self._take_next_locked))
            item = ready[0]
        self.consume_pressure_resume()
        return item

    def close(self) -> None:
        """Stop iteration and cancel any in-flight ``recv_many`` leg.

        Terminal state is established here before ``proactor.cancel()`` so a
        blocked ``take_next()`` wakes even though ``on_result`` ignores
        deliveries once ``_closed`` is set.
        """

        operation: ContinuousOperation[_RecvManyValue] | None
        with self._cond:
            if self._closed:
                return
            self._closed = True
            operation = self._current_operation
            self._current_operation = None
            self._pressure_pending = False
            self._ready.clear()
            self._reorder_buffer.reset()
            if not self._stream_done:
                self._stream_error = io_cancellation_error()
                self._stream_done = True
            self._cond.notify_all()
        if operation is not None and not operation.done():
            self._proactor.cancel(operation)
            operation.finish_operation(MultishotDelivery(index=None, exception=io_cancellation_error(), more=False))


def open_recv_iter_buffer(
    sock: socket.socket,
    *,
    proactor: _RecvIterProactor,
    buf_group: _BufGroupLike,
    scheduler: BaseScheduler | None = None,
) -> RecvIterBuffer:
    """Construct a receive bridge for ``sock_recv_iter`` and stream readers."""

    return RecvIterBuffer(sock=sock, buf_group=buf_group, proactor=proactor, scheduler=scheduler)


class SendBuffer:
    """Ordered outbound queue bridging ``sock_sendall`` callbacks and ``drain()``.

    At most one send operation is active per buffer. Completions may arrive on a
    proactor worker thread; ``drain()`` and ``flush()`` block on the scheduler
    thread via ``CrossThreadCondition``.

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
        self._cond = CrossThreadCondition(scheduler=scheduler)
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
        """Approximate bytes queued or in the active ``sock_sendall`` leg.

        The counters may be read without the buffer lock, so concurrent
        completion callbacks can yield a briefly stale snapshot.
        """

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
        # Safe outside the lock: this path only runs when no send leg is active.
        self._submit_leg(chunk_to_send)

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

    def _submit_leg(self, chunk: bytes) -> None:
        """Submit one ``sock_sendall`` leg; caller must hold no active leg.

        Called outside ``self._cond`` only after the caller has reserved this
        chunk as the sole in-flight leg (``write()`` or ``_on_leg_complete``
        chaining). At most one leg is active, so failure handling here cannot
        race another submit.

        Submit-time failures (``sock_sendall`` or ``add_done_callback``) restore
        the chunk to ``_pending`` so bytes are not lost. Leg completion failures
        are handled in ``_on_leg_complete`` instead; partially sent data is not
        restored. Both paths record a sticky ``_send_error``; the buffer does not
        retry automatically.
        """

        try:
            self._submit(chunk)
        except BaseException as exc:
            with self._cond:
                self._pending.appendleft(chunk)
                self._pending_bytes += len(chunk)
                self._active = False
                self._in_flight_bytes = 0
                self._send_error = exc
                self._cond.notify_all()
            raise

    def _on_leg_complete(self) -> None:
        next_chunk: bytes | None = None
        waiter = self._active_waiter
        assert waiter is not None
        self._active_waiter = None
        assert waiter.poll()
        leg_error = waiter.exception()
        waiter.forget()
        with self._cond:
            self._in_flight_bytes = 0
            if leg_error is not None:
                self._send_error = leg_error
            if self._send_error is None and self._pending:
                next_chunk = self._pending.popleft()
                self._pending_bytes -= len(next_chunk)
                self._in_flight_bytes = len(next_chunk)
            else:
                self._active = False
                self._maybe_shutdown()
            self._cond.notify_all()
        if next_chunk is not None:
            # Safe outside the lock: chaining reserved this chunk as the only leg.
            self._submit_leg(next_chunk)


def open_send_buffer(
    sock: socket.socket,
    *,
    io: ProactorIOManager,
    scheduler: Any = None,
) -> SendBuffer:
    """Construct an outbound bridge for stream writers and other send paths."""

    return SendBuffer(sock=sock, io=io, scheduler=scheduler)
