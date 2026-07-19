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
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias, cast

from .continuous_callbacks import ReorderBuffer, marshal_to_scheduler
from .io_waiter import IOWaiter, IOWaiterSync
from .locks import Condition, CrossThreadCondition
from .operations import ContinuousOperation, MultishotDelivery, SupportsOperation, io_cancellation_error
from .scheduler import get_running_scheduler
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
# Hold small writes until at least this many bytes are pending (or flush/drain/eof).
# Goal is throughput / CPU: amortise sock_sendall + proactor/uring overhead across
# more payload, not minimise send latency. ``0`` submits any non-empty backlog
# immediately when idle.
_DEFAULT_MIN_WRITE = 2048


class _BufGroupLike(Protocol):
    @property
    def buffer_size(self) -> int: ...

    @property
    def buffer_count(self) -> int: ...

    @property
    def leased_count(self) -> int: ...

    def close(self) -> None: ...


class _RecvIterProactor(Protocol):
    def recv_many(
        self,
        sock: socket.socket,
        callback: Any,
        *,
        buf_group: _BufGroupLike,
        base_sequence: int = 0,
    ) -> ContinuousOperation[_RecvManyValue]: ...

    def cancel(self, operation: SupportsOperation[Any]) -> SupportsOperation[None]: ...


_RecvManyStarter: TypeAlias = Callable[
    ...,
    ContinuousOperation[_RecvManyValue],
]


def _is_enobufs_delivery(delivery: MultishotDelivery) -> bool:
    exc = delivery.exception
    return isinstance(exc, OSError) and exc.errno == errno.ENOBUFS


class RecvIterBuffer:
    """Ordered receive buffer bridging ``recv_many`` callbacks and ``sock_recv_iter``.

    Worker-thread ``recv_many`` deliveries are marshalled onto the scheduler
    thread, reordered there, and exposed to ``take_next()`` via a tealet
    ``Condition``. The condition is only for consumer wait/signal (lost-wakeup
    safety): ``take_next`` parks, and delivery/close notify. Other state
    (``_closed``, current op, resubmit) is updated without the lock — same
    cooperative rule as asyncio: no other tealet can run mid-function unless we
    block, and this class only blocks in ``take_next``.

    Resubmit gating uses ``buf_group.leased_count`` against the pool low-water mark
    (``leased_count < buffer_count / 2``). That tracks real uring ``BufGroup`` slots
    via ``BufView`` release and synthetic-pool leases on Python 3.12+.

    On older Python, synthetic pools skip view leases (no PEP 688), so
    ``leased_count`` does not reflect consumer-held chunks and backpressure via the
    buffer mechanism is effectively dropped there.

    After copying chunk data, call ``view.release()`` or drop the view; on Python
    3.12+ that returns leased pool slots via PEP 688.

    Close cancels in-flight IO and leaves ready/reorder for the consumer to
    drain until a terminal; when no live leg remains, a synthetic cancel
    delivery wakes a parked ``take_next``. ``take_next`` is the generator
    feed: after EOF (``None``) or a raised error it is not called again.
    Selector / unarmed cancel may inject a terminal with ``index=None`` ahead
    of heaped legs; leftovers are simply dropped with the buffer. When
    ``owns_pool`` is set, ``buffer_pool.close()`` runs on close (cache return
    may leave some slots still leased until views are released — that is
    expected).

    ``owns_pool`` is independent of ``pool.release_callback``. The callback
    decides what ``pool.close()`` does (return to a cache vs free); this flag
    decides whether *this* buffer may call ``pool.close()`` at all. Default
    false: a pool passed in is borrowed (caller-managed acquire, shared pool,
    or multi-connection explicit pool). Set true only when the constructing
    layer transferred a lease into this buffer for its lifetime (for example
    ``pooled_default_stream_factory`` after ``acquire_recv_buffer_pool``).
    """

    def __init__(
        self,
        *,
        sock: socket.socket,
        buffer_pool: _BufGroupLike,
        proactor: _RecvIterProactor,
        scheduler: BaseScheduler | None = None,
        recv_many: _RecvManyStarter | None = None,
        owns_pool: bool = False,
    ) -> None:
        if scheduler is None:
            scheduler = get_running_scheduler()
        self._sock = sock
        self._buffer_pool = buffer_pool
        self._owns_pool = owns_pool
        # cancel unfinished ContinuousOperations only; start via recv_many override when set
        self._proactor = proactor
        self._recv_many = proactor.recv_many if recv_many is None else recv_many
        self._scheduler = scheduler
        self._cond = Condition()
        self._reorder_buffer = ReorderBuffer(self._on_ordered_delivery, start=0)
        self._ready: deque[MultishotDelivery] = deque()
        self._pressure_pending = False
        self._next_base = 0
        # set by _schedule_resubmit; cleared when a new leg actually starts
        self._resubmit_pending = False
        self._current_operation: ContinuousOperation[_RecvManyValue] | None = None
        self._closed = False
        self.on_result = marshal_to_scheduler(scheduler, self._reorder_buffer.deliver)
        self._start_recv_many(base_sequence=0)

    def _start_recv_many(self, *, base_sequence: int) -> None:
        if self._closed:
            return
        self._resubmit_pending = False
        self._current_operation = self._recv_many(
            self._sock,
            self.on_result,
            buf_group=self._buffer_pool,
            base_sequence=base_sequence,
        )

    def _schedule_resubmit(self, *, base_sequence: int) -> None:
        self._next_base = base_sequence
        # leg ended; hold off on a new ``recv_many`` until queues drain and the pool is low.
        self._current_operation = None
        self._resubmit_pending = True
        self._reorder_buffer.arm_next_index(base_sequence)

    def _finish_leg(self, delivery: MultishotDelivery) -> None:
        """Mark the continuous leg done and drop ``_current_operation`` when it matches."""

        operation = delivery.operation
        if operation is not None:
            operation.finish_operation(delivery)
            if self._current_operation is operation:
                self._current_operation = None
        elif self._current_operation is not None and self._current_operation.done():
            self._current_operation = None

    def _pool_at_low_water(self) -> bool:
        """Return True when ``leased_count < buffer_count / 2`` (safe to re-submit ``recv_many``)."""

        pool = self._buffer_pool
        return pool.leased_count * 2 < pool.buffer_count

    def _signal_pressure_if_pending(self) -> bool:
        if self._pressure_pending:
            return False
        self._pressure_pending = True
        return True

    def _on_ordered_delivery(self, delivery: MultishotDelivery) -> None:
        # hold the condition only for ready-queue mutation + notify (consumer wait path)
        index = delivery.index
        finish_leg = False
        with self._cond:
            notify = False
            finish_leg = not delivery.more
            if _is_enobufs_delivery(delivery):
                assert index is not None
                if self._closed:
                    delivery = delivery._replace(exception=io_cancellation_error(), more=False)
                    self._ready.append(delivery)
                    notify = True
                else:
                    self._schedule_resubmit(base_sequence=index)
                    if self._signal_pressure_if_pending():
                        notify = True
            else:
                self._ready.append(delivery)
                notify = True
                if delivery.value is not None:
                    assert index is not None
                    data = delivery.value
                    if not delivery.more:
                        if data:
                            self._schedule_resubmit(base_sequence=index + 1)
            if notify:
                self._cond.notify_all()

        if finish_leg:
            self._finish_leg(delivery)

    def _should_resubmit(self) -> bool:
        if self._ready or self._reorder_buffer.pending:
            return False
        return self._pool_at_low_water()

    def consume_pressure_resume(self) -> None:
        """Start a fresh ``recv_many`` once the pool has drained below the low-water mark."""

        if (
            self._closed
            or self._current_operation is not None
            or not self._resubmit_pending
            or not self._should_resubmit()
        ):
            return
        self._start_recv_many(base_sequence=self._next_base)

    def _take_next_locked(self) -> _RecvIterReady | None:
        if self._pressure_pending:
            self._pressure_pending = False
            return ((RECV_MANY_BUFFER_PRESSURE, memoryview(b"")),)
        if self._ready:
            delivery = self._ready.popleft()
            if delivery.exception is not None:
                raise delivery.exception
            chunk = delivery.value
            index = delivery.index
            assert index is not None
            if chunk is None or not chunk:
                return (None,)
            return ((index, chunk),)
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

    def _inject_cancel_delivery(
        self,
        *,
        operation: ContinuousOperation[_RecvManyValue] | None = None,
    ) -> None:
        """Push a synthetic cancel terminal through the ordered stream path.

        ``index=None`` opts out of reorder sequencing (same as selector /
        unarmed proactor cancel). ``_on_ordered_delivery`` queues the
        terminal, wakes ``take_next``, and runs ``_finish_leg`` so
        ``finish_operation`` happens on the normal delivery path rather than
        as a direct close-side call.
        """

        self._reorder_buffer.deliver(
            MultishotDelivery(
                index=None,
                exception=io_cancellation_error(),
                more=False,
                operation=operation,
            )
        )

    def close(self) -> None:
        """Shut down receive IO; the consumer observes cancel via ``take_next``.

        Sets ``_closed`` so no further ``recv_many`` legs start. Ready and
        reorder queues are left for the consumer to drain until a terminal
        (cancel, EOF, or error). ``take_next`` after EOF or a raised error is
        undefined (the generator does not continue).

        An unfinished leg is cancelled via the proactor only. Unarmed cancel
        (selector, deferred, never-submitted) injects a terminal cancel
        delivery into the multishot stream through
        ``ProactorBase._terminalise_cancelled``; armed uring legs only post
        ``ASYNC_CANCEL`` and finish when the target CQE arrives. Do not call
        ``finish_operation`` from close for armed ops — early finish is only
        the synthetic cancel delivery path.

        When no unfinished leg is live (for example after ``ENOBUFS`` pressure
        or a finished but undrained leg), inject a synthetic cancel delivery
        so a parked ``take_next`` still wakes. If ``owns_pool`` was set, calls
        ``buffer_pool.close()`` (cache return may overlap still-leased slots;
        that is expected). Borrowed pools are left alone.
        """

        if self._closed:
            return
        self._closed = True
        operation = self._current_operation
        self._pressure_pending = False
        self._resubmit_pending = False
        live_unfinished = operation is not None and not operation.done()
        if live_unfinished:
            # unarmed: proactor cancel injects cancel delivery + finish_leg
            # armed uring: ASYNC_CANCEL only; terminal CQE finishes the stream
            self._proactor.cancel(operation)
        else:
            self._inject_cancel_delivery()
        if self._owns_pool:
            self._buffer_pool.close()


def open_recv_iter_buffer(
    sock: socket.socket,
    *,
    proactor: _RecvIterProactor,
    buffer_pool: _BufGroupLike,
    scheduler: BaseScheduler | None = None,
    recv_many: _RecvManyStarter | None = None,
    owns_pool: bool = False,
) -> RecvIterBuffer:
    """Construct a receive bridge for ``sock_recv_iter`` and stream readers.

    ``recv_many`` defaults to ``proactor.recv_many``. Pass an override (for
    example ``ProactorIOManager._recv_many``) to start legs without changing
    cancel, which always goes through ``proactor.cancel``.

    ``buffer_pool`` is the provided-buffer (or synthetic) pool used for
    ``recv_many``. Pass ``owns_pool=True`` only when this buffer should call
    ``buffer_pool.close()`` on its own close; default is a borrow.
    """

    return RecvIterBuffer(
        sock=sock,
        buffer_pool=buffer_pool,
        proactor=proactor,
        scheduler=scheduler,
        recv_many=recv_many,
        owns_pool=owns_pool,
    )


class SendBuffer:
    """Ordered outbound queue bridging ``sock_sendall`` callbacks and ``drain()``.

    At most one send operation is active per buffer. ``write()`` always appends
    into a single pending ``bytearray``. A leg starts only when pending reaches
    ``min_write``, or when ``flush()`` / ``drain()`` / ``write_eof()`` force a
    send. While a leg is in flight, further writes keep coalescing for the next
    leg.

    ``min_write`` is a throughput knob: each ``sock_sendall`` pays fixed
    proactor/uring and callback cost, so small idle submits waste CPU. Batching
    amortises that overhead; it is not aimed at wire latency (call ``drain()``
    or ``flush()`` when the app needs data on the wire).

    Completions may arrive on a proactor worker thread; ``drain()`` and
    ``flush()`` block on the scheduler thread via ``CrossThreadCondition``.

    ``drain()`` force-starts any held backlog, then follows asyncio transport
    watermarks: return while ``pending_bytes <= high_water``, otherwise block
    until ``pending_bytes <= low_water``. ``flush()`` blocks until empty.

    Scatter/gather (``sendmsg`` / multi-buffer submit) is future work once the
    proactor exposes a vector send path.
    """

    def __init__(
        self,
        *,
        sock: socket.socket,
        io: ProactorIOManager,
        scheduler: Any = None,
        high_water: int | None = None,
        low_water: int | None = None,
        min_write: int | None = None,
    ) -> None:
        self._sock = sock
        self._io = io
        self._cond = CrossThreadCondition(scheduler=scheduler)
        # None when empty; bytearray of coalesced bytes not yet in a send leg
        self._pending: bytearray | None = None
        self._pending_bytes = 0
        self._in_flight_bytes = 0
        self._active = False
        self._active_waiter: IOWaiter[None] | IOWaiterSync[None] | None = None
        self._send_error: BaseException | None = None
        self._closed = False
        self._eof_pending = False
        self._write_eof_done = False
        self._set_write_buffer_limits(high=high_water, low=low_water)
        if min_write is None:
            min_write = _DEFAULT_MIN_WRITE
        if min_write < 0:
            raise ValueError(f"min_write ({min_write!r}) must be >= 0")
        self._min_write = min_write

    @property
    def pending_bytes(self) -> int:
        """Approximate bytes queued or in the active ``sock_sendall`` leg.

        The counters may be read without the buffer lock, so concurrent
        completion callbacks can yield a briefly stale snapshot.
        """

        return self._pending_bytes + self._in_flight_bytes

    @property
    def min_write(self) -> int:
        """Minimum pending bytes before an idle ``write()`` starts a send leg.

        Sized to amortise transport submit cost; not a latency target.
        """

        return self._min_write

    def get_write_buffer_limits(self) -> tuple[int, int]:
        """Return ``(low_water, high_water)``."""

        return (self._low_water, self._high_water)

    def set_write_buffer_limits(self, high: int | None = None, low: int | None = None) -> None:
        """Configure asyncio-style drain watermarks."""

        with self._cond:
            self._set_write_buffer_limits(high=high, low=low)
            self._cond.notify_all()

    def write(self, data: SocketSendBuffer) -> None:
        """Queue data for transmission; start a leg only at ``min_write`` or force.

        Always copies into the pending ``bytearray`` so the caller may reuse
        its buffer. Does not submit while ``pending_bytes < min_write`` unless
        a leg is already active (then bytes join the next leg) or a later
        ``flush()`` / ``write_eof()`` / high-water ``drain()`` forces send.
        """

        if not data:
            return
        # copy so the caller can reuse its buffer (asyncio proactor style)
        chunk = bytes(data)
        to_send: bytes | None = None
        with self._cond:
            if self._closed:
                raise RuntimeError("SendBuffer is closed")
            if self._eof_pending:
                raise RuntimeError("cannot write() after write_eof()")
            if self._send_error is not None:
                raise self._send_error
            self._append_pending(chunk)
            to_send = self._reserve_leg(force=False)
        if to_send is not None:
            # Safe outside the lock: reserve set _active for this sole leg.
            self._submit_leg(to_send)

    def drain(self) -> None:
        """Kick any held backlog; block only above ``high_water``.

        Always force-starts a pending leg so ``write()`` + ``drain()`` (the
        usual stream-writer pattern) ships data even when still below
        ``min_write``. Multiple ``write()`` calls before one ``drain()`` still
        coalesce. When ``pending_bytes > high_water``, wait until
        ``pending_bytes <= low_water``. Unlike ``flush()``, some data may
        remain in flight or queued after ``drain()`` returns.
        """

        to_send: bytes | None = None
        with self._cond:
            if self._send_error is not None:
                raise self._send_error
            # write+drain must not leave a sub-min_write buffer stranded
            to_send = self._reserve_leg(force=True)
            over_high = self._pending_bytes + self._in_flight_bytes > self._high_water
        if to_send is not None:
            self._submit_leg(to_send)
        if not over_high:
            return
        with self._cond:
            if self._send_error is not None:
                raise self._send_error
            if self._pending_bytes + self._in_flight_bytes <= self._low_water:
                return
            self._cond.swait_for(self._drain_ready)

    def flush(self) -> None:
        """Force-send any held backlog and block until the queue is empty."""

        to_send: bytes | None = None
        with self._cond:
            if self._send_error is not None:
                raise self._send_error
            to_send = self._reserve_leg(force=True)
        if to_send is not None:
            self._submit_leg(to_send)
        with self._cond:
            if self._send_error is not None:
                raise self._send_error
            self._cond.swait_for(self._flush_ready)

    def write_eof(self) -> None:
        """Mark end-of-write; force-send backlog, then ``SHUT_WR`` when idle."""

        to_send: bytes | None = None
        with self._cond:
            if self._closed:
                raise RuntimeError("SendBuffer is closed")
            if self._send_error is not None:
                raise self._send_error
            if self._eof_pending:
                return
            self._eof_pending = True
            # partial buffer must leave before shutdown
            to_send = self._reserve_leg(force=True)
            if to_send is None:
                self._maybe_shutdown()
            self._cond.notify_all()
        if to_send is not None:
            self._submit_leg(to_send)

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

    def _append_pending(self, chunk: bytes) -> None:
        """Extend the coalesced backlog. Caller must hold ``self._cond``."""

        if self._pending is None:
            self._pending = bytearray(chunk)
        else:
            self._pending.extend(chunk)
        self._pending_bytes += len(chunk)

    def _reserve_leg(self, *, force: bool) -> bytes | None:
        """If idle and ready, mark active and return the next send payload.

        When ``force`` is false, require ``pending_bytes >= min_write``.
        Caller must hold ``self._cond``. Returns ``None`` if a leg is already
        active, the backlog is empty, or the size threshold is not met.
        """

        if self._active or self._send_error is not None:
            return None
        if not self._pending:
            return None
        if not force and self._pending_bytes < self._min_write:
            return None
        self._active = True
        return self._take_pending()

    def _submit_leg(self, chunk: bytes | memoryview) -> None:
        """Submit one ``sock_sendall`` leg; caller must hold no active leg.

        Called outside ``self._cond`` only after the caller has reserved this
        chunk as the sole in-flight leg (``write()`` or ``_on_leg_complete``
        chaining). At most one leg is active, so failure handling here cannot
        race another submit.

        Failures from ``sock_sendall`` itself prepend the chunk to ``_pending``
        (data written while submit was in progress stays after it). After a
        waitable is obtained, ``add_done_callback`` may run ``_on_leg_complete``
        nested (eager ``IOWaiterSync``); exceptions from that path must **not**
        re-queue ``chunk`` — bytes may already be on the wire or owned by a live
        proactor leg. Leg completion failures are handled in ``_on_leg_complete``;
        partially sent data is not restored. Both paths record a sticky
        ``_send_error``; the buffer does not retry automatically.
        """

        try:
            # sock_sendall returns IOWaiterSync (eager) or IOWaiter (proactor)
            waiter = cast(IOWaiter[None] | IOWaiterSync[None], self._io.sock_sendall(self._sock, chunk))
        except BaseException as exc:
            with self._cond:
                self._prepend_pending(bytes(chunk))
                self._active = False
                self._in_flight_bytes = 0
                self._send_error = exc
                self._cond.notify_all()
            raise
        self._active_waiter = waiter
        # Nested completion (IOWaiterSync) may re-enter _on_leg_complete / _submit_leg.
        # Do not wrap this in try/except that re-prepends `chunk`.
        waiter.add_done_callback(self._on_leg_complete)

    def _prepend_pending(self, chunk: bytes) -> None:
        """Restore ``chunk`` ahead of any bytes queued during a failed submit.

        Caller must hold ``self._cond``.
        """

        if self._pending is None:
            self._pending = bytearray(chunk)
        else:
            restored = bytearray(chunk)
            restored.extend(self._pending)
            self._pending = restored
        self._pending_bytes = len(self._pending)

    def _take_pending(self) -> bytes | None:
        """Detach the coalesced backlog as the next in-flight leg, or ``None``.

        Caller must hold ``self._cond``. Does not clear ``_active``.
        """

        pending = self._pending
        if not pending:
            self._pending = None
            self._pending_bytes = 0
            return None
        chunk = bytes(pending)
        self._pending = None
        self._pending_bytes = 0
        self._in_flight_bytes = len(chunk)
        return chunk

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
            if self._send_error is None:
                # backlog written while we were in flight: send it without
                # re-applying min_write (pipeline is already warm)
                next_chunk = self._take_pending()
            if next_chunk is None:
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
