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
from .io_waiter import IOWaitable
from .locks import CrossThreadCondition, PulseEvent
from .operations import ContinuousOperation, MultishotDelivery, SupportsOperation, io_cancellation_error
from .scheduler import get_running_scheduler
from .stream_diag import recv_iter_path_begin, recv_iter_path_finish, recv_iter_path_mark
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


# placeholder in RecvIterBuffer._current_operation while recv_many is on the stack
_RECV_MANY_STARTING: Any = object()


class RecvIterBuffer:
    """Ordered receive buffer bridging ``recv_many`` callbacks and ``sock_recv_iter``.

    Worker-thread deliveries are marshalled onto the scheduler, reordered, and
    exposed via ``take_next()`` on a tealet ``PulseEvent`` (consumer wait/signal
    only). Other state is lock-free under the cooperative tealet rule.

    Resubmit uses pool low-water (``leased_count < buffer_count / 2``). On
    Python 3.12+ release leased views after copying; older synthetic pools skip
    view leases so backpressure is weaker there.

    Close cancels a live leg or injects synthetic cancel when none is live;
    drain ready until a terminal. ``take_next`` after EOF or a raised error is
    undefined. ``owns_pool`` means this buffer calls ``buffer_pool.close()`` on
    close (borrowed pools leave that to the owner). Cache return may overlap
    still-leased slots — expected.
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
        fd = sock.fileno()
        recv_iter_path_begin(fd)
        if scheduler is None:
            scheduler = get_running_scheduler()
            recv_iter_path_mark(fd, "scheduler")
        self._sock = sock
        self._buffer_pool = buffer_pool
        self._owns_pool = owns_pool
        # cancel unfinished ContinuousOperations only; start via recv_many override when set
        self._proactor = proactor
        self._recv_many = proactor.recv_many if recv_many is None else recv_many
        self._scheduler = scheduler
        # edge-triggered cooperative pulse; swait_for needs no yield between check and swait
        self._pevent = PulseEvent()
        self._reorder_buffer = ReorderBuffer(self._on_ordered_delivery, start=0)
        self._ready: deque[MultishotDelivery] = deque()
        self._pressure_pending = False
        self._next_base = 0
        self._current_operation: ContinuousOperation[_RecvManyValue] | None = None
        self._closed = False
        recv_iter_path_mark(fd, "setup")
        self.on_result = marshal_to_scheduler(scheduler, self._reorder_buffer.deliver)
        recv_iter_path_mark(fd, "marshal_cb")
        self._start_recv_many(base_sequence=0)
        recv_iter_path_finish(fd)

    def _start_recv_many(self, *, base_sequence: int) -> None:
        if self._closed:
            return
        fd = self._sock.fileno()
        recv_iter_path_mark(fd, "recv_many_enter")
        # SelectorProactor can deliver on this stack before recv_many returns (full
        # synthetic-pool ENOBUFS, eager readable steps) via marshal_to_scheduler
        # immediate=True. Nested _on_ordered_delivery may _schedule_resubmit and
        # clear _current_operation to None — resubmit only arms the next base;
        # the actual next leg waits for drain / low-water via consume_pressure_resume.
        #
        # Publish a sentinel first so that clear is visible. After return, install
        # the real op only if the sentinel is still there. Unconditional assign
        # would reinstall a done op over the nested clear and stall resume
        # (consume_pressure_resume treats any non-None current as a live leg).
        self._current_operation = _RECV_MANY_STARTING
        try:
            operation = self._recv_many(
                self._sock,
                self.on_result,
                buf_group=self._buffer_pool,
                base_sequence=base_sequence,
            )
        except BaseException:
            if self._current_operation is _RECV_MANY_STARTING:
                self._current_operation = None
            raise
        if self._current_operation is _RECV_MANY_STARTING:
            self._current_operation = operation
        recv_iter_path_mark(fd, "recv_store")

    def _schedule_resubmit(self, *, base_sequence: int) -> None:
        # only ENOBUFS / more=False-with-data; EOF leaves the done op in place
        self._next_base = base_sequence
        self._current_operation = None
        self._reorder_buffer.arm_next_index(base_sequence)

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
        # ready-queue mutation + pulse (consumer wait path); no lock under cooperative rule
        index = delivery.index
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
                # resubmit only while the stream is open; after close, still queue
                # for drain but do not clear/arm a next leg
                if not delivery.more and data and not self._closed:
                    self._schedule_resubmit(base_sequence=index + 1)
        if notify:
            self._pevent.set()

        if finish_leg:
            # finish only; clear is _schedule_resubmit's job (blocks resume after EOF)
            operation = delivery.operation
            if operation is not None:
                operation.finish_operation(delivery)

    def _should_resubmit(self) -> bool:
        if self._ready or self._reorder_buffer.pending:
            return False
        return self._pool_at_low_water()

    def consume_pressure_resume(self) -> None:
        """Start a fresh ``recv_many`` when the current leg was cleared for resubmit."""

        if self._closed or self._current_operation is not None or not self._should_resubmit():
            return
        self._start_recv_many(base_sequence=self._next_base)

    def _take_next_ready(self) -> _RecvIterReady | None:
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
        ready = cast(_RecvIterReady, self._pevent.swait_for(self._take_next_ready))
        self.consume_pressure_resume()
        return ready[0]

    def close(self) -> None:
        """Cancel receive IO; consumer sees cancel (or prior terminal) via ``take_next``.

        Live unfinished leg: ``proactor.cancel`` only (unarmed injects into the
        stream; armed uring waits for the target CQE). Otherwise inject
        ``ECANCELED`` with ``index=None`` so a parked ``take_next`` wakes.
        ``owns_pool`` closes the pool immediately.
        """

        if self._closed:
            return
        self._closed = True
        operation = self._current_operation
        self._pressure_pending = False
        # _RECV_MANY_STARTING is only present while recv_many is on the stack
        if operation is not None and operation is not _RECV_MANY_STARTING and not operation.done():
            self._proactor.cancel(operation)
        else:
            # ENOBUFS gap, done EOF/error op, installing, or no leg yet
            self._reorder_buffer.deliver(MultishotDelivery(index=None, exception=io_cancellation_error(), more=False))
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


# Owned outbound backlog: a single write holds ``bytes``; coalescing promotes
# to ``bytearray``. Detached intact for ``sock_sendall`` (no second materialise).
_PendingSend: TypeAlias = bytes | bytearray


class SendBuffer:
    """Ordered outbound queue bridging ``sock_sendall`` callbacks and ``drain()``.

    At most one send operation is active per buffer. The first ``write()`` into
    an empty backlog takes possession of the payload (``bytes`` kept by
    reference; mutable buffers snapshotted once). Further writes promote the
    backlog to a ``bytearray`` and extend it. A leg starts when pending reaches
    ``min_write``, or when ``flush()`` / ``drain()`` / ``write_eof()`` force a
    send. ``_take_pending`` detaches that buffer and hands it to IO without an
    extra copy. While a leg is in flight, further writes coalesce for the next
    leg; on completion any pending is submitted even if below ``min_write`` so
    flush can drain to empty (message boundaries) without stranding a tiny tail.

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
        # None when empty; single owned chunk or coalesced bytearray
        self._pending: _PendingSend | None = None
        self._pending_bytes = 0
        self._in_flight_bytes = 0
        self._active = False
        # IOWaitable (not IOWaiter | IOWaiterSync): avoid runtime cast(...|...) on
        # every leg — evaluating that union in cast() was ~4 us per submit.
        self._active_waiter: IOWaitable[None] | None = None
        self._send_error: BaseException | None = None
        self._closed = False
        self._eof_pending = False
        self._write_eof_done = False
        self._close_when_idle = False
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

        Empty backlog: take possession (``bytes`` by reference; mutable
        inputs snapshotted once). Non-empty backlog: promote to ``bytearray``
        and extend. Does not submit while ``pending_bytes < min_write`` unless
        a leg is already active (then bytes join the next leg) or a later
        ``flush()`` / ``write_eof()`` / high-water ``drain()`` forces send.
        """

        if not data:
            return
        to_send: _PendingSend | None = None
        with self._cond:
            if self._closed:
                raise RuntimeError("SendBuffer is closed")
            if self._eof_pending:
                raise RuntimeError("cannot write() after write_eof()")
            if self._send_error is not None:
                raise self._send_error
            self._append_pending(data)
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

        to_send: _PendingSend | None = None
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

        to_send: _PendingSend | None = None
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

        to_send: _PendingSend | None = None
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

    def _own_chunk(self, data: SocketSendBuffer) -> _PendingSend:
        """Take possession of ``data`` for the pending backlog.

        ``bytes`` are immutable — keep the object by reference. Mutable inputs
        (``bytearray``, ``memoryview``, …) are snapshotted into an owned
        ``bytearray`` so a later coalesce only extends with the new write and
        does not copy the first payload again.
        """

        if type(data) is bytes:
            return data
        return bytearray(data)

    def _append_pending(self, data: SocketSendBuffer) -> None:
        """Queue ``data`` into the backlog. Caller must hold ``self._cond``."""

        if self._pending is None:
            owned = self._own_chunk(data)
            self._pending = owned
            self._pending_bytes = len(owned)
            return
        # coalesce: promote a lone bytes object to bytearray, then extend
        pending = self._pending
        if type(pending) is bytes:
            pending = bytearray(pending)
        else:
            assert type(pending) is bytearray
        pending.extend(data)
        self._pending = pending
        self._pending_bytes = len(pending)

    def _reserve_leg(self, *, force: bool) -> _PendingSend | None:
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

    def _submit_leg(self, chunk: SocketSendBuffer) -> None:
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
            waiter = self._io.sock_sendall(self._sock, chunk)
        except BaseException as exc:
            with self._cond:
                self._prepend_pending(chunk)
                self._active = False
                self._in_flight_bytes = 0
                self._send_error = exc
                self._cond.notify_all()
            raise
        self._active_waiter = waiter
        # Nested completion (IOWaiterSync) may re-enter _on_leg_complete / _submit_leg.
        # Do not wrap this in try/except that re-prepends `chunk`.
        waiter.add_done_callback(self._on_leg_complete)

    def _prepend_pending(self, chunk: SocketSendBuffer) -> None:
        """Restore ``chunk`` ahead of any bytes queued during a failed submit.

        Caller must hold ``self._cond``. Detached leg payloads are already owned
        (``bytes`` / ``bytearray``); other buffer types are snapshotted.
        """

        if self._pending is None:
            if isinstance(chunk, (bytes, bytearray)):
                self._pending = chunk
            else:
                self._pending = bytes(chunk)
            self._pending_bytes = len(self._pending)
            return
        restored = bytearray(chunk)
        restored.extend(self._pending)
        self._pending = restored
        self._pending_bytes = len(self._pending)

    def _take_pending(self) -> _PendingSend | None:
        """Detach the backlog and hand it to IO (no extra materialise).

        Caller must hold ``self._cond``. Does not clear ``_active``.
        """

        pending = self._pending
        if not pending:
            self._pending = None
            self._pending_bytes = 0
            return None
        self._pending = None
        self._pending_bytes = 0
        self._in_flight_bytes = len(pending)
        return pending

    def steal_pending(self) -> _PendingSend | None:
        """Detach queued bytes if no send is in flight. Raises a sticky send error."""

        with self._cond:
            if self._send_error is not None:
                raise self._send_error
            if self._active:
                return None
            pending = self._pending
            self._pending = None
            self._pending_bytes = 0
            return pending

    def arm_close_when_idle(self) -> bool:
        """Close the socket from the last send completion. True if already idle."""

        with self._cond:
            if self._send_error is not None:
                raise self._send_error
            if not self._active and not self._pending:
                return True
            self._close_when_idle = True
            return False

    def _on_leg_complete(self) -> None:
        next_chunk: _PendingSend | None = None
        waiter = self._active_waiter
        assert waiter is not None
        self._active_waiter = None
        assert waiter.poll()
        leg_error = waiter.exception()
        waiter.forget()
        close_now = False
        with self._cond:
            self._in_flight_bytes = 0
            if leg_error is not None:
                self._send_error = leg_error
            if self._send_error is None:
                # ship any warm backlog (even tiny): min_write is for cold idle
                # starts only; flush waits for empty and must not strand a tail
                # after the in-flight leg finishes
                next_chunk = self._take_pending()
            if next_chunk is None:
                self._active = False
                self._maybe_shutdown()
                close_now = self._close_when_idle
                self._close_when_idle = False
            self._cond.notify_all()
        if next_chunk is not None:
            # Safe outside the lock: _active stays true while chaining this leg.
            self._submit_leg(next_chunk)
        elif close_now and self._sock.fileno() != -1:
            self._io.sock_close(self._sock)


def open_send_buffer(
    sock: socket.socket,
    *,
    io: ProactorIOManager,
    scheduler: Any = None,
) -> SendBuffer:
    """Construct an outbound bridge for stream writers and other send paths."""

    return SendBuffer(sock=sock, io=io, scheduler=scheduler)
