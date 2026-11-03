"""Blocking producer/consumer bridges over proactor callback IO.

``RecvIterBuffer`` and ``SendBuffer`` sit under ``scheduler.io``: they turn
continuous ``recv_many`` delivery and counted ``sock_sendall_cb`` calls into
tealet-blocking ``take_next()`` / ``write()`` / ``drain()`` APIs. ``streams``
and other callers open them through ``ProactorIOManager`` factories.
"""

from __future__ import annotations

import errno
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias, cast

from .delivery import MultishotDelivery, OpHandle, ReorderBuffer, io_cancellation_error, marshal_to_scheduler
from .locks import PulseEvent
from .scheduler import get_running_scheduler
from .socket_helpers import socket
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
# Stored for callers. ``write()`` does not submit at this size; ``drain()``
# and ``write_eof()`` submit the whole backlog as one send.
_DEFAULT_MIN_WRITE = 2048


class _BufGroupLike(Protocol):
    @property
    def buffer_size(self) -> int: ...

    @property
    def buffer_count(self) -> int: ...

    @property
    def leased_count(self) -> int: ...

    @property
    def inflight_count(self) -> int: ...

    def close(self) -> None: ...


class _RecvIterProactor(Protocol):
    def recv_many(
        self,
        sock: socket.socket,
        callback: Any,
        *,
        buf_group: _BufGroupLike,
        base_sequence: int = 0,
        set_op: Callable[[OpHandle], object] | None = None,
    ) -> OpHandle: ...

    def cancel_nowait(self, handle: OpHandle) -> None: ...


_RecvManyStarter: TypeAlias = Callable[
    ...,
    OpHandle,
]


def _is_enobufs_delivery(delivery: MultishotDelivery) -> bool:
    exc = delivery.exception
    return isinstance(exc, OSError) and exc.errno == errno.ENOBUFS


class RecvIterBuffer:
    """Ordered receive buffer bridging ``recv_many`` callbacks and ``sock_recv_iter``.

    Worker-thread deliveries are marshalled onto the scheduler, reordered, and
    exposed via ``take_next()`` on a tealet ``PulseEvent`` (consumer wait/signal
    only). Other state is lock-free under the cooperative tealet rule.

    Resubmit uses pool low-water (``leased_count < buffer_count / 2``). On
    Python 3.12+ release leased views after copying; older synthetic pools skip
    view leases so backpressure is weaker there.

    Close cancels a live unfinished leg and otherwise posts a sequenced
    ``ECANCELED`` at the next expected index. A close that lands before
    ``set_op`` publishes the handle is applied there. Drain ready until a
    terminal. ``take_next`` after EOF or a
    raised error is undefined. ``owns_pool`` hands the group back on close:
    ``release_pool`` when set (the IO manager cache), otherwise
    ``buffer_pool.close()``. Borrowed pools leave that to the owner.
    Cache return may overlap still-leased slots — expected.
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
        release_pool: Callable[[_BufGroupLike], None] | None = None,
    ) -> None:
        fd = sock.fileno()
        recv_iter_path_begin(fd)
        if scheduler is None:
            scheduler = get_running_scheduler()
            recv_iter_path_mark(fd, "scheduler")
        self._sock = sock
        self._buffer_pool = buffer_pool
        self._owns_pool = owns_pool
        self._release_pool = release_pool
        self._proactor = proactor
        self._recv_many = proactor.recv_many if recv_many is None else recv_many
        self._scheduler = scheduler
        # edge-triggered cooperative pulse; swait_for needs no yield between check and swait
        self._pevent = PulseEvent()
        self._reorder_buffer = ReorderBuffer(self._on_ordered_delivery, start=0)
        self._ready: deque[MultishotDelivery] = deque()
        self._pressure_pending = False
        self._next_base = 0
        self._current_operation: OpHandle | None = None
        self._recv_ended = False
        self._closed = False
        recv_iter_path_mark(fd, "setup")
        self.on_result = marshal_to_scheduler(scheduler, self._reorder_buffer.deliver)
        recv_iter_path_mark(fd, "marshal_cb")
        self._start_recv_many(base_sequence=0)
        recv_iter_path_finish(fd)

    def _start_recv_many(self, *, base_sequence: int) -> None:
        if self._closed or self._recv_ended:
            return
        fd = self._sock.fileno()
        recv_iter_path_mark(fd, "recv_many_enter")
        # set_op runs before a same-stack completion (selector ENOBUFS, or any
        # delivery marshal_to_scheduler runs immediately). that completion may
        # clear _current_operation: the leg is finished and resume waits for
        # low-water. do not write the returned handle back over that clear.
        # a close that arrived before the handle existed cancels it here.

        def set_op(operation: OpHandle) -> None:
            if self._closed:
                self._proactor.cancel_nowait(operation)
                return
            self._current_operation = operation

        try:
            self._recv_many(
                self._sock,
                self.on_result,
                buf_group=self._buffer_pool,
                base_sequence=base_sequence,
                set_op=set_op,
            )
        except BaseException:
            # not queued, or the callback already dropped the handle
            self._current_operation = None
            raise
        recv_iter_path_mark(fd, "recv_store")

    def _schedule_resubmit(self, *, base_sequence: int) -> None:
        # only ENOBUFS / more=False-with-data; EOF sets _recv_ended instead
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
                data = delivery.value
                # resubmit only while the stream is open; after close, still queue
                # for drain but do not clear/arm a next leg
                if not delivery.more and data and not self._closed:
                    self._schedule_resubmit(base_sequence=index + 1)
        if finish_leg and not _is_enobufs_delivery(delivery):
            data = delivery.value
            if not (data and not self._closed):
                # EOF, error, or cancel: drop the token (do not re-arm)
                self._current_operation = None
                self._recv_ended = True

        if notify:
            self._pevent.set()

    def _should_resubmit(self) -> bool:
        if self._ready or self._reorder_buffer.pending:
            return False
        return self._pool_at_low_water()

    def consume_pressure_resume(self) -> None:
        """Start a fresh ``recv_many`` when the current leg was cleared for resubmit."""

        if self._closed or self._recv_ended or self._current_operation is not None or not self._should_resubmit():
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

    def _deliver_close_terminal(self) -> None:
        self._reorder_buffer.deliver(
            MultishotDelivery(
                index=self._reorder_buffer.next_index,
                exception=io_cancellation_error(),
                more=False,
            )
        )

    def close(self) -> None:
        """Stop receive IO; consumer sees cancel (or a prior terminal) via ``take_next``.

        Live unfinished leg: ``cancel_nowait`` (numeric ``ECANCELED`` through
        reorder). No live token (ENOBUFS gap, EOF): sequenced ``ECANCELED``
        at the next expected index. A close before ``set_op`` has published
        the handle is applied when that handle appears. ``owns_pool`` hands
        the group to ``release_pool``, or ``close()``s it when no releaser
        was passed.
        """

        if self._closed:
            return
        self._closed = True
        operation = self._current_operation
        self._pressure_pending = False
        if operation is not None:
            self._current_operation = None
            self._proactor.cancel_nowait(operation)
        else:
            self._deliver_close_terminal()
        if self._owns_pool:
            release_pool = self._release_pool
            if release_pool is not None:
                release_pool(self._buffer_pool)
            else:
                self._buffer_pool.close()


def open_recv_iter_buffer(
    sock: socket.socket,
    *,
    proactor: _RecvIterProactor,
    buffer_pool: _BufGroupLike,
    scheduler: BaseScheduler | None = None,
    recv_many: _RecvManyStarter | None = None,
    owns_pool: bool = False,
    release_pool: Callable[[_BufGroupLike], None] | None = None,
) -> RecvIterBuffer:
    """Construct a receive bridge for ``sock_recv_iter`` and stream readers.

    ``recv_many`` defaults to ``proactor.recv_many``. Pass an override to start
    legs without changing cancel (``proactor.cancel_nowait``; ``cancel()`` remains
    the waitable teardown path).

    ``buffer_pool`` is the provided-buffer (or synthetic) pool used for
    ``recv_many``. Pass ``owns_pool=True`` only when this buffer should hand
    the group back on its own close. ``release_pool`` is that handoff (the
    IO manager cache). Without it, close calls ``buffer_pool.close()``.
    Default is a borrow.
    """

    return RecvIterBuffer(
        sock=sock,
        buffer_pool=buffer_pool,
        proactor=proactor,
        scheduler=scheduler,
        recv_many=recv_many,
        owns_pool=owns_pool,
        release_pool=release_pool,
    )


# Owned outbound backlog: a single write holds ``bytes``; coalescing promotes
# to ``bytearray``. Detached intact for ``sock_sendall_cb`` (no second materialise).
_PendingSend: TypeAlias = bytes | bytearray


class SendBuffer:
    """Outbound queue for one task. ``write()`` only appends.

    The first ``write()`` into an empty backlog keeps ``bytes`` by reference
    and snapshots a mutable buffer once. Further writes promote that backlog
    to a ``bytearray`` and extend it. ``drain()`` and ``write_eof()`` detach
    the whole backlog, add its length to the in-flight count, and submit one
    ``sock_sendall_cb``. The completion is ``callback(nbytes, exception)`` on
    the scheduler (``call_on_scheduler`` inside the IO manager): inline on a
    live turn, otherwise queued with the driver woken. It subtracts that
    length and ``PulseEvent.set()`` wakes a parked ``drain()`` or
    ``wait_closed()``. It does not submit the next chunk. The buffer is not
    re-entrant, so those updates are not under a tealet lock.

    ``drain()`` parks only when the in-flight count is above the high water
    mark, and resumes at or below the low water mark. ``min_write`` is stored
    for callers and does not start a send.

    ``close()`` does not park. Bytes not yet submitted are one more counted
    ``sock_sendall_cb``. ``sock_close_cb`` is submitted behind that send, and
    behind ``SHUT_WR`` when ``write_eof()`` has run. ``wait_closed()`` parks
    until that close has completed. The socket FIFO runs the shutdown first,
    so it has finished by then. It is then safe to leave the scheduler.

    A failed send subtracts the same length and stores the error, so a waiter
    does not stay parked. The ring marks a close parked behind that send
    abortive. ``write_eof()`` submits ``SHUT_WR`` immediately; the socket
    FIFO runs it after the counted send.
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
        self._scheduler = scheduler
        # wakes drain() / wait_closed(). no lock: one task mutates, the
        # callback only runs while that task is parked or on its stack.
        self._pulse = PulseEvent()
        # None when empty; single owned chunk or coalesced bytearray
        self._pending: _PendingSend | None = None
        self._pending_bytes = 0
        self._in_flight_bytes = 0
        self._send_error: BaseException | None = None
        self._closed = False
        self._connection_lost = False
        self._close_submitted = False
        self._close_done = False
        self._eof_pending = False
        self._shutdown_submitted = False
        self._shutdown_done = False
        self._set_write_buffer_limits(high=high_water, low=low_water)
        if min_write is None:
            min_write = _DEFAULT_MIN_WRITE
        if min_write < 0:
            raise ValueError(f"min_write ({min_write!r}) must be >= 0")
        self._min_write = min_write

    @property
    def pending_bytes(self) -> int:
        """Bytes still queued plus bytes in counted ``sock_sendall_cb`` calls.

        A completion that has been queued but not run yet is still included.
        """

        return self._pending_bytes + self._in_flight_bytes

    @property
    def min_write(self) -> int:
        """Stored batching hint. ``write()`` does not submit at this size."""

        return self._min_write

    def get_write_buffer_limits(self) -> tuple[int, int]:
        """Return ``(low_water, high_water)``."""

        return (self._low_water, self._high_water)

    def set_write_buffer_limits(self, high: int | None = None, low: int | None = None) -> None:
        """Configure asyncio-style drain watermarks."""

        self._set_write_buffer_limits(high=high, low=low)
        self._pulse.set()

    def write(self, data: SocketSendBuffer) -> None:
        """Append ``data`` to the backlog. Does not submit.

        Empty backlog: take possession (``bytes`` by reference; mutable
        inputs snapshotted once). Non-empty backlog: promote to ``bytearray``
        and extend.
        """

        if not data:
            return
        self._raise_if_failed()
        if self._closed:
            raise RuntimeError("SendBuffer is closed")
        if self._eof_pending:
            raise RuntimeError("cannot write() after write_eof()")
        self._append_pending(data)

    def drain(self) -> None:
        """Submit the backlog and park when the in-flight count is above high water.

        One ``sock_sendall`` takes whatever is queued. A later ``drain()`` may
        submit another. The sum of those lengths is the backpressure bound.
        Parks while the count is above ``high_water``, until a completion
        brings it to ``low_water`` or below. Returns with later writes still
        able to queue, and with earlier sends still in flight under the mark.
        """

        self._submit_backlog()
        self._raise_if_failed()
        if self._in_flight_bytes > self._high_water:
            self._pulse.swait_for(self._drain_ready)

    def write_eof(self) -> None:
        """Send the backlog and submit ``SHUT_WR`` behind that send."""

        if self._closed:
            raise RuntimeError("SendBuffer is closed")
        self._raise_if_failed()
        if self._eof_pending:
            return
        self._eof_pending = True
        chunk = self._detach_for_send()
        if chunk is not None:
            self._submit_counted(chunk)
        self._submit_shutdown()

    def close(self) -> None:
        """Reject further writes, send any tail, and submit a waitable close.

        Does not park. ``wait_closed()`` waits for the close completion. A
        ``SHUT_WR`` submitted earlier is ahead of that close on the socket.
        """

        if self._closed:
            return
        self._closed = True
        chunk = self._detach_for_send()
        try:
            if chunk is not None:
                self._submit_counted(chunk)
        finally:
            self._submit_close()

    def wait_closed(self) -> None:
        """Park until ``connection_lost``. Calls ``close()`` first."""

        self.close()
        self._pulse.swait_for(self._closed_ready)

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def connection_lost(self) -> bool:
        """True once the close has completed and no counted send is in flight."""

        return self._connection_lost

    @property
    def eof_pending(self) -> bool:
        return self._eof_pending

    @property
    def write_eof_done(self) -> bool:
        """True once the ``SHUT_WR`` completion has arrived."""

        return self._shutdown_done

    def _drain_ready(self) -> bool:
        self._raise_if_failed()
        return self._in_flight_bytes <= self._low_water

    def _closed_ready(self) -> bool:
        self._raise_if_failed()
        return self._connection_lost

    def _raise_if_failed(self) -> None:
        if self._send_error is not None:
            raise self._send_error

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

    def _append_pending(self, data: SocketSendBuffer) -> None:
        """Queue ``data`` into the backlog."""

        if self._pending is None:
            # bytes stay by reference. A mutable buffer is snapshotted once so
            # a later coalesce only copies the new write.
            owned = data if type(data) is bytes else bytearray(data)
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

    def _submit_backlog(self) -> None:
        """Detach the backlog and submit it as one counted ``sock_sendall_cb``."""

        if self._closed:
            raise RuntimeError("SendBuffer is closed")
        self._raise_if_failed()
        chunk = self._detach_for_send()
        if chunk is not None:
            self._submit_counted(chunk)

    def _detach_for_send(self) -> _PendingSend | None:
        """Move the backlog into the in-flight count."""

        chunk = self._take_pending()
        if chunk:
            self._in_flight_bytes += len(chunk)
        return chunk

    def _submit_counted(self, chunk: _PendingSend) -> None:
        """Submit ``chunk``. The completion subtracts the length it reports.

        A submit failure puts the chunk back and subtracts that length.
        """

        try:
            self._io.sock_sendall_cb(self._sock, chunk, self._finish_leg)
        except BaseException as exc:
            self._fail_submit(chunk, len(chunk), exc)
            raise

    def _submit_shutdown(self) -> None:
        """Submit ``SHUT_WR``. The socket FIFO runs it after counted sends."""

        if self._shutdown_submitted:
            return
        self._shutdown_submitted = True
        try:
            self._io.sock_shutdown_cb(self._sock, socket.SHUT_WR, self._finish_shutdown)
        except BaseException as exc:
            self._finish_shutdown_result(exc)
            raise

    def _finish_shutdown(self, _result: object, exc: BaseException | None) -> None:
        self._finish_shutdown_result(exc)

    def _finish_shutdown_result(self, exc: BaseException | None) -> None:
        self._shutdown_done = True
        if exc is not None and self._send_error is None:
            self._send_error = exc
        self._pulse.set()

    def _submit_close(self) -> None:
        """Submit ``close`` behind any send or shutdown on this socket."""

        if self._close_submitted:
            return
        self._close_submitted = True
        try:
            self._io.sock_close_cb(self._sock, self._finish_close)
        except BaseException as exc:
            self._finish_close_result(exc)
            raise

    def _finish_close(self, _result: object, exc: BaseException | None) -> None:
        self._finish_close_result(exc)

    def _finish_close_result(self, exc: BaseException | None) -> None:
        self._close_done = True
        if exc is not None and self._send_error is None:
            self._send_error = exc
        self._note_connection_lost()
        self._pulse.set()

    def _note_connection_lost(self) -> None:
        """Set the flag once the close has completed and no send is in flight.

        A ``SHUT_WR`` is submitted ahead of the close, so its completion has
        already been delivered.
        """

        if not self._closed or self._in_flight_bytes != 0 or not self._close_done:
            return
        if self._send_error is not None:
            return
        self._connection_lost = True

    def _fail_submit(self, chunk: _PendingSend, length: int, exc: BaseException) -> None:
        self._prepend_pending(chunk)
        self._in_flight_bytes -= length
        assert self._in_flight_bytes >= 0
        if self._send_error is None:
            self._send_error = exc
        self._note_connection_lost()
        self._pulse.set()

    def _finish_leg(self, reported: object, exc: BaseException | None) -> None:
        """Subtract the length ``send_all`` reported and wake waiters.

        Runs on the scheduler. ``reported`` is that length on failure too.
        """

        assert isinstance(reported, int)
        self._in_flight_bytes -= reported
        assert self._in_flight_bytes >= 0
        if exc is not None and self._send_error is None:
            self._send_error = exc
        self._note_connection_lost()
        self._pulse.set()

    def _prepend_pending(self, chunk: SocketSendBuffer) -> None:
        """Restore ``chunk`` ahead of any bytes queued during a failed submit.

        Detached leg payloads are already owned (``bytes`` / ``bytearray``);
        other buffer types are snapshotted.
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
        """Detach the backlog. Does not touch in-flight."""

        pending = self._pending
        if not pending:
            self._pending = None
            self._pending_bytes = 0
            return None
        self._pending = None
        self._pending_bytes = 0
        return pending


def open_send_buffer(
    sock: socket.socket,
    *,
    io: ProactorIOManager,
    scheduler: Any = None,
) -> SendBuffer:
    """Construct an outbound bridge for stream writers and other send paths."""

    return SendBuffer(sock=sock, io=io, scheduler=scheduler)
