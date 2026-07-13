from __future__ import annotations

import errno
import heapq
import socket
import threading
from collections import deque
from typing import Any, Generic, Protocol, TypeAlias, TypeVar

from .locks import ThreadsafeEvent
from .tasks import CancelledError
from .operations import ContinuousOperation, MultishotDelivery, Operation

T_Cargo = TypeVar("T_Cargo")

# ``sock_recv_iter`` still yields this index for provided-buffer pool pressure.
RECV_MANY_BUFFER_PRESSURE = -1
_RecvManyValue = memoryview
_RecvIterYield: TypeAlias = tuple[int, memoryview]


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


class _OrderedIngestBuffer(Generic[T_Cargo]):
    """Hold out-of-order indexed items and release them in strict sequence."""

    def __init__(self, *, start: int = 0) -> None:
        self._next_index = start
        self._heap: list[tuple[int, T_Cargo]] = []

    @property
    def next_index(self) -> int:
        return self._next_index

    def push(self, item: tuple[int, T_Cargo]) -> None:
        """Buffer one out-of-order item.

        Duplicate indices violate the ``recv_many`` transport contract.
        """

        if __debug__:
            index = item[0]
            assert index >= self._next_index, "stale recv_many index"
            assert all(existing[0] != index for existing in self._heap), "duplicate recv_many index"
        heapq.heappush(self._heap, item)

    def pushpop(self, item: tuple[int, T_Cargo]) -> tuple[int, T_Cargo] | None:
        """Fast path when ``item[0]`` equals ``next_index``; else ``push()`` then ``pop()``."""

        if item[0] == self._next_index:
            self._next_index += 1
            return item
        self.push(item)
        return self.pop()

    def pop(self) -> tuple[int, T_Cargo] | None:
        """Return the next ready heap item, or ``None`` when it is not ``next_index``."""

        if self._heap and self._heap[0][0] == self._next_index:
            self._next_index += 1
            return heapq.heappop(self._heap)
        return None

    def __bool__(self) -> bool:
        if self._heap:
            return self._heap[0][0] == self._next_index
        return False

    def __len__(self) -> int:
        return len(self._heap)

    def reset(self, *, start: int = 0) -> None:
        self._heap.clear()
        self._next_index = start


def _is_enobufs_delivery(delivery: MultishotDelivery) -> bool:
    exc = delivery.exception
    return isinstance(exc, OSError) and exc.errno == errno.ENOBUFS


class RecvIterBuffer:
    """Ordered receive buffer bridging ``recv_many`` callbacks and ``sock_recv_iter``.

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
        scheduler: Any = None,
    ) -> None:
        self._sock = sock
        self._buf_group = buf_group
        self._proactor = proactor
        self._lock = threading.Lock()
        self._event = ThreadsafeEvent(scheduler)
        self._reorder = _OrderedIngestBuffer[memoryview]()
        self._ready: deque[tuple[int, memoryview]] = deque()
        self._pressure_pending = False
        self._next_base = 0
        self._stream_done = False
        self._stream_error: BaseException | None = None
        self._current_operation: ContinuousOperation[_RecvManyValue] | None = None
        self._closed = False
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
        with self._lock:
            if self._closed:
                if not operation.done():
                    self._proactor.cancel(operation)
                return
            self._current_operation = operation

    def _schedule_resubmit(self, *, base_sequence: int) -> None:
        self._next_base = base_sequence
        # leg ended; hold off on a new ``recv_many`` until queues drain and the pool is low.
        self._current_operation = None

    def _pool_at_low_water(self) -> bool:
        """Return True when ``leased_count < buffer_count / 2`` (safe to re-submit ``recv_many``)."""

        buf_group = self._buf_group
        return buf_group.leased_count * 2 < buf_group.buffer_count

    def _signal_pressure_if_pending(self) -> bool:
        if self._pressure_pending:
            return False
        self._pressure_pending = True
        return True

    def on_result(self, delivery: MultishotDelivery) -> None:
        notify = False
        with self._lock:
            if self._closed:
                return
            if _is_enobufs_delivery(delivery):
                self._schedule_resubmit(base_sequence=delivery.index)
                if self._signal_pressure_if_pending():
                    notify = True
            elif delivery.exception is not None:
                self._stream_error = delivery.exception
                self._stream_done = True
                notify = True
            elif delivery.value is not None:
                data = delivery.value
                ready = self._reorder.pushpop((delivery.index, data))
                if ready is not None:
                    self._ready.append(ready)
                notify = bool(self._ready) or len(self._reorder)
                if not delivery.more:
                    if data:
                        self._schedule_resubmit(base_sequence=delivery.index + 1)
                    else:
                        self._stream_done = True
                        self._stream_error = None
        if notify:
            self._event.set()

    def _should_resubmit(self) -> bool:
        if self._ready or len(self._reorder):
            return False
        return self._pool_at_low_water()

    def consume_pressure_resume(self) -> None:
        """Start a fresh ``recv_many`` once the pool has drained below the low-water mark."""

        with self._lock:
            if self._closed or self._current_operation is not None or self._stream_done or not self._should_resubmit():
                return
            base_sequence = self._next_base
        self._start_recv_many(base_sequence=base_sequence)

    def take_next(self) -> _RecvIterYield | None:
        while True:
            try:
                with self._lock:
                    if self._pressure_pending:
                        self._pressure_pending = False
                        return (RECV_MANY_BUFFER_PRESSURE, memoryview(b""))
                    ready_item: tuple[int, memoryview] | None = None
                    if self._ready:
                        ready_item = self._ready.popleft()
                    elif self._reorder:
                        ready_item = self._reorder.pop()
                    if ready_item is not None:
                        index, chunk = ready_item
                        if not chunk:
                            self._stream_done = True
                            self._stream_error = None
                            return None
                        return (index, chunk)
                    if self._stream_done:
                        if self._stream_error is not None:
                            raise self._stream_error
                        return None
                    self._event.clear()
            finally:
                self.consume_pressure_resume()
            self._event.swait()

    def has_pending_chunks(self) -> bool:
        """Return True when ordered chunks are already queued for ``take_next()``."""

        with self._lock:
            return bool(self._ready or len(self._reorder))

    def close(self) -> None:
        """Stop iteration and cancel any in-flight ``recv_many`` leg.

        Terminal state is established here before ``proactor.cancel()`` so a
        blocked ``take_next()`` wakes even though ``on_result`` ignores
        deliveries once ``_closed`` is set.
        """

        operation: ContinuousOperation[_RecvManyValue] | None
        with self._lock:
            if self._closed:
                return
            self._closed = True
            operation = self._current_operation
            self._current_operation = None
            self._pressure_pending = False
            self._ready.clear()
            self._reorder.reset()
            if not self._stream_done:
                self._stream_error = CancelledError()
                self._stream_done = True
        self._event.set()
        if operation is not None and not operation.done():
            self._proactor.cancel(operation)
