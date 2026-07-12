from __future__ import annotations

import errno
import heapq
import socket
import threading
from collections import deque
from .tasks import CancelledError
from typing import Any, Generic, Protocol, TypeAlias, TypeVar

from .locks import ThreadsafeEvent
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


def _is_enobufs_delivery(delivery: MultishotDelivery[_RecvManyValue]) -> bool:
    exc = delivery.exception
    return isinstance(exc, OSError) and exc.errno == errno.ENOBUFS


class RecvIterBuffer:
    """Ordered receive buffer bridging ``recv_many`` callbacks and ``sock_recv_iter``."""

    def __init__(
        self,
        *,
        sock: socket.socket,
        buf_group: _BufGroupLike,
        proactor: _RecvIterProactor,
    ) -> None:
        self._sock = sock
        self._buf_group = buf_group
        self._proactor = proactor
        self._lock = threading.Lock()
        self._event = ThreadsafeEvent()
        self._reorder = _OrderedIngestBuffer[memoryview]()
        self._ready: deque[tuple[int, memoryview]] = deque()
        self._pressure_pending = False
        self._awaiting_resubmit = False
        self._stream_base = 0
        self._next_base = 0
        self._stream_done = False
        self._stream_error: BaseException | None = None
        self._streams: list[ContinuousOperation[_RecvManyValue]] = []
        self._closed = False
        self._start_recv_many(base_sequence=0)

    def _start_recv_many(self, *, base_sequence: int) -> None:
        stream = self._proactor.recv_many(
            self._sock,
            self.on_result,
            buf_group=self._buf_group,
            base_sequence=base_sequence,
        )
        self._stream_base = base_sequence
        self._streams.append(stream)
        stream.add_done_callback(self._on_stream_done)

    def _schedule_resubmit(self, *, leg_index: int) -> None:
        self._next_base = self._stream_base + leg_index
        self._awaiting_resubmit = True

    def _on_stream_done(self, stream: Operation[Any]) -> None:
        with self._lock:
            if stream.cancelled():
                self._stream_error = CancelledError()
                self._stream_done = True
            else:
                exception = stream.exception()
                if exception is not None:
                    self._stream_error = exception
                    self._stream_done = True
                elif not self._awaiting_resubmit:
                    self._stream_done = True
        self._event.set()

    def on_result(self, delivery: MultishotDelivery[_RecvManyValue]) -> None:
        notify = False
        with self._lock:
            if self._closed:
                return
            if _is_enobufs_delivery(delivery):
                if self._pressure_pending:
                    return
                if delivery.index is not None:
                    self._schedule_resubmit(leg_index=delivery.index)
                self._pressure_pending = True
                notify = True
            elif delivery.exception is not None:
                self._stream_error = delivery.exception
                self._stream_done = True
                notify = True
            elif delivery.value is not None and delivery.index is not None:
                leg_index = delivery.index
                data = delivery.value
                global_index = self._stream_base + leg_index
                ready = self._reorder.pushpop((global_index, data))
                if ready is not None:
                    self._ready.append(ready)
                notify = bool(self._ready) or len(self._reorder)
                if not delivery.more:
                    if data:
                        self._schedule_resubmit(leg_index=leg_index + 1)
                    else:
                        self._stream_done = True
                        self._stream_error = None
        if notify:
            self._event.set()

    def _should_resubmit(self) -> bool:
        if self._ready or len(self._reorder):
            return False
        buf_group = self._buf_group
        required_free = max(1, buf_group.buffer_count // 2)
        return buf_group.buffer_count - buf_group.leased_count >= required_free

    def consume_pressure_resume(self) -> None:
        """Start a fresh ``recv_many`` once the buffer pool has enough free slots."""

        with self._lock:
            if not self._awaiting_resubmit or not self._should_resubmit():
                return
            base_sequence = self._next_base
            self._awaiting_resubmit = False
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

    def close(self) -> None:
        streams: list[ContinuousOperation[_RecvManyValue]]
        with self._lock:
            if self._closed:
                return
            self._closed = True
            streams = list(self._streams)
            self._pressure_pending = False
            self._awaiting_resubmit = False
            self._ready.clear()
            self._reorder.reset()
        for stream in streams:
            if not stream.done():
                self._proactor.cancel(stream)