from __future__ import annotations

import heapq
import threading
from collections import deque
from collections.abc import Callable
from .tasks import CancelledError
from typing import Any, Generic, Protocol, TypeAlias, TypeVar, cast

from .locks import ThreadsafeEvent
from .operations import ContinuousOperation, Operation

T_Cargo = TypeVar("T_Cargo")

# ``recv_many`` result-callback index signalling provided-buffer pool pressure.
RECV_MANY_BUFFER_PRESSURE = -1
_RecvManyResume = Callable[[], None]
_RecvManyResult = tuple[int, memoryview | _RecvManyResume]
# ``index`` may be ``RECV_MANY_BUFFER_PRESSURE`` (-1) for pressure tokens.
_RecvIterYield: TypeAlias = tuple[int, memoryview]


class _BufGroupLike(Protocol):
    @property
    def buffer_count(self) -> int: ...

    @property
    def leased_count(self) -> int: ...


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


class RecvIterBuffer:
    """Ordered receive buffer bridging ``recv_many`` callbacks and ``sock_recv_iter``."""

    def __init__(
        self,
        *,
        buf_group: _BufGroupLike,
    ) -> None:
        self._buf_group = buf_group
        self._resume: _RecvManyResume | None = None
        self._lock = threading.Lock()
        self._event = ThreadsafeEvent()
        self._reorder = _OrderedIngestBuffer[memoryview]()
        self._ready: deque[tuple[int, memoryview]] = deque()
        self._pressure_pending = False
        self._stream_done = False
        self._stream_error: BaseException | None = None
        self._stream: ContinuousOperation[_RecvManyResult] | None = None
        self._closed = False

    def attach_stream(self, stream: ContinuousOperation[_RecvManyResult]) -> None:
        self._stream = stream
        stream.add_done_callback(self._on_stream_done)

    def _on_stream_done(self, stream: Operation[Any]) -> None:
        with self._lock:
            if stream.cancelled():
                self._stream_error = CancelledError()
            else:
                exception = stream.exception()
                if exception is not None:
                    self._stream_error = exception
            self._stream_done = True
        self._event.set()

    def on_result(self, result: _RecvManyResult) -> None:
        index, data = result
        notify = False
        with self._lock:
            if self._closed:
                return
            if index == RECV_MANY_BUFFER_PRESSURE:
                # recv_many emits at most one pressure callback until the
                # consumer advances past the pressure yield and recv restarts.
                if self._pressure_pending:
                    return
                self._resume = cast(_RecvManyResume, data)
                self._pressure_pending = True
                notify = True
            else:
                ready = self._reorder.pushpop((index, cast(memoryview, data)))
                if ready is not None:
                    self._ready.append(ready)
                notify = bool(self._ready) or len(self._reorder)
        if notify:
            self._event.set()

    def _should_resume(self) -> _RecvManyResume | None:
        # pressure/resume only applies while multishot recv is still active; EOF
        # is the terminal recv_many message and completes the stream, so any
        # leftover _resume after EOF delivery is stale and backend resume no-ops
        if self._resume is None:
            return None
        if self._ready or len(self._reorder):
            return None
        buf_group = self._buf_group
        # half the pool must be free; single-slot pools still need one free slot
        required_free = max(1, buf_group.buffer_count // 2)
        if buf_group.buffer_count - buf_group.leased_count < required_free:
            return None
        resume_fn = self._resume
        self._resume = None
        return resume_fn

    def consume_pressure_resume(self) -> None:
        """Re-arm ``recv_many`` once the buffer pool has enough free slots."""

        with self._lock:
            resume_fn = self._should_resume()
        if resume_fn is not None:
            resume_fn()

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
                            # ordered EOF beats a concurrent stream error/cancel race
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
                with self._lock:
                    pending_resume = self._should_resume()
                if pending_resume is not None:
                    pending_resume()
            self._event.swait()

    def close(self) -> None:
        stream: ContinuousOperation[_RecvManyResult] | None
        with self._lock:
            if self._closed:
                return
            self._closed = True
            stream = self._stream
            self._pressure_pending = False
            self._ready.clear()
            self._reorder.reset()
        if stream is not None and not stream.done():
            stream.cancel()
