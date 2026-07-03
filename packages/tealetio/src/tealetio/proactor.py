from __future__ import annotations

import asyncio as _asyncio
import errno
import heapq
import os
import selectors
import socket
import struct
import sys
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import CancelledError
from dataclasses import dataclass, field
from typing import Any, Generic, NoReturn, Protocol, TypeAlias, TypeVar, cast

import uring_api

from . import compat
from .files import ProactorFile, parse_open_mode
from .locks import ThreadsafeEvent
from .operations import ContinuousOperation, ContinuousStepResult, Operation
from .poll_helpers import poll_mask_to_selector_events as _poll_mask_to_selector_events
from .poll_helpers import probe_poll_fd_now as _probe_poll_fd_now
from .scheduler import (
    AsyncDrivingMixin,
    AsyncSchedulerDrivingAPI,
    BaseScheduler,
    RunnableQueueFactory,
    SyncDrivingMixin,
    SyncSchedulerDrivingAPI,
)

T = TypeVar("T")

__all__ = [
    "ContinuousOperation",
    "Operation",
    "AsyncProactorScheduler",
    "Proactor",
    "ProactorBase",
    "ProactorFactory",
    "ProactorScheduler",
    "SelectorProactor",
    "SyncProactorScheduler",
    "ThreadedSelectorProactor",
    "UringProactor",
    "ProactorFile",
    "RECV_MANY_BUFFER_PRESSURE",
    "RecvBufferPool",
]


_DoneCallback = Callable[[Operation[Any]], object]
_CompletionCallback = Callable[[], object]
_ResultCallback = Callable[[T], object]
_ProgressCallback = Callable[[int], object]
_RecvProgressCallback = Callable[[bytes], object]
_Clock = Callable[[], float]
_DEFAULT_URING_COMPLETION_THREADS = 2
_DEFAULT_URING_COMPLETION_THREAD_NICE = -5
_DEFAULT_URING_RECV_MANY_BUFFER_SIZE = 16 * 1024
_DEFAULT_URING_RECV_MANY_BUFFER_COUNT = 256
_DEFAULT_RECVITER_BUFFER_SIZE = 16 * 1024
_DEFAULT_RECVITER_BUFFER_COUNT = 8
_DEFAULT_SELECTOR_RECV_MANY_CHUNK_SIZE = 8192
# ``recv_many`` result-callback index signalling provided-buffer pool pressure.
RECV_MANY_BUFFER_PRESSURE = -1
_RecvManyResume = Callable[[], None]
_RecvManyResult = tuple[int, memoryview | _RecvManyResume]
_RecvManyCallback = Callable[[_RecvManyResult], object]
# ``index`` may be ``RECV_MANY_BUFFER_PRESSURE`` (-1) for pressure tokens.
_RecvIterYield: TypeAlias = tuple[int, memoryview]
_DEFAULT_ACCEPT_FLAGS = getattr(socket, "SOCK_NONBLOCK", 0) | getattr(socket, "SOCK_CLOEXEC", 0)
_DEFAULT_OPENAT_DFD = getattr(os, "AT_FDCWD", -100)

T_Cargo = TypeVar("T_Cargo")


def _stat_result_from_statx(buf: bytes | bytearray | memoryview) -> os.stat_result:
    """Build ``os.stat_result`` from a completed io_uring statx buffer."""

    if len(buf) < uring_api.STATX_BUFFER_SIZE:
        raise ValueError("statx buffer must be at least STATX_BUFFER_SIZE bytes")
    mask = struct.unpack_from("<I", buf, 0)[0]
    if not (mask & uring_api.STATX_SIZE):
        raise ValueError("statx buffer does not contain STATX_SIZE fields")
    nlink, uid, gid, mode = struct.unpack_from("<IIIH", buf, 16)
    ino, size, _blocks = struct.unpack_from("<QQQ", buf, 32)
    atime_sec, atime_nsec = struct.unpack_from("<qi", buf, 64)
    ctime_sec, ctime_nsec = struct.unpack_from("<qi", buf, 96)
    mtime_sec, mtime_nsec = struct.unpack_from("<qi", buf, 112)
    _rdev_major, _rdev_minor, dev_major, dev_minor = struct.unpack_from("<IIII", buf, 128)
    dev = os.makedev(dev_major, dev_minor)
    # os.stat_result accepts a 10-field sequence; extra tuple entries mis-map attributes.
    return os.stat_result(
        (
            mode,
            ino,
            dev,
            nlink,
            uid,
            gid,
            size,
            atime_sec + atime_nsec / 1_000_000_000,
            mtime_sec + mtime_nsec / 1_000_000_000,
            ctime_sec + ctime_nsec / 1_000_000_000,
        )
    )


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


_UringRing: TypeAlias = uring_api.Ring
_UringCompletion: TypeAlias = uring_api.Completion
_UringBufGroup: TypeAlias = uring_api.BufGroup


class RecvBufferPool(Protocol):
    """Provided-buffer pool surface shared by uring and selector backends.

    ``leased_count`` tracks how many receive chunks consumers still hold.
    When the pool is full, ``recv_many`` / ``sock_recv_iter`` pause and surface
    ``(RECV_MANY_BUFFER_PRESSURE, resume)`` so callers can drop views and
    call ``resume()`` to regulate inbound flow.
    """

    @property
    def buffer_size(self) -> int: ...

    @property
    def buffer_count(self) -> int: ...

    @property
    def leased_count(self) -> int: ...


def _supports_release_buffer() -> bool:
    """Return True when PEP 688 ``__release_buffer__`` exporters are usable.

    Selector leased chunks rely on ``__release_buffer__`` to decrement
    ``leased_count`` when consumers drop chunk views. On older CPython we
    fall back to unpaced ``recv_many`` without pool pressure (see below).
    """

    return sys.version_info >= (3, 12)


class _SelectorBufGroup:
    """Synthetic provided-buffer pool for selector ``recv_many`` flow control.

    There is no kernel buffer ring on the selector path; this pool only
    counts in-flight chunk views so ``recv_many`` can mirror uring backpressure.
    Consumers should drop ``memoryview`` references (or call ``resume()`` after
    pressure) to return slots and let the proactor read again.
    """

    def __init__(self, buffer_size: int, buffer_count: int) -> None:
        self.buffer_size = buffer_size
        self.buffer_count = buffer_count
        self.leased_count = 0

    def _note_leased(self) -> None:
        self.leased_count += 1

    def _note_unleased(self) -> None:
        if self.leased_count:
            self.leased_count -= 1

    def note_chunk_released(self) -> None:
        """Explicitly return one leased slot (tests and manual consumers)."""

        self._note_unleased()


class _LeasedChunk:
    """PEP 688 buffer exporter whose release returns a selector pool slot."""

    def __init__(self, data: bytearray, pool: _SelectorBufGroup) -> None:
        self._data = data
        self._pool = pool
        self._held: memoryview | None = None

    def __buffer__(self, flags: int) -> memoryview:
        if self._held is not None:
            raise AssertionError("leased chunk buffer is already held")
        self._held = memoryview(self._data)
        return self._held

    def __release_buffer__(self, view: memoryview) -> None:
        if self._held is not view:
            raise AssertionError("released view does not match active leased chunk")
        self._held.release()
        self._held = None
        self._pool._note_unleased()


def _leased_selector_memoryview(data: bytes | bytearray, pool: _SelectorBufGroup) -> memoryview:
    pool._note_leased()
    payload = data if type(data) is bytearray else bytearray(data)
    return memoryview(_LeasedChunk(payload, pool))


_UringRingFactory = Callable[[int, int], _UringRing]
_UringSendSubmit = Callable[[int, Any, object], _UringCompletion]
_UringSendtoSubmit = Callable[[int, Any, Any, object], _UringCompletion]


def _default_uring_ring_factory(entries: int, flags: int) -> _UringRing:
    return uring_api.Ring(entries=entries, flags=flags)


def _configure_accepted_socket(sock: socket.socket) -> socket.socket:
    sock.setblocking(False)
    os.set_inheritable(sock.fileno(), False)
    return sock


class Proactor(Protocol):
    """Minimal completion-oriented IO backend used by `ProactorScheduler`."""

    def close(self) -> None: ...

    def break_wait(self) -> None: ...

    def cancel_operation(self, operation: Operation[Any]) -> None: ...

    def set_completion_callback(self, callback: _CompletionCallback | None) -> None: ...

    def bind_loop(self, loop: _asyncio.AbstractEventLoop) -> None: ...

    def get_time(self) -> float: ...

    def set_clock(self, clock: _Clock) -> None: ...

    def has_pending_operations(self) -> bool: ...

    def wait(self, deadline: float | None = None) -> None: ...

    async def wait_async(self, deadline: float | None = None) -> None: ...

    def recv(self, sock: socket.socket, n: int) -> Operation[bytes]: ...

    def recv_into(self, sock: socket.socket, buf: Any) -> Operation[int]: ...

    def recvfrom(self, sock: socket.socket, bufsize: int) -> Operation[tuple[bytes, Any]]: ...

    def recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> Operation[tuple[int, Any]]: ...

    def sendall(self, sock: socket.socket, data: Any, progress: _ProgressCallback | None = None) -> Operation[None]: ...

    def sendto(self, sock: socket.socket, data: Any, address: Any) -> Operation[int]: ...

    def accept(self, sock: socket.socket) -> Operation[tuple[socket.socket, Any]]: ...

    def accept_many(
        self,
        sock: socket.socket,
        callback: Callable[[tuple[socket.socket, Any]], object],
    ) -> ContinuousOperation[tuple[socket.socket, Any]]: ...

    def connect(self, sock: socket.socket, address: Any) -> Operation[None]: ...

    def openat(self, path: str, flags: int, mode: int = 0, *, dfd: int = _DEFAULT_OPENAT_DFD) -> Operation[int]: ...

    def read(self, fd: int, n: int, offset: int) -> Operation[bytes]: ...

    def read_into(self, fd: int, buf: Any, offset: int) -> Operation[int]: ...

    def write(self, fd: int, data: Any, offset: int) -> Operation[int]: ...

    def stat(self, path: str = "", *, fd: int = -1) -> Operation[os.stat_result]: ...

    def stat_fdsize(self, fd: int) -> Operation[int]: ...

    def recv_many(
        self,
        sock: socket.socket,
        callback: _RecvManyCallback,
        *,
        buf_group: RecvBufferPool,
    ) -> ContinuousOperation[_RecvManyResult]: ...

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> RecvBufferPool: ...

    def shared_recv_buffer_pool(self) -> RecvBufferPool: ...

    def set_shared_recv_buffer_pool(self, pool: RecvBufferPool) -> None: ...

    def poll(self, fd: int, mask: int) -> Operation[int]: ...

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Callable[[int], object],
    ) -> ContinuousOperation[int]: ...


ProactorFactory = Callable[[], Proactor]


class _RecvIterBuffer:
    """Ordered receive buffer bridging ``recv_many`` callbacks and ``sock_recv_iter``."""

    def __init__(
        self,
        *,
        buf_group: RecvBufferPool,
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
            proactor = stream._proactor
            if proactor is not None:
                proactor.cancel_operation(stream)
            else:
                stream.cancel()


class ProactorBase:
    """Shared helpers for concrete proactor backends."""

    def __init__(self, *, completion_callback: _CompletionCallback | None = None) -> None:
        self._closed = False
        self._completion_callback = completion_callback
        self._clock = time.monotonic
        self._async_wait_loop: _asyncio.AbstractEventLoop | None = None
        self._shared_recv_buffer_pool: RecvBufferPool | None = None

    def set_completion_callback(self, callback: _CompletionCallback | None) -> None:
        """Set the callback invoked when backend completions may be ready."""

        self._completion_callback = callback

    def bind_loop(self, loop: _asyncio.AbstractEventLoop) -> None:
        """Bind this proactor to an asyncio event loop for async waits."""

        if self._async_wait_loop is None:
            self._async_wait_loop = loop
            return
        if self._async_wait_loop is not loop:
            raise RuntimeError(f"{type(self).__name__} is already bound to a different event loop")

    def get_time(self) -> float:
        """Return the proactor clock value."""

        return self._clock()

    def set_clock(self, clock: _Clock) -> None:
        """Set the clock used for deadline-oriented waits."""

        self._clock = clock

    def _timeout_until_deadline(self, deadline: float | None) -> float | None:
        if deadline is None:
            return None
        if deadline == 0:
            return 0.0
        return max(0.0, deadline - self.get_time())

    def _notify_completion(self) -> None:
        callback = self._completion_callback
        if callback is not None:
            callback()

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("proactor is closed")

    def recv_many(
        self,
        sock: socket.socket,
        callback: _RecvManyCallback,
        *,
        buf_group: RecvBufferPool,
    ) -> ContinuousOperation[_RecvManyResult]:
        raise NotImplementedError

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> RecvBufferPool:
        raise NotImplementedError(f"{type(self).__name__} does not provide receive buffer pools")

    def _default_shared_recv_buffer_pool_sizes(self) -> tuple[int, int]:
        return _DEFAULT_RECVITER_BUFFER_SIZE, _DEFAULT_RECVITER_BUFFER_COUNT

    def shared_recv_buffer_pool(self) -> RecvBufferPool:
        """Return this proactor's lazy shared provided-buffer pool."""

        pool = self._shared_recv_buffer_pool
        if pool is None:
            buffer_size, buffer_count = self._default_shared_recv_buffer_pool_sizes()
            pool = self.create_recv_buffer_pool(buffer_size, buffer_count)
            self._shared_recv_buffer_pool = pool
        return pool

    def set_shared_recv_buffer_pool(self, pool: RecvBufferPool) -> None:
        """Replace this proactor's shared provided-buffer pool."""

        self._shared_recv_buffer_pool = pool

    def _clear_shared_recv_buffer_pool(self) -> None:
        self._shared_recv_buffer_pool = None

    def openat(self, path: str, flags: int, mode: int = 0, *, dfd: int = _DEFAULT_OPENAT_DFD) -> Operation[int]:
        raise NotImplementedError

    def read(self, fd: int, n: int, offset: int) -> Operation[bytes]:
        raise NotImplementedError

    def read_into(self, fd: int, buf: Any, offset: int) -> Operation[int]:
        raise NotImplementedError

    def write(self, fd: int, data: Any, offset: int) -> Operation[int]:
        raise NotImplementedError

    def stat(self, path: str = "", *, fd: int = -1) -> Operation[os.stat_result]:
        """Return file metadata, completing synchronously via ``os.stat`` / ``os.fstat``."""

        self._check_open()
        if fd < 0 and not path:
            raise ValueError("stat() requires fd >= 0 or a non-empty path")
        operation = Operation[os.stat_result](
            kind="stat",
            fileobj=fd if fd >= 0 else path,
        )
        try:
            if fd >= 0:
                operation._set_result(os.fstat(fd))
            else:
                operation._set_result(os.stat(path))
        except OSError as exc:
            operation._set_exception(exc)
        return operation

    def stat_fdsize(self, fd: int) -> Operation[int]:
        """Return the byte length of an open file descriptor."""

        self._check_open()
        if fd < 0:
            raise ValueError("stat_fdsize() requires fd >= 0")
        operation = Operation[int](kind="stat_fdsize", fileobj=fd)
        try:
            operation._set_result(os.fstat(fd).st_size)
        except OSError as exc:
            operation._set_exception(exc)
        return operation

    def poll(self, fd: int, mask: int) -> Operation[int]:
        raise NotImplementedError

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Callable[[int], object],
    ) -> ContinuousOperation[int]:
        raise NotImplementedError


@dataclass
class _SelectorRecvManyState:
    paused: bool = False
    pressure_emitted: bool = False


@dataclass
class _FdEntry:
    reader: Operation[Any] | ContinuousOperation[Any] | None = None
    writer: Operation[Any] | ContinuousOperation[Any] | None = None

    def empty(self) -> bool:
        return self.reader is None and self.writer is None


_UringEntryComplete = Callable[["UringProactor", "_UringEntry", "_UringCompletion"], Operation[Any] | None]
_UringEntrySubmit = Callable[[], _UringCompletion]


@dataclass
class _MultishotLegState:
    """Per-leg state for deferred multishot termination handling."""

    nonterminal_seen: int = 0
    pending_final: _UringCompletion | None = None
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


@dataclass
class _UringEntry:
    operation: Operation[Any]
    complete: _UringEntryComplete
    data: memoryview | None = None
    offset: int = 0
    progress: _ProgressCallback | None = None
    completion: _UringCompletion | None = None
    active: bool = True
    stream_sequence: int = 0
    resubmit: _UringEntrySubmit | None = None
    multishot_leg: _MultishotLegState | None = None
    buf_group: _UringBufGroup | None = None

    def completions_to_process(
        self,
        completion: _UringCompletion,
    ) -> tuple[_UringCompletion | None, _UringCompletion | None]:
        """Return which completions are ready for ``_complete_uring_operation``.

        Non-multishot completions are always returned as ``(completion, None)``.
        Multishot legs may defer a terminating completion (no ``F_MORE``) until
        every earlier non-terminating completion in the leg has been observed.
        When a deferred termination becomes ready, it is returned as the second
        element alongside the completion that unblocked it.
        """

        if not completion.multishot:
            return (completion, None)
        leg = self.multishot_leg
        assert leg is not None
        with leg.lock:
            if self.operation.done():
                leg.pending_final = None
                return (None, None)
            is_termination = not bool(completion.flags & uring_api.IORING_CQE_F_MORE)
            if is_termination:
                if leg.nonterminal_seen < completion.sequence:
                    leg.pending_final = completion
                    return (None, None)
                leg.pending_final = None
                return (completion, None)
            leg.nonterminal_seen += 1
            pending = leg.pending_final
            if pending is not None and leg.nonterminal_seen >= pending.sequence:
                leg.pending_final = None
                return (completion, pending)
            return (completion, None)


@dataclass
class _UringSubmission:
    entry: _UringEntry | None
    submit: _UringEntrySubmit


class SelectorProactor(ProactorBase):
    """Completion-oriented proactor prototype backed by a selector."""

    def __init__(
        self,
        selector: selectors.BaseSelector | None = None,
        *,
        completion_callback: _CompletionCallback | None = None,
    ) -> None:
        super().__init__(completion_callback=completion_callback)
        self._lock = threading.RLock()
        self._selector = selector if selector is not None else compat.released_default_selector()
        self._fd_operations: dict[int, _FdEntry] = {}
        self._wakeup_reader, self._wakeup_writer = socket.socketpair()
        self._wakeup_reader.setblocking(False)
        self._wakeup_writer.setblocking(False)
        self._selector.register(self._wakeup_reader.fileno(), selectors.EVENT_READ, None)
        self._recv_many_repressure_pending: set[ContinuousOperation[Any]] = set()

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> _SelectorBufGroup:
        """Create a synthetic provided-buffer pool for ``recv_many`` / ``sock_recv_iter``."""

        return _SelectorBufGroup(buffer_size, buffer_count)

    def create_buf_group(self, buffer_size: int, buffer_count: int) -> _SelectorBufGroup:
        return self.create_recv_buffer_pool(buffer_size, buffer_count)

    def _selector_recv_many_resume(
        self,
        operation: ContinuousOperation[_RecvManyResult],
        state: _SelectorRecvManyState,
        buf_group: _SelectorBufGroup,
    ) -> _RecvManyResume:
        def resume() -> None:
            if operation.done():
                return
            state.paused = False
            if buf_group.leased_count >= buf_group.buffer_count:
                state.pressure_emitted = False
                self._recv_many_repressure_pending.add(operation)
            else:
                self.break_wait()

        return resume

    def _service_recv_many_repressure_pending(self) -> list[Operation[Any]]:
        """Re-emit recv_many pressure deferred from a premature resume."""

        completed: list[Operation[Any]] = []
        for operation in list(self._recv_many_repressure_pending):
            self._recv_many_repressure_pending.discard(operation)
            if operation.done():
                continue
            fileobj = operation.fileobj
            if fileobj is None:
                continue
            fd = fileobj if isinstance(fileobj, int) else cast(socket.socket, fileobj).fileno()
            step = operation._continuous_step
            if step is None:
                continue
            self._step_continuous_fd_operation(fd, selectors.EVENT_READ, operation, completed)
        return completed

    def has_pending_operations(self) -> bool:
        """Return True if operations are waiting for backend completion."""

        with self._lock:
            return bool(self._fd_operations)

    def close(self) -> None:
        """Close selector and wakeup resources."""

        self._wake_selector()
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._clear_shared_recv_buffer_pool()
            self._selector.close()
            self._wakeup_reader.close()
            self._wakeup_writer.close()

    def break_wait(self) -> None:
        """Interrupt a thread blocked in `wait` without completing operations."""

        self._wake_selector()

    def _wake_selector(self) -> None:
        """Wake a thread blocked in the selector."""

        try:
            self._wakeup_writer.send(b"\0")
        except (BlockingIOError, OSError):
            pass

    def _after_selector_registration_changed(self) -> None:
        pass

    def wait(self, deadline: float | None = None) -> None:
        """Wait until `deadline` and drive ready operations."""

        with self._lock:
            self._check_open()
            completed = self._poll(deadline)
        if completed:
            self._notify_completion()

    def _poll(self, deadline: float | None = None) -> list[Operation[Any]]:
        completed = self._service_recv_many_repressure_pending()
        if completed:
            return completed
        select_released = getattr(self._selector, "select_released", None)
        wakeup_fd = self._wakeup_reader.fileno()
        while True:
            timeout = self._timeout_until_deadline(deadline)
            if select_released is None:
                events = self._selector.select(timeout)
            else:
                events = cast(compat.SelectReleasedSelector, self._selector).select_released(timeout, self._lock)
            completed: list[Operation[Any]] = []
            woke = False
            for key, mask in events:
                fd = key.fd
                if fd == wakeup_fd:
                    self._drain_wakeup()
                    woke = True
                    continue
                entry = self._fd_operations.get(fd)
                if entry is not None and entry.reader is not None and entry.reader is entry.writer:
                    if mask & (selectors.EVENT_READ | selectors.EVENT_WRITE):
                        self._step_fd_operation(fd, selectors.EVENT_READ, completed)
                    continue
                if mask & selectors.EVENT_READ:
                    self._step_fd_operation(fd, selectors.EVENT_READ, completed)
                if mask & selectors.EVENT_WRITE:
                    self._step_fd_operation(fd, selectors.EVENT_WRITE, completed)
            if completed or woke or timeout == 0 or not events:
                return completed

    async def wait_async(self, deadline: float | None = None) -> None:
        """Wait asynchronously until `deadline` and drive ready operations."""

        self._check_open()
        if deadline == 0:
            self.wait(0)
            return

        timeout = self._timeout_until_deadline(deadline)
        if timeout == 0:
            return

        loop = self._async_wait_loop
        assert loop is not None
        await loop.run_in_executor(None, self.wait, deadline)

    def recv(self, sock: socket.socket, n: int) -> Operation[bytes]:
        """Submit a socket receive operation."""

        operation = Operation[bytes](kind="recv", fileobj=sock, proactor=self)

        def attempt() -> bytes:
            return sock.recv(n)

        self._submit_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def recv_into(self, sock: socket.socket, buf: Any) -> Operation[int]:
        """Submit a socket receive-into operation."""

        operation = Operation[int](kind="recv_into", fileobj=sock, proactor=self)

        def attempt() -> int:
            return sock.recv_into(buf)

        self._submit_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def recvfrom(self, sock: socket.socket, bufsize: int) -> Operation[tuple[bytes, Any]]:
        """Submit a datagram receive operation."""

        operation = Operation[tuple[bytes, Any]](kind="recvfrom", fileobj=sock, proactor=self)

        def attempt() -> tuple[bytes, Any]:
            return sock.recvfrom(bufsize)

        self._submit_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> Operation[tuple[int, Any]]:
        """Submit a datagram receive-into operation."""

        operation = Operation[tuple[int, Any]](kind="recvfrom_into", fileobj=sock, proactor=self)

        def attempt() -> tuple[int, Any]:
            if nbytes:
                return sock.recvfrom_into(buf, nbytes)
            return sock.recvfrom_into(buf)

        self._submit_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def send(self, sock: socket.socket, data: Any) -> Operation[int]:
        """Submit a socket send operation."""

        operation = Operation[int](kind="send", fileobj=sock, proactor=self)

        def attempt() -> int:
            return sock.send(data)

        self._submit_socket_operation(sock, selectors.EVENT_WRITE, operation, attempt)
        return operation

    def sendto(self, sock: socket.socket, data: Any, address: Any) -> Operation[int]:
        """Submit a datagram send operation."""

        operation = Operation[int](kind="sendto", fileobj=sock, proactor=self)

        def attempt() -> int:
            return sock.sendto(data, address)

        self._submit_socket_operation(sock, selectors.EVENT_WRITE, operation, attempt)
        return operation

    def sendall(self, sock: socket.socket, data: Any, progress: _ProgressCallback | None = None) -> Operation[None]:
        """Submit a socket send-all operation."""

        operation = Operation[None](kind="sendall", fileobj=sock, proactor=self)
        view = memoryview(data)
        offset = 0

        def attempt() -> None:
            nonlocal offset
            while offset < len(view):
                sent = sock.send(view[offset:])
                if sent == 0:
                    raise BlockingIOError(errno.EWOULDBLOCK, "socket send returned zero bytes")
                offset += sent
                if progress is not None:
                    progress(offset)
            return None

        self._submit_socket_operation(sock, selectors.EVENT_WRITE, operation, attempt)
        return operation

    def accept(self, sock: socket.socket) -> Operation[tuple[socket.socket, Any]]:
        """Submit a socket accept operation."""

        operation = Operation[tuple[socket.socket, Any]](kind="accept", fileobj=sock, proactor=self)

        def attempt() -> tuple[socket.socket, Any]:
            conn, address = sock.accept()
            _configure_accepted_socket(conn)
            return conn, address

        self._submit_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def accept_many(
        self,
        sock: socket.socket,
        callback: Callable[[tuple[socket.socket, Any]], object],
    ) -> ContinuousOperation[tuple[socket.socket, Any]]:
        """Start accepting connections until the operation is cancelled or fails.

        `callback` may run on any backend worker thread.
        """

        operation = ContinuousOperation[tuple[socket.socket, Any]](
            kind="accept_many",
            fileobj=sock,
            proactor=self,
            result_callback=callback,
        )

        def step() -> ContinuousStepResult:
            progressed = False
            while True:
                try:
                    conn, address = sock.accept()
                except (BlockingIOError, InterruptedError):
                    return ContinuousStepResult(progressed=progressed)
                _configure_accepted_socket(conn)
                operation._emit_result((conn, address))
                progressed = True

        self._submit_socket_continuous_operation(sock, selectors.EVENT_READ, operation, step)
        return operation

    def connect(self, sock: socket.socket, address: Any) -> Operation[None]:
        """Submit a non-blocking socket connect operation."""

        operation = Operation[None](kind="connect", fileobj=sock, proactor=self)
        started = False

        def attempt() -> None:
            nonlocal started
            if not started:
                started = True
                try:
                    sock.connect(address)
                    return None
                except (BlockingIOError, InterruptedError):
                    raise BlockingIOError(errno.EINPROGRESS, "connect in progress") from None
                except OSError as exc:
                    if exc.errno in (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
                        raise BlockingIOError(exc.errno, exc.strerror) from None
                    raise
            err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err == 0:
                return None
            if err in (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
                raise BlockingIOError(err, errno.errorcode.get(err, "connect in progress"))
            raise OSError(err, errno.errorcode.get(err, "socket connect failed"))

        self._submit_socket_operation(sock, selectors.EVENT_WRITE, operation, attempt)
        return operation

    def recv_many(
        self,
        sock: socket.socket,
        callback: _RecvManyCallback,
        *,
        buf_group: RecvBufferPool,
    ) -> ContinuousOperation[_RecvManyResult]:
        """Start receiving byte chunks until EOF, cancellation, or failure.

        `callback` may run on any backend worker thread. Each result is an
        ordinal `(index, data)` pair with read-only `data` as a leased
        ``memoryview``; EOF emits a final empty view before completing the
        continuous operation. Chunk sizes follow the kernel; this implementation
        reads up to 8 KiB per ``recv()`` call. When the synthetic
        ``buf_group`` pool is exhausted, the callback also receives
        ``(RECV_MANY_BUFFER_PRESSURE, resume)``; call ``resume()`` after
        dropping held views to re-arm the next read.

        ``buf_group`` must be a provided-buffer pool from
        ``create_recv_buffer_pool()`` or ``shared_recv_buffer_pool()``.
        """

        operation = ContinuousOperation[_RecvManyResult](
            kind="recv_many",
            fileobj=sock,
            proactor=self,
            result_callback=callback,
        )
        sequence = 0

        # pre-3.12: no __release_buffer__, so leased views cannot return slots
        # automatically; keep the legacy drain loop and ignore buf_group pressure.
        if not _supports_release_buffer():

            def step_unpaced() -> ContinuousStepResult:
                nonlocal sequence
                progressed = False
                while True:
                    try:
                        data = sock.recv(_DEFAULT_SELECTOR_RECV_MANY_CHUNK_SIZE)
                    except (BlockingIOError, InterruptedError):
                        return ContinuousStepResult(progressed=progressed)
                    if not data:
                        operation._emit_result((sequence, memoryview(b"")))
                        sequence += 1
                        return ContinuousStepResult(progressed=True, done=True)
                    operation._emit_result((sequence, memoryview(data)))
                    sequence += 1
                    progressed = True

            self._submit_socket_continuous_operation(sock, selectors.EVENT_READ, operation, step_unpaced)
            return operation

        # paced path: one chunk per step; pool exhaustion yields RECV_MANY_BUFFER_PRESSURE.
        resolved_group = cast(_SelectorBufGroup, buf_group)
        state = _SelectorRecvManyState()

        def step() -> ContinuousStepResult:
            nonlocal sequence
            if state.paused:
                return ContinuousStepResult(progressed=False)
            if resolved_group.leased_count >= resolved_group.buffer_count:
                if not state.pressure_emitted:
                    state.pressure_emitted = True
                    state.paused = True
                    operation._emit_result(
                        (
                            RECV_MANY_BUFFER_PRESSURE,
                            self._selector_recv_many_resume(operation, state, resolved_group),
                        )
                    )
                    return ContinuousStepResult(progressed=True)
                return ContinuousStepResult(progressed=False)
            try:
                data = sock.recv(_DEFAULT_SELECTOR_RECV_MANY_CHUNK_SIZE)
            except (BlockingIOError, InterruptedError):
                return ContinuousStepResult(progressed=False)
            if not data:
                operation._emit_result((sequence, memoryview(b"")))
                sequence += 1
                return ContinuousStepResult(progressed=True, done=True)
            operation._emit_result((sequence, _leased_selector_memoryview(data, resolved_group)))
            sequence += 1
            state.pressure_emitted = False
            return ContinuousStepResult(progressed=True)

        self._submit_socket_continuous_operation(sock, selectors.EVENT_READ, operation, step)
        return operation

    def poll(self, fd: int, mask: int) -> Operation[int]:
        """Wait until an fd reports the requested poll events."""

        operation = Operation[int](kind="poll", fileobj=fd, proactor=self)

        def attempt() -> int:
            return _probe_poll_fd_now(fd, mask)

        self._submit_fd_operation(fd, mask, operation, attempt)
        return operation

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Callable[[int], object],
    ) -> ContinuousOperation[int]:
        """Emit poll event masks whenever the fd becomes ready.

        `callback` may run on any backend worker thread.
        """

        operation = ContinuousOperation[int](
            kind="poll_many",
            fileobj=fd,
            proactor=self,
            result_callback=callback,
        )

        def step() -> ContinuousStepResult:
            try:
                result = _probe_poll_fd_now(fd, mask)
            except BlockingIOError:
                return ContinuousStepResult(progressed=False)
            operation._emit_result(result)
            return ContinuousStepResult(progressed=True)

        self._submit_fd_continuous_operation(fd, mask, operation, step)
        return operation

    def _submit_fd_operation(
        self,
        fd: int,
        poll_mask: int,
        operation: Operation[T],
        attempt: Callable[[], T],
    ) -> None:
        with self._lock:
            self._check_open()
            self._check_fd(fd)
            selector_events = _poll_mask_to_selector_events(poll_mask)
            if selector_events & selectors.EVENT_READ:
                self._check_fd_operation_available(fd, selectors.EVENT_READ)
            if selector_events & selectors.EVENT_WRITE:
                self._check_fd_operation_available(fd, selectors.EVENT_WRITE)
            if self._try_complete_operation(operation, attempt):
                return
            self._reserve_fd_poll_operation(fd, selector_events, operation)
            operation._attempt = attempt
            self._update_selector_registration(fd)
        self._after_selector_registration_changed()

    def _submit_fd_continuous_operation(
        self,
        fd: int,
        poll_mask: int,
        operation: ContinuousOperation[T],
        step: Callable[[], ContinuousStepResult],
    ) -> None:
        with self._lock:
            self._check_open()
            self._check_fd(fd)
            selector_events = _poll_mask_to_selector_events(poll_mask)
            if selector_events & selectors.EVENT_READ:
                self._check_fd_operation_available(fd, selectors.EVENT_READ)
            if selector_events & selectors.EVENT_WRITE:
                self._check_fd_operation_available(fd, selectors.EVENT_WRITE)
            self._reserve_fd_poll_operation(fd, selector_events, operation)
            operation._continuous_step = step
            if self._try_step_continuous_operation(fd, operation, step):
                return
            self._update_selector_registration(fd)
        self._after_selector_registration_changed()

    def _try_step_continuous_operation(
        self,
        fd: int,
        operation: ContinuousOperation[T],
        step: Callable[[], ContinuousStepResult],
    ) -> bool:
        """Run one continuous step synchronously. Return True if the operation finished."""

        try:
            step_result = step()
        except (BlockingIOError, InterruptedError):
            return False
        except BaseException as exc:
            self._remove_operation(operation)
            operation._set_exception(exc)
            return True
        if step_result.done:
            self._remove_operation(operation)
            operation._set_result(None)
            return True
        if step_result.progressed:
            self._update_selector_registration(fd)
        return False

    def _reserve_fd_poll_operation(self, fd: int, selector_events: int, operation: Operation[Any]) -> None:
        entry = self._fd_operations.setdefault(fd, _FdEntry())
        if selector_events & selectors.EVENT_READ:
            entry.reader = operation
        if selector_events & selectors.EVENT_WRITE:
            entry.writer = operation

    def _submit_socket_operation(
        self,
        sock: socket.socket,
        event: int,
        operation: Operation[T],
        attempt: Callable[[], T],
    ) -> None:
        with self._lock:
            self._check_open()
            self._check_socket(sock)
            fd = sock.fileno()
            self._check_fd_operation_available(fd, event)
            if self._try_complete_operation(operation, attempt):
                return
            self._reserve_fd_operation(fd, event, operation)
            operation._attempt = attempt
            self._update_selector_registration(fd)
        self._after_selector_registration_changed()

    def _submit_socket_continuous_operation(
        self,
        sock: socket.socket,
        event: int,
        operation: ContinuousOperation[T],
        step: Callable[[], ContinuousStepResult],
    ) -> None:
        with self._lock:
            self._check_open()
            self._check_socket(sock)
            fd = sock.fileno()
            self._check_fd_operation_available(fd, event)
            self._reserve_fd_operation(fd, event, operation)
            operation._continuous_step = step
            self._update_selector_registration(fd)
        self._after_selector_registration_changed()

    def _try_complete_operation(self, operation: Operation[T], attempt: Callable[[], T]) -> bool:
        try:
            result = attempt()
        except (BlockingIOError, InterruptedError):
            return False
        except BaseException as exc:
            operation._set_exception(exc)
        else:
            operation._set_result(result)
        return True

    def _check_fd_operation_available(self, fd: int, event: int) -> None:
        entry = self._fd_operations.get(fd)
        if entry is None:
            return
        current = entry.reader if event == selectors.EVENT_READ else entry.writer
        if current is not None:
            raise RuntimeError("an operation is already pending for this fd and direction")

    def _reserve_fd_operation(self, fd: int, event: int, operation: Operation[Any]) -> None:
        self._check_fd_operation_available(fd, event)
        entry = self._fd_operations.setdefault(fd, _FdEntry())
        if event == selectors.EVENT_READ:
            entry.reader = operation
        else:
            entry.writer = operation

    def cancel_operation(self, operation: Operation[Any]) -> None:
        with self._lock:
            removed = self._remove_operation(operation)
        if not removed:
            return
        operation._set_cancelled()
        self._after_selector_registration_changed()

    def _remove_operation(self, operation: Operation[Any]) -> bool:
        for fd, entry in list(self._fd_operations.items()):
            removed = False
            if entry.reader is operation:
                entry.reader = None
                removed = True
            if entry.writer is operation:
                entry.writer = None
                removed = True
            if removed:
                if entry.empty():
                    del self._fd_operations[fd]
                self._update_selector_registration(fd)
                return True
        return False

    def _step_fd_operation(self, fd: int, event: int, completed: list[Operation[Any]]) -> None:
        entry = self._fd_operations.get(fd)
        if entry is None:
            return
        operation = entry.reader if event == selectors.EVENT_READ else entry.writer
        if operation is None or operation.done():
            return
        if isinstance(operation, ContinuousOperation):
            self._step_continuous_fd_operation(fd, event, operation, completed)
            return
        attempt = cast(Callable[[], Any], operation._attempt)
        assert attempt is not None
        try:
            result = attempt()
        except (BlockingIOError, InterruptedError):
            self._update_selector_registration(fd)
            return
        except BaseException as exc:
            self._remove_operation(operation)
            operation._set_exception(exc)
        else:
            self._remove_operation(operation)
            operation._set_result(result)
        completed.append(operation)

    def _step_continuous_fd_operation(
        self,
        fd: int,
        event: int,
        operation: ContinuousOperation[Any],
        completed: list[Operation[Any]],
    ) -> None:
        step = operation._continuous_step
        assert step is not None
        try:
            step_result = step()
        except (BlockingIOError, InterruptedError):
            self._update_selector_registration(fd)
            return
        except BaseException as exc:
            self._remove_operation(operation)
            operation._set_exception(exc)
            completed.append(operation)
            return
        if step_result.done:
            self._remove_operation(operation)
            operation._set_result(None)
        else:
            self._update_selector_registration(fd)
        if step_result.progressed or step_result.done:
            completed.append(operation)

    def _selector_mask_for_fd(self, fd: int) -> int:
        entry = self._fd_operations.get(fd)
        if entry is None:
            return 0
        mask = 0
        if entry.reader is not None:
            mask |= selectors.EVENT_READ
        if entry.writer is not None:
            mask |= selectors.EVENT_WRITE
        return mask

    def _update_selector_registration(self, fd: int) -> None:
        if self._closed:
            return
        mask = self._selector_mask_for_fd(fd)
        try:
            self._selector.get_key(fd)
        except KeyError:
            if mask:
                self._selector.register(fd, mask, fd)
            return
        if mask:
            self._selector.modify(fd, mask, fd)
            return
        try:
            self._selector.unregister(fd)
        except (KeyError, ValueError, OSError):
            pass

    def _drain_wakeup(self) -> None:
        while True:
            try:
                if not self._wakeup_reader.recv(4096):
                    return
            except BlockingIOError:
                return
            except OSError:
                return

    def _check_socket(self, sock: socket.socket) -> None:
        if sock.getblocking():
            raise ValueError("socket must be non-blocking")
        if sock.fileno() < 0:
            raise ValueError("socket is closed")

    def _check_fd(self, fd: int) -> None:
        if fd < 0:
            raise ValueError("fd is closed")


class ThreadedSelectorProactor(SelectorProactor):
    """Selector proactor that polls readiness from a worker thread."""

    def __init__(
        self,
        selector: selectors.BaseSelector | None = None,
        *,
        completion_callback: _CompletionCallback | None = None,
    ) -> None:
        if selector is None:
            selector = compat.released_default_selector()
        elif not hasattr(selector, "select_released"):
            raise TypeError("ThreadedSelectorProactor requires a selector with select_released()")
        super().__init__(selector, completion_callback=completion_callback)
        self._completed_ready = threading.Event()
        self._worker_started = False
        self._worker_stop = threading.Event()
        self._worker = threading.Thread(target=self._worker_main, name="tealetio-selector-proactor", daemon=True)

    def close(self) -> None:
        """Stop the worker thread and close selector resources."""

        self._worker_stop.set()
        self._completed_ready.set()
        self._wake_selector()
        if self._closed:
            return
        if self._worker_started and threading.current_thread() is not self._worker:
            self._worker.join()
        super().close()

    def break_wait(self) -> None:
        """Interrupt a thread blocked in `wait` without completing operations."""

        self._completed_ready.set()

    def _after_selector_registration_changed(self) -> None:
        self._wake_selector()

    def wait(self, deadline: float | None = None) -> None:
        """Wait until completed operations are signalled."""

        self._check_open()
        self._ensure_worker_started()
        if deadline == 0:
            self._completed_ready.clear()
            return

        timeout = self._timeout_until_deadline(deadline)
        if timeout == 0:
            return
        self._wait_for_completed(timeout)
        self._completed_ready.clear()

    async def wait_async(self, deadline: float | None = None) -> None:
        """Wait asynchronously until completed operations are signalled."""

        self._check_open()
        loop = self._async_wait_loop
        assert loop is not None
        self._ensure_worker_started()
        if deadline == 0:
            return

        timeout = self._timeout_until_deadline(deadline)
        if timeout == 0:
            return
        await loop.run_in_executor(None, self._wait_for_completed, timeout)
        self._completed_ready.clear()

    def _notify_completed(self) -> None:
        self._completed_ready.set()
        self._notify_completion()

    def _ensure_worker_started(self) -> None:
        with self._lock:
            if self._worker_started:
                return
            self._worker_started = True
            self._worker.start()

    def _worker_main(self) -> None:
        while not self._worker_stop.is_set():
            try:
                with self._lock:
                    completed = self._poll(None)
            except (OSError, ValueError, RuntimeError):
                return
            if completed:
                self._notify_completed()

    def _wait_for_completed(self, timeout: float | None) -> None:
        self._completed_ready.wait(timeout)


class UringProactor(ProactorBase):
    """io_uring-backed proactor using Python-owned completion service threads."""

    def __init__(
        self,
        entries: int = 8,
        flags: int = 0,
        *,
        completion_callback: _CompletionCallback | None = None,
        ring_factory: _UringRingFactory | None = None,
        completion_threads: int = _DEFAULT_URING_COMPLETION_THREADS,
        completion_thread_nice: int | None = _DEFAULT_URING_COMPLETION_THREAD_NICE,
    ) -> None:
        if completion_threads <= 0:
            raise ValueError("completion_threads must be at least 1")
        if ring_factory is None:
            ring_factory = _default_uring_ring_factory
        super().__init__(completion_callback=completion_callback)
        self._ring = ring_factory(entries, flags)
        try:
            self._capabilities = uring_api.probe(entries=entries, flags=flags)
        except (OSError, RuntimeError, NotImplementedError):
            self._capabilities = {}
        self._submit_send: _UringSendSubmit = self._ring.submit_send
        if self._capabilities.get("IORING_OP_SEND_ZC", False):
            self._submit_send = self._ring.submit_send_zc
        self._submit_sendto: _UringSendtoSubmit = self._ring.submit_sendto
        if self._capabilities.get("IORING_OP_SENDMSG_ZC", False):
            self._submit_sendto = self._ring.submit_sendmsg_zc
        # continuous *many ops prefer kernel multishot when probed; otherwise they
        # emulate the stream by resubmitting the matching one-shot opcode after
        # each completion (see the *_oneshot delivery handlers below).
        self._completion_thread_nice = completion_thread_nice
        self._pending_tokens: list[None] = []
        self._deferred_submissions: list[_UringSubmission] = []
        self._retrying_deferred_submissions = False
        self._wait_ready = threading.Event()
        self._async_wait_thread_id: int | None = None
        self._async_wait_event: _asyncio.Event | None = None
        self._ring.callback = self._deliver_uring_completion
        self._service_threads = [
            threading.Thread(target=self._service_thread_main, name=f"tealetio-uring-{index}")
            for index in range(completion_threads)
        ]
        try:
            for thread in self._service_threads:
                thread.start()
            self._wait_until_service_started()
        except BaseException:
            self._ring.stop_serving()
            for thread in self._service_threads:
                if thread.is_alive():
                    thread.join()
            self._ring.callback = None
            self._ring.close()
            raise

    @property
    def ring(self) -> _UringRing:
        """Return the low-level `uring_api.Ring` object owned by this proactor."""

        return self._ring

    @property
    def capabilities(self) -> dict[str, bool]:
        """Return the io_uring capability probe for this proactor's ring parameters.

        Populated once at construction from ``uring_api.probe(entries=..., flags=...)``.
        """

        return dict(self._capabilities)

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> _UringBufGroup:
        """Create a provided-buffer group for ``recv_many`` / ``sock_recv_iter``."""

        return self._ring.create_buf_group(buffer_size, buffer_count)

    def create_buf_group(self, buffer_size: int, buffer_count: int) -> _UringBufGroup:
        return self.create_recv_buffer_pool(buffer_size, buffer_count)

    def _default_shared_recv_buffer_pool_sizes(self) -> tuple[int, int]:
        return _DEFAULT_URING_RECV_MANY_BUFFER_SIZE, _DEFAULT_URING_RECV_MANY_BUFFER_COUNT

    def _recv_many_resume_callable(self, entry: _UringEntry) -> _RecvManyResume:
        def resume() -> None:
            if entry.operation.done() or entry.resubmit is None or entry.active:
                return
            self._queue_entry_resubmit(entry)
            self._retry_deferred_submissions()

        return resume

    def _service_thread_main(self) -> None:
        self._apply_completion_thread_nice()
        self._ring.serve_completions()

    def _apply_completion_thread_nice(self) -> None:
        nice = self._completion_thread_nice
        if nice is None or not hasattr(os, "setpriority"):
            return
        try:
            os.setpriority(os.PRIO_PROCESS, 0, nice)
        except (AttributeError, OSError, PermissionError, ValueError):
            return

    def _wait_until_service_started(self) -> None:
        deadline = time.monotonic() + 1.0
        while (
            not self._ring.running
            and any(thread.is_alive() for thread in self._service_threads)
            and time.monotonic() < deadline
        ):
            time.sleep(0.001)
        if not self._ring.running:
            raise RuntimeError("uring completion service failed to start")

    def has_pending_operations(self) -> bool:
        """Return True if operations are waiting for backend completion."""

        return bool(self._pending_tokens or self._deferred_submissions)

    def close(self) -> None:
        """Close the owned `io_uring` ring."""

        if self._closed:
            return
        self._closed = True
        self._clear_shared_recv_buffer_pool()
        self._ring.stop_serving()
        for thread in self._service_threads:
            thread.join()
        self._pending_tokens.clear()
        self._deferred_submissions.clear()
        self.break_wait()
        self._ring.callback = None
        self._ring.close()

    def break_wait(self) -> None:
        """Interrupt a thread blocked in `wait` without completing operations."""

        self._wait_ready.set()
        loop = self._async_wait_loop
        event = self._async_wait_event
        if event is None:
            return
        if loop is None or self._async_wait_thread_id == threading.get_ident():
            event.set()
            return
        try:
            loop.call_soon_threadsafe(event.set)
        except RuntimeError:
            pass

    def bind_loop(self, loop: _asyncio.AbstractEventLoop) -> None:
        """Bind this proactor to an asyncio event loop for async waits."""

        if self._async_wait_loop is None:
            super().bind_loop(loop)
            self._async_wait_thread_id = threading.get_ident()
            self._async_wait_event = _asyncio.Event()
            return
        super().bind_loop(loop)

    def wait(self, deadline: float | None = None) -> None:
        """Wait until completed operations are signalled."""

        self._check_open()
        if deadline == 0:
            return

        timeout = self._timeout_until_deadline(deadline)
        if timeout == 0:
            return
        self._wait_for_completed(timeout)

    async def wait_async(self, deadline: float | None = None) -> None:
        """Wait asynchronously until completed operations are signalled."""

        self._check_open()
        if deadline == 0:
            return

        timeout = self._timeout_until_deadline(deadline)
        if timeout == 0:
            return
        event = self._async_wait_event
        assert event is not None
        if event.is_set() or self._wait_ready.is_set():
            event.clear()
            self._wait_ready.clear()
            return
        woken = False
        try:
            if timeout is None:
                await event.wait()
            else:
                await compat.wait_for_timeout(event.wait(), timeout)
            woken = True
        except _asyncio.TimeoutError:
            return
        finally:
            if woken:
                event.clear()
                self._wait_ready.clear()

    def _notify_completed(self) -> None:
        self.break_wait()
        self._notify_completion()

    def _wait_for_completed(self, timeout: float | None) -> None:
        if self._wait_ready.wait(timeout):
            self._wait_ready.clear()

    def recv(self, sock: socket.socket, n: int) -> Operation[bytes]:
        """Submit a socket receive operation."""

        operation = Operation[bytes](kind="recv", fileobj=sock, proactor=self)
        data = memoryview(bytearray(n))
        entry = _UringEntry(operation=operation, complete=UringProactor._complete_uring_recv, data=data)
        self._submit_uring_entry(entry, lambda: self._ring.submit_recv(sock.fileno(), data, entry))
        return operation

    def _complete_uring_recv(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[bytes]:
        assert entry.data is not None
        operation = cast(Operation[bytes], entry.operation)
        operation._set_result(entry.data[: completion.res].tobytes())
        return operation

    def recv_into(self, sock: socket.socket, buf: Any) -> Operation[int]:
        """Submit a socket receive-into operation."""

        operation = Operation[int](kind="recv_into", fileobj=sock, proactor=self)
        entry = _UringEntry(operation=operation, complete=UringProactor._complete_uring_recv_into, data=memoryview(buf))
        self._submit_uring_entry(entry, lambda: self._ring.submit_recv(sock.fileno(), buf, entry))
        return operation

    def _complete_uring_recv_into(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], entry.operation)
        operation._set_result(completion.res)
        return operation

    def recvfrom(self, sock: socket.socket, bufsize: int) -> Operation[tuple[bytes, Any]]:
        """Submit a datagram receive operation."""

        operation = Operation[tuple[bytes, Any]](kind="recvfrom", fileobj=sock, proactor=self)
        data = memoryview(bytearray(bufsize))
        self._submit_recvmsg(sock, operation, data, UringProactor._complete_uring_recvfrom)
        return operation

    def _complete_uring_recvfrom(
        self, entry: _UringEntry, completion: _UringCompletion
    ) -> Operation[tuple[bytes, Any]]:
        assert entry.data is not None
        operation = cast(Operation[tuple[bytes, Any]], entry.operation)
        operation._set_result((entry.data[: completion.res].tobytes(), completion.result))
        return operation

    def recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> Operation[tuple[int, Any]]:
        """Submit a datagram receive-into operation."""

        operation = Operation[tuple[int, Any]](kind="recvfrom_into", fileobj=sock, proactor=self)
        data = memoryview(buf)
        if nbytes < 0:
            raise ValueError("negative buffersize in recvfrom_into")
        if nbytes > len(data):
            raise ValueError("nbytes is greater than the length of the buffer")
        if nbytes:
            data = data[:nbytes]
        self._submit_recvmsg(sock, operation, data, UringProactor._complete_uring_recvfrom_into)
        return operation

    def _complete_uring_recvfrom_into(
        self,
        entry: _UringEntry,
        completion: _UringCompletion,
    ) -> Operation[tuple[int, Any]]:
        operation = cast(Operation[tuple[int, Any]], entry.operation)
        operation._set_result((completion.res, completion.result))
        return operation

    def sendall(self, sock: socket.socket, data: Any, progress: _ProgressCallback | None = None) -> Operation[None]:
        """Submit a socket send-all operation."""

        operation = Operation[None](kind="sendall", fileobj=sock, proactor=self)
        payload = memoryview(data)
        if not payload:
            self._check_open()
            operation._set_result(None)
            return operation
        self._submit_sendall(sock, operation, payload, 0, progress)
        return operation

    def _complete_uring_sendall(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[None] | None:
        operation = cast(Operation[None], entry.operation)
        res = completion.res
        if res == 0:
            operation._set_exception(BlockingIOError(errno.EWOULDBLOCK, "socket send returned zero bytes"))
            return operation
        assert entry.data is not None
        offset = entry.offset + res
        if entry.progress is not None:
            try:
                entry.progress(offset)
            except BaseException as exc:
                operation._set_exception(exc)
                return operation
        if offset >= len(entry.data):
            operation._set_result(None)
            return operation
        sock = cast(socket.socket, operation.fileobj)
        self._submit_sendall(sock, operation, entry.data, offset, entry.progress)
        return None

    def sendto(self, sock: socket.socket, data: Any, address: Any) -> Operation[int]:
        """Submit a datagram send operation."""

        operation = Operation[int](kind="sendto", fileobj=sock, proactor=self)
        payload = memoryview(data)
        entry = _UringEntry(operation=operation, complete=UringProactor._complete_uring_sendto, data=payload)
        self._submit_uring_entry(entry, lambda: self._submit_sendto(sock.fileno(), payload, address, entry))
        return operation

    def _complete_uring_sendto(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], entry.operation)
        operation._set_result(completion.res)
        return operation

    def accept(self, sock: socket.socket) -> Operation[tuple[socket.socket, Any]]:
        """Submit a socket accept operation."""

        operation = Operation[tuple[socket.socket, Any]](kind="accept", fileobj=sock, proactor=self)
        entry = _UringEntry(operation=operation, complete=UringProactor._complete_uring_accept)
        self._submit_uring_entry(entry, lambda: self._ring.submit_accept(sock.fileno(), entry, _DEFAULT_ACCEPT_FLAGS))
        return operation

    def _complete_uring_accept(
        self, entry: _UringEntry, completion: _UringCompletion
    ) -> Operation[tuple[socket.socket, Any]]:
        fd, address = cast(tuple[int, Any], completion.result)
        conn = socket.socket(fileno=fd)
        _configure_accepted_socket(conn)
        operation = cast(Operation[tuple[socket.socket, Any]], entry.operation)
        operation._set_result((conn, address))
        return operation

    def accept_many(
        self,
        sock: socket.socket,
        callback: Callable[[tuple[socket.socket, Any]], object],
    ) -> ContinuousOperation[tuple[socket.socket, Any]]:
        """Start a continuous accept operation.

        Uses multishot accept when the runtime probe accepts it; otherwise
        resubmits one-shot ``submit_accept()`` after each connection. `callback`
        may run on any uring completion service thread.
        """

        operation = ContinuousOperation[tuple[socket.socket, Any]](
            kind="accept_many",
            fileobj=sock,
            proactor=self,
            result_callback=callback,
        )
        if self._capabilities.get("IORING_ACCEPT_MULTISHOT", False):
            # one multishot accept stays armed until F_MORE clears or we cancel.
            entry = _UringEntry(
                operation=operation,
                complete=UringProactor._deliver_uring_accept_many,
                multishot_leg=_MultishotLegState(),
            )
            self._submit_uring_entry(
                entry,
                lambda: self._ring.submit_accept_multishot(sock.fileno(), entry, _DEFAULT_ACCEPT_FLAGS),
            )
            return operation

        # fallback: accept one connection, emit it, queue another submit_accept().
        entry = _UringEntry(operation=operation, complete=UringProactor._deliver_uring_accept_many_oneshot)

        def submit_accept() -> _UringCompletion:
            return self._ring.submit_accept(sock.fileno(), entry, _DEFAULT_ACCEPT_FLAGS)

        entry.resubmit = submit_accept
        self._submit_uring_entry(entry, submit_accept)
        return operation

    def _deliver_uring_accept_many_oneshot(
        self,
        entry: _UringEntry,
        completion: _UringCompletion,
    ) -> Operation[Any] | None:
        # one-shot accept completes per connection; re-arm via the deferred queue.
        operation = cast(ContinuousOperation[tuple[socket.socket, Any]], entry.operation)
        res = completion.res
        if res < 0:
            self._deactivate_uring_entry(entry)
            operation._set_exception(OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")))
            return operation
        fd, address = cast(tuple[int, Any], completion.result)
        conn = socket.socket(fileno=fd)
        _configure_accepted_socket(conn)
        operation._emit_result((conn, address))
        if operation.done():
            return operation
        self._queue_entry_resubmit(entry)
        return None

    def _deliver_uring_accept_many(
        self,
        entry: _UringEntry,
        completion: _UringCompletion,
    ) -> Operation[Any] | None:
        operation = cast(ContinuousOperation[tuple[socket.socket, Any]], entry.operation)
        res = completion.res
        if res < 0:
            self._deactivate_uring_entry(entry)
            operation._set_exception(OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")))
            return operation
        fd, address = cast(tuple[int, Any], completion.result)
        conn = socket.socket(fileno=fd)
        _configure_accepted_socket(conn)
        operation._emit_result((conn, address))
        if not completion.flags & uring_api.IORING_CQE_F_MORE:
            operation._set_result(None)
            self._deactivate_uring_entry(entry)
        return operation

    def connect(self, sock: socket.socket, address: Any) -> Operation[None]:
        """Submit a non-blocking socket connect operation."""

        operation = Operation[None](kind="connect", fileobj=sock, proactor=self)
        entry = _UringEntry(operation=operation, complete=UringProactor._complete_uring_connect)
        self._submit_uring_entry(entry, lambda: self._ring.submit_connect(sock.fileno(), address, entry))
        return operation

    def _complete_uring_connect(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[None]:
        operation = cast(Operation[None], entry.operation)
        operation._set_result(None)
        return operation

    def openat(self, path: str, flags: int, mode: int = 0, *, dfd: int = _DEFAULT_OPENAT_DFD) -> Operation[int]:
        """Submit an io_uring openat operation and return the opened fd on success."""

        operation = Operation[int](kind="openat", fileobj=path, proactor=self)
        entry = _UringEntry(operation=operation, complete=UringProactor._complete_uring_openat)
        self._submit_uring_entry(entry, lambda: self._ring.submit_openat(path, flags, mode, entry, dfd=dfd))
        return operation

    def _complete_uring_openat(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], entry.operation)
        operation._set_result(completion.res)
        return operation

    def read(self, fd: int, n: int, offset: int) -> Operation[bytes]:
        """Submit a positioned file read that completes with the bytes read."""

        operation = Operation[bytes](kind="read", fileobj=fd, proactor=self)
        data = memoryview(bytearray(n))
        entry = _UringEntry(operation=operation, complete=UringProactor._complete_uring_read, data=data)
        self._submit_uring_entry(entry, lambda: self._ring.submit_read(fd, data, offset, entry))
        return operation

    def _complete_uring_read(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[bytes]:
        assert entry.data is not None
        operation = cast(Operation[bytes], entry.operation)
        operation._set_result(entry.data[: completion.res].tobytes())
        return operation

    def read_into(self, fd: int, buf: Any, offset: int) -> Operation[int]:
        """Submit a positioned file read into a caller-provided buffer."""

        operation = Operation[int](kind="read_into", fileobj=fd, proactor=self)
        entry = _UringEntry(operation=operation, complete=UringProactor._complete_uring_read_into, data=memoryview(buf))
        self._submit_uring_entry(entry, lambda: self._ring.submit_read(fd, buf, offset, entry))
        return operation

    def _complete_uring_read_into(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], entry.operation)
        operation._set_result(completion.res)
        return operation

    def write(self, fd: int, data: Any, offset: int) -> Operation[int]:
        """Submit a positioned file write and return the byte count written."""

        operation = Operation[int](kind="write", fileobj=fd, proactor=self)
        payload = memoryview(data)
        entry = _UringEntry(operation=operation, complete=UringProactor._complete_uring_write, data=payload)
        self._submit_uring_entry(entry, lambda: self._ring.submit_write(fd, payload, offset, entry))
        return operation

    def _complete_uring_write(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], entry.operation)
        operation._set_result(completion.res)
        return operation

    def stat(self, path: str = "", *, fd: int = -1) -> Operation[os.stat_result]:
        """Return file metadata via io_uring statx when probed, else blocking ``os.stat``."""

        self._check_open()
        if fd < 0 and not path:
            raise ValueError("stat() requires fd >= 0 or a non-empty path")
        if not self._capabilities.get("IORING_OP_STATX", False) or not hasattr(self._ring, "submit_statx"):
            return super().stat(path, fd=fd)

        operation = Operation[os.stat_result](
            kind="stat",
            fileobj=fd if fd >= 0 else path,
            proactor=self,
        )
        buf = bytearray(uring_api.STATX_BUFFER_SIZE)
        if fd >= 0:
            dfd = fd
            stat_path = ""
            stat_flags = uring_api.AT_EMPTY_PATH
        else:
            dfd = uring_api.AT_FDCWD
            stat_path = path
            stat_flags = 0
        entry = _UringEntry(operation=operation, complete=UringProactor._complete_uring_stat, data=memoryview(buf))
        self._submit_uring_entry(
            entry,
            lambda: self._ring.submit_statx(
                dfd,
                stat_path,
                stat_flags,
                uring_api.STATX_BASIC_STATS,
                buf,
                entry,
            ),
        )
        return operation

    def _complete_uring_stat(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[os.stat_result]:
        assert entry.data is not None
        operation = cast(Operation[os.stat_result], entry.operation)
        try:
            operation._set_result(_stat_result_from_statx(entry.data))
        except ValueError as exc:
            operation._set_exception(exc)
        return operation

    def stat_fdsize(self, fd: int) -> Operation[int]:
        """Return file byte length via io_uring statx_fdsize when probed, else blocking ``os.fstat``.

        When statx_fdsize completes without a parsed size, the completion handler
        falls back to blocking ``os.fstat`` on the uring completion thread. That
        path should be rare; the blocking submit-time fallback via ``super()`` is
        used when statx is unavailable.
        """

        self._check_open()
        if fd < 0:
            raise ValueError("stat_fdsize() requires fd >= 0")
        if not self._capabilities.get("IORING_OP_STATX", False) or not hasattr(self._ring, "submit_statx_fdsize"):
            return super().stat_fdsize(fd)

        operation = Operation[int](kind="stat_fdsize", fileobj=fd, proactor=self)
        entry = _UringEntry(operation=operation, complete=UringProactor._complete_uring_stat_fdsize)
        self._submit_uring_entry(entry, lambda: self._ring.submit_statx_fdsize(fd, entry))
        return operation

    def _complete_uring_stat_fdsize(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[int]:
        # Rare statx_fdsize parse miss: recover with blocking fstat on this thread.
        operation = cast(Operation[int], entry.operation)
        size = completion.result
        if size is None:
            try:
                operation._set_result(os.fstat(cast(int, operation.fileobj)).st_size)
            except OSError as exc:
                operation._set_exception(exc)
            return operation
        operation._set_result(cast(int, size))
        return operation

    def recv_many(
        self,
        sock: socket.socket,
        callback: _RecvManyCallback,
        *,
        buf_group: RecvBufferPool,
    ) -> ContinuousOperation[_RecvManyResult]:
        """Start a continuous receive operation that completes on EOF.

        `callback` may run on any uring completion service thread.

        When multishot provided-buffer receive is available, each result is an
        ordinal `(index, data)` pair with read-only `data` as a `memoryview`
        into a leased kernel buffer. Callback delivery may arrive out of order
        across completion threads; consumers that need stream order must
        reorder by index themselves. Chunk sizes come from the operation's
        ``BufGroup`` pool. Holding live views can pin provided buffers and
        stall further receives. When the pool is exhausted the backend emits
        ``(RECV_MANY_BUFFER_PRESSURE, resume)`` where ``resume()`` re-arms
        multishot receive; consumers choose when to call it.

        When multishot receive is unavailable, the proactor falls back to
        repeated one-shot ``submit_recv()`` into a reused buffer. Chunks are
        independent ``memoryview`` objects over copied bytes (not leased
        ``BufView`` results), chunk size is up to 8 KiB, indices stay in-order,
        and ``RECV_MANY_BUFFER_PRESSURE`` is never emitted.

        EOF always emits a final empty view before completing the operation.

        ``buf_group`` must be a provided-buffer pool from
        ``create_recv_buffer_pool()`` or ``shared_recv_buffer_pool()``.
        """

        operation = ContinuousOperation[_RecvManyResult](
            kind="recv_many",
            fileobj=sock,
            proactor=self,
            result_callback=callback,
        )
        if self._capabilities.get("IORING_RECV_MULTISHOT", False):
            uring_group = cast(_UringBufGroup, buf_group)
            # provided-buffer multishot: leased BufViews, ENOBUFS resume callback path.
            entry = _UringEntry(
                operation=operation,
                complete=UringProactor._deliver_uring_recv_many,
                multishot_leg=_MultishotLegState(),
                buf_group=uring_group,
            )

            def submit_recv_many() -> _UringCompletion:
                return self._ring.submit_recv_multishot(sock.fileno(), uring_group, entry)

            entry.resubmit = submit_recv_many
            self._submit_uring_entry(entry, submit_recv_many)
            return operation

        # degraded fallback: copy each recv into an owned view and resubmit recv.
        buffer = bytearray(_DEFAULT_SELECTOR_RECV_MANY_CHUNK_SIZE)
        entry = _UringEntry(
            operation=operation,
            complete=UringProactor._deliver_uring_recv_many_oneshot,
            data=memoryview(buffer),
        )

        def submit_recv() -> _UringCompletion:
            return self._ring.submit_recv(sock.fileno(), buffer, entry)

        entry.resubmit = submit_recv
        self._submit_uring_entry(entry, submit_recv)
        return operation

    def _deliver_uring_recv_many_oneshot(
        self, entry: _UringEntry, completion: _UringCompletion
    ) -> Operation[Any] | None:
        # not BufView-based: copy out of the reused recv buffer so resubmit is safe.
        operation = cast(ContinuousOperation[_RecvManyResult], entry.operation)
        res = completion.res
        if res < 0:
            self._deactivate_uring_entry(entry)
            operation._set_exception(OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")))
            return operation
        index = entry.stream_sequence
        if res == 0:
            operation._emit_result((index, memoryview(b"")))
            operation._set_result(None)
            self._deactivate_uring_entry(entry)
            return operation
        view = entry.data
        assert view is not None
        chunk = bytes(view[:res])
        operation._emit_result((index, memoryview(chunk)))
        entry.stream_sequence += 1
        if operation.done():
            return operation
        self._queue_entry_resubmit(entry)
        return None

    def poll(self, fd: int, mask: int) -> Operation[int]:
        """Submit a one-shot io_uring poll operation."""

        # mask and fd go straight to io_uring; bad values show up as CQE errors.
        # selector validates masks (select() fd lists) and fd>=0; no per-fd exclusivity.
        operation = Operation[int](kind="poll", fileobj=fd, proactor=self)
        entry = _UringEntry(operation=operation, complete=UringProactor._complete_uring_poll)
        self._submit_uring_entry(entry, lambda: self._ring.submit_poll(fd, mask, entry))
        return operation

    def _complete_uring_poll(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], entry.operation)
        operation._set_result(completion.res)
        return operation

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Callable[[int], object],
    ) -> ContinuousOperation[int]:
        """Start a continuous io_uring poll operation.

        Uses multishot poll when the runtime probe accepts it; otherwise falls
        back to resubmitting one-shot ``submit_poll()`` after each readiness
        event. `callback` may run on any uring completion service thread.
        """

        # mask handling matches poll(); no pre-validation on the uring path.
        operation = ContinuousOperation[int](
            kind="poll_many",
            fileobj=fd,
            proactor=self,
            result_callback=callback,
        )
        if self._capabilities.get("IORING_POLL_MULTISHOT", False):
            # kernel keeps the poll armed; cancel via submit_poll_remove().
            entry = _UringEntry(
                operation=operation,
                complete=UringProactor._deliver_uring_poll_many,
                multishot_leg=_MultishotLegState(),
            )
            self._submit_uring_entry(entry, lambda: self._ring.submit_poll_multishot(fd, mask, entry))
            return operation

        # fallback: one-shot submit_poll per readiness event.
        entry = _UringEntry(operation=operation, complete=UringProactor._deliver_uring_poll_many_oneshot)

        def submit_poll() -> _UringCompletion:
            return self._ring.submit_poll(fd, mask, entry)

        entry.resubmit = submit_poll
        self._submit_uring_entry(entry, submit_poll)
        return operation

    def _deliver_uring_poll_many_oneshot(
        self, entry: _UringEntry, completion: _UringCompletion
    ) -> Operation[Any] | None:
        # emit the mask, then queue another submit_poll() unless cancelled.
        operation = cast(ContinuousOperation[int], entry.operation)
        res = completion.res
        if res < 0:
            self._deactivate_uring_entry(entry)
            operation._set_exception(OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")))
            return operation
        operation._emit_result(res)
        if operation.done():
            return operation
        self._queue_entry_resubmit(entry)
        return None

    def _deliver_uring_poll_many(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[Any] | None:
        operation = cast(ContinuousOperation[int], entry.operation)
        res = completion.res
        if res < 0:
            self._deactivate_uring_entry(entry)
            operation._set_exception(OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")))
            return operation
        operation._emit_result(res)
        if not completion.flags & uring_api.IORING_CQE_F_MORE:
            operation._set_result(None)
            self._deactivate_uring_entry(entry)
        return operation

    def _deliver_uring_recv_many(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[Any] | None:
        operation = cast(ContinuousOperation[_RecvManyResult], entry.operation)
        res = completion.res
        index = entry.stream_sequence + completion.sequence

        if res < 0:
            self._deactivate_uring_entry(entry)
            if res == -errno.ENOBUFS:
                entry.stream_sequence += completion.sequence
                if entry.multishot_leg is not None:
                    entry.multishot_leg.nonterminal_seen = 0
                    entry.multishot_leg.pending_final = None
                operation._emit_result((RECV_MANY_BUFFER_PRESSURE, self._recv_many_resume_callable(entry)))
                return None
            operation._set_exception(OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")))
            return operation

        if res == 0:
            operation._emit_result((index, memoryview(b"")))
        else:
            operation._emit_result((index, memoryview(cast(Any, completion.result))))

        if not bool(completion.flags & uring_api.IORING_CQE_F_MORE):
            operation._set_result(None)
            self._deactivate_uring_entry(entry)
        return operation

    def _deactivate_uring_entry(self, entry: _UringEntry) -> None:
        if entry.active:
            entry.active = False
            self._pending_tokens.pop()

    def cancel_operation(self, operation: Operation[Any]) -> None:
        if self._cancel_deferred_operation(operation):
            self.break_wait()
            return
        cancel_target = operation._cancel_target
        if cancel_target is not None:
            # multishot poll registrations tear down via poll_remove; one-shot
            # fallbacks (poll/accept/recv *many) cancel the pending sqe instead.
            if operation.kind == "poll_many" and self._capabilities.get("IORING_POLL_MULTISHOT", False):
                self._submit_poll_remove(cast(_UringCompletion, cancel_target))
            else:
                self._submit_cancel(cast(_UringCompletion, cancel_target))
        cancelled = operation._set_cancelled()
        if cancelled:
            self.break_wait()

    def _deliver_uring_completion(self, completion: _UringCompletion) -> None:
        if completion.kind == uring_api.COMPLETION_KIND_POLL_REMOVE:
            target = cast(_UringCompletion, completion.user_data)
            self._deactivate_uring_entry(cast(_UringEntry, target.user_data))
            self._retry_deferred_submissions()
            return
        if completion.kind == uring_api.COMPLETION_KIND_CANCEL:
            self._retry_deferred_submissions()
            return
        entry = cast(_UringEntry, completion.user_data)
        first, second = entry.completions_to_process(completion)
        completed_operation: Operation[Any] | None = None
        for pending in (first, second):
            if pending is None:
                continue
            result = self._complete_uring_operation(pending)
            if result is not None:
                completed_operation = result
        self._retry_deferred_submissions()
        if completed_operation is not None:
            self._notify_completed()

    def _queue_entry_resubmit(self, entry: _UringEntry) -> None:
        submit = entry.resubmit
        assert submit is not None
        self._deferred_submissions.append(_UringSubmission(entry=entry, submit=submit))
        self.break_wait()

    def _submit_uring_entry(self, entry: _UringEntry, submit: _UringEntrySubmit) -> bool:
        self._pending_tokens.append(None)
        try:
            entry.completion = submit()
            entry.active = True
        except uring_api.SubmissionQueueFull:
            self._pending_tokens.pop()
            self._deferred_submissions.append(_UringSubmission(entry=entry, submit=submit))
            return False
        except BaseException:
            self._pending_tokens.pop()
            entry.active = False
            self.break_wait()
            raise
        entry.operation._cancel_target = entry.completion
        return True

    def _submit_cancel(self, completion: _UringCompletion) -> bool:
        try:
            self._ring.submit_cancel(completion)
        except uring_api.SubmissionQueueFull:
            self._deferred_submissions.append(
                _UringSubmission(entry=None, submit=lambda: self._ring.submit_cancel(completion))
            )
            return False
        return True

    def _submit_poll_remove(self, completion: _UringCompletion) -> bool:
        try:
            self._ring.submit_poll_remove(completion)
        except uring_api.SubmissionQueueFull:
            self._deferred_submissions.append(
                _UringSubmission(entry=None, submit=lambda: self._ring.submit_poll_remove(completion))
            )
            return False
        return True

    def _retry_deferred_submissions(self) -> None:
        if self._retrying_deferred_submissions:
            return
        self._retrying_deferred_submissions = True
        try:
            while self._deferred_submissions:
                submission = self._deferred_submissions.pop(0)
                entry = submission.entry
                if entry is None:
                    try:
                        submission.submit()
                    except uring_api.SubmissionQueueFull:
                        self._deferred_submissions.append(submission)
                        break
                    continue
                if entry.operation.done():
                    entry.active = False
                    continue
                if not self._submit_uring_entry(entry, submission.submit):
                    break
        finally:
            self._retrying_deferred_submissions = False

    def _cancel_deferred_operation(self, operation: Operation[Any]) -> bool:
        for index, submission in enumerate(self._deferred_submissions):
            entry = submission.entry
            if entry is not None and entry.operation is operation:
                del self._deferred_submissions[index]
                entry.active = False
                operation._set_cancelled()
                return True
        return False

    def _submit_sendall(
        self,
        sock: socket.socket,
        operation: Operation[None],
        data: memoryview,
        offset: int,
        progress: _ProgressCallback | None,
    ) -> None:
        entry = _UringEntry(
            operation=operation,
            complete=UringProactor._complete_uring_sendall,
            data=data,
            offset=offset,
            progress=progress,
        )
        self._submit_uring_entry(entry, lambda: self._submit_send(sock.fileno(), data[offset:], entry))

    def _submit_recvmsg(
        self,
        sock: socket.socket,
        operation: Operation[Any],
        data: memoryview,
        complete: _UringEntryComplete,
    ) -> None:
        entry = _UringEntry(operation=operation, complete=complete, data=data)
        self._submit_uring_entry(entry, lambda: self._ring.submit_recvmsg(sock.fileno(), data, entry))

    def _complete_uring_operation(
        self,
        completion: _UringCompletion,
    ) -> Operation[Any] | None:
        entry = cast(_UringEntry, completion.user_data)
        res = completion.res
        assert entry.active
        has_more = bool(completion.flags & uring_api.IORING_CQE_F_MORE)
        if completion.multishot:
            if entry.operation.done():
                return entry.operation
            return entry.complete(self, entry, completion)
        if not has_more:
            self._deactivate_uring_entry(entry)
        if entry.operation.done():
            return entry.operation
        if res < 0:
            entry.operation._set_exception(OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")))
            return entry.operation
        return entry.complete(self, entry, completion)

    def _raise_unsupported(self, operation: str) -> NoReturn:
        self._check_open()
        raise NotImplementedError(f"UringProactor does not yet support {operation} operations")


class ProactorScheduler(BaseScheduler):
    """Shared proactor-backed cooperative scheduling mechanics."""

    def __init__(
        self,
        proactor_factory: ProactorFactory | None = None,
        *,
        runnable_queue_factory: RunnableQueueFactory | None = None,
    ) -> None:
        super().__init__(runnable_queue_factory=runnable_queue_factory)
        if proactor_factory is None:
            proactor_factory = SelectorProactor
        self._proactor = proactor_factory()
        self._proactor.set_clock(self.time)

    @property
    def proactor(self) -> Proactor:
        """Return the proactor backend owned by this scheduler."""

        return self._proactor

    def close(self) -> None:
        """Close proactor and scheduler-owned resources."""

        self._proactor.close()
        BaseScheduler.close(self)

    # -- Driver wakeup -------------------------------------------------

    def _break_wait_threadsafe(self) -> None:
        self._proactor.break_wait()

    def _break_wait(self) -> None:
        self._proactor.break_wait()

    def _wait_thread(self) -> None:
        deadline = self._next_timer_deadline()
        self._proactor.wait(deadline)

    # -- Operation waits ----------------------------------------------

    def wait_operation(self, operation: Operation[T]) -> T:
        """Block the current tealet until `operation` completes."""

        if operation.done():
            return operation.result()

        ready = ThreadsafeEvent()

        def wake(_operation: Operation[Any]) -> None:
            ready.set()

        operation.add_done_callback(wake)
        try:
            ready.swait()
        finally:
            if not operation.done():
                operation.remove_done_callback(wake)
                operation.cancel()
        return operation.result()

    # -- Asyncio-style socket helpers ---------------------------------

    def sock_recv(self, sock: socket.socket, n: int) -> bytes:
        """Receive up to `n` bytes from a non-blocking socket."""

        return self.wait_operation(self._proactor.recv(sock, n))

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> RecvBufferPool:
        """Create a provided-buffer pool for ``sock_recv_iter`` and ``recv_many``.

        The returned object satisfies ``RecvBufferPool`` and may be passed to
        ``sock_recv_iter(..., buffer_pool=pool)`` regardless of which proactor
        backend is mounted. Selector backends use a leased-view shim; uring
        backends use a kernel buffer group.
        """

        return self._proactor.create_recv_buffer_pool(buffer_size, buffer_count)

    def shared_recv_buffer_pool(self) -> RecvBufferPool:
        """Return the proactor's shared provided-buffer pool.

        Lazily creates the default pool on first use unless
        ``set_shared_recv_buffer_pool()`` installed a custom pool first.
        """

        return self._proactor.shared_recv_buffer_pool()

    def set_shared_recv_buffer_pool(self, pool: RecvBufferPool) -> None:
        """Replace the proactor's shared provided-buffer pool."""

        self._proactor.set_shared_recv_buffer_pool(pool)

    def _resolve_recv_buffer_pool(self, buffer_pool: RecvBufferPool | None) -> RecvBufferPool:
        if buffer_pool is None:
            return self._proactor.shared_recv_buffer_pool()
        return buffer_pool

    def _open_sock_recv_iter(self, sock: socket.socket, buffer_pool: RecvBufferPool | None) -> _RecvIterBuffer:
        """Start ``recv_many`` and return the ordered receive buffer."""

        pool = self._resolve_recv_buffer_pool(buffer_pool)
        buffer = _RecvIterBuffer(buf_group=pool)
        stream = self._proactor.recv_many(sock, buffer.on_result, buf_group=pool)
        buffer.attach_stream(stream)
        return buffer

    def sock_recv_iter(
        self, sock: socket.socket, buffer_pool: RecvBufferPool | None = None
    ) -> Iterator[_RecvIterYield]:
        """Incrementally receive byte chunks until EOF as a blocking iterator.

        Each ``recv_many`` chunk is reordered into stream-index order before it
        is yielded as a read-only ``memoryview``. Copy with ``bytes(data)`` when
        owned storage is required past the current iteration step.

        ``buffer_pool`` selects the provided-buffer pool. ``None`` uses the
        proactor's shared pool from ``shared_recv_buffer_pool()``; pass a pool
        from ``create_recv_buffer_pool()`` for dedicated sizing.

        ``(RECV_MANY_BUFFER_PRESSURE, memoryview(b\"\"))`` is yielded when the
        provided-buffer pool is exhausted. Drop held views when that token appears.
        Receive restarts on a following ``take_next()`` once internal queues drain
        and at least half of the attached pool's slots are free.

        Must be consumed from a scheduler tealet so ``ThreadsafeEvent`` waits
        block cooperatively.
        """

        buffer = self._open_sock_recv_iter(sock, buffer_pool)
        try:
            while True:
                item = buffer.take_next()
                if item is None:
                    break
                yield item
        finally:
            buffer.close()

    def sock_recvall(
        self,
        sock: socket.socket,
        progress: _RecvProgressCallback | None = None,
        *,
        buffer_pool: RecvBufferPool | None = None,
    ) -> bytes:
        """Receive byte chunks until EOF and return their concatenation.

        ``buffer_pool`` selects the provided-buffer pool. ``None`` uses the
        proactor's shared pool from ``shared_recv_buffer_pool()``.
        """

        if progress is None:
            process = bytes
        else:

            def process(chunk: memoryview) -> bytes:
                cargo = bytes(chunk)
                if cargo:
                    progress(cargo)
                return cargo

        return b"".join((process(chunk) for _, chunk in self.sock_recv_iter(sock, buffer_pool)))

    def sock_recv_into(self, sock: socket.socket, buf: Any) -> int:
        """Receive bytes from a non-blocking socket into `buf`."""

        return self.wait_operation(self._proactor.recv_into(sock, buf))

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> tuple[bytes, Any]:
        """Receive datagram bytes and address from a non-blocking socket."""

        return self.wait_operation(self._proactor.recvfrom(sock, bufsize))

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> tuple[int, Any]:
        """Receive datagram bytes into `buf` from a non-blocking socket."""

        return self.wait_operation(self._proactor.recvfrom_into(sock, buf, nbytes))

    def sock_sendall(self, sock: socket.socket, data: Any, progress: _ProgressCallback | None = None) -> None:
        """Send all `data` through a non-blocking socket."""

        return self.wait_operation(self._proactor.sendall(sock, data, progress))

    def sock_send_iter(
        self,
        sock: socket.socket,
        chunks: Iterable[bytes | bytearray | memoryview],
    ) -> None:
        """Send every chunk from ``chunks`` through a non-blocking socket.

        Each non-empty chunk is sent with the proactor's ``sendall`` path before
        the next chunk is pulled from the iterable. Track progress in the
        iterable or generator you pass when you need it; call ``sock_sendall``
        directly when a single buffer needs ``progress=`` reporting.

        Must be called from a scheduler tealet so socket waits block
        cooperatively.
        """

        for chunk in chunks:
            if not chunk:
                continue
            self.sock_sendall(sock, memoryview(chunk))

    def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> int:
        """Send one datagram through a non-blocking socket."""

        return self.wait_operation(self._proactor.sendto(sock, data, address))

    def sock_accept(self, sock: socket.socket) -> tuple[socket.socket, Any]:
        """Accept one connection from a non-blocking listening socket."""

        return self.wait_operation(self._proactor.accept(sock))

    def sock_connect(self, sock: socket.socket, address: Any) -> None:
        """Connect a non-blocking socket to `address`."""

        return self.wait_operation(self._proactor.connect(sock, address))

    def poll(self, fd: int, mask: int) -> int:
        """Wait until an fd reports events in `mask` and return the readiness bitmask."""

        return self.wait_operation(self._proactor.poll(fd, mask))

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Callable[[int], object],
    ) -> ContinuousOperation[int]:
        """Emit readiness bitmasks until cancelled or the backend reports a terminal error."""

        return self._proactor.poll_many(fd, mask, callback)

    def open(self, path: str, mode: str = "rb") -> ProactorFile:
        """Open a positioned binary file through the proactor backend."""

        flags, file_mode = parse_open_mode(mode)
        try:
            fd = self.wait_operation(self._proactor.openat(path, flags, file_mode))
        except NotImplementedError as exc:
            raise NotImplementedError("scheduler file I/O requires a proactor with openat support") from exc
        try:
            return ProactorFile(
                self,
                self._proactor,
                fd,
                path=path,
                flags=flags,
                append="a" in mode,
            )
        except BaseException:
            try:
                os.close(fd)
            except OSError:
                pass
            raise

    def _has_pending_driver_work(self) -> bool:
        return self._proactor.has_pending_operations() or BaseScheduler._has_pending_driver_work(self)


class SyncProactorScheduler(SyncDrivingMixin, ProactorScheduler, SyncSchedulerDrivingAPI):
    """Synchronous scheduler whose IO wait point is a proactor backend."""

    async def _driver_wait(self) -> None:
        self._wait_thread()


class AsyncProactorScheduler(AsyncDrivingMixin, ProactorScheduler, AsyncSchedulerDrivingAPI):
    """Async-hosted scheduler whose IO wait point is a proactor backend."""

    def __init__(
        self,
        proactor_factory: ProactorFactory | None = None,
        *,
        runnable_queue_factory: RunnableQueueFactory | None = None,
    ) -> None:
        super().__init__(proactor_factory=proactor_factory, runnable_queue_factory=runnable_queue_factory)
        self._wakeup_loop: _asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: _asyncio.AbstractEventLoop) -> None:
        """Bind this scheduler to an asyncio event loop clock and completion wakeups."""

        if self._wakeup_loop is not None and self._wakeup_loop is not loop:
            raise RuntimeError("AsyncProactorScheduler is already bound to a different event loop")
        self._wakeup_loop = loop
        self._time = loop.time
        self._proactor.bind_loop(loop)

        def wake_loop() -> None:
            loop.call_soon_threadsafe(lambda: None)

        self._proactor.set_completion_callback(wake_loop)

    def _lazy_bind_running_loop(self) -> None:
        if self._wakeup_loop is None:
            self.bind_loop(_asyncio.get_running_loop())

    def _before_arun(self) -> None:
        self._lazy_bind_running_loop()

    def close(self) -> None:
        """Close proactor and scheduler-owned resources."""

        self._proactor.set_completion_callback(None)
        super().close()

    async def _driver_wait(self) -> None:
        self._lazy_bind_running_loop()
        deadline = self._next_timer_deadline()
        await self._proactor.wait_async(deadline)
