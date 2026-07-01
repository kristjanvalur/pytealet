from __future__ import annotations

import asyncio as _asyncio
import errno
import os
import select
import selectors
import socket
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator
from concurrent.futures import CancelledError
from dataclasses import dataclass, field
from typing import Any, Generic, Literal, NoReturn, Protocol, TypeAlias, TypeVar, cast, overload

import uring_api

from . import compat
from .locks import ThreadsafeEvent
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
    "RECV_MANY_BUFFER_PRESSURE",
]


class InvalidStateError(Exception):
    """Raised when an operation result is requested before completion."""


_DoneCallback = Callable[["Operation[Any]"], object]
_CompletionCallback = Callable[[], object]
_ResultCallback = Callable[[T], object]
_ProgressCallback = Callable[[int], object]
_Clock = Callable[[], float]
_DEFAULT_URING_COMPLETION_THREADS = 2
_DEFAULT_URING_COMPLETION_THREAD_NICE = -5
_DEFAULT_URING_RECV_MANY_BUFFER_SIZE = 16 * 1024
_DEFAULT_URING_RECV_MANY_BUFFER_COUNT = 256
_DEFAULT_SELECTOR_RECV_MANY_CHUNK_SIZE = 8192
# ``recv_many`` result-callback index signalling provided-buffer pool pressure.
RECV_MANY_BUFFER_PRESSURE = -1
_DEFAULT_ACCEPT_FLAGS = getattr(socket, "SOCK_NONBLOCK", 0) | getattr(socket, "SOCK_CLOEXEC", 0)

T_Cargo = TypeVar("T_Cargo")


class _OrderedIngestBuffer(Generic[T_Cargo]):
    """Hold out-of-order indexed items and release them in strict sequence."""

    def __init__(self, *, start: int = 0) -> None:
        self._next_emit = start
        self._pending: dict[int, T_Cargo] = {}

    def ingest(self, index: int, cargo: T_Cargo) -> list[tuple[int, T_Cargo]]:
        """Accept one item and return any consecutive ready `(index, cargo)` pairs."""

        if index != self._next_emit:
            self._pending[index] = cargo
            return []
        return self._drain_from(index, cargo)

    def map_pending(self, transform: Callable[[T_Cargo], T_Cargo]) -> None:
        """Rewrite buffered items that are still waiting for earlier indices."""

        for index in list(self._pending):
            self._pending[index] = transform(self._pending[index])

    def clear(self) -> None:
        self._pending.clear()

    def _drain_from(self, index: int, cargo: T_Cargo) -> list[tuple[int, T_Cargo]]:
        ready: list[tuple[int, T_Cargo]] = [(index, cargo)]
        self._next_emit = index + 1
        while self._next_emit in self._pending:
            next_index = self._next_emit
            ready.append((next_index, self._pending.pop(next_index)))
            self._next_emit += 1
        return ready


_UringRing: TypeAlias = uring_api.Ring
_UringCompletion: TypeAlias = uring_api.Completion
_UringBufGroup: TypeAlias = uring_api.BufGroup


_UringRingFactory = Callable[[int, int], _UringRing]
_UringBufGroupFactory = Callable[[_UringRing], _UringBufGroup]
_UringSendSubmit = Callable[[int, Any, object], _UringCompletion]


def _default_uring_ring_factory(entries: int, flags: int) -> _UringRing:
    return uring_api.Ring(entries=entries, flags=flags)


def _default_uring_buf_group_factory(ring: _UringRing) -> _UringBufGroup:
    return ring.create_buf_group(_DEFAULT_URING_RECV_MANY_BUFFER_SIZE, _DEFAULT_URING_RECV_MANY_BUFFER_COUNT)


_POLL_READ_MASK = select.POLLIN | select.POLLPRI | getattr(select, "POLLRDHUP", 0)


def _poll_mask_to_selector_events(mask: int) -> int:
    events = 0
    if mask & _POLL_READ_MASK:
        events |= selectors.EVENT_READ
    if mask & select.POLLOUT:
        events |= selectors.EVENT_WRITE
    if mask & (select.POLLERR | select.POLLHUP):
        events |= selectors.EVENT_READ | selectors.EVENT_WRITE
    if events == 0:
        raise ValueError("poll mask must request at least one supported event")
    return events


def _probe_poll_fd_now(fd: int, mask: int) -> int:
    read_fds: list[int] = []
    write_fds: list[int] = []
    exc_fds: list[int] = []
    if mask & _POLL_READ_MASK:
        read_fds.append(fd)
    if mask & select.POLLOUT:
        write_fds.append(fd)
    if mask & (select.POLLERR | select.POLLHUP):
        exc_fds.append(fd)
    if not (read_fds or write_fds or exc_fds):
        raise ValueError("poll mask must request at least one supported event")
    ready_r, ready_w, ready_x = select.select(read_fds, write_fds, exc_fds, 0)
    result = 0
    if ready_r:
        result |= mask & _POLL_READ_MASK
    if ready_w:
        result |= mask & select.POLLOUT
    if ready_x:
        result |= mask & (select.POLLERR | select.POLLHUP)
    if result:
        return result
    raise BlockingIOError(errno.EWOULDBLOCK, "fd is not ready")


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

    def recvall(self, sock: socket.socket, progress: _ProgressCallback | None = None) -> Operation[bytes]: ...

    @overload
    def recvgen(self, sock: socket.socket, *, allow_memview: Literal[False] = False) -> Iterator[tuple[int, bytes]]: ...

    @overload
    def recvgen(
        self,
        sock: socket.socket,
        *,
        allow_memview: Literal[True],
    ) -> Iterator[tuple[int, memoryview | bytes | None]]: ...

    def recvgen(
        self,
        sock: socket.socket,
        *,
        allow_memview: bool = False,
    ) -> Iterator[tuple[int, memoryview | bytes | None]]: ...

    def sendto(self, sock: socket.socket, data: Any, address: Any) -> Operation[int]: ...

    def accept(self, sock: socket.socket) -> Operation[tuple[socket.socket, Any]]: ...

    def accept_many(
        self,
        sock: socket.socket,
        callback: Callable[[tuple[socket.socket, Any]], object],
    ) -> ContinuousOperation[tuple[socket.socket, Any]]: ...

    def connect(self, sock: socket.socket, address: Any) -> Operation[None]: ...

    def recv_many(
        self,
        sock: socket.socket,
        callback: Callable[[tuple[int, memoryview]], object],
    ) -> ContinuousOperation[tuple[int, memoryview]]: ...

    def poll(self, fd: int, mask: int) -> Operation[int]: ...

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Callable[[int], object],
    ) -> ContinuousOperation[int]: ...


ProactorFactory = Callable[[], Proactor]


def _recvall_adopt_chunk(
    chunks: dict[int, memoryview | bytes],
    pending_views: set[int],
    index: int,
    data: memoryview,
) -> None:
    chunks[index] = data
    pending_views.add(index)


def _recvall_relieve_pressure(
    chunks: dict[int, memoryview | bytes],
    pending_views: set[int],
) -> None:
    for index in pending_views:
        chunk = chunks.get(index)
        if type(chunk) is memoryview:
            chunks[index] = bytes(chunk)
    pending_views.clear()


def _recvall_release_pending_views(
    chunks: dict[int, memoryview | bytes],
    pending_views: set[int],
) -> None:
    # Drop the last recvall-owned references to borrowed chunk views. On modern
    # Python (PEP 688), memoryview uses release() rather than close(); refcount
    # teardown is enough to return leased uring buffers.
    for index in pending_views:
        chunks.pop(index, None)
    pending_views.clear()


def _flush_recvgen_pending_chunk(chunk: memoryview | bytes) -> memoryview | bytes:
    if type(chunk) is memoryview:
        return bytes(chunk)
    return chunk


class _RecvGenBuffer:
    """Ordered receive buffer bridging ``recv_many`` callbacks and ``recvgen``."""

    def __init__(self, *, allow_memview: bool = False) -> None:
        self._allow_memview = allow_memview
        self._lock = threading.Lock()
        self._event = ThreadsafeEvent()
        self._reorder = _OrderedIngestBuffer[memoryview | bytes]()
        self._ready: deque[tuple[int, memoryview | bytes]] = deque()
        self._pressure_pending = False
        self._stream_done = False
        self._stream_error: BaseException | None = None
        self._stream: ContinuousOperation[tuple[int, memoryview]] | None = None
        self._closed = False

    def attach_stream(self, stream: ContinuousOperation[tuple[int, memoryview]]) -> None:
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
                elif not self._stream_done:
                    self._stream_done = True
        self._event.set()

    def on_result(self, result: tuple[int, memoryview]) -> None:
        index, data = result
        notify = False
        with self._lock:
            if index == RECV_MANY_BUFFER_PRESSURE:
                self._flush_all_views()
                if self._allow_memview:
                    self._pressure_pending = True
                notify = True
            else:
                notify = self._ingest(index, data)
        if notify:
            self._event.set()

    def _ingest(self, index: int, data: memoryview) -> bool:
        ready = self._reorder.ingest(index, data)
        if ready:
            self._ready.extend(ready)
        return bool(ready)

    def _flush_all_views(self) -> None:
        flushed_ready: deque[tuple[int, memoryview | bytes]] = deque()
        for index, chunk in self._ready:
            if type(chunk) is memoryview:
                chunk = bytes(chunk)
            flushed_ready.append((index, chunk))
        self._ready = flushed_ready
        self._reorder.map_pending(_flush_recvgen_pending_chunk)

    def _has_waitable_work_locked(self) -> bool:
        return self._stream_error is not None or self._pressure_pending or bool(self._ready) or self._stream_done

    def take_next(self) -> tuple[int, memoryview | bytes | None] | None:
        while True:
            with self._lock:
                if self._stream_error is not None:
                    self._event.clear()
                    raise self._stream_error
                if self._pressure_pending:
                    self._pressure_pending = False
                    if not self._has_waitable_work_locked():
                        self._event.clear()
                    return RECV_MANY_BUFFER_PRESSURE, None
                if self._ready:
                    index, chunk = self._ready.popleft()
                    if len(chunk) == 0:
                        self._stream_done = True
                        if not self._has_waitable_work_locked():
                            self._event.clear()
                        return None
                    if not self._allow_memview and type(chunk) is memoryview:
                        chunk = bytes(chunk)
                    if not self._has_waitable_work_locked():
                        self._event.clear()
                    return index, chunk
                if self._stream_done:
                    self._event.clear()
                    return None
                # discard any stale signal before blocking; clear() is idempotent
                self._event.clear()
            self._event.swait()

    def close(self) -> None:
        stream: ContinuousOperation[tuple[int, memoryview]] | None
        with self._lock:
            if self._closed:
                return
            self._closed = True
            stream = self._stream
            self._ready.clear()
            self._reorder.clear()
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
        callback: Callable[[tuple[int, memoryview]], object],
    ) -> ContinuousOperation[tuple[int, memoryview]]:
        raise NotImplementedError

    def poll(self, fd: int, mask: int) -> Operation[int]:
        raise NotImplementedError

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Callable[[int], object],
    ) -> ContinuousOperation[int]:
        raise NotImplementedError

    def recvall(self, sock: socket.socket, progress: _ProgressCallback | None = None) -> Operation[bytes]:
        """Receive chunks until EOF and complete with the full byte string.

        Chunks start as borrowed ``recv_many`` views and stay unconverted until
        ``recv_many`` reports provided-buffer pressure, when every held view is
        copied to ``bytes`` to return leased slots. Remaining chunk views are
        dropped in a ``finally`` block after the stream completes.
        """

        operation: _LinkedOperation[bytes] = _LinkedOperation(kind="recvall", fileobj=sock)
        chunks: dict[int, memoryview | bytes] = {}
        pending_views: set[int] = set()
        total = 0

        def on_result(result: tuple[int, memoryview]) -> None:
            nonlocal total
            index, data = result
            if index == RECV_MANY_BUFFER_PRESSURE:
                _recvall_relieve_pressure(chunks, pending_views)
                return
            if len(data) == 0:
                return
            _recvall_adopt_chunk(chunks, pending_views, index, data)
            total += len(data)
            if progress is not None:
                progress(total)

        stream = self.recv_many(sock, on_result)
        operation._linked_operation = stream

        def on_done(done_stream: Operation[Any]) -> None:
            try:
                if done_stream.cancelled():
                    operation._set_cancelled()
                    return
                exception = done_stream.exception()
                if exception is not None:
                    operation._set_exception(exception)
                    return
                operation._set_result(b"".join(bytes(chunks[index]) for index in sorted(chunks)))
            finally:
                _recvall_release_pending_views(chunks, pending_views)

        stream.add_done_callback(on_done)
        return operation

    @overload
    def recvgen(self, sock: socket.socket, *, allow_memview: Literal[False] = False) -> Iterator[tuple[int, bytes]]: ...

    @overload
    def recvgen(
        self,
        sock: socket.socket,
        *,
        allow_memview: Literal[True],
    ) -> Iterator[tuple[int, memoryview | bytes | None]]: ...

    def recvgen(
        self,
        sock: socket.socket,
        *,
        allow_memview: bool = False,
    ) -> Iterator[tuple[int, memoryview | bytes | None]]:
        """Incrementally receive byte chunks until EOF as a blocking generator.

        Each ``recv_many`` chunk is reordered into stream-index order before it
        is yielded. By default each chunk is copied to ``bytes`` when dequeued
        so borrowed kernel views are released promptly; queued views are also
        copied to ``bytes`` on provided-buffer pressure so leased slots can
        return to the shared pool.

        With ``allow_memview=True``, chunks may be yielded as borrowed
        ``memoryview`` objects and ``(RECV_MANY_BUFFER_PRESSURE, None)`` may be
        yielded when the provided-buffer pool is exhausted. Consumers must then
        release every ``memoryview`` they still hold, for example by copying to
        ``bytes`` and dropping references or calling ``memoryview.release()``.

        The generator must be consumed from a scheduler tealet so
        ``ThreadsafeEvent`` waits can block cooperatively.
        """

        buffer = _RecvGenBuffer(allow_memview=allow_memview)
        stream = self.recv_many(sock, buffer.on_result)
        buffer.attach_stream(stream)
        try:
            while True:
                item = buffer.take_next()
                if item is None:
                    break
                yield item
        finally:
            buffer.close()


class Operation(Generic[T]):
    """Future-shaped IO operation owned by a proactor backend."""

    def __init__(self, *, kind: str, fileobj: object | None = None, proactor: Proactor | None = None) -> None:
        self.kind = kind
        self.fileobj = fileobj
        self._proactor = proactor
        self._lock = threading.Lock()
        self._done = False
        self._cancelled = False
        self._result: T | None = None
        self._exception: BaseException | None = None
        self._callbacks: list[_DoneCallback] | None = []
        self._attempt: Callable[[], T] | None = None
        self._cancel_target: object | None = None

    def done(self) -> bool:
        """Return True if the operation has completed."""

        return self._done

    def cancelled(self) -> bool:
        """Return True if the operation completed by cancellation."""

        return self._cancelled

    def cancel(self) -> None:
        """Cancel the operation if it has not completed yet."""

        if self._done:
            return
        proactor = self._proactor
        if proactor is not None:
            proactor.cancel_operation(self)
            return
        self._set_cancelled()

    def result(self) -> T:
        """Return the operation result, or raise its completion exception."""

        if not self._done:
            raise InvalidStateError("operation result is not ready")
        exception = self._exception
        result = self._result
        if exception is not None:
            raise exception
        return cast(T, result)

    def exception(self) -> BaseException | None:
        """Return the operation exception, or None for successful completion."""

        if not self._done:
            raise InvalidStateError("operation exception is not ready")
        return self._exception

    def add_done_callback(self, callback: _DoneCallback) -> None:
        """Register `callback` to run when the operation completes."""

        with self._lock:
            if self._done:
                run_now = True
            else:
                assert self._callbacks is not None
                self._callbacks.append(callback)
                run_now = False
        if run_now:
            callback(self)

    def remove_done_callback(self, callback: _DoneCallback) -> int:
        """Remove matching done callbacks and return the number removed."""

        with self._lock:
            if self._callbacks is None:
                return 0
            removed = 0
            kept: list[_DoneCallback] = []
            for stored_callback in self._callbacks:
                if stored_callback is callback:
                    removed += 1
                else:
                    kept.append(stored_callback)
            self._callbacks = kept
            return removed

    def _set_result(self, result: T) -> None:
        self._finish(result=result)

    def _set_exception(self, exc: BaseException) -> None:
        self._finish(exception=exc)

    def _set_cancelled(self) -> bool:
        return self._finish(exception=CancelledError(), cancelled=True)

    def _finish(
        self,
        *,
        result: T | None = None,
        exception: BaseException | None = None,
        cancelled: bool = False,
    ) -> bool:
        with self._lock:
            if self._done:
                if cancelled:
                    return False
                raise InvalidStateError("operation already done")
            self._result = result
            self._exception = exception
            self._cancelled = cancelled
            self._done = True
            callbacks = self._callbacks
            self._callbacks = None
        assert callbacks is not None
        for callback in callbacks:
            callback(self)
        return True


class _LinkedOperation(Operation[T]):
    """Operation whose cancellation propagates to another operation."""

    def __init__(self, *, kind: str, fileobj: object | None = None) -> None:
        super().__init__(kind=kind, fileobj=fileobj)
        self._linked_operation: Operation[Any] | None = None

    def cancel(self) -> None:
        if self.done():
            return
        linked_operation = self._linked_operation
        if linked_operation is not None and not linked_operation.done():
            linked_operation.cancel()
        self._set_cancelled()


class ContinuousOperation(Operation[None], Generic[T]):
    """Long-lived IO operation that emits multiple results before finishing.

    Result callbacks may run on any backend worker thread. Callers that need
    thread affinity must marshal from the callback into the desired thread or
    event loop themselves.
    """

    def __init__(
        self,
        *,
        kind: str,
        fileobj: object | None = None,
        proactor: Proactor | None = None,
        result_callback: _ResultCallback[T] | None = None,
    ) -> None:
        super().__init__(kind=kind, fileobj=fileobj, proactor=proactor)
        self._result_callbacks: list[_ResultCallback[T]] = []
        self._continuous_step: Callable[[], _ContinuousStepResult] | None = None
        if result_callback is not None:
            self._result_callbacks.append(result_callback)

    def add_result_callback(self, callback: _ResultCallback[T]) -> None:
        """Register `callback` for each result produced by the operation."""

        with self._lock:
            if self._done:
                raise InvalidStateError("continuous operation is already done")
            self._result_callbacks.append(callback)

    def _emit_result(self, result: T) -> None:
        with self._lock:
            if self._done:
                return
            callbacks = list(self._result_callbacks)
        for callback in callbacks:
            callback(result)


@dataclass
class _ContinuousStepResult:
    progressed: bool = False
    done: bool = False


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

        def step() -> _ContinuousStepResult:
            progressed = False
            while True:
                try:
                    conn, address = sock.accept()
                except (BlockingIOError, InterruptedError):
                    return _ContinuousStepResult(progressed=progressed)
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
        callback: Callable[[tuple[int, memoryview]], object],
    ) -> ContinuousOperation[tuple[int, memoryview]]:
        """Start receiving byte chunks until EOF, cancellation, or failure.

        `callback` may run on any backend worker thread. Each result is an
        ordinal `(index, data)` pair with read-only `data` as a `memoryview`;
        EOF emits a final empty view before completing the continuous operation.
        Chunk sizes follow the kernel; this implementation reads up to 8 KiB
        per ``recv()`` call.
        """

        operation = ContinuousOperation[tuple[int, memoryview]](
            kind="recv_many",
            fileobj=sock,
            proactor=self,
            result_callback=callback,
        )
        sequence = 0

        def step() -> _ContinuousStepResult:
            nonlocal sequence
            progressed = False
            while True:
                try:
                    data = sock.recv(_DEFAULT_SELECTOR_RECV_MANY_CHUNK_SIZE)
                except (BlockingIOError, InterruptedError):
                    return _ContinuousStepResult(progressed=progressed)
                if not data:
                    operation._emit_result((sequence, memoryview(b"")))
                    sequence += 1
                    return _ContinuousStepResult(progressed=True, done=True)
                operation._emit_result((sequence, memoryview(data)))
                sequence += 1
                progressed = True

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

        def step() -> _ContinuousStepResult:
            try:
                result = _probe_poll_fd_now(fd, mask)
            except BlockingIOError:
                return _ContinuousStepResult(progressed=False)
            operation._emit_result(result)
            return _ContinuousStepResult(progressed=True)

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
        step: Callable[[], _ContinuousStepResult],
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
        step: Callable[[], _ContinuousStepResult],
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
        step: Callable[[], _ContinuousStepResult],
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
        buf_group_factory: _UringBufGroupFactory | None = None,
        completion_threads: int = _DEFAULT_URING_COMPLETION_THREADS,
        completion_thread_nice: int | None = _DEFAULT_URING_COMPLETION_THREAD_NICE,
    ) -> None:
        if completion_threads <= 0:
            raise ValueError("completion_threads must be at least 1")
        if ring_factory is None:
            ring_factory = _default_uring_ring_factory
        if buf_group_factory is None:
            buf_group_factory = _default_uring_buf_group_factory
        super().__init__(completion_callback=completion_callback)
        self._ring = ring_factory(entries, flags)
        self._buf_group_factory = buf_group_factory
        self._recv_many_buf_group: _UringBufGroup | None = None
        try:
            self._capabilities = uring_api.probe(entries=entries, flags=flags)
        except (OSError, RuntimeError, NotImplementedError):
            self._capabilities = {}
        self._submit_send: _UringSendSubmit = self._ring.submit_send
        if self._capabilities.get("IORING_OP_SEND_ZC", False) and hasattr(self._ring, "submit_send_zc"):
            self._submit_send = self._ring.submit_send_zc
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

    def _get_recv_many_buf_group(self) -> _UringBufGroup:
        buf_group = self._recv_many_buf_group
        if buf_group is None:
            buf_group = self._buf_group_factory(self._ring)
            self._recv_many_buf_group = buf_group
        return buf_group

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
        self._submit_uring_entry(entry, lambda: self._ring.submit_sendto(sock.fileno(), payload, address, entry))
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

    def recv_many(
        self,
        sock: socket.socket,
        callback: Callable[[tuple[int, memoryview]], object],
    ) -> ContinuousOperation[tuple[int, memoryview]]:
        """Start a continuous receive operation that completes on EOF.

        `callback` may run on any uring completion service thread.

        When multishot provided-buffer receive is available, each result is an
        ordinal `(index, data)` pair with read-only `data` as a `memoryview`
        into a leased kernel buffer. Callback delivery may arrive out of order
        across completion threads; consumers that need stream order must
        reorder by index themselves. Chunk sizes come from the shared
        ``BufGroup`` pool. Holding live views can pin provided buffers and
        stall further receives. When the pool is exhausted the backend emits
        ``(RECV_MANY_BUFFER_PRESSURE, empty_view)`` and resubmits the multishot
        receive.

        When multishot receive is unavailable, the proactor falls back to
        repeated one-shot ``submit_recv()`` into a reused buffer. Chunks are
        independent ``memoryview`` objects over copied bytes (not leased
        ``BufView`` results), chunk size is up to 8 KiB, indices stay in-order,
        and ``RECV_MANY_BUFFER_PRESSURE`` is never emitted.

        EOF always emits a final empty view before completing the operation.
        """

        operation = ContinuousOperation[tuple[int, memoryview]](
            kind="recv_many",
            fileobj=sock,
            proactor=self,
            result_callback=callback,
        )
        if self._capabilities.get("IORING_RECV_MULTISHOT", False):
            # provided-buffer multishot: leased BufViews, ENOBUFS resubmit path.
            entry = _UringEntry(
                operation=operation,
                complete=UringProactor._deliver_uring_recv_many,
                multishot_leg=_MultishotLegState(),
            )

            def submit_recv_many() -> _UringCompletion:
                return self._ring.submit_recv_multishot(sock.fileno(), self._get_recv_many_buf_group(), entry)

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
        operation = cast(ContinuousOperation[tuple[int, memoryview]], entry.operation)
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
        operation = cast(ContinuousOperation[tuple[int, memoryview]], entry.operation)
        res = completion.res
        index = entry.stream_sequence + completion.sequence

        if res < 0:
            self._deactivate_uring_entry(entry)
            if res == -errno.ENOBUFS:
                entry.stream_sequence += completion.sequence
                if entry.multishot_leg is not None:
                    entry.multishot_leg.nonterminal_seen = 0
                    entry.multishot_leg.pending_final = None
                operation._emit_result((RECV_MANY_BUFFER_PRESSURE, memoryview(b"")))
                self._queue_entry_resubmit(entry)
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

    def sock_recvall(self, sock: socket.socket, progress: _ProgressCallback | None = None) -> bytes:
        """Receive byte chunks until EOF and return their concatenation."""

        return self.wait_operation(self._proactor.recvall(sock, progress))

    @overload
    def sock_recvgen(
        self, sock: socket.socket, *, allow_memview: Literal[False] = False
    ) -> Iterator[tuple[int, bytes]]: ...

    @overload
    def sock_recvgen(
        self,
        sock: socket.socket,
        *,
        allow_memview: Literal[True],
    ) -> Iterator[tuple[int, memoryview | bytes | None]]: ...

    def sock_recvgen(
        self,
        sock: socket.socket,
        *,
        allow_memview: bool = False,
    ) -> Iterator[tuple[int, memoryview | bytes | None]]:
        """Incrementally receive byte chunks until EOF as a blocking generator."""

        return self._proactor.recvgen(sock, allow_memview=allow_memview)

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

    def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> int:
        """Send one datagram through a non-blocking socket."""

        return self.wait_operation(self._proactor.sendto(sock, data, address))

    def sock_accept(self, sock: socket.socket) -> tuple[socket.socket, Any]:
        """Accept one connection from a non-blocking listening socket."""

        return self.wait_operation(self._proactor.accept(sock))

    def sock_connect(self, sock: socket.socket, address: Any) -> None:
        """Connect a non-blocking socket to `address`."""

        return self.wait_operation(self._proactor.connect(sock, address))

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
