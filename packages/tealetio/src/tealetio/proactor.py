from __future__ import annotations

import asyncio as _asyncio
import errno
import os
import selectors
import socket
import struct
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, NoReturn, Protocol, TypeAlias, TypeVar, cast

import uring_api

from . import compat
from .files import IOFile, ProactorFile
from .io_manager import (
    FileIO,
    PollIO,
    ProactorAccess,
    ProactorIOManager,
    ProactorSocketIO,
    ServerIO,
    SocketIO,
    SocketSendBuffer,
    SupportsProactorIO,
)
from .recv_iter import (
    RECV_MANY_BUFFER_PRESSURE,
    RecvIterBuffer,
    _RecvManyResult,
    _RecvManyResume,
)
from .socket_helpers import configure_scheduler_socket, socket_from_uring_fd
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
    "FileIO",
    "IOFile",
    "PollIO",
    "ProactorAccess",
    "ProactorIOManager",
    "ProactorScheduler",
    "ProactorSocketIO",
    "ServerIO",
    "SocketIO",
    "SupportsProactorIO",
    "SelectorProactor",
    "CreateSocketResult",
    "SyncProactorScheduler",
    "ThreadedSelectorProactor",
    "UringProactor",
    "UringSubmissionStats",
    "ProactorFile",
    "RECV_MANY_BUFFER_PRESSURE",
    "RecvBufferPool",
    "AcceptManyResult",
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
_RecvManyCallback = Callable[[_RecvManyResult], object]
_RecvIterBuffer = RecvIterBuffer
AcceptManyResult: TypeAlias = tuple[socket.socket, Any, bytes | None, BaseException | None]
_AcceptManyCallback = Callable[[AcceptManyResult], object]
_MAX_ACCEPT_RECV_SIZE = 2**16
CreateSocketResult: TypeAlias = tuple[socket.socket, bool, bool]


def _sync_create_scheduler_socket(family: int, type: int, proto: int = 0) -> socket.socket:
    return configure_scheduler_socket(socket.socket(family, type, proto))


def _close_owned_socket(sock: socket.socket) -> None:
    try:
        sock.close()
    except OSError:
        pass


def _validate_create_socket_hints(
    connect_to: Any | None,
    initial_data: SocketSendBuffer | None,
) -> None:
    if initial_data is not None and connect_to is None:
        raise ValueError("initial_data requires connect_to")


def _close_raw_fd(fd: int) -> None:
    try:
        os.close(fd)
    except OSError:
        pass


def _handoff_accept_many(
    parent: ContinuousOperation[AcceptManyResult],
    conn: socket.socket,
    address: Any,
    initial_data: bytes | None,
    recv_error: BaseException | None,
) -> bool:
    """Emit one accepted connection or close the socket when the parent is done."""

    if parent._emit_result((conn, address, initial_data, recv_error)):
        return True
    conn.close()
    return False


_DEFAULT_ACCEPT_FLAGS = getattr(socket, "SOCK_NONBLOCK", 0) | getattr(socket, "SOCK_CLOEXEC", 0)


def _normalize_accept_recv_size(recv_size: int | None) -> int | None:
    if recv_size is None:
        return None
    if recv_size <= 0:
        raise ValueError("recv_size must be positive when provided")
    if recv_size > _MAX_ACCEPT_RECV_SIZE:
        return _MAX_ACCEPT_RECV_SIZE
    return recv_size


_DEFAULT_OPENAT_DFD = getattr(os, "AT_FDCWD", -100)


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



def _default_uring_ring_factory(entries: int, flags: int) -> _UringRing:
    return uring_api.Ring(entries=entries, flags=flags)


class Proactor(Protocol):
    """Minimal completion-oriented IO backend used by `ProactorScheduler`."""

    def close(self) -> None: ...

    def break_wait(self) -> None: ...

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

    def send(
        self,
        sock: socket.socket,
        data: Any,
        progress: _ProgressCallback | None = None,
    ) -> Operation[None]: ...

    def sendto(self, sock: socket.socket, data: Any, address: Any) -> Operation[int]: ...

    def accept(self, sock: socket.socket) -> Operation[tuple[socket.socket, Any]]: ...

    def accept_many(
        self,
        sock: socket.socket,
        callback: _AcceptManyCallback,
        *,
        recv_size: int | None = None,
    ) -> ContinuousOperation[AcceptManyResult]:
        """Accept connections until cancelled or failed.

        Callback results are ``(socket, address, initial_data, recv_error)``.
        When ``recv_error`` is set the callback must close the socket (or
        delegate to a helper such as ``start_server`` that does).
        """

        ...

    def connect(
        self,
        sock: socket.socket,
        address: Any,
        *,
        initial: SocketSendBuffer | None = None,
    ) -> Operation[None] | Operation[bool]:
        """Connect a socket.

        When ``initial`` is provided the operation completes with ``True`` when
        connect-time send was performed (including an empty buffer). Backends
        that ignore ``initial`` complete with a falsy result like a plain
        connect. A send failure after a successful connect fails the operation;
        the caller owns the socket.
        """

        ...

    def create_socket(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
        connect_to: Any | None = None,
        initial_data: SocketSendBuffer | None = None,
    ) -> Operation[CreateSocketResult]:
        """Create a scheduler-contract socket.

        Returns a non-blocking, close-on-exec ``socket.socket`` together with
        connect/send outcome flags. On success the operation completes with
        ``(socket, is_connected, initial_sent)``. ``initial_sent`` is ``True``
        when ``initial_data`` was provided and the connect/send chain flushed it
        (including an empty buffer). It is ``False`` when ``initial_data`` was
        omitted or the backend ignored connect/send hints.
        ``connect_to`` and ``initial_data`` are optional hints; backends may
        ignore them and return ``(socket, False, False)`` after creation only.
        ``UringProactor`` honours the hints only when ``IORING_OP_SOCKET`` is
        probed, chaining socket creation, connect, and ``sendall``. Without that
        opcode it falls back to stdlib creation and does not connect or send. Any
        failure after creation closes the socket before the operation completes.
        """

        ...

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
class _FdSlot:
    operation: Operation[Any] | ContinuousOperation[Any]
    attempt: Callable[[], Any] | None = None
    step: Callable[[], ContinuousStepResult] | None = None


@dataclass
class _FdEntry:
    reader: _FdSlot | None = None
    writer: _FdSlot | None = None

    def empty(self) -> bool:
        return self.reader is None and self.writer is None


_UringEntryComplete = Callable[["_UringEntry", "_UringCompletion"], Operation[Any] | None]
_UringEntrySubmit = Callable[[], _UringCompletion]


@dataclass
class _MultishotLegState:
    """Per-leg state for deferred multishot termination handling."""

    nonterminal_seen: int = 0
    pending_final: _UringCompletion | None = None
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


@dataclass(frozen=True, slots=True)
class _ChainDeliver:
    """Successful chain outcome passed to the owning operation's deliver hook.

    Fields are optional so new legs can attach data without changing call sites.
    ``None`` at the deliver hook means no boxed outcome (reserved for future legs).
    """

    nbytes: int | None = None


@dataclass
class _ChainState:
    """Per-operation io_uring chain metadata for multi-leg submissions."""

    root: _UringEntry
    current: _UringEntry
    deliver: Callable[[_ChainDeliver | None], None]
    fail: Callable[[BaseException], None]


class _UringEntry:
    """Per-submission io_uring completion state.

    Operation-specific context and the owning proactor are captured by the
    ``complete`` callback closure. One-shot re-arm submit callables live in
    ``submit_box`` lists referenced from ``complete`` and resume closures.
    ``multishot_leg`` is created automatically when ``multishot=True`` and is
    consulted before ``complete`` runs to order multishot CQEs.
    """

    __slots__ = (
        "operation",
        "complete",
        "completion",
        "active",
        "multishot_leg",
        "parent",
        "chain",
    )

    def __init__(
        self,
        operation: Operation[Any],
        complete: _UringEntryComplete,
        *,
        multishot: bool = False,
        parent: _UringEntry | None = None,
        chain: _ChainState | None = None,
    ) -> None:
        self.operation = operation
        self.complete = complete
        self.completion = None
        self.active = False
        self.multishot_leg = _MultishotLegState() if multishot else None
        self.parent = parent
        self.chain = chain

    def completions_to_process(
        self,
        completion: _UringCompletion,
    ) -> tuple[_UringCompletion, ...]:
        """Return completions ready for ``_complete_uring_operation``.

        Returns an empty tuple when the CQE should be dropped (operation done,
        or a multishot termination is being deferred). One-shot completions
        return a single-element tuple. Multishot legs may return two when a
        deferred termination becomes ready alongside the unblocking CQE.
        """

        if not completion.multishot:
            return (completion,)
        leg = self.multishot_leg
        assert leg is not None
        with leg.lock:
            if self.operation.done():
                leg.pending_final = None
                return ()
            is_termination = not bool(completion.flags & uring_api.IORING_CQE_F_MORE)
            if is_termination:
                if leg.nonterminal_seen < completion.sequence:
                    leg.pending_final = completion
                    return ()
                leg.pending_final = None
                return (completion,)
            leg.nonterminal_seen += 1
            pending = leg.pending_final
            if pending is not None and leg.nonterminal_seen >= pending.sequence:
                leg.pending_final = None
                return (completion, pending)
            return (completion,)


@dataclass(frozen=True)
class UringSubmissionStats:
    """Observed io_uring submission pressure for tuning ring queue depth.

    Counters are updated without locking and may be slightly inconsistent
    under concurrent completion-thread delivery; they are intended for
    operator tuning, not exact accounting.
    """

    submit_attempts: int
    submit_queue_full: int
    deferred_queue_peak: int


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
            entry = self._fd_operations.get(fd)
            if entry is None or entry.reader is None:
                continue
            slot = entry.reader
            if slot.operation is not operation or slot.step is None:
                continue
            self._step_continuous_fd_operation(fd, selectors.EVENT_READ, operation, slot.step, completed)
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

        operation = Operation[bytes](kind="recv", fileobj=sock)

        def attempt() -> bytes:
            return sock.recv(n)

        self._submit_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def recv_into(self, sock: socket.socket, buf: Any) -> Operation[int]:
        """Submit a socket receive-into operation."""

        operation = Operation[int](kind="recv_into", fileobj=sock)

        def attempt() -> int:
            return sock.recv_into(buf)

        self._submit_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def recvfrom(self, sock: socket.socket, bufsize: int) -> Operation[tuple[bytes, Any]]:
        """Submit a datagram receive operation."""

        operation = Operation[tuple[bytes, Any]](kind="recvfrom", fileobj=sock)

        def attempt() -> tuple[bytes, Any]:
            return sock.recvfrom(bufsize)

        self._submit_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> Operation[tuple[int, Any]]:
        """Submit a datagram receive-into operation."""

        operation = Operation[tuple[int, Any]](kind="recvfrom_into", fileobj=sock)

        def attempt() -> tuple[int, Any]:
            if nbytes:
                return sock.recvfrom_into(buf, nbytes)
            return sock.recvfrom_into(buf)

        self._submit_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def send(
        self,
        sock: socket.socket,
        data: Any,
        progress: _ProgressCallback | None = None,
    ) -> Operation[None]:
        """Submit a stream send that drains ``data`` before completing."""

        operation = Operation[None](kind="send", fileobj=sock)
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

    def sendto(self, sock: socket.socket, data: Any, address: Any) -> Operation[int]:
        """Submit a datagram send operation."""

        operation = Operation[int](kind="sendto", fileobj=sock)

        def attempt() -> int:
            return sock.sendto(data, address)

        self._submit_socket_operation(sock, selectors.EVENT_WRITE, operation, attempt)
        return operation

    def accept(self, sock: socket.socket) -> Operation[tuple[socket.socket, Any]]:
        """Submit a socket accept operation."""

        operation = Operation[tuple[socket.socket, Any]](kind="accept", fileobj=sock)

        def attempt() -> tuple[socket.socket, Any]:
            conn, address = sock.accept()
            configure_scheduler_socket(conn)
            return conn, address

        self._submit_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def accept_many(
        self,
        sock: socket.socket,
        callback: _AcceptManyCallback,
        *,
        recv_size: int | None = None,
    ) -> ContinuousOperation[AcceptManyResult]:
        """Start accepting connections until the operation is cancelled or fails.

        `callback` may run on any backend worker thread. Each accepted connection
        is delivered as ``(socket, address, initial_data, recv_error)``.
        ``recv_error`` is ``None`` on success. When it is set the callback must
        close the socket. ``recv_size`` is an optional hint; this backend does
        not capture initial bytes and always delivers ``initial_data`` as
        ``None``.
        """

        recv_size = _normalize_accept_recv_size(recv_size)

        operation = ContinuousOperation[AcceptManyResult](
            kind="accept_many",
            fileobj=sock,
            result_callback=callback,
        )

        def step() -> ContinuousStepResult:
            progressed = False
            while True:
                try:
                    conn, address = sock.accept()
                except (BlockingIOError, InterruptedError):
                    return ContinuousStepResult(progressed=progressed)
                configure_scheduler_socket(conn)
                _handoff_accept_many(operation, conn, address, None, None)
                progressed = True

        self._submit_socket_continuous_operation(sock, selectors.EVENT_READ, operation, step)
        return operation

    def create_socket(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
        connect_to: Any | None = None,
        initial_data: SocketSendBuffer | None = None,
    ) -> Operation[CreateSocketResult]:
        """Create a scheduler-contract socket."""

        _validate_create_socket_hints(connect_to, initial_data)
        del flags, connect_to, initial_data
        operation = Operation[CreateSocketResult](kind="create_socket", fileobj=(family, type, proto))
        try:
            sock = _sync_create_scheduler_socket(family, type, proto)
        except OSError as exc:
            operation._set_exception(exc)
            return operation
        operation._set_result((sock, False, False))
        return operation

    def connect(
        self,
        sock: socket.socket,
        address: Any,
        *,
        initial: SocketSendBuffer | None = None,
    ) -> Operation[None] | Operation[bool]:
        """Submit a non-blocking socket connect operation.

        When ``initial`` is provided, ``UringProactor`` may chain connect-time
        send. ``SelectorProactor`` connects only and completes with ``False``;
        use ``ProactorIOManager.sock_connect()`` or ``sock_create()`` when you
        need connect-time data on any backend.
        """

        started = False

        def finish_connect() -> None:
            nonlocal started
            if not started:
                started = True
                try:
                    sock.connect(address)
                except (BlockingIOError, InterruptedError):
                    raise BlockingIOError(errno.EINPROGRESS, "connect in progress") from None
                except OSError as exc:
                    if exc.errno in (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
                        raise BlockingIOError(exc.errno, exc.strerror) from None
                    raise
                return
            err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err == 0:
                return
            if err in (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
                raise BlockingIOError(err, errno.errorcode.get(err, "connect in progress"))
            raise OSError(err, errno.errorcode.get(err, "socket connect failed"))

        if initial is None:
            operation = Operation[None](kind="connect", fileobj=sock)

            def attempt() -> None:
                finish_connect()

            self._submit_socket_operation(sock, selectors.EVENT_WRITE, operation, attempt)
            return operation

        operation = Operation[bool](kind="connect", fileobj=sock)

        def attempt_with_initial() -> bool:
            finish_connect()
            return False

        self._submit_socket_operation(sock, selectors.EVENT_WRITE, operation, attempt_with_initial)
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

        operation = Operation[int](kind="poll", fileobj=fd)

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
            self._reserve_fd_poll_operation(fd, selector_events, operation, attempt)
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
            self._reserve_fd_poll_operation(fd, selector_events, operation, step=step)
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

    def _reserve_fd_poll_operation(
        self,
        fd: int,
        selector_events: int,
        operation: Operation[Any],
        attempt: Callable[[], Any] | None = None,
        *,
        step: Callable[[], ContinuousStepResult] | None = None,
    ) -> None:
        slot = _FdSlot(operation=operation, attempt=attempt, step=step)
        entry = self._fd_operations.setdefault(fd, _FdEntry())
        if selector_events & selectors.EVENT_READ:
            entry.reader = slot
        if selector_events & selectors.EVENT_WRITE:
            entry.writer = slot
        self._bind_selector_cancel(operation)

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
            self._reserve_fd_operation(fd, event, operation, attempt=attempt)
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
            self._reserve_fd_operation(fd, event, operation, step=step)
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

    def _reserve_fd_operation(
        self,
        fd: int,
        event: int,
        operation: Operation[Any],
        *,
        attempt: Callable[[], Any] | None = None,
        step: Callable[[], ContinuousStepResult] | None = None,
    ) -> None:
        self._check_fd_operation_available(fd, event)
        slot = _FdSlot(operation=operation, attempt=attempt, step=step)
        entry = self._fd_operations.setdefault(fd, _FdEntry())
        if event == selectors.EVENT_READ:
            entry.reader = slot
        else:
            entry.writer = slot
        self._bind_selector_cancel(operation)

    def _bind_selector_cancel(self, operation: Operation[Any]) -> None:
        def cancel() -> None:
            with self._lock:
                removed = self._remove_operation(operation)
            if not removed:
                return
            if operation._set_cancelled():
                self._after_selector_registration_changed()

        operation.set_cancel(cancel)

    def _remove_operation(self, operation: Operation[Any]) -> bool:
        for fd, entry in list(self._fd_operations.items()):
            removed = False
            if entry.reader is not None and entry.reader.operation is operation:
                entry.reader = None
                removed = True
            if entry.writer is not None and entry.writer.operation is operation:
                entry.writer = None
                removed = True
            if removed:
                if entry.empty():
                    del self._fd_operations[fd]
                self._update_selector_registration(fd)
                return True
        return False

    def _require_fd_slot_driver(
        self,
        fd: int,
        operation: Operation[Any],
        slot: _FdSlot,
        *,
        continuous: bool,
    ) -> Callable[[], Any]:
        if continuous:
            step = slot.step
            if step is None:
                self._remove_operation(operation)
                raise RuntimeError(f"continuous operation {operation.kind!r} missing step driver on fd {fd}")
            return step
        attempt = slot.attempt
        if attempt is None:
            self._remove_operation(operation)
            raise RuntimeError(f"operation {operation.kind!r} missing attempt driver on fd {fd}")
        return attempt

    def _step_fd_operation(self, fd: int, event: int, completed: list[Operation[Any]]) -> None:
        entry = self._fd_operations.get(fd)
        if entry is None:
            return
        slot = entry.reader if event == selectors.EVENT_READ else entry.writer
        if slot is None:
            return
        operation = slot.operation
        if operation.done():
            return
        if isinstance(operation, ContinuousOperation):
            step = self._require_fd_slot_driver(fd, operation, slot, continuous=True)
            self._step_continuous_fd_operation(fd, event, operation, step, completed)
            return
        attempt = self._require_fd_slot_driver(fd, operation, slot, continuous=False)
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
        step: Callable[[], ContinuousStepResult],
        completed: list[Operation[Any]],
    ) -> None:
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
        self._send_zc_supported = self._capabilities.get("IORING_OP_SEND_ZC", False)
        self._sendmsg_zc_supported = self._capabilities.get("IORING_OP_SENDMSG_ZC", False)
        # continuous *many ops prefer kernel multishot when probed; otherwise they
        # emulate the stream by resubmitting the matching one-shot opcode after
        # each completion (see the *_oneshot delivery handlers below).
        self._completion_thread_nice = completion_thread_nice
        self._pending_tokens: list[None] = []
        self._deferred_submissions: list[_UringSubmission] = []
        self._retrying_deferred_submissions = False
        self._submit_attempts = 0
        self._submit_queue_full = 0
        self._deferred_queue_peak = 0
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

    @property
    def submission_stats(self) -> UringSubmissionStats:
        """Return observed submission-queue pressure counters."""

        return UringSubmissionStats(
            submit_attempts=self._submit_attempts,
            submit_queue_full=self._submit_queue_full,
            deferred_queue_peak=self._deferred_queue_peak,
        )

    def reset_submission_stats(self) -> None:
        """Reset submission pressure counters to zero."""

        self._submit_attempts = 0
        self._submit_queue_full = 0
        self._deferred_queue_peak = 0

    def _note_submit_attempt(self) -> None:
        self._submit_attempts += 1

    def _note_submit_queue_full(self) -> None:
        self._submit_queue_full += 1

    def _enqueue_deferred_submission(self, submission: _UringSubmission) -> None:
        self._deferred_submissions.append(submission)
        deferred_count = len(self._deferred_submissions)
        if deferred_count > self._deferred_queue_peak:
            self._deferred_queue_peak = deferred_count

    def _uring_chain_root_entry(
        self,
        operation: Operation[Any],
        complete: _UringEntryComplete,
        *,
        deliver: Callable[[_ChainDeliver | None], None],
        fail: Callable[[BaseException], None],
        owned_sock: list[socket.socket | None] | None = None,
    ) -> _UringEntry:
        """Create the root leg of a multi-submission chain with chain-aware cancel."""

        entry = _UringEntry(operation=operation, complete=complete)
        chain = _ChainState(
            root=entry,
            current=entry,
            deliver=deliver,
            fail=fail,
        )
        entry.chain = chain

        def cancel() -> None:
            self._cancel_uring_chain(chain, operation, owned_sock)

        operation.set_cancel(cancel)
        return entry

    def _arm_chain_leg(
        self,
        chain: _ChainState,
        parent: _UringEntry,
        complete: _UringEntryComplete,
    ) -> _UringEntry:
        """Create a child leg sharing the chain root operation and update ``current``."""

        child = _UringEntry(
            chain.root.operation,
            complete,
            parent=parent,
            chain=chain,
        )
        chain.current = child
        return child

    def _chained_sendall(
        self,
        chain: _ChainState,
        parent: _UringEntry,
        sock: socket.socket,
        payload: memoryview,
    ) -> _UringEntry:
        send_offset = [0]

        def fini_chained_sendall(entry: _UringEntry, completion: _UringCompletion) -> Operation[Any] | None:
            operation = entry.operation
            if operation.done():
                return operation
            res = completion.res
            if res == 0:
                chain.fail(BlockingIOError(errno.EWOULDBLOCK, "socket send returned zero bytes"))
                return operation
            if res < 0:
                chain.fail(OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")))
                return operation
            send_offset[0] += res
            if send_offset[0] >= payload.nbytes:
                chain.deliver(_ChainDeliver(nbytes=send_offset[0]))
                return operation
            self._submit_chained_sendall_chunk(entry, sock, payload, send_offset)
            return None

        send_entry = self._arm_chain_leg(chain, parent, fini_chained_sendall)
        self._submit_chained_sendall_chunk(send_entry, sock, payload, send_offset)
        return send_entry

    def _submit_chained_sendall_chunk(
        self,
        entry: _UringEntry,
        sock: socket.socket,
        payload: memoryview,
        send_offset: list[int],
    ) -> None:
        self._submit_uring_entry(
            entry,
            lambda: (
                self._ring.submit_send_zc(sock.fileno(), payload[send_offset[0] :], entry)
                if self._send_zc_supported and sock.family != socket.AF_UNIX
                else self._ring.submit_send(sock.fileno(), payload[send_offset[0] :], entry)
            ),
        )

    def _fini_sock_connect_leg(
        self,
        sock: socket.socket,
        payload: memoryview,
        owned_sock: list[socket.socket | None] | None = None,
    ) -> _UringEntryComplete:
        def fini(entry: _UringEntry, completion: _UringCompletion) -> Operation[Any] | None:
            chain = entry.chain
            assert chain is not None
            operation = entry.operation
            if operation.done():
                return operation
            res = completion.res
            if res < 0:
                chain.fail(OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")))
                return operation
            if operation.done():
                if owned_sock is not None and owned_sock[0] is not None:
                    _close_owned_socket(owned_sock[0])
                    owned_sock[0] = None
                return operation
            if not payload:
                chain.deliver(_ChainDeliver())
                return operation
            self._chained_sendall(chain, entry, sock, payload)
            return None

        return fini

    def _chained_sock_connect(
        self,
        chain: _ChainState,
        parent: _UringEntry,
        sock: socket.socket,
        address: Any,
        payload: memoryview,
        *,
        owned_sock: list[socket.socket | None] | None = None,
    ) -> _UringEntry:
        if chain.root.operation.done():
            return parent
        connect_entry = self._arm_chain_leg(
            chain,
            parent,
            self._fini_sock_connect_leg(sock, payload, owned_sock),
        )
        self._submit_uring_entry(
            connect_entry,
            lambda: self._ring.submit_connect(sock.fileno(), address, connect_entry),
        )
        return connect_entry

    def _cancel_uring_chain(
        self,
        chain: _ChainState,
        operation: Operation[Any],
        owned_sock: list[socket.socket | None] | None = None,
    ) -> None:
        if self._cancel_all_deferred_for_operation(operation):
            if owned_sock is not None and owned_sock[0] is not None:
                _close_owned_socket(owned_sock[0])
                owned_sock[0] = None
            # Deferred cancel already marked the operation cancelled.
            self.break_wait()
            return

        entry = chain.current
        while entry is not None:
            completion = entry.completion
            if completion is not None:
                self._submit_cancel(completion)
            if entry is not chain.root:
                if entry.active:
                    self._deactivate_uring_entry(entry)
                else:
                    entry.completion = None
            entry = entry.parent

        if owned_sock is not None and owned_sock[0] is not None:
            _close_owned_socket(owned_sock[0])
            owned_sock[0] = None

        if operation._set_cancelled():
            self.break_wait()

    def _cancel_all_deferred_for_operation(self, operation: Operation[Any]) -> bool:
        cancelled = False
        index = 0
        while index < len(self._deferred_submissions):
            submission = self._deferred_submissions[index]
            entry = submission.entry
            if entry is not None and entry.operation is operation:
                del self._deferred_submissions[index]
                entry.active = False
                entry.completion = None
                cancelled = True
            else:
                index += 1
        if cancelled:
            operation._set_cancelled()
        return cancelled

    def _uring_entry(
        self,
        operation: Operation[Any],
        complete: _UringEntryComplete,
        *,
        multishot: bool = False,
        poll_remove: bool = False,
    ) -> _UringEntry:
        entry = _UringEntry(operation=operation, complete=complete, multishot=multishot)
        teardown = self._submit_poll_remove if poll_remove else self._submit_cancel

        def cancel() -> None:
            # Deferred resubmit legs are dropped here; in-flight legs use the
            # pending Completion handle when entry.completion is still set.
            if self._cancel_deferred_operation(operation):
                self.break_wait()
                return
            completion = entry.completion
            if completion is not None:
                teardown(completion)
            if operation._set_cancelled():
                self.break_wait()

        operation.set_cancel(cancel)
        return entry

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> _UringBufGroup:
        """Create a provided-buffer group for ``recv_many`` / ``sock_recv_iter``."""

        return self._ring.create_buf_group(buffer_size, buffer_count)

    def create_buf_group(self, buffer_size: int, buffer_count: int) -> _UringBufGroup:
        return self.create_recv_buffer_pool(buffer_size, buffer_count)

    def _default_shared_recv_buffer_pool_sizes(self) -> tuple[int, int]:
        return _DEFAULT_URING_RECV_MANY_BUFFER_SIZE, _DEFAULT_URING_RECV_MANY_BUFFER_COUNT

    def _recv_many_resume_callable(self, entry: _UringEntry, submit_box: list[_UringEntrySubmit]) -> _RecvManyResume:
        def resume() -> None:
            if entry.operation.done() or entry.active:
                return
            self._queue_entry_resubmit(entry, submit_box[0])
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

        operation = Operation[bytes](kind="recv", fileobj=sock)
        data = memoryview(bytearray(n))
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_recv(entry, completion, data),
        )
        self._submit_uring_entry(entry, lambda: self._ring.submit_recv(sock.fileno(), data, entry))
        return operation

    def _complete_uring_recv(
        self, entry: _UringEntry, completion: _UringCompletion, data: memoryview
    ) -> Operation[bytes]:
        operation = cast(Operation[bytes], entry.operation)
        operation._set_result(data[: completion.res].tobytes())
        return operation

    def recv_into(self, sock: socket.socket, buf: Any) -> Operation[int]:
        """Submit a socket receive-into operation."""

        operation = Operation[int](kind="recv_into", fileobj=sock)
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_recv_into(entry, completion),
        )
        self._submit_uring_entry(entry, lambda: self._ring.submit_recv(sock.fileno(), buf, entry))
        return operation

    def _complete_uring_recv_into(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], entry.operation)
        operation._set_result(completion.res)
        return operation

    def recvfrom(self, sock: socket.socket, bufsize: int) -> Operation[tuple[bytes, Any]]:
        """Submit a datagram receive operation."""

        operation = Operation[tuple[bytes, Any]](kind="recvfrom", fileobj=sock)
        data = memoryview(bytearray(bufsize))
        self._submit_recvmsg(
            sock,
            operation,
            data,
            lambda entry, completion: self._complete_uring_recvfrom(entry, completion, data),
        )
        return operation

    def _complete_uring_recvfrom(
        self, entry: _UringEntry, completion: _UringCompletion, data: memoryview
    ) -> Operation[tuple[bytes, Any]]:
        operation = cast(Operation[tuple[bytes, Any]], entry.operation)
        operation._set_result((data[: completion.res].tobytes(), completion.result))
        return operation

    def recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> Operation[tuple[int, Any]]:
        """Submit a datagram receive-into operation."""

        operation = Operation[tuple[int, Any]](kind="recvfrom_into", fileobj=sock)
        data = memoryview(buf)
        if nbytes < 0:
            raise ValueError("negative buffersize in recvfrom_into")
        if nbytes > len(data):
            raise ValueError("nbytes is greater than the length of the buffer")
        if nbytes:
            data = data[:nbytes]
        self._submit_recvmsg(
            sock,
            operation,
            data,
            lambda entry, completion: self._complete_uring_recvfrom_into(entry, completion),
        )
        return operation

    def _complete_uring_recvfrom_into(
        self,
        entry: _UringEntry,
        completion: _UringCompletion,
    ) -> Operation[tuple[int, Any]]:
        operation = cast(Operation[tuple[int, Any]], entry.operation)
        operation._set_result((completion.res, completion.result))
        return operation

    def send(
        self,
        sock: socket.socket,
        data: Any,
        progress: _ProgressCallback | None = None,
    ) -> Operation[None]:
        """Submit a stream send that drains ``data`` before completing."""

        operation = Operation[None](kind="send", fileobj=sock)
        payload = memoryview(data)
        if not payload:
            self._check_open()
            operation._set_result(None)
            return operation
        self._submit_sendall(sock, operation, payload, 0, progress)
        return operation

    def _complete_uring_sendall(
        self,
        entry: _UringEntry,
        completion: _UringCompletion,
        data: memoryview,
        offset: int,
        progress: _ProgressCallback | None,
    ) -> Operation[None] | None:
        operation = cast(Operation[None], entry.operation)
        res = completion.res
        if res == 0:
            operation._set_exception(BlockingIOError(errno.EWOULDBLOCK, "socket send returned zero bytes"))
            return operation
        offset += res
        if progress is not None:
            try:
                progress(offset)
            except BaseException as exc:
                operation._set_exception(exc)
                return operation
        if offset >= len(data):
            operation._set_result(None)
            return operation
        sock = cast(socket.socket, operation.fileobj)
        self._submit_sendall(sock, operation, data, offset, progress)
        return None

    def sendto(self, sock: socket.socket, data: Any, address: Any) -> Operation[int]:
        """Submit a datagram send operation."""

        operation = Operation[int](kind="sendto", fileobj=sock)
        payload = memoryview(data)
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_sendto(entry, completion),
        )
        self._submit_uring_entry(
            entry,
            lambda: (
                self._ring.submit_sendmsg_zc(sock.fileno(), payload, address, entry)
                if self._sendmsg_zc_supported and sock.family != socket.AF_UNIX
                else self._ring.submit_sendto(sock.fileno(), payload, address, entry)
            ),
        )
        return operation

    def _complete_uring_sendto(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], entry.operation)
        operation._set_result(completion.res)
        return operation

    def accept(self, sock: socket.socket) -> Operation[tuple[socket.socket, Any]]:
        """Submit a socket accept operation."""

        operation = Operation[tuple[socket.socket, Any]](kind="accept", fileobj=sock)
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_accept(entry, completion),
        )
        self._submit_uring_entry(entry, lambda: self._ring.submit_accept(sock.fileno(), entry, _DEFAULT_ACCEPT_FLAGS))
        return operation

    def _complete_uring_accept(
        self, entry: _UringEntry, completion: _UringCompletion
    ) -> Operation[tuple[socket.socket, Any]]:
        fd, address = cast(tuple[int, Any], completion.result)
        conn = socket_from_uring_fd(fd)
        operation = cast(Operation[tuple[socket.socket, Any]], entry.operation)
        operation._set_result((conn, address))
        return operation

    def accept_many(
        self,
        sock: socket.socket,
        callback: _AcceptManyCallback,
        *,
        recv_size: int | None = None,
    ) -> ContinuousOperation[AcceptManyResult]:
        """Start a continuous accept operation.

        Uses multishot accept when the runtime probe accepts it; otherwise
        resubmits one-shot ``submit_accept()`` after each connection. `callback`
        may run on any uring completion service thread.

        Each accepted connection is delivered as ``(socket, address, initial_data,
        recv_error)``. ``recv_error`` is ``None`` on success; when set the
        callback must close the socket (or delegate to a helper such as
        ``start_server`` that does). ``initial_data`` is ``None`` when no
        initial bytes were captured. ``recv_size`` is an optional hint: when
        multishot accept is available,
        each accept completion arms a ``receive_on_accept`` recv leg and the
        parent callback runs only after data arrives (or the peer closes without
        sending, in which case the connection is dropped). When the hint cannot
        be honoured, connections are delivered with ``initial_data`` set to
        ``None``.
        """

        recv_size = _normalize_accept_recv_size(recv_size)

        operation = ContinuousOperation[AcceptManyResult](
            kind="accept_many",
            fileobj=sock,
            result_callback=callback,
        )
        pending_recv: list[_UringEntry] = []
        accept_finished: list[bool] = [False]
        accept_entry_ref: list[_UringEntry | None] = [None]
        if self._capabilities.get("IORING_ACCEPT_MULTISHOT", False):
            # one multishot accept stays armed until F_MORE clears or we cancel.
            entry = self._uring_entry(
                operation,
                lambda entry, completion: self._deliver_uring_accept_many(
                    entry,
                    completion,
                    recv_size,
                    pending_recv,
                    accept_finished,
                    accept_entry_ref,
                ),
                multishot=True,
            )
            accept_entry_ref[0] = entry
            self._bind_accept_many_cancel(operation, pending_recv)
            self._submit_uring_entry(
                entry,
                lambda: self._ring.submit_accept_multishot(sock.fileno(), entry, _DEFAULT_ACCEPT_FLAGS),
            )
            return operation

        # fallback: accept one connection, emit it, queue another submit_accept().
        submit_box: list[_UringEntrySubmit] = []
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._deliver_uring_accept_many_oneshot(entry, completion, submit_box),
        )
        self._bind_accept_many_cancel(operation, pending_recv)

        def submit_accept() -> _UringCompletion:
            return self._ring.submit_accept(sock.fileno(), entry, _DEFAULT_ACCEPT_FLAGS)

        submit_box.append(submit_accept)
        self._submit_uring_entry(entry, submit_accept)
        return operation

    def _bind_accept_many_cancel(
        self,
        operation: ContinuousOperation[AcceptManyResult],
        pending_recv: list[_UringEntry],
    ) -> None:
        backend_cancel = operation._cancel
        if backend_cancel is None:
            return

        def cancel() -> None:
            backend_cancel()
            self._cancel_pending_receive_on_accept(pending_recv)

        operation.set_cancel(cancel)

    def _cancel_pending_receive_on_accept(self, pending_recv: list[_UringEntry]) -> None:
        while pending_recv:
            entry = pending_recv.pop()
            completion = entry.completion
            if completion is not None:
                self._submit_cancel(completion)
            cast(socket.socket, entry.operation.fileobj).close()
            if not entry.operation.done():
                entry.operation._set_cancelled()
            if entry.active:
                self._deactivate_uring_entry(entry)
            else:
                entry.completion = None

    def _finish_accept_many_if_ready(
        self,
        operation: ContinuousOperation[AcceptManyResult],
        pending_recv: list[_UringEntry],
        accept_finished: list[bool],
    ) -> None:
        if accept_finished[0] and not pending_recv and not operation.done():
            operation._set_result(None)

    def _fail_accept_many_operation(
        self,
        operation: ContinuousOperation[AcceptManyResult],
        pending_recv: list[_UringEntry],
        accept_entry_ref: list[_UringEntry | None],
        exc: BaseException,
    ) -> None:
        self._cancel_pending_receive_on_accept(pending_recv)
        accept_entry = accept_entry_ref[0]
        if accept_entry is not None:
            if accept_entry.active:
                completion = accept_entry.completion
                if completion is not None:
                    self._submit_cancel(completion)
                accept_entry.active = False
            accept_entry_ref[0] = None
        if not operation.done():
            operation._set_exception(exc)

    def _deliver_uring_accept_many_oneshot(
        self,
        entry: _UringEntry,
        completion: _UringCompletion,
        submit_box: list[_UringEntrySubmit],
    ) -> Operation[Any] | None:
        # one-shot accept completes per connection; re-arm via the deferred queue.
        operation = cast(ContinuousOperation[AcceptManyResult], entry.operation)
        res = completion.res
        if res < 0:
            self._deactivate_uring_entry(entry)
            operation._set_exception(OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")))
            return operation
        fd, address = cast(tuple[int, Any], completion.result)
        conn = socket_from_uring_fd(fd)
        _handoff_accept_many(operation, conn, address, None, None)
        if operation.done():
            return operation
        self._queue_entry_resubmit(entry, submit_box[0])
        return None

    def _deliver_uring_accept_many(
        self,
        entry: _UringEntry,
        completion: _UringCompletion,
        recv_size: int | None,
        pending_recv: list[_UringEntry],
        accept_finished: list[bool],
        accept_entry_ref: list[_UringEntry | None],
    ) -> Operation[Any] | None:
        operation = cast(ContinuousOperation[AcceptManyResult], entry.operation)
        res = completion.res
        if res < 0:
            self._deactivate_uring_entry(entry)
            accept_entry_ref[0] = None
            self._fail_accept_many_operation(
                operation,
                pending_recv,
                accept_entry_ref,
                OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")),
            )
            return operation
        fd, address = cast(tuple[int, Any], completion.result)
        conn = socket_from_uring_fd(fd)
        if operation.done():
            conn.close()
        elif recv_size is None:
            _handoff_accept_many(operation, conn, address, None, None)
        else:
            buffer = bytearray(recv_size)
            view = memoryview(buffer)
            recv_operation = Operation[None](kind="receive_on_accept", fileobj=conn)
            recv_entry = self._uring_entry(
                recv_operation,
                lambda recv_entry, recv_completion: self._deliver_receive_on_accept(
                    recv_entry,
                    recv_completion,
                    operation,
                    conn,
                    address,
                    view,
                    pending_recv,
                    accept_finished,
                    accept_entry_ref,
                ),
            )
            # Re-check before arming: cancel may have completed after the guard above.
            if operation.done():
                conn.close()
            else:
                pending_recv.append(recv_entry)
                self._submit_uring_entry(recv_entry, lambda: self._ring.submit_recv(conn.fileno(), buffer, recv_entry))
        if not completion.flags & uring_api.IORING_CQE_F_MORE:
            self._deactivate_uring_entry(entry)
            accept_entry_ref[0] = None
            if pending_recv:
                accept_finished[0] = True
            else:
                operation._set_result(None)
        return operation

    def _deliver_receive_on_accept(
        self,
        entry: _UringEntry,
        completion: _UringCompletion,
        parent: ContinuousOperation[AcceptManyResult],
        conn: socket.socket,
        address: Any,
        data: memoryview,
        pending_recv: list[_UringEntry],
        accept_finished: list[bool],
        accept_entry_ref: list[_UringEntry | None],
    ) -> Operation[Any] | None:
        recv_operation = entry.operation
        res = completion.res
        try:
            pending_recv.remove(entry)
        except ValueError:
            pass
        if parent.done():
            conn.close()
            recv_operation._set_result(None)
            self._finish_accept_many_if_ready(parent, pending_recv, accept_finished)
            return recv_operation
        if res < 0:
            _handoff_accept_many(
                parent,
                conn,
                address,
                None,
                OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")),
            )
            recv_operation._set_result(None)
            self._finish_accept_many_if_ready(parent, pending_recv, accept_finished)
            return recv_operation
        if res == 0:
            conn.close()
            recv_operation._set_result(None)
            self._finish_accept_many_if_ready(parent, pending_recv, accept_finished)
            return recv_operation
        _handoff_accept_many(parent, conn, address, data[:res].tobytes(), None)
        recv_operation._set_result(None)
        self._finish_accept_many_if_ready(parent, pending_recv, accept_finished)
        return recv_operation

    def create_socket(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
        connect_to: Any | None = None,
        initial_data: SocketSendBuffer | None = None,
    ) -> Operation[CreateSocketResult]:
        """Create a scheduler-contract socket."""

        _validate_create_socket_hints(connect_to, initial_data)
        operation = Operation[CreateSocketResult](kind="create_socket", fileobj=(family, type, proto))

        if self._capabilities.get("IORING_OP_SOCKET", False):
            socket_flags = flags | _DEFAULT_ACCEPT_FLAGS
            owned_sock: list[socket.socket | None] = [None]

            def deliver(_result: _ChainDeliver | None) -> None:
                sock = owned_sock[0]
                assert sock is not None
                operation._set_result((sock, True, initial_data is not None))

            def fail(exc: BaseException) -> None:
                sock = owned_sock[0]
                if sock is not None:
                    self._fail_create_socket(operation, sock, exc)
                    owned_sock[0] = None
                elif not operation.done():
                    operation._set_exception(exc)

            entry = self._uring_chain_root_entry(
                operation,
                lambda entry, completion: self._fini_create_socket(
                    entry,
                    completion,
                    operation,
                    connect_to,
                    initial_data,
                    owned_sock,
                ),
                deliver=deliver,
                fail=fail,
                owned_sock=owned_sock,
            )
            self._submit_uring_entry(
                entry,
                lambda: self._ring.submit_socket(family, type, proto, socket_flags, entry),
            )
            return operation

        try:
            sock = _sync_create_scheduler_socket(family, type, proto)
        except OSError as exc:
            operation._set_exception(exc)
            return operation
        operation._set_result((sock, False, False))
        return operation

    def connect(
        self,
        sock: socket.socket,
        address: Any,
        *,
        initial: SocketSendBuffer | None = None,
    ) -> Operation[None] | Operation[bool]:
        """Submit a non-blocking socket connect operation."""

        if initial is None:
            operation = Operation[None](kind="connect", fileobj=sock)
            entry = self._uring_entry(
                operation,
                lambda entry, completion: self._complete_uring_connect(entry, completion),
            )
            self._submit_uring_entry(entry, lambda: self._ring.submit_connect(sock.fileno(), address, entry))
            return operation

        operation = Operation[bool](kind="connect", fileobj=sock)
        payload = memoryview(initial)
        entry = self._uring_chain_root_entry(
            operation,
            self._fini_sock_connect_leg(sock, payload),
            deliver=lambda _result: operation._set_result(True),
            fail=operation._set_exception,
        )
        self._submit_uring_entry(entry, lambda: self._ring.submit_connect(sock.fileno(), address, entry))
        return operation

    def _complete_uring_connect(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[None]:
        operation = cast(Operation[None], entry.operation)
        operation._set_result(None)
        return operation

    def _fail_create_socket(
        self,
        operation: Operation[CreateSocketResult],
        sock: socket.socket,
        exc: BaseException,
    ) -> Operation[CreateSocketResult]:
        _close_owned_socket(sock)
        if not operation.done():
            operation._set_exception(exc)
        return operation

    def _fini_create_socket(
        self,
        entry: _UringEntry,
        completion: _UringCompletion,
        operation: Operation[CreateSocketResult],
        connect_to: Any | None,
        initial_data: SocketSendBuffer | None,
        owned_sock: list[socket.socket | None],
    ) -> Operation[CreateSocketResult] | None:
        chain = entry.chain
        assert chain is not None
        res = completion.res
        if res < 0:
            if not operation.done():
                operation._set_exception(OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")))
            return operation
        if operation.done():
            _close_raw_fd(res)
            return operation
        sock = socket_from_uring_fd(res)
        owned_sock[0] = sock
        if connect_to is None:
            operation._set_result((sock, False, False))
            return operation
        payload = memoryview(initial_data) if initial_data is not None else memoryview(b"")
        self._chained_sock_connect(chain, entry, sock, connect_to, payload, owned_sock=owned_sock)
        return None

    def openat(self, path: str, flags: int, mode: int = 0, *, dfd: int = _DEFAULT_OPENAT_DFD) -> Operation[int]:
        """Submit an io_uring openat operation and return the opened fd on success."""

        operation = Operation[int](kind="openat", fileobj=path)
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_openat(entry, completion),
        )
        self._submit_uring_entry(entry, lambda: self._ring.submit_openat(path, flags, mode, entry, dfd=dfd))
        return operation

    def _complete_uring_openat(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], entry.operation)
        operation._set_result(completion.res)
        return operation

    def read(self, fd: int, n: int, offset: int) -> Operation[bytes]:
        """Submit a positioned file read that completes with the bytes read."""

        operation = Operation[bytes](kind="read", fileobj=fd)
        data = memoryview(bytearray(n))
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_read(entry, completion, data),
        )
        self._submit_uring_entry(entry, lambda: self._ring.submit_read(fd, data, offset, entry))
        return operation

    def _complete_uring_read(
        self, entry: _UringEntry, completion: _UringCompletion, data: memoryview
    ) -> Operation[bytes]:
        operation = cast(Operation[bytes], entry.operation)
        operation._set_result(data[: completion.res].tobytes())
        return operation

    def read_into(self, fd: int, buf: Any, offset: int) -> Operation[int]:
        """Submit a positioned file read into a caller-provided buffer."""

        operation = Operation[int](kind="read_into", fileobj=fd)
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_read_into(entry, completion),
        )
        self._submit_uring_entry(entry, lambda: self._ring.submit_read(fd, buf, offset, entry))
        return operation

    def _complete_uring_read_into(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], entry.operation)
        operation._set_result(completion.res)
        return operation

    def write(self, fd: int, data: Any, offset: int) -> Operation[int]:
        """Submit a positioned file write and return the byte count written."""

        operation = Operation[int](kind="write", fileobj=fd)
        payload = memoryview(data)
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_write(entry, completion),
        )
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
        stat_buf = memoryview(buf)
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_stat(entry, completion, stat_buf),
        )
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

    def _complete_uring_stat(
        self, entry: _UringEntry, completion: _UringCompletion, data: memoryview
    ) -> Operation[os.stat_result]:
        operation = cast(Operation[os.stat_result], entry.operation)
        try:
            operation._set_result(_stat_result_from_statx(data))
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

        operation = Operation[int](kind="stat_fdsize", fileobj=fd)
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_stat_fdsize(entry, completion),
        )
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
            result_callback=callback,
        )
        if self._capabilities.get("IORING_RECV_MULTISHOT", False):
            uring_group = cast(_UringBufGroup, buf_group)
            # provided-buffer multishot: leased BufViews, ENOBUFS resume callback path.
            # mutable box so ENOBUFS recovery can advance the base index in-place.
            stream_sequence = [0]
            submit_box: list[_UringEntrySubmit] = []
            entry = self._uring_entry(
                operation,
                lambda entry, completion: self._deliver_uring_recv_many(entry, completion, stream_sequence, submit_box),
                multishot=True,
            )

            def submit_recv_many() -> _UringCompletion:
                return self._ring.submit_recv_multishot(sock.fileno(), uring_group, entry)

            submit_box.append(submit_recv_many)
            self._submit_uring_entry(entry, submit_recv_many)
            return operation

        # degraded fallback: copy each recv into an owned view and resubmit recv.
        buffer = bytearray(_DEFAULT_SELECTOR_RECV_MANY_CHUNK_SIZE)
        view = memoryview(buffer)
        stream_sequence = [0]
        submit_box: list[_UringEntrySubmit] = []
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._deliver_uring_recv_many_oneshot(
                entry, completion, view, stream_sequence, submit_box
            ),
        )

        def submit_recv() -> _UringCompletion:
            return self._ring.submit_recv(sock.fileno(), buffer, entry)

        submit_box.append(submit_recv)
        self._submit_uring_entry(entry, submit_recv)
        return operation

    def _deliver_uring_recv_many_oneshot(
        self,
        entry: _UringEntry,
        completion: _UringCompletion,
        data: memoryview,
        stream_sequence: list[int],
        submit_box: list[_UringEntrySubmit],
    ) -> Operation[Any] | None:
        # not BufView-based: copy out of the reused recv buffer so resubmit is safe.
        operation = cast(ContinuousOperation[_RecvManyResult], entry.operation)
        res = completion.res
        if res < 0:
            self._deactivate_uring_entry(entry)
            operation._set_exception(OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")))
            return operation
        index = stream_sequence[0]
        if res == 0:
            operation._emit_result((index, memoryview(b"")))
            operation._set_result(None)
            self._deactivate_uring_entry(entry)
            return operation
        chunk = bytes(data[:res])
        operation._emit_result((index, memoryview(chunk)))
        stream_sequence[0] = index + 1
        if operation.done():
            return operation
        self._queue_entry_resubmit(entry, submit_box[0])
        return None

    def poll(self, fd: int, mask: int) -> Operation[int]:
        """Submit a one-shot io_uring poll operation."""

        # mask and fd go straight to io_uring; bad values show up as CQE errors.
        # selector validates masks (select() fd lists) and fd>=0; no per-fd exclusivity.
        operation = Operation[int](kind="poll", fileobj=fd)
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_poll(entry, completion),
        )
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
            result_callback=callback,
        )
        if self._capabilities.get("IORING_POLL_MULTISHOT", False):
            # kernel keeps the poll armed; cancel via submit_poll_remove().
            entry = self._uring_entry(
                operation,
                lambda entry, completion: self._deliver_uring_poll_many(entry, completion),
                multishot=True,
                poll_remove=True,
            )
            self._submit_uring_entry(entry, lambda: self._ring.submit_poll_multishot(fd, mask, entry))
            return operation

        # fallback: one-shot submit_poll per readiness event.
        submit_box: list[_UringEntrySubmit] = []
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._deliver_uring_poll_many_oneshot(entry, completion, submit_box),
        )

        def submit_poll() -> _UringCompletion:
            return self._ring.submit_poll(fd, mask, entry)

        submit_box.append(submit_poll)
        self._submit_uring_entry(entry, submit_poll)
        return operation

    def _deliver_uring_poll_many_oneshot(
        self,
        entry: _UringEntry,
        completion: _UringCompletion,
        submit_box: list[_UringEntrySubmit],
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
        self._queue_entry_resubmit(entry, submit_box[0])
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

    def _deliver_uring_recv_many(
        self,
        entry: _UringEntry,
        completion: _UringCompletion,
        stream_sequence: list[int],
        submit_box: list[_UringEntrySubmit],
    ) -> Operation[Any] | None:
        operation = cast(ContinuousOperation[_RecvManyResult], entry.operation)
        res = completion.res
        index = stream_sequence[0] + completion.sequence

        if res < 0:
            self._deactivate_uring_entry(entry)
            if res == -errno.ENOBUFS:
                stream_sequence[0] += completion.sequence
                multishot_leg = entry.multishot_leg
                if multishot_leg is not None:
                    multishot_leg.nonterminal_seen = 0
                    multishot_leg.pending_final = None
                operation._emit_result((RECV_MANY_BUFFER_PRESSURE, self._recv_many_resume_callable(entry, submit_box)))
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
        # Drop the pending Completion handle so entry <-> completion.user_data
        # cycles do not linger after this leg is done. Multishot entries keep the
        # handle only while active (cancel/poll_remove still need it).
        if entry.active:
            entry.active = False
            self._pending_tokens.pop()
        entry.completion = None
        # Break operation._cancel -> entry closure cycles once the operation is
        # finished. Mid-stream legs (ENOBUFS resume, sendall chunking) rebind or
        # keep the hook while the operation is still live.
        if entry.operation.done():
            entry.operation.set_cancel(None)

    def _fail_uring_entry(self, entry: _UringEntry, exc: BaseException) -> None:
        self._deactivate_uring_entry(entry)
        if entry.operation._set_exception(exc):
            self.break_wait()
            self._notify_completed()

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
        to_process = entry.completions_to_process(completion)
        if not to_process and entry.operation.done():
            # Late multishot CQEs after cancel/terminal finish: drop the leg
            # without re-entering delivery (completions_to_process already
            # discarded them).
            self._deactivate_uring_entry(entry)
            self._retry_deferred_submissions()
            if not self.has_pending_operations():
                self.break_wait()
            return
        completed_operation: Operation[Any] | None = None
        for pending in to_process:
            result = self._complete_uring_operation(pending)
            if result is not None:
                completed_operation = result
        self._retry_deferred_submissions()
        if completed_operation is not None:
            self._notify_completed()
        elif not self.has_pending_operations():
            self.break_wait()

    def _queue_entry_resubmit(self, entry: _UringEntry, submit: _UringEntrySubmit) -> None:
        self._enqueue_deferred_submission(_UringSubmission(entry=entry, submit=submit))
        self.break_wait()

    def _submit_uring_entry(self, entry: _UringEntry, submit: _UringEntrySubmit) -> bool:
        self._pending_tokens.append(None)
        try:
            entry.active = True
            self._note_submit_attempt()
            entry.completion = submit()
        except uring_api.SubmissionQueueFull:
            self._note_submit_queue_full()
            entry.active = False
            self._pending_tokens.pop()
            self._enqueue_deferred_submission(_UringSubmission(entry=entry, submit=submit))
            return False
        except BaseException:
            self._pending_tokens.pop()
            entry.active = False
            self.break_wait()
            raise
        return True

    def _submit_cancel(self, completion: _UringCompletion) -> bool:
        try:
            self._note_submit_attempt()
            self._ring.submit_cancel(completion)
        except uring_api.SubmissionQueueFull:
            self._note_submit_queue_full()
            self._enqueue_deferred_submission(
                _UringSubmission(entry=None, submit=lambda: self._ring.submit_cancel(completion))
            )
            return False
        return True

    def _submit_poll_remove(self, completion: _UringCompletion) -> bool:
        try:
            self._note_submit_attempt()
            self._ring.submit_poll_remove(completion)
        except uring_api.SubmissionQueueFull:
            self._note_submit_queue_full()
            self._enqueue_deferred_submission(
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
                        self._note_submit_attempt()
                        submission.submit()
                    except uring_api.SubmissionQueueFull:
                        self._note_submit_queue_full()
                        self._enqueue_deferred_submission(submission)
                        break
                    continue
                if entry.operation.done():
                    entry.active = False
                    continue
                try:
                    if not self._submit_uring_entry(entry, submission.submit):
                        break
                except Exception as exc:
                    self._fail_uring_entry(entry, exc)
        finally:
            self._retrying_deferred_submissions = False

    def _cancel_deferred_operation(self, operation: Operation[Any]) -> bool:
        for index, submission in enumerate(self._deferred_submissions):
            entry = submission.entry
            if entry is not None and entry.operation is operation:
                del self._deferred_submissions[index]
                entry.active = False
                entry.completion = None
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
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_sendall(entry, completion, data, offset, progress),
        )
        self._submit_uring_entry(
            entry,
            lambda: (
                self._ring.submit_send_zc(sock.fileno(), data[offset:], entry)
                if self._send_zc_supported and sock.family != socket.AF_UNIX
                else self._ring.submit_send(sock.fileno(), data[offset:], entry)
            ),
        )

    def _submit_recvmsg(
        self,
        sock: socket.socket,
        operation: Operation[Any],
        data: memoryview,
        complete: _UringEntryComplete,
    ) -> None:
        entry = self._uring_entry(operation, complete)
        self._submit_uring_entry(entry, lambda: self._ring.submit_recvmsg(sock.fileno(), data, entry))

    def _complete_uring_operation(
        self,
        completion: _UringCompletion,
    ) -> Operation[Any] | None:
        entry = cast(_UringEntry, completion.user_data)
        res = completion.res
        if entry.operation.kind == "receive_on_accept" and not entry.active:
            self._deactivate_uring_entry(entry)
            return None
        if entry.parent is not None and not entry.active:
            self._deactivate_uring_entry(entry)
            return None
        if entry.operation.done():
            if entry.active:
                self._deactivate_uring_entry(entry)
            if (
                entry.operation.kind == "create_socket"
                and entry.parent is None
                and completion.kind == uring_api.COMPLETION_KIND_SOCKET
                and res >= 0
            ):
                _close_raw_fd(res)
            return None
        assert entry.active
        has_more = bool(completion.flags & uring_api.IORING_CQE_F_MORE)
        if completion.multishot:
            if entry.operation.done():
                self._deactivate_uring_entry(entry)
                return entry.operation
            return entry.complete(entry, completion)
        if not has_more:
            self._deactivate_uring_entry(entry)
        if entry.operation.done():
            return entry.operation
        if res < 0 and entry.parent is None and entry.operation.kind != "receive_on_accept":
            entry.operation._set_exception(OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")))
            return entry.operation
        return entry.complete(entry, completion)

    def _raise_unsupported(self, operation: str) -> NoReturn:
        self._check_open()
        raise NotImplementedError(f"UringProactor does not yet support {operation} operations")


def _default_proactor_factory() -> Proactor:
    return SelectorProactor()


class ProactorScheduler(BaseScheduler):
    """Shared proactor-backed cooperative scheduling mechanics."""

    def __init__(
        self,
        proactor_factory: ProactorFactory | None = None,
        *,
        runnable_queue_factory: RunnableQueueFactory | None = None,
    ) -> None:
        super().__init__(runnable_queue_factory=runnable_queue_factory)
        factory = proactor_factory if proactor_factory is not None else _default_proactor_factory
        self._proactor = factory()
        self._proactor.set_clock(self.time)
        self._io = ProactorIOManager(self._proactor)

    @property
    def io(self) -> ProactorIOManager:
        """Return the blocking IO facade for this scheduler."""

        return self._io

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
