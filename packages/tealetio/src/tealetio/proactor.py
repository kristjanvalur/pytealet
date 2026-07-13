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
    SupportsProactorIO,
)
from .socket_helpers import abortive_close, configure_scheduler_socket, socket_from_uring_fd
from .operations import ContinuousOperation, ContinuousStepResult, MultishotDelivery, Operation, T_co
from .tasks import CancelledError
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
    "SyncProactorScheduler",
    "ThreadedSelectorProactor",
    "UringProactor",
    "UringSubmissionStats",
    "ProactorFile",
    "MultishotDelivery",
    "RecvBufferPool",
    "SyntheticRecvBufferPool",
    "AcceptManyResult",
]

_PROVIDED_BUFFER_UNAVAILABLE_ERRNOS = frozenset({errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP})


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
_RecvManyValue = memoryview
_RecvManyCallback = Callable[[MultishotDelivery], object]
AcceptManyResult: TypeAlias = socket.socket
_AcceptManyCallback = Callable[[MultishotDelivery], object]
_PollManyCallback = Callable[[MultishotDelivery], object]


def _sync_create_scheduler_socket(family: int, type: int, proto: int = 0) -> socket.socket:
    return configure_scheduler_socket(socket.socket(family, type, proto))


def _spawn_recv_many_operation(
    sock: socket.socket,
    callback: _RecvManyCallback,
) -> ContinuousOperation[_RecvManyValue]:
    return ContinuousOperation[_RecvManyValue](
        kind="recv_many",
        fileobj=sock,
        result_callback=callback,
    )


def _spawn_accept_many_operation(
    sock: socket.socket,
    callback: _AcceptManyCallback,
) -> ContinuousOperation[AcceptManyResult]:
    return ContinuousOperation[AcceptManyResult](
        kind="accept_many",
        fileobj=sock,
        result_callback=callback,
    )


def _close_owned_socket(sock: socket.socket) -> None:
    try:
        sock.close()
    except OSError:
        pass


def _deliver_sync_void_socket_op(
    proactor: object,
    sock: socket.socket,
    kind: str,
    action: Callable[[], object],
) -> Operation[None]:
    operation = Operation[None](kind=kind, fileobj=sock)
    try:
        action()
        operation.deliver(proactor, result=None)
    except OSError as exc:
        operation.deliver(proactor, exception=exc)
    return operation


def _deliver_sync_void_fd_op(
    proactor: object,
    fd: int,
    kind: str,
    action: Callable[[], object],
) -> Operation[None]:
    operation = Operation[None](kind=kind, fileobj=fd)
    try:
        action()
        operation.deliver(proactor, result=None)
    except OSError as exc:
        operation.deliver(proactor, exception=exc)
    return operation


def _spawn_operation(
    kind: str,
    fileobj: object | None = None,
) -> Operation[Any]:
    return Operation(kind=kind, fileobj=fileobj)


def _uring_entry_of(operation: Operation[Any]) -> "_UringEntry | None":
    return cast("_UringEntry | None", getattr(operation, "_uring_entry", None))


def _close_raw_fd(fd: int) -> None:
    try:
        os.close(fd)
    except OSError:
        pass


def _handoff_accept_many(
    parent: ContinuousOperation[AcceptManyResult],
    conn: socket.socket,
    *,
    more: bool = True,
) -> bool:
    """Emit one accepted connection or close the socket when the parent is done."""

    if parent._emit_result(conn, more=more):
        return True
    abortive_close(conn)
    return False


def _enobufs_error() -> OSError:
    return OSError(errno.ENOBUFS, errno.errorcode.get(errno.ENOBUFS, "no buffer space"))


_DEFAULT_ACCEPT_FLAGS = getattr(socket, "SOCK_NONBLOCK", 0) | getattr(socket, "SOCK_CLOEXEC", 0)


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
    """Receive buffer pool surface shared by uring ``BufGroup`` and ``SyntheticRecvBufferPool``.

    ``leased_count`` tracks how many receive chunks consumers still hold.
    On uring multishot, when the pool is full, ``recv_many`` surfaces
    ``errno.ENOBUFS`` through ``MultishotDelivery.exception`` and completes
    the current leg; consumers drop held views and start a fresh
    ``recv_many()`` to continue. ``SyntheticRecvBufferPool`` mirrors the same
    accounting for degraded selector and uring receive paths.
    """

    @property
    def buffer_size(self) -> int: ...

    @property
    def buffer_count(self) -> int: ...

    @property
    def leased_count(self) -> int: ...


def _supports_release_buffer() -> bool:
    """Return True when PEP 688 ``__release_buffer__`` exporters are usable."""

    return sys.version_info >= (3, 12)


def _provided_buffer_create_unavailable(exc: BaseException) -> bool:
    return isinstance(exc, OSError) and exc.errno in _PROVIDED_BUFFER_UNAVAILABLE_ERRNOS


class SyntheticRecvBufferPool:
    """Lease-counted buffer pool used when PBUF rings are unavailable or on selector.

    Proactor receive paths deliver copied chunks; ``RecvIterBuffer`` tracks
    leases against this pool so ``sock_recv_iter`` backpressure matches the
    uring provided-buffer contract.
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
    """PEP 688 buffer exporter whose release returns a synthetic pool slot."""

    def __init__(self, data: bytearray, pool: SyntheticRecvBufferPool) -> None:
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


def _leased_synthetic_memoryview(data: bytes | bytearray, pool: SyntheticRecvBufferPool) -> memoryview:
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

    def recv(
        self,
        sock: socket.socket,
        n: int,
    ) -> Operation[bytes]: ...

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

    def accept(self, sock: socket.socket) -> Operation[socket.socket]: ...

    def accept_many(
        self,
        sock: socket.socket,
        callback: _AcceptManyCallback,
    ) -> ContinuousOperation[AcceptManyResult]:
        """Accept connections until cancelled or failed.

        Each callback receives the accepted ``socket``. Call
        ``socket.getpeername()`` when the peer address is needed. Use
        ``ProactorIOManager.accept_many`` for accept-time reads and richer
        delivery shapes.
        """

        ...

    def connect(
        self,
        sock: socket.socket,
        address: Any,
    ) -> Operation[None]:
        """Connect a socket.

        For ``AF_UNIX``, the connect completes synchronously via a brief
        blocking ``sock.connect()`` and ``deliver()``, including when chained
        from ``sock_create``. Inet sockets use the backend's async path.
        ``ProactorIOManager.sock_connect`` composes connect-time send via
        ``IOWaitGroup`` instead.
        """

        ...

    def shutdown(self, sock: socket.socket, how: int) -> Operation[None]:
        """Submit ``socket.shutdown(how)`` for ``sock``."""

        ...

    def close_socket(self, sock: socket.socket) -> Operation[None]:
        """Submit socket close and release the Python wrapper fd."""

        ...

    def create_socket(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
    ) -> Operation[socket.socket]:
        """Create a scheduler-contract socket.

        ``ProactorIOManager.sock_create`` composes create→connect (and optional
        send) via ``IOWaitGroup``.
        """

        ...

    def openat(self, path: str, flags: int, mode: int = 0, *, dfd: int = _DEFAULT_OPENAT_DFD) -> Operation[int]: ...

    def read(self, fd: int, n: int, offset: int) -> Operation[bytes]: ...

    def read_into(self, fd: int, buf: Any, offset: int) -> Operation[int]: ...

    def write(self, fd: int, data: Any, offset: int) -> Operation[int]: ...

    def close_fd(self, fd: int) -> Operation[None]:
        """Close a caller-owned raw file descriptor."""

        ...

    def stat(self, path: str = "", *, fd: int = -1) -> Operation[os.stat_result]: ...

    def stat_fdsize(self, fd: int) -> Operation[int]: ...

    def recv_many(
        self,
        sock: socket.socket,
        callback: _RecvManyCallback,
        *,
        buf_group: RecvBufferPool,
        base_sequence: int = 0,
    ) -> ContinuousOperation[_RecvManyValue]: ...

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> RecvBufferPool: ...

    def shared_recv_buffer_pool(self) -> RecvBufferPool: ...

    def set_shared_recv_buffer_pool(self, pool: RecvBufferPool) -> None: ...

    def poll(self, fd: int, mask: int) -> Operation[int]: ...

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: _PollManyCallback,
    ) -> ContinuousOperation[int]: ...

    def cancel(self, operation: Operation[Any]) -> Operation[None]:
        """Cancel ``operation`` and return the ring cancel operation when applicable."""

        ...


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
        base_sequence: int = 0,
    ) -> ContinuousOperation[_RecvManyValue]:
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

    def _completed_cancel_operation(self, kind: str, target: Operation[Any]) -> Operation[None]:
        cancel_op = Operation[None](kind=kind, fileobj=target)
        cancel_op._finish(result=None)
        return cancel_op

    def _terminalise_cancelled(self, operation: Operation[Any]) -> None:
        if not operation.done():
            operation._finish(exception=CancelledError(), cancelled=True)

    def cancel(self, operation: Operation[Any]) -> Operation[None]:
        raise NotImplementedError

    def openat(self, path: str, flags: int, mode: int = 0, *, dfd: int = _DEFAULT_OPENAT_DFD) -> Operation[int]:
        raise NotImplementedError

    def read(self, fd: int, n: int, offset: int) -> Operation[bytes]:
        raise NotImplementedError

    def read_into(self, fd: int, buf: Any, offset: int) -> Operation[int]:
        raise NotImplementedError

    def write(self, fd: int, data: Any, offset: int) -> Operation[int]:
        raise NotImplementedError

    def close_fd(self, fd: int) -> Operation[None]:
        """Close a caller-owned raw file descriptor."""

        self._check_open()
        if fd < 0:
            operation = Operation[None](kind="close_fd", fileobj=fd)
            operation._finish(result=None)
            return operation
        return _deliver_sync_void_fd_op(self, fd, "close_fd", lambda: _close_raw_fd(fd))

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
                operation._finish(result=os.fstat(fd))
            else:
                operation._finish(result=os.stat(path))
        except OSError as exc:
            operation._finish(exception=exc)
        return operation

    def stat_fdsize(self, fd: int) -> Operation[int]:
        """Return the byte length of an open file descriptor."""

        self._check_open()
        if fd < 0:
            raise ValueError("stat_fdsize() requires fd >= 0")
        operation = Operation[int](kind="stat_fdsize", fileobj=fd)
        try:
            operation._finish(result=os.fstat(fd).st_size)
        except OSError as exc:
            operation._finish(exception=exc)
        return operation

    def poll(self, fd: int, mask: int) -> Operation[int]:
        raise NotImplementedError

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: _PollManyCallback,
    ) -> ContinuousOperation[int]:
        raise NotImplementedError

    def _sync_unix_connect(
        self,
        sock: socket.socket,
        address: Any,
    ) -> Operation[None]:
        """Complete a UNIX-domain connect synchronously and deliver the result.

        io_uring ``submit_connect`` does not accept UNIX sockaddr paths today.
        Both proactor backends use this path so chained ``connect`` legs from
        ``sock_create`` / ``sock_connect`` behave uniformly at the io_manager
        layer even when the operation finishes before the caller's ``wait()`` returns.
        """

        def finish_connect() -> None:
            sock.setblocking(True)
            try:
                sock.connect(address)
            finally:
                sock.setblocking(False)

        operation = cast(Operation[None], _spawn_operation("connect", sock))
        try:
            finish_connect()
            operation.deliver(self, result=None)
        except OSError as exc:
            operation.deliver(self, exception=exc)
        return operation


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
    enobufs_index: int | None = None
    leg_base: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


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
        "poll_remove",
    )

    def __init__(
        self,
        operation: "UringOperation[Any] | UringContinuousOperation[Any]",
        complete: _UringEntryComplete,
        *,
        multishot: bool = False,
        poll_remove: bool = False,
    ) -> None:
        self.operation = operation
        self.complete = complete
        self.completion = None
        self.active = False
        self.multishot_leg = _MultishotLegState() if multishot else None
        self.poll_remove = poll_remove

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
                # ENOBUFS carries the leg index; deliver immediately and let consumers
                # reorder any stragglers while they start a fresh recv_many() later.
                if getattr(completion, "res", 0) == -errno.ENOBUFS:
                    leg.pending_final = None
                    return (completion,)
                leg_sequence = int(completion.sequence) - leg.leg_base
                if leg.nonterminal_seen < leg_sequence:
                    leg.pending_final = completion
                    return ()
                leg.pending_final = None
                return (completion,)
            leg.nonterminal_seen += 1
            pending = leg.pending_final
            if pending is not None and leg.nonterminal_seen >= int(pending.sequence) - leg.leg_base:
                leg.pending_final = None
                return (completion, pending)
            return (completion,)


class UringOperation(Operation[T]):
    """``Operation`` subclass carrying the active io_uring leg for ``UringProactor``."""

    def __init__(
        self,
        kind: str,
        fileobj: object | None = None,
    ) -> None:
        super().__init__(kind, fileobj)
        self._uring_entry: _UringEntry | None = None


class UringContinuousOperation(ContinuousOperation[T_co]):
    """``ContinuousOperation`` subclass carrying the active io_uring leg."""

    def __init__(
        self,
        kind: str,
        fileobj: object | None = None,
        result_callback: Callable[[MultishotDelivery], object] | None = None,
    ) -> None:
        super().__init__(kind, fileobj, result_callback)
        self._uring_entry: _UringEntry | None = None


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
    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> SyntheticRecvBufferPool:
        """Create a synthetic provided-buffer pool for ``recv_many`` / ``sock_recv_iter``."""

        return SyntheticRecvBufferPool(buffer_size, buffer_count)

    def create_buf_group(self, buffer_size: int, buffer_count: int) -> SyntheticRecvBufferPool:
        return self.create_recv_buffer_pool(buffer_size, buffer_count)

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

    def recv(
        self,
        sock: socket.socket,
        n: int,
    ) -> Operation[bytes]:
        """Submit a socket receive operation."""

        operation = cast(Operation[bytes], _spawn_operation("recv", sock))

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

        operation = cast(Operation[None], _spawn_operation("send", sock))
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

    def accept(self, sock: socket.socket) -> Operation[socket.socket]:
        """Submit a socket accept operation."""

        operation = Operation[socket.socket](kind="accept", fileobj=sock)

        def attempt() -> socket.socket:
            conn, _address = sock.accept()
            configure_scheduler_socket(conn)
            return conn

        self._submit_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def shutdown(self, sock: socket.socket, how: int) -> Operation[None]:
        """Submit ``socket.shutdown(how)`` for ``sock``."""

        return _deliver_sync_void_socket_op(self, sock, "shutdown", lambda: sock.shutdown(how))

    def close_socket(self, sock: socket.socket) -> Operation[None]:
        """Submit socket close and release the Python wrapper fd."""

        return _deliver_sync_void_socket_op(self, sock, "close_socket", sock.close)

    def accept_many(
        self,
        sock: socket.socket,
        callback: _AcceptManyCallback,
    ) -> ContinuousOperation[AcceptManyResult]:
        """Accept connections and deliver each via the result callback.

        Without io_uring multishot accept this issues one ``accept()`` per
        ``accept_many`` call, emits the connection, and **finishes** the
        ``ContinuousOperation``. Callers must resubmit (``StreamServer`` re-arms
        in a loop; ``scheduler.io.accept_many().wait()`` returns after each leg).
        This differs from oneshot ``poll_many`` fallbacks, which resubmit inside
        the proactor until cancel. With multishot (``UringProactor`` only) one
        kernel leg may deliver many connections until cancel, error, or terminal CQE.

        `callback` may run on any backend worker thread. Each accepted connection
        is delivered as the accepted ``socket``. Call ``socket.getpeername()`` when
        the peer address is needed.
        """

        operation = _spawn_accept_many_operation(sock, callback)

        def step() -> ContinuousStepResult:
            try:
                conn, _address = sock.accept()
            except (BlockingIOError, InterruptedError):
                return ContinuousStepResult(progressed=False)
            configure_scheduler_socket(conn)
            _handoff_accept_many(operation, conn)
            return ContinuousStepResult(progressed=True, done=True)

        self._submit_socket_continuous_operation(sock, selectors.EVENT_READ, operation, step)
        return operation

    def create_socket(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
    ) -> Operation[socket.socket]:
        """Create a scheduler-contract socket."""

        del flags
        operation = cast(
            Operation[socket.socket],
            _spawn_operation("create_socket", (family, type, proto)),
        )
        try:
            sock = _sync_create_scheduler_socket(family, type, proto)
        except OSError as exc:
            operation.deliver(self, exception=exc)
            return operation
        operation.deliver(self, result=sock)
        return operation

    def connect(
        self,
        sock: socket.socket,
        address: Any,
    ) -> Operation[None]:
        """Submit a socket connect operation."""

        if sock.family == socket.AF_UNIX:
            return self._sync_unix_connect(sock, address)

        return self._submit_selector_connect(sock, address)

    def _submit_selector_connect(
        self,
        sock: socket.socket,
        address: Any,
    ) -> Operation[None]:
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

        operation = cast(Operation[None], _spawn_operation("connect", sock))

        def attempt() -> None:
            finish_connect()

        self._submit_socket_operation(sock, selectors.EVENT_WRITE, operation, attempt)
        return operation

    def recv_many(
        self,
        sock: socket.socket,
        callback: _RecvManyCallback,
        *,
        buf_group: RecvBufferPool,
        base_sequence: int = 0,
    ) -> ContinuousOperation[_RecvManyValue]:
        """Submit one ``recv()`` and deliver a single ``MultishotDelivery``.

        `callback` may run on any backend worker thread. This backend does not
        provide native multishot receive: one ``recv()`` result is delivered at
        ``base_sequence`` with ``more=False`` (data, empty EOF, or ``exception``),
        then the operation completes. Callers that need a byte stream must start
        a fresh ``recv_many()`` for each further chunk.

        ``buf_group`` sizes ``SyntheticRecvBufferPool`` lease accounting;
        selector receive delivers copied ``memoryview`` data per call.
        """

        operation = _spawn_recv_many_operation(sock, callback)

        def step() -> ContinuousStepResult:
            try:
                data = sock.recv(_DEFAULT_SELECTOR_RECV_MANY_CHUNK_SIZE)
            except (BlockingIOError, InterruptedError):
                return ContinuousStepResult(progressed=False)
            except OSError as exc:
                operation._emit_delivery(
                    MultishotDelivery(index=base_sequence, exception=exc, more=False)
                )
                return ContinuousStepResult(progressed=True, done=True)
            if not data:
                operation._emit_result(memoryview(b""), index=base_sequence, more=False)
            else:
                operation._emit_result(memoryview(data), index=base_sequence, more=False)
            return ContinuousStepResult(progressed=True, done=True)

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
        callback: _PollManyCallback,
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
            operation._emit_result(result, more=True)
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
            operation._finish(exception=exc)
            return True
        if step_result.done:
            self._remove_operation(operation)
            operation._finish(result=None)
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
            operation.deliver(self, exception=exc)
        else:
            operation.deliver(self, result=result)
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

    def cancel(self, operation: Operation[Any]) -> Operation[None]:
        if operation.done():
            return self._completed_cancel_operation("cancel", operation)
        with self._lock:
            removed = self._remove_operation(operation)
        if removed:
            self._after_selector_registration_changed()
        self._terminalise_cancelled(operation)
        return self._completed_cancel_operation("cancel", operation)

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
            operation.deliver(self, exception=exc)
        else:
            self._remove_operation(operation)
            operation.deliver(self, result=result)
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
            operation._finish(exception=exc)
            completed.append(operation)
            return
        if step_result.done:
            self._remove_operation(operation)
            operation._finish(result=None)
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
        self._provided_buffers_supported: bool | None = None
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

    def _uring_entry(
        self,
        operation: "UringOperation[Any] | UringContinuousOperation[Any]",
        complete: _UringEntryComplete,
        *,
        multishot: bool = False,
        poll_remove: bool = False,
    ) -> _UringEntry:
        entry = _UringEntry(
            operation=operation,
            complete=complete,
            multishot=multishot,
            poll_remove=poll_remove,
        )
        operation._uring_entry = entry
        return entry

    def cancel(self, operation: Operation[Any]) -> Operation[None]:
        if operation.done():
            return self._completed_cancel_operation("cancel", operation)

        entry = _uring_entry_of(operation)
        if entry is not None:
            completion = entry.completion
            if completion is None:
                self._cancel_deferred_operation(operation)
                self._deactivate_uring_entry(entry)
                cancel_op = self._completed_cancel_operation("cancel", operation)
            elif entry.poll_remove:
                cancel_op = self._submit_cancel_op(
                    completion,
                    kind="poll_remove",
                    submit=self._ring.submit_poll_remove,
                )
            else:
                cancel_op = self._submit_cancel_op(
                    completion,
                    kind="cancel",
                    submit=self._ring.submit_cancel,
                )
            self.break_wait()
        elif self._cancel_deferred_operation(operation):
            self.break_wait()
            cancel_op = self._completed_cancel_operation("cancel", operation)
        else:
            cancel_op = self._completed_cancel_operation("cancel", operation)

        self._terminalise_cancelled(operation)
        return cancel_op

    def _submit_cancel_op(
        self,
        target_completion: _UringCompletion,
        *,
        kind: str,
        submit: Callable[[_UringCompletion, _UringEntry], _UringCompletion],
    ) -> Operation[None]:
        cancel_operation = UringOperation(kind, target_completion)

        def complete_cancel(entry: _UringEntry, completion: _UringCompletion) -> Operation[Any] | None:
            operation = cast(Operation[None], entry.operation)
            operation.deliver(self, result=None)
            return operation

        entry = self._uring_entry(cancel_operation, complete_cancel)
        self._submit_uring_entry(entry, lambda: submit(target_completion, entry))
        return cancel_operation

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> RecvBufferPool:
        """Create a provided-buffer group, or ``SyntheticRecvBufferPool`` when PBUF rings fail."""

        if self._provided_buffers_supported is False:
            return SyntheticRecvBufferPool(buffer_size, buffer_count)
        try:
            pool = self._ring.create_buf_group(buffer_size, buffer_count)
        except OSError as exc:
            if not _provided_buffer_create_unavailable(exc):
                raise
            self._provided_buffers_supported = False
            return SyntheticRecvBufferPool(buffer_size, buffer_count)
        self._provided_buffers_supported = True
        return pool

    def create_buf_group(self, buffer_size: int, buffer_count: int) -> RecvBufferPool:
        return self.create_recv_buffer_pool(buffer_size, buffer_count)

    def _default_shared_recv_buffer_pool_sizes(self) -> tuple[int, int]:
        return _DEFAULT_URING_RECV_MANY_BUFFER_SIZE, _DEFAULT_URING_RECV_MANY_BUFFER_COUNT

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

    def recv(
        self,
        sock: socket.socket,
        n: int,
    ) -> Operation[bytes]:
        """Submit a socket receive operation."""

        operation = UringOperation("recv", sock)
        if n == 0:
            operation.deliver(self, result=b"")
            return operation
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
        operation.deliver(self, result=data[: completion.res].tobytes())
        return operation

    def recv_into(self, sock: socket.socket, buf: Any) -> Operation[int]:
        """Submit a socket receive-into operation."""

        operation = UringOperation("recv_into", sock)
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_recv_into(entry, completion),
        )
        self._submit_uring_entry(entry, lambda: self._ring.submit_recv(sock.fileno(), buf, entry))
        return operation

    def _complete_uring_recv_into(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], entry.operation)
        operation._finish(result=completion.res)
        return operation

    def recvfrom(self, sock: socket.socket, bufsize: int) -> Operation[tuple[bytes, Any]]:
        """Submit a datagram receive operation."""

        operation = UringOperation("recvfrom", sock)
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
        operation._finish(result=(data[: completion.res].tobytes(), completion.result))
        return operation

    def recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> Operation[tuple[int, Any]]:
        """Submit a datagram receive-into operation."""

        operation = UringOperation("recvfrom_into", sock)
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
        operation._finish(result=(completion.res, completion.result))
        return operation

    def send(
        self,
        sock: socket.socket,
        data: Any,
        progress: _ProgressCallback | None = None,
    ) -> Operation[None]:
        """Submit a stream send that drains ``data`` before completing."""

        operation = UringOperation("send", sock)
        payload = memoryview(data)
        if not payload:
            self._check_open()
            operation.deliver(self, result=None)
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
            operation.deliver(self, exception=BlockingIOError(errno.EWOULDBLOCK, "socket send returned zero bytes"))
            return operation
        offset += res
        if progress is not None:
            try:
                progress(offset)
            except BaseException as exc:
                operation.deliver(self, exception=exc)
                return operation
        if offset >= len(data):
            operation.deliver(self, result=None)
            return operation
        sock = cast(socket.socket, operation.fileobj)
        self._submit_sendall(sock, cast("UringOperation[None]", entry.operation), data, offset, progress)
        return None

    def sendto(self, sock: socket.socket, data: Any, address: Any) -> Operation[int]:
        """Submit a datagram send operation."""

        operation = UringOperation("sendto", sock)
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
        operation._finish(result=completion.res)
        return operation

    def accept(self, sock: socket.socket) -> Operation[socket.socket]:
        """Submit a socket accept operation."""

        operation = UringOperation("accept", sock)
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_accept(entry, completion),
        )
        self._submit_uring_entry(entry, lambda: self._ring.submit_accept(sock.fileno(), entry, _DEFAULT_ACCEPT_FLAGS))
        return operation

    def _complete_uring_accept(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[socket.socket]:
        conn = socket_from_uring_fd(completion.res)
        operation = cast(Operation[socket.socket], entry.operation)
        operation._finish(result=conn)
        return operation

    def shutdown(self, sock: socket.socket, how: int) -> Operation[None]:
        """Submit ``socket.shutdown(how)`` for ``sock``."""

        operation = UringOperation("shutdown", sock)
        if sock.fileno() == -1:
            operation.deliver(self, exception=OSError(errno.EBADF, "Bad file descriptor"))
            return operation
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_void_op(entry, completion),
        )
        self._submit_uring_entry(entry, lambda: self._ring.submit_shutdown(sock.fileno(), how, entry))
        return operation

    def close_socket(self, sock: socket.socket) -> Operation[None]:
        """Submit socket close and release the Python wrapper fd."""

        operation = UringOperation("close_socket", sock)
        if sock.fileno() == -1:
            operation.deliver(self, result=None)
            return operation
        fd = sock.detach()
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_void_op(entry, completion),
        )
        self._submit_uring_entry(entry, lambda: self._ring.submit_close(fd, entry))
        return operation

    def close_fd(self, fd: int) -> Operation[None]:
        """Submit raw fd close for caller-owned descriptors (for example from ``openat``)."""

        operation = UringOperation("close_fd", fd)
        if fd < 0:
            operation.deliver(self, result=None)
            return operation
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_void_op(entry, completion),
        )
        self._submit_uring_entry(entry, lambda: self._ring.submit_close(fd, entry))
        return operation

    def _complete_uring_void_op(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[None]:
        operation = cast(Operation[None], entry.operation)
        res = completion.res
        if res < 0:
            self._deactivate_uring_entry(entry)
            operation.deliver(
                self,
                exception=OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")),
            )
            return operation
        operation.deliver(self, result=None)
        return operation

    def accept_many(
        self,
        sock: socket.socket,
        callback: _AcceptManyCallback,
    ) -> ContinuousOperation[AcceptManyResult]:
        """Accept connections and deliver each via the result callback.

        Uses multishot accept when the runtime probe accepts it; otherwise
        submits one ``submit_accept()``, emits the connection, and finishes so
        callers re-arm. `callback` may run on any uring completion service thread.

        Each accepted connection is delivered as the accepted ``socket``. Call
        ``socket.getpeername()`` when the peer address is needed. Use
        ``ProactorIOManager.accept_many`` for accept-time reads and richer
        delivery shapes.
        """

        operation = UringContinuousOperation[AcceptManyResult]("accept_many", sock, callback)
        accept_entry_ref: list[_UringEntry | None] = [None]
        if self._capabilities.get("IORING_ACCEPT_MULTISHOT", False):
            # one multishot accept stays armed until F_MORE clears or we cancel.
            entry = self._uring_entry(
                operation,
                lambda entry, completion: self._deliver_uring_accept_many(
                    entry,
                    completion,
                    accept_entry_ref,
                ),
                multishot=True,
            )
            accept_entry_ref[0] = entry
            self._submit_uring_entry(
                entry,
                lambda: self._ring.submit_accept_multishot(sock.fileno(), entry, _DEFAULT_ACCEPT_FLAGS),
            )
            return operation

        # emulated accept_many: one accept, emit, finish; callers re-arm (for example StreamServer).
        submit_box: list[_UringEntrySubmit] = []
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._deliver_uring_accept_many_oneshot(entry, completion, submit_box),
        )

        def submit_accept() -> _UringCompletion:
            return self._ring.submit_accept(sock.fileno(), entry, _DEFAULT_ACCEPT_FLAGS)

        submit_box.append(submit_accept)
        self._submit_uring_entry(entry, submit_accept)
        return operation

    def _fail_accept_many_operation(
        self,
        operation: ContinuousOperation[AcceptManyResult],
        accept_entry_ref: list[_UringEntry | None],
        exc: BaseException,
    ) -> None:
        accept_entry = accept_entry_ref[0]
        if accept_entry is not None:
            if accept_entry.active:
                completion = accept_entry.completion
                if completion is not None:
                    self._submit_cancel_op(
                        completion,
                        kind="cancel",
                        submit=self._ring.submit_cancel,
                    )
                accept_entry.active = False
            accept_entry_ref[0] = None
        if not operation.done():
            operation._finish(exception=exc)

    def _deliver_uring_accept_many_oneshot(
        self,
        entry: _UringEntry,
        completion: _UringCompletion,
        submit_box: list[_UringEntrySubmit],
    ) -> Operation[Any] | None:
        del submit_box
        operation = cast(ContinuousOperation[AcceptManyResult], entry.operation)
        res = completion.res
        if res < 0:
            self._deactivate_uring_entry(entry)
            operation._finish(exception=OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")))
            return operation
        conn = socket_from_uring_fd(completion.res)
        _handoff_accept_many(operation, conn, more=False)
        self._deactivate_uring_entry(entry)
        operation._finish(result=None)
        return operation

    def _deliver_uring_accept_many(
        self,
        entry: _UringEntry,
        completion: _UringCompletion,
        accept_entry_ref: list[_UringEntry | None],
    ) -> Operation[Any] | None:
        operation = cast(ContinuousOperation[AcceptManyResult], entry.operation)
        res = completion.res
        if res < 0:
            self._deactivate_uring_entry(entry)
            accept_entry_ref[0] = None
            self._fail_accept_many_operation(
                operation,
                accept_entry_ref,
                OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")),
            )
            return operation
        conn = socket_from_uring_fd(completion.res)
        more = bool(completion.flags & uring_api.IORING_CQE_F_MORE)
        if operation.done():
            abortive_close(conn)
        else:
            _handoff_accept_many(operation, conn, more=more)
        if not more:
            self._deactivate_uring_entry(entry)
            accept_entry_ref[0] = None
            if not operation.done():
                operation._finish(result=None)
        return operation

    def create_socket(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
    ) -> Operation[socket.socket]:
        """Create a scheduler-contract socket."""

        if self._capabilities.get("IORING_OP_SOCKET", False):
            socket_type = type | flags | _DEFAULT_ACCEPT_FLAGS
            operation = UringOperation("create_socket", (family, type, proto))
            entry = self._uring_entry(
                operation,
                lambda entry, completion: self._complete_uring_create_socket(entry, completion),
            )
            self._submit_uring_entry(
                entry,
                lambda: self._ring.submit_socket(family, socket_type, proto, 0, entry),
            )
            return operation

        operation = UringOperation("create_socket", (family, type, proto))
        try:
            sock = _sync_create_scheduler_socket(family, type, proto)
        except OSError as exc:
            operation.deliver(self, exception=exc)
            return operation
        operation.deliver(self, result=sock)
        return operation

    def connect(
        self,
        sock: socket.socket,
        address: Any,
    ) -> Operation[None]:
        """Submit a socket connect operation."""

        if sock.family == socket.AF_UNIX:
            return self._sync_unix_connect(sock, address)

        operation = UringOperation("connect", sock)
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_connect(entry, completion),
        )
        self._submit_uring_entry(entry, lambda: self._ring.submit_connect(sock.fileno(), address, entry))
        return operation

    def _complete_uring_connect(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[None]:
        operation = cast(Operation[None], entry.operation)
        operation.deliver(self, result=None)
        return operation

    def _complete_uring_create_socket(
        self,
        entry: _UringEntry,
        completion: _UringCompletion,
    ) -> Operation[socket.socket]:
        operation = cast(Operation[socket.socket], entry.operation)
        operation.deliver(self, result=socket_from_uring_fd(completion.res))
        return operation

    def openat(self, path: str, flags: int, mode: int = 0, *, dfd: int = _DEFAULT_OPENAT_DFD) -> Operation[int]:
        """Submit an io_uring openat operation and return the opened fd on success."""

        operation = UringOperation("openat", path)
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_openat(entry, completion),
        )
        self._submit_uring_entry(entry, lambda: self._ring.submit_openat(path, flags, mode, entry, dfd=dfd))
        return operation

    def _complete_uring_openat(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], entry.operation)
        operation._finish(result=completion.res)
        return operation

    def read(self, fd: int, n: int, offset: int) -> Operation[bytes]:
        """Submit a positioned file read that completes with the bytes read."""

        operation = UringOperation("read", fd)
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
        operation._finish(result=data[: completion.res].tobytes())
        return operation

    def read_into(self, fd: int, buf: Any, offset: int) -> Operation[int]:
        """Submit a positioned file read into a caller-provided buffer."""

        operation = UringOperation("read_into", fd)
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_read_into(entry, completion),
        )
        self._submit_uring_entry(entry, lambda: self._ring.submit_read(fd, buf, offset, entry))
        return operation

    def _complete_uring_read_into(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], entry.operation)
        operation._finish(result=completion.res)
        return operation

    def write(self, fd: int, data: Any, offset: int) -> Operation[int]:
        """Submit a positioned file write and return the byte count written."""

        operation = UringOperation("write", fd)
        payload = memoryview(data)
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_write(entry, completion),
        )
        self._submit_uring_entry(entry, lambda: self._ring.submit_write(fd, payload, offset, entry))
        return operation

    def _complete_uring_write(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], entry.operation)
        operation._finish(result=completion.res)
        return operation

    def stat(self, path: str = "", *, fd: int = -1) -> Operation[os.stat_result]:
        """Return file metadata via io_uring statx when probed, else blocking ``os.stat``."""

        self._check_open()
        if fd < 0 and not path:
            raise ValueError("stat() requires fd >= 0 or a non-empty path")
        if not self._capabilities.get("IORING_OP_STATX", False) or not hasattr(self._ring, "submit_statx"):
            return super().stat(path, fd=fd)

        operation = UringOperation("stat", fd if fd >= 0 else path)
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
            operation._finish(result=_stat_result_from_statx(data))
        except ValueError as exc:
            operation._finish(exception=exc)
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

        operation = UringOperation("stat_fdsize", fd)
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
                operation._finish(result=os.fstat(cast(int, operation.fileobj)).st_size)
            except OSError as exc:
                operation._finish(exception=exc)
            return operation
        operation._finish(result=cast(int, size))
        return operation

    def recv_many(
        self,
        sock: socket.socket,
        callback: _RecvManyCallback,
        *,
        buf_group: RecvBufferPool,
        base_sequence: int = 0,
    ) -> ContinuousOperation[_RecvManyValue]:
        """Start a continuous receive operation that completes on EOF.

        `callback` may run on any uring completion service thread.

        When multishot provided-buffer receive is available, each callback
        receives ``MultishotDelivery`` with stream ``index`` (``completion.sequence``,
        seeded by ``base_sequence`` at submit), leased ``memoryview`` data in
        ``value``, optional ``exception``, and ``more``. Callback delivery may
        arrive out of order across completion threads; consumers that need
        stream order must reorder by index themselves. Chunk sizes come from the
        operation's ``BufGroup`` pool. Holding live views can pin provided
        buffers and stall further receives. ``errno.ENOBUFS`` is delivered
        through ``exception`` at the terminal ``index`` and completes the current
        leg. ``more=False`` with non-empty data means the leg stopped before EOF;
        consumers drop held views and start a fresh ``recv_many()`` with
        ``base_sequence`` set to ``index + 1``.

        When multishot receive is unavailable, the proactor submits one
        ``submit_recv()`` and delivers a single ``MultishotDelivery`` at
        ``base_sequence`` with ``more=False`` (copied ``memoryview`` data, empty
        EOF, or ``exception``), then completes. Callers that need a byte stream
        must start a fresh ``recv_many()`` for each further chunk.

        ``buf_group`` must be a provided-buffer pool from
        ``create_recv_buffer_pool()`` or ``shared_recv_buffer_pool()``.
        """

        operation = UringContinuousOperation[_RecvManyValue]("recv_many", sock, callback)
        if self._capabilities.get("IORING_RECV_MULTISHOT", False):
            uring_group = cast(_UringBufGroup, buf_group)
            leg_base = base_sequence
            entry = self._uring_entry(
                operation,
                lambda entry, completion: self._deliver_uring_recv_many(entry, completion),
                multishot=True,
            )

            def submit_recv_many() -> _UringCompletion:
                leg = entry.multishot_leg
                assert leg is not None
                leg.leg_base = leg_base
                leg.nonterminal_seen = 0
                leg.pending_final = None
                return self._ring.submit_recv_multishot(
                    sock.fileno(),
                    uring_group,
                    entry,
                    base_sequence=leg_base,
                )

            self._submit_uring_entry(entry, submit_recv_many)
            return operation

        buffer = bytearray(_DEFAULT_SELECTOR_RECV_MANY_CHUNK_SIZE)
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._deliver_uring_recv_oneshot(
                entry, completion, buffer, base_sequence
            ),
        )
        self._submit_uring_entry(entry, lambda: self._ring.submit_recv(sock.fileno(), buffer, entry))
        return operation

    def _deliver_uring_recv_oneshot(
        self,
        entry: _UringEntry,
        completion: _UringCompletion,
        buffer: bytearray,
        base_sequence: int,
    ) -> Operation[Any] | None:
        operation = cast(ContinuousOperation[_RecvManyValue], entry.operation)
        res = completion.res
        if res < 0:
            operation._emit_delivery(
                MultishotDelivery(
                    index=base_sequence,
                    exception=OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")),
                    more=False,
                )
            )
            operation._finish(result=None)
            self._deactivate_uring_entry(entry)
            return operation
        if res == 0:
            operation._emit_result(memoryview(b""), index=base_sequence, more=False)
        else:
            operation._emit_result(memoryview(bytes(buffer[:res])), index=base_sequence, more=False)
        operation._finish(result=None)
        self._deactivate_uring_entry(entry)
        return operation

    def poll(self, fd: int, mask: int) -> Operation[int]:
        """Submit a one-shot io_uring poll operation."""

        # mask and fd go straight to io_uring; bad values show up as CQE errors.
        # selector validates masks (select() fd lists) and fd>=0; no per-fd exclusivity.
        operation = UringOperation("poll", fd)
        entry = self._uring_entry(
            operation,
            lambda entry, completion: self._complete_uring_poll(entry, completion),
        )
        self._submit_uring_entry(entry, lambda: self._ring.submit_poll(fd, mask, entry))
        return operation

    def _complete_uring_poll(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], entry.operation)
        operation._finish(result=completion.res)
        return operation

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: _PollManyCallback,
    ) -> ContinuousOperation[int]:
        """Start a continuous io_uring poll operation.

        Uses multishot poll when the runtime probe accepts it; otherwise falls
        back to resubmitting one-shot ``submit_poll()`` after each readiness
        event. `callback` may run on any uring completion service thread.
        """

        # mask handling matches poll(); no pre-validation on the uring path.
        operation = UringContinuousOperation[int]("poll_many", fd, callback)
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
            operation._finish(exception=OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")))
            return operation
        more = True
        operation._emit_result(res, more=more)
        if operation.done():
            return operation
        self._queue_entry_resubmit(entry, submit_box[0])
        return None

    def _deliver_uring_poll_many(self, entry: _UringEntry, completion: _UringCompletion) -> Operation[Any] | None:
        operation = cast(ContinuousOperation[int], entry.operation)
        res = completion.res
        if res < 0:
            self._deactivate_uring_entry(entry)
            operation._finish(exception=OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")))
            return operation
        more = bool(completion.flags & uring_api.IORING_CQE_F_MORE)
        operation._emit_result(res, more=more)
        if not more:
            operation._finish(result=None)
            self._deactivate_uring_entry(entry)
        return operation

    def _maybe_finish_recv_many_enobufs_leg(
        self,
        entry: _UringEntry,
        multishot_leg: _MultishotLegState,
        operation: ContinuousOperation[_RecvManyValue],
    ) -> None:
        enobufs_index = multishot_leg.enobufs_index
        if enobufs_index is None:
            return
        if multishot_leg.nonterminal_seen >= enobufs_index - multishot_leg.leg_base:
            multishot_leg.enobufs_index = None
            operation._finish(result=None)
            self._deactivate_uring_entry(entry)

    def _deliver_uring_recv_many(
        self,
        entry: _UringEntry,
        completion: _UringCompletion,
    ) -> Operation[Any] | None:
        operation = cast(ContinuousOperation[_RecvManyValue], entry.operation)
        res = completion.res
        multishot_leg = entry.multishot_leg
        assert multishot_leg is not None
        index = int(completion.sequence)

        if res < 0:
            if res == -errno.ENOBUFS:
                multishot_leg.pending_final = None
                multishot_leg.enobufs_index = index
                operation._emit_delivery(
                    MultishotDelivery(
                        index=index,
                        value=memoryview(b""),
                        exception=_enobufs_error(),
                        more=False,
                    )
                )
                self._maybe_finish_recv_many_enobufs_leg(entry, multishot_leg, operation)
                return operation
            self._deactivate_uring_entry(entry)
            operation._finish(exception=OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")))
            return operation

        more = bool(completion.flags & uring_api.IORING_CQE_F_MORE)
        if res == 0:
            operation._emit_result(memoryview(b""), index=index, more=more)
        else:
            operation._emit_result(
                memoryview(cast(Any, completion.result)),
                index=index,
                more=more,
            )

        self._maybe_finish_recv_many_enobufs_leg(entry, multishot_leg, operation)
        if operation.done():
            return operation
        if not more:
            operation._finish(result=None)
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
        if entry.operation._uring_entry is entry:
            entry.operation._uring_entry = None

    def _fail_uring_entry(self, entry: _UringEntry, exc: BaseException) -> None:
        self._deactivate_uring_entry(entry)
        operation = entry.operation
        if operation.done():
            return
        operation.deliver(self, exception=exc)
        if operation.done():
            self.break_wait()
            self._notify_completed()

    def _deliver_uring_completion(self, completions: list[_UringCompletion]) -> None:
        completed_operation: Operation[Any] | None = None
        for completion in completions:
            if completion.kind == uring_api.COMPLETION_KIND_POLL_REMOVE:
                result = self._complete_uring_operation(completion)
                if result is not None:
                    completed_operation = result
                poll_target = completion.cancel_target
                if poll_target is not None:
                    poll_entry = cast(_UringCompletion, poll_target).user_data
                    if isinstance(poll_entry, _UringEntry):
                        self._deactivate_uring_entry(poll_entry)
                continue
            if completion.kind == uring_api.COMPLETION_KIND_CANCEL:
                result = self._complete_uring_operation(completion)
                if result is not None:
                    completed_operation = result
                continue
            entry = cast(_UringEntry, completion.user_data)
            to_process = entry.completions_to_process(completion)
            if not to_process and entry.operation.done():
                # Late multishot CQEs after cancel/terminal finish: drop the leg
                # without re-entering delivery (completions_to_process already
                # discarded them).
                self._deactivate_uring_entry(entry)
                continue
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
            entry.operation._uring_entry = entry
            self._note_submit_attempt()
            entry.completion = submit()
            # cancel() may have terminalised the target while submit() was in
            # flight; drop the leg promptly so pending tokens stay accurate.
            if entry.operation.done():
                self._deactivate_uring_entry(entry)
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
                uring_operation = cast("UringOperation[Any] | UringContinuousOperation[Any]", operation)
                if uring_operation._uring_entry is entry:
                    uring_operation._uring_entry = None
                return True
        return False

    def _submit_sendall(
        self,
        sock: socket.socket,
        operation: "UringOperation[None]",
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
        operation: "UringOperation[Any]",
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
        if entry.operation.done():
            if entry.active:
                self._deactivate_uring_entry(entry)
            if (
                entry.operation.kind == "create_socket"
                and completion.kind == uring_api.COMPLETION_KIND_SOCKET
                and res >= 0
            ):
                _close_raw_fd(res)
            return None
        has_more = bool(completion.flags & uring_api.IORING_CQE_F_MORE)
        if completion.multishot:
            if entry.operation.done():
                if entry.active:
                    self._deactivate_uring_entry(entry)
                return entry.operation
            multishot_leg = entry.multishot_leg
            if not entry.active:
                if multishot_leg is not None and entry.operation.kind == "recv_many":
                    return entry.complete(entry, completion)
                return None
            return entry.complete(entry, completion)
        assert entry.active
        if not has_more:
            self._deactivate_uring_entry(entry)
        if entry.operation.done():
            return entry.operation
        if res < 0:
            entry.operation.deliver(
                self,
                exception=OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")),
            )
            return entry.operation
        return entry.complete(entry, completion)

    def _raise_unsupported(self, operation: str) -> NoReturn:
        self._check_open()
        raise NotImplementedError(f"UringProactor does not yet support {operation} operations")


def _default_proactor_factory() -> Proactor:
    if uring_api.is_available():
        return UringProactor()
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
        self._io = ProactorIOManager(self, self._proactor)

    @property
    def io(self) -> ProactorIOManager:
        """Return the blocking IO facade for this scheduler."""

        return self._io

    @property
    def proactor(self) -> Proactor:
        """Return the proactor backend owned by this scheduler."""

        return self._proactor

    def close(self) -> None:
        """Close proactor and scheduler-owned resources.

        Exceptional ``IOWaiter.wait()`` exits and ``Proactor.cancel()`` submit
        async ring-cancel / ``poll_remove`` teardown legs without awaiting them.
        ``UringProactor.has_pending_operations()`` may stay true briefly until
        those CQEs complete. Pump ``proactor.wait()`` or ``wait()`` on returned
        teardown operations when strict ring quiescence is required before
        ``UringProactor.close()``.
        """

        self._io.close()
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
