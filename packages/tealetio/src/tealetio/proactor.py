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
from dataclasses import dataclass
from typing import Any, NoReturn, Protocol, TypeAlias, TypeVar, cast, overload

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
from .socket_helpers import (
    configure_scheduler_socket,
    is_soft_accept_errno as _is_soft_accept_errno,
    is_soft_accept_error as _is_soft_accept_error,
    socket_from_uring_fd,
)
from .operations import (
    ContinuousOperation,
    ContinuousStepResult,
    MultishotDelivery,
    Operation,
    SupportsContinuousOperation,
    SupportsOperation,
    T_co,
    io_cancellation_error,
)
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
    "SupportsContinuousOperation",
    "SupportsOperation",
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
    "SyncUringProactor",
    "ThreadedSelectorProactor",
    "UringProactor",
    "UringSubmissionStats",
    "ProactorFile",
    "MultishotDelivery",
    "RecvBufferPool",
    "SyntheticRecvBufferPool",
    "AcceptManyResult",
]


_DoneCallback = Callable[[SupportsOperation[Any]], object]
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
_RecvMultishotImpl = Callable[..., ContinuousOperation[_RecvManyValue]]
AcceptManyResult: TypeAlias = socket.socket
_AcceptManyCallback = Callable[[MultishotDelivery], object]
_AcceptMultishotImpl = Callable[..., ContinuousOperation[AcceptManyResult]]
_PollManyCallback = Callable[[MultishotDelivery], object]


class WakeupManager(Protocol):
    """Cross-thread wakeup primitive for proactor ``wait`` / ``wait_async``."""

    def wakeup(self) -> None:
        """Wake sync and async waiters, or latch until ``wait()`` / ``poll()``."""

    def wait(self, timeout: float | None = None) -> bool:
        """Block until ``wakeup()`` or ``timeout`` elapses."""

    def poll(self) -> bool:
        """Return whether a wakeup is pending, consuming it when true."""

    async def wait_async(self, timeout: float | None = None) -> None:
        """Await ``wakeup()`` or ``timeout`` on the running event loop."""


class EventWakeupManager:
    """Threading and asyncio event pair for proactor wait hosts."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._async_loop: _asyncio.AbstractEventLoop | None = None
        self._async_waiter: _asyncio.Event | None = None

    def wait(self, timeout: float | None = None) -> bool:
        woke = self._event.wait(timeout=timeout)
        if woke:
            self._event.clear()
        return woke

    def wakeup(self) -> None:
        self._event.set()
        waiter = self._async_waiter
        if waiter is not None:
            loop = self._async_loop
            assert loop is not None
            try:
                loop.call_soon_threadsafe(waiter.set)
            except RuntimeError:
                return

    def poll(self) -> bool:
        result = self._event.is_set()
        if result:
            self._event.clear()
        return result

    def bind_loop(self, loop: _asyncio.AbstractEventLoop) -> None:
        """Wire the asyncio loop and waiter used by ``wakeup()`` / ``wait_async()``."""

        if self._async_loop is None:
            self._async_loop = loop
        elif self._async_loop is not loop:
            raise RuntimeError("EventWakeupManager is already bound to a different event loop")
        if self._async_waiter is None:
            self._async_waiter = _asyncio.Event()

    async def wait_async(self, timeout: float | None = None) -> None:
        if self.poll():
            return

        waiter = self._async_waiter
        assert waiter is not None

        waiter.clear()
        try:
            if timeout is None:
                await waiter.wait()
            else:
                try:
                    await _asyncio.wait_for(waiter.wait(), timeout=timeout)
                except _asyncio.TimeoutError:
                    pass
        finally:
            waiter.clear()


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


def _uring_cqe_oserror(res: int) -> OSError:
    return OSError(-res, errno.errorcode.get(-res, "io_uring operation failed"))


def _recv_many_error_delivery(*, index: int, res: int) -> MultishotDelivery:
    return MultishotDelivery(
        index=index,
        value=memoryview(b""),
        exception=_uring_cqe_oserror(res),
        more=False,
    )


def _recv_many_enobufs_delivery(*, index: int) -> MultishotDelivery:
    return MultishotDelivery(
        index=index,
        value=memoryview(b""),
        exception=_enobufs_error(),
        more=False,
    )


def _continuous_error_delivery(exc: BaseException, *, index: int | None = 0) -> MultishotDelivery:
    return MultishotDelivery(index=index, exception=exc, more=False)


def _soft_accept_terminal_delivery(*, index: int | None = 0) -> MultishotDelivery:
    """Terminal accept leg with no connection and no failure (re-arm friendly).

    Hosts re-arm rather than fail the server on transient EMFILE/etc. Under
    sustained fd pressure the listen fd often stays readable, so re-arm can
    busy-loop; see ``socket_helpers`` soft-accept note.
    """

    return MultishotDelivery(index=index, value=None, exception=None, more=False)


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
    index: int = 0,
) -> None:
    """Emit one accepted connection to the parent result callback."""

    parent._emit_result(conn, more=more, index=index)


def _enobufs_error() -> OSError:
    return OSError(errno.ENOBUFS, errno.errorcode.get(errno.ENOBUFS, "no buffer space"))


def _synthetic_recv_pool_is_full(buf_group: RecvBufferPool) -> bool:
    if not _is_synthetic_recv_buffer_pool(buf_group):
        return False
    return buf_group.leased_count >= buf_group.buffer_count


def _complete_recv_many_enobufs(
    operation: ContinuousOperation[_RecvManyValue],
    *,
    index: int,
) -> ContinuousOperation[_RecvManyValue]:
    operation._finish_with_terminal_delivery(_recv_many_enobufs_delivery(index=index))
    return operation


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

    Optional ``release_callback`` is an owner hook used by ``close()``: when set,
    ``close()`` returns the pool to its owner (for example the IO manager size
    cache) instead of destroying it. ``close()`` does not clear the hook. Soft
    returns (cache free list) keep it so a second ``close()`` is a soft no-op;
    clear the hook only immediately before intentional hard dispose (for uring
    ``BufGroup``, no-callback ``close()`` frees the kernel ring).
    """

    @property
    def buffer_size(self) -> int: ...

    @property
    def buffer_count(self) -> int: ...

    @property
    def leased_count(self) -> int: ...

    release_callback: Callable[["RecvBufferPool"], object] | None

    def close(self) -> None: ...


def _supports_release_buffer() -> bool:
    """Return True when PEP 688 ``__release_buffer__`` exporters are usable."""

    return sys.version_info >= (3, 12)


def _is_synthetic_recv_buffer_pool(buf_group: RecvBufferPool) -> bool:
    return isinstance(buf_group, SyntheticRecvBufferPool)


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
        self.release_callback: Callable[[RecvBufferPool], object] | None = None

    def close(self) -> None:
        """Return to owner via ``release_callback``, or drop the synthetic pool.

        Does not clear ``release_callback``; the owner (for example the size
        cache) clears it only when hard-disposing. A second ``close()`` while
        free re-enters the owner and is a soft no-op.
        """

        release = self.release_callback
        if release is not None:
            release(self)

    def _note_leased(self) -> None:
        self.leased_count += 1

    def _note_unleased(self) -> None:
        if self.leased_count:
            self.leased_count -= 1

    def note_chunk_released(self) -> None:
        """Explicitly return one leased slot (tests and manual consumers)."""

        self._note_unleased()

    def lease_delivery_chunk(self, data: bytes | bytearray) -> memoryview:
        """Return a pool-leased view for one delivered receive chunk when supported."""

        return _leased_synthetic_memoryview(data, self)

    def wrap_delivery_chunk(self, data: memoryview) -> memoryview:
        """Lease an already-delivered chunk when lease accounting applies."""

        if not data or getattr(data.obj, "__release_buffer__", None) is not None:
            return data
        if not _supports_release_buffer():
            return data
        return self.lease_delivery_chunk(bytes(data))


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


def _selector_recv_many_chunk_view(data: bytes, buf_group: RecvBufferPool) -> memoryview:
    if not data:
        return memoryview(b"")
    if _is_synthetic_recv_buffer_pool(buf_group):
        return cast(SyntheticRecvBufferPool, buf_group).lease_delivery_chunk(data)
    return memoryview(data)


def _leased_synthetic_memoryview(data: bytes | bytearray, pool: SyntheticRecvBufferPool) -> memoryview:
    payload = data if type(data) is bytearray else bytearray(data)
    if not _supports_release_buffer():
        # PEP 688 buffer exporters need Python 3.12+; callers cannot release pool
        # slots via memoryview.release() on older builds, so skip lease accounting.
        return memoryview(payload)
    pool._note_leased()
    return memoryview(_LeasedChunk(payload, pool))


_UringRingFactory = Callable[[int, int], _UringRing]


def _default_uring_ring_factory(entries: int, flags: int) -> _UringRing:
    return uring_api.Ring(entries=entries, flags=flags)


class Proactor(Protocol):
    """Minimal completion-oriented IO backend used by `ProactorScheduler`."""

    def close(self) -> None: ...

    def wake_wait(self) -> None: ...

    def set_delivery_exception_handler(
        self,
        handler: Callable[[dict[str, Any]], object] | None,
    ) -> None: ...

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
        *,
        base_sequence: int = 0,
    ) -> ContinuousOperation[AcceptManyResult]:
        """Accept connections until cancelled or failed.

        Each callback receives the accepted ``socket``. Call
        ``socket.getpeername()`` when the peer address is needed. Use
        ``ProactorIOManager.accept_many`` for accept-time reads and richer
        delivery shapes.

        ``base_sequence`` seeds delivery ``index`` for the first accept leg
        (multishot: first kernel sequence; oneshot/selector: that single leg).
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

        ``ProactorIOManager.sock_create`` creates sockets directly and only
        composes connect (and optional send) via ``IOWaitGroup``. This method
        remains for direct proactor callers (including the uring socket op).
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

    def cancel(self, operation: SupportsOperation[Any]) -> SupportsOperation[None]:
        """Cancel ``operation`` and return the ring cancel operation when applicable."""

        ...

    def recycle_operation(self, operation: SupportsOperation[Any]) -> None:
        """Return a finished waitable to a backend freelist when safe (optional)."""

        ...


ProactorFactory = Callable[[], Proactor]


class ProactorBase:
    """Shared helpers for concrete proactor backends."""

    def __init__(self) -> None:
        self._closed = False
        self._clock = time.monotonic
        self._async_wait_loop: _asyncio.AbstractEventLoop | None = None
        self._async_break: Callable[[], object] | None = None
        self._shared_recv_buffer_pool: RecvBufferPool | None = None
        self._delivery_exception_handler: Callable[[dict[str, Any]], object] | None = None

    def recycle_operation(self, operation: SupportsOperation[Any]) -> None:
        """Return a finished waitable to a freelist when the backend supports it.

        Default is a no-op (selector and other backends). ``UringProactor`` pools
        finished one-shot and non-``poll_many`` continuous waitables when
        terminal and not ring-live.
        """

        return

    def set_delivery_exception_handler(
        self,
        handler: Callable[[dict[str, Any]], object] | None,
    ) -> None:
        """Route continuous delivery-callback failures to ``handler`` when set."""

        self._delivery_exception_handler = handler

    @overload
    def _guard_delivery_callback(
        self,
        callback: Callable[[MultishotDelivery], object],
    ) -> Callable[[MultishotDelivery], object]: ...

    @overload
    def _guard_delivery_callback(self, callback: None) -> None: ...

    def _guard_delivery_callback(
        self,
        callback: Callable[[MultishotDelivery], object] | None,
    ) -> Callable[[MultishotDelivery], object] | None:
        if callback is None:
            return None

        def guarded(delivery: MultishotDelivery) -> None:
            # Delivery callbacks own ``finish_operation``; the guard only routes failures.
            try:
                callback(delivery)
            except BaseException as exc:
                handler = self._delivery_exception_handler
                if handler is None:
                    raise
                handler(
                    {
                        "message": "Exception in delivery callback",
                        "exception": exc,
                        "delivery": delivery,
                    }
                )

        return guarded

    def set_async_break(self, callback: Callable[[], object] | None) -> None:
        """Optional hook ``SelectorProactor.wake_wait()`` runs after selector wakeup."""

        self._async_break = callback

    def _run_async_break(self) -> None:
        break_cb = self._async_break
        if break_cb is not None:
            break_cb()

    def bind_loop(self, loop: _asyncio.AbstractEventLoop) -> None:
        """Bind this proactor to an asyncio event loop for async waits."""

        if self._async_wait_loop is None:
            self._async_wait_loop = loop
        elif self._async_wait_loop is not loop:
            raise RuntimeError(f"{type(self).__name__} is already bound to a different event loop")
        self._bind_wakeup_loop(loop)

    def _bind_wakeup_loop(self, loop: _asyncio.AbstractEventLoop) -> None:
        return

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
        """Apply local cancel when the backend will not produce a completion.

        One-shot ops finish with ``OSError(ECANCELED)``. Continuous ops must still
        emit a terminal ``MultishotDelivery`` to the result (deliver) callback —
        including never-submitted / deferred legs — so consumers can
        ``finish_operation`` and observe cancel on the multishot stream. Do not
        skip that path for ``_finish`` alone.
        """

        if operation.done():
            return
        cancel_exc = io_cancellation_error()
        if isinstance(operation, ContinuousOperation):
            operation._finish_with_terminal_delivery(
                _continuous_error_delivery(cancel_exc, index=None),
            )
            return
        operation._finish(exception=cancel_exc)

    def cancel(self, operation: SupportsOperation[Any]) -> SupportsOperation[None]:
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


# Uring waitables are themselves Completion.user_data (no separate _UringEntry).
_UringOp: TypeAlias = "UringOperation[Any] | UringContinuousOperation[Any]"
# Stable complete path: unbound UringProactor method; context in cq0..cq3.
_UringOpComplete = Callable[["UringProactor", "_UringOp", "_UringCompletion"], Operation[Any] | None]
# Stable submit path: one module-level function per opcode family; args live on the op.
_UringSqImpl = Callable[["UringProactor", "_UringOp"], "_UringCompletion"]

_URING_OP_SQ_SLOTS = (
    "_pooled",
    "complete",
    "completion",
    "poll_remove",
    "sq_impl",
    "sq0",
    "sq1",
    "sq2",
    "sq3",
    "sq4",
    "cq0",
    "cq1",
    "cq2",
    "cq3",
)

_DEFAULT_URING_OP_POOL_MAX = 256


def _init_uring_ring_leg_fields(op: "_UringOp") -> None:
    """Initialise every ring-leg slot (constructors and freelist reinit)."""

    op.complete = None
    op.completion = None
    op.poll_remove = False
    op.sq_impl = None
    op.sq0 = None
    op.sq1 = None
    op.sq2 = None
    op.sq3 = None
    op.sq4 = None
    op.cq0 = None
    op.cq1 = None
    op.cq2 = None
    op.cq3 = None


class UringOperation(Operation[T]):
    """Uring waitable: public result surface plus the active ring leg.

    Passed as ``uring_api.Completion.user_data`` so delivery does not need a
    separate Entry object. Ring-leg fields are filled by ``_prepare_uring_op``
    / ``_arm_sq``. Finished waitables return to the proactor freelist via
    ``recycle_operation`` (``IOWaiter.wait()`` / ``forget()`` on the common path).
    """

    __slots__ = _URING_OP_SQ_SLOTS
    _pooled: bool
    complete: _UringOpComplete | None
    # Live ring Completion, or None when idle / deactivated. Installed by
    # ``Ring.pre_submit`` before ``io_uring_submit`` (see ``_on_uring_pre_submit``).
    completion: _UringCompletion | None
    poll_remove: bool
    sq_impl: _UringSqImpl | None
    sq0: object
    sq1: object
    sq2: object
    sq3: object
    sq4: object
    cq0: object
    cq1: object
    cq2: object
    cq3: object

    def __init__(
        self,
        proactor: "UringProactor",
        kind: str,
        fileobj: object | None = None,
    ) -> None:
        super().__init__(kind, fileobj, pending_bucket=proactor._pending_operations)
        self._pooled = False
        _init_uring_ring_leg_fields(self)

    def _reinit_from_pool(
        self,
        kind: str,
        fileobj: object | None,
        pending_bucket: list[None],
    ) -> None:
        # Structural fields for a new life; sq/cq/complete filled by prepare/arm.
        pending_bucket.append(None)
        self._pending_bucket = pending_bucket
        self.kind = kind
        self.fileobj = fileobj
        self._resolved = None
        self._callbacks = []
        self._pooled = False
        _init_uring_ring_leg_fields(self)

    def _scrub_for_pool(self) -> None:
        # Drop refs that pin large objects while idle (result cargo, buffers).
        # complete / poll_remove / sq_impl are left for reinit+prepare.
        # _resolved is cleared after the freelist gate has accepted this op.
        self.fileobj = None
        self._resolved = None
        self._callbacks = []
        self.completion = None
        self.sq0 = None
        self.sq1 = None
        self.sq2 = None
        self.sq3 = None
        self.sq4 = None
        self.cq0 = None
        self.cq1 = None
        self.cq2 = None
        self.cq3 = None


class UringContinuousOperation(ContinuousOperation[T_co]):
    """Continuous uring waitable; freelist-capable like ``UringOperation``."""

    __slots__ = _URING_OP_SQ_SLOTS
    _pooled: bool
    complete: _UringOpComplete | None
    # Live ring Completion, or None when idle / deactivated. Installed by
    # ``Ring.pre_submit`` before ``io_uring_submit`` (see ``_on_uring_pre_submit``).
    completion: _UringCompletion | None
    poll_remove: bool
    sq_impl: _UringSqImpl | None
    sq0: object
    sq1: object
    sq2: object
    sq3: object
    sq4: object
    cq0: object
    cq1: object
    cq2: object
    cq3: object

    def __init__(
        self,
        proactor: "UringProactor",
        kind: str,
        fileobj: object | None = None,
        result_callback: Callable[[MultishotDelivery], object] | None = None,
    ) -> None:
        super().__init__(
            kind,
            fileobj,
            result_callback,
            pending_bucket=proactor._pending_operations,
        )
        self._pooled = False
        _init_uring_ring_leg_fields(self)

    def _reinit_from_pool(
        self,
        kind: str,
        fileobj: object | None,
        result_callback: Callable[[MultishotDelivery], object] | None,
        pending_bucket: list[None],
    ) -> None:
        pending_bucket.append(None)
        self._pending_bucket = pending_bucket
        self.kind = kind
        self.fileobj = fileobj
        self._resolved = None
        self._callbacks = []
        self._result_callback = result_callback
        self._pooled = False
        _init_uring_ring_leg_fields(self)

    def _scrub_for_pool(self) -> None:
        # Drop result cargo and delivery callback while idle in the pool.
        self.fileobj = None
        self._resolved = None
        self._callbacks = []
        self._result_callback = None
        self.completion = None
        self.sq0 = None
        self.sq1 = None
        self.sq2 = None
        self.sq3 = None
        self.sq4 = None
        self.cq0 = None
        self.cq1 = None
        self.cq2 = None
        self.cq3 = None


class _UringOpPool:
    """Capped freelist for one-shot and continuous uring waitables.

    ``poll_many`` is never recycled: io_uring does not promise that no poll CQE
    arrives after ``POLL_REMOVE`` is finalized, and completion threads can still
    deliver a late poll into Python after stop settles. Pooling that waitable
    would alias ``user_data``. Other continuous streams (``recv_many`` /
    ``accept_many``) finish through ordered terminal CQEs and may be freelisted
    when terminal and ``completion is None``.

    Owned by ``UringProactor``. Release via ``recycle_operation`` / ``IOWaiter``.
    """

    __slots__ = (
        "_max",
        "_one_shot",
        "_continuous",
        "hits",
        "misses",
        "releases",
        "drops",
    )

    def __init__(self, max_size: int) -> None:
        if max_size < 0:
            raise ValueError("op_pool_max must be >= 0")
        self._max = max_size
        self._one_shot: list[UringOperation[Any]] = []
        self._continuous: list[UringContinuousOperation[Any]] = []
        self.hits = 0
        self.misses = 0
        self.releases = 0
        self.drops = 0

    def acquire_one_shot(
        self,
        proactor: "UringProactor",
        kind: str,
        fileobj: object | None = None,
    ) -> UringOperation[Any]:
        if self._one_shot:
            op = self._one_shot.pop()
            op._reinit_from_pool(kind, fileobj, proactor._pending_operations)
            self.hits += 1
            return op
        self.misses += 1
        return UringOperation(proactor, kind, fileobj)

    def acquire_continuous(
        self,
        proactor: "UringProactor",
        kind: str,
        fileobj: object | None = None,
        result_callback: Callable[[MultishotDelivery], object] | None = None,
    ) -> UringContinuousOperation[Any]:
        if self._continuous:
            op = self._continuous.pop()
            op._reinit_from_pool(kind, fileobj, result_callback, proactor._pending_operations)
            self.hits += 1
            return op
        self.misses += 1
        return UringContinuousOperation(proactor, kind, fileobj, result_callback)

    def release(self, operation: object) -> None:
        if isinstance(operation, UringContinuousOperation):
            # poll_many only: never pool (late CQEs after stop / POLL_REMOVE).
            if operation.kind == "poll_many":
                return
            self._release_into(self._continuous, operation)
        elif isinstance(operation, UringOperation):
            self._release_into(self._one_shot, operation)

    def _release_into(
        self,
        pool: list[Any],
        op: UringOperation[Any] | UringContinuousOperation[Any],
    ) -> None:
        if self._max == 0 or op._pooled:
            return
        if op._resolved is None:
            return
        # Still bound to a ring Completion: CQEs may deliver through user_data.
        if op.completion is not None:
            return
        if len(pool) >= self._max:
            self.drops += 1
            return
        op._scrub_for_pool()
        op._pooled = True
        pool.append(op)
        self.releases += 1

    def clear(self) -> None:
        self._one_shot.clear()
        self._continuous.clear()

    def stats(self) -> dict[str, int]:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "releases": self.releases,
            "drops": self.drops,
            "size": len(self._one_shot) + len(self._continuous),
            "max": self._max,
        }


@dataclass(frozen=True)
class UringSubmissionStats:
    """Observed io_uring submission backpressure for tuning ring queue depth."""

    submit_queue_full: int
    deferred_queue_peak: int


# --- stable ring submit implementations (no per-call allocation) ---
# sq0..sq4 are typed object on the waitable; each recipe knows the real shapes.


def _sq_recv(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_recv(cast(int, op.sq0), op.sq1, op)


def _sq_recvmsg(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_recvmsg(cast(int, op.sq0), op.sq1, op)


def _sq_recv_buf(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_recv_buf(cast(int, op.sq0), cast(Any, op.sq1), op)


def _sq_recv_multishot(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_recv_multishot(
        cast(int, op.sq0), cast(Any, op.sq1), op, cast(int, op.sq2), cast(int, op.sq3)
    )


def _sq_send(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_send(cast(int, op.sq0), op.sq1, op)


def _sq_send_zc(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_send_zc(cast(int, op.sq0), op.sq1, op)


def _sq_sendto(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_sendto(cast(int, op.sq0), op.sq1, op.sq2, op)


def _sq_sendmsg_zc(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_sendmsg_zc(cast(int, op.sq0), op.sq1, op.sq2, op)


def _sq_accept(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_accept(cast(int, op.sq0), op, cast(int, op.sq1))


def _sq_accept_multishot(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_accept_multishot(cast(int, op.sq0), op, cast(int, op.sq1), cast(int, op.sq2))


def _sq_connect(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_connect(cast(int, op.sq0), op.sq1, op)


def _sq_shutdown(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_shutdown(cast(int, op.sq0), cast(int, op.sq1), op)


def _sq_close(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_close(cast(int, op.sq0), op)


def _sq_socket(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_socket(cast(int, op.sq0), cast(int, op.sq1), cast(int, op.sq2), cast(int, op.sq3), op)


def _sq_openat(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_openat(
        cast(str, op.sq0), cast(int, op.sq1), cast(int, op.sq2), op, dfd=cast(int, op.sq3)
    )


def _sq_read(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_read(cast(int, op.sq0), op.sq1, cast(int, op.sq2), op)


def _sq_write(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_write(cast(int, op.sq0), op.sq1, cast(int, op.sq2), op)


def _sq_statx(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_statx(
        cast(int, op.sq0), cast(str, op.sq1), cast(int, op.sq2), cast(int, op.sq3), op.sq4, op
    )


def _sq_statx_fdsize(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_statx_fdsize(cast(int, op.sq0), op)


def _sq_poll(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_poll(cast(int, op.sq0), cast(int, op.sq1), op)


def _sq_poll_multishot(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_poll_multishot(cast(int, op.sq0), cast(int, op.sq1), op)


def _sq_cancel(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_cancel(cast(_UringCompletion, op.sq0), op)


def _sq_poll_remove(proactor: "UringProactor", op: _UringOp) -> _UringCompletion:
    return proactor._ring.submit_poll_remove(cast(_UringCompletion, op.sq0), op)


class SelectorProactor(ProactorBase):
    """Completion-oriented proactor prototype backed by a selector."""

    def __init__(
        self,
        selector: selectors.BaseSelector | None = None,
    ) -> None:
        super().__init__()
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

    def wake_wait(self) -> None:
        """Wake a thread blocked in `wait`."""

        self._wake_selector()
        self._run_async_break()

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
            self._poll(deadline)

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
        *,
        base_sequence: int = 0,
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

        ``base_sequence`` is the delivery ``index`` for this accept leg.
        """

        operation = _spawn_accept_many_operation(sock, self._guard_delivery_callback(callback))

        def step() -> ContinuousStepResult:
            try:
                conn, _address = sock.accept()
            except (BlockingIOError, InterruptedError):
                return ContinuousStepResult(progressed=False)
            except OSError as exc:
                # Soft: quiet terminal so StreamServer re-arms (does not fail the
                # accept loop). Can spin under sustained EMFILE — see
                # _soft_accept_terminal_delivery / socket_helpers.
                if _is_soft_accept_error(exc):
                    operation._finish_with_terminal_delivery(
                        _soft_accept_terminal_delivery(index=base_sequence),
                    )
                    return ContinuousStepResult(progressed=True, done=True)
                raise
            configure_scheduler_socket(conn)
            _handoff_accept_many(operation, conn, more=False, index=base_sequence)
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
        selector receive delivers copied ``memoryview`` data per call. When the
        synthetic pool is already full, ``recv_many()`` delivers ``errno.ENOBUFS``
        immediately without submitting ``recv()``.
        """

        operation = _spawn_recv_many_operation(sock, self._guard_delivery_callback(callback))
        if _synthetic_recv_pool_is_full(buf_group):
            return _complete_recv_many_enobufs(operation, index=base_sequence)

        def step() -> ContinuousStepResult:
            try:
                data = sock.recv(_DEFAULT_SELECTOR_RECV_MANY_CHUNK_SIZE)
            except (BlockingIOError, InterruptedError):
                return ContinuousStepResult(progressed=False)
            except OSError as exc:
                operation._finish_with_terminal_delivery(
                    MultishotDelivery(index=base_sequence, exception=exc, more=False)
                )
                return ContinuousStepResult(progressed=True, done=True)
            chunk = _selector_recv_many_chunk_view(data, buf_group)
            operation._emit_result(chunk, index=base_sequence, more=False)
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
            result_callback=self._guard_delivery_callback(callback),
        )
        next_index = 0

        def step() -> ContinuousStepResult:
            nonlocal next_index
            try:
                result = _probe_poll_fd_now(fd, mask)
            except BlockingIOError:
                return ContinuousStepResult(progressed=False)
            operation._emit_result(result, more=True, index=next_index)
            next_index += 1
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
        """Run one continuous step synchronously. Return True when the leg ended."""

        try:
            step_result = step()
        except (BlockingIOError, InterruptedError):
            return False
        except BaseException as exc:
            self._remove_operation(operation)
            operation._finish_with_terminal_delivery(_continuous_error_delivery(exc))
            return True
        if step_result.done:
            self._remove_operation(operation)
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

    def cancel(self, operation: SupportsOperation[Any]) -> SupportsOperation[None]:
        op = cast(Operation[Any], operation)
        if op.done():
            return self._completed_cancel_operation("cancel", op)
        with self._lock:
            removed = self._remove_operation(op)
        if removed:
            self._after_selector_registration_changed()
        self._terminalise_cancelled(op)
        return self._completed_cancel_operation("cancel", op)

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
            operation._finish_with_terminal_delivery(_continuous_error_delivery(exc))
            completed.append(operation)
            return
        if step_result.done:
            self._remove_operation(operation)
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
    ) -> None:
        if selector is None:
            selector = compat.released_default_selector()
        elif not hasattr(selector, "select_released"):
            raise TypeError("ThreadedSelectorProactor requires a selector with select_released()")
        super().__init__(selector)
        self._completed_wait = EventWakeupManager()
        self._worker_started = False
        self._worker_stop = threading.Event()
        self._worker = threading.Thread(target=self._worker_main, name="tealetio-selector-proactor", daemon=True)

    def _bind_wakeup_loop(self, loop: _asyncio.AbstractEventLoop) -> None:
        self._completed_wait.bind_loop(loop)

    def close(self) -> None:
        """Stop the worker thread and close selector resources."""

        self._worker_stop.set()
        self._completed_wait.wakeup()
        self._wake_selector()
        if self._closed:
            return
        if self._worker_started and threading.current_thread() is not self._worker:
            self._worker.join()
        super().close()

    def wake_wait(self) -> None:
        """Wake a thread blocked in `wait`."""

        self._completed_wait.wakeup()

    def _after_selector_registration_changed(self) -> None:
        self._wake_selector()

    def wait(self, deadline: float | None = None) -> None:
        """Wait until completed operations are signalled."""

        self._check_open()
        self._ensure_worker_started()
        if deadline == 0:
            return

        timeout = self._timeout_until_deadline(deadline)
        if timeout == 0:
            return
        self._wait_for_completed(timeout)

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
        if self._completed_wait.poll():
            return
        await self._completed_wait.wait_async(timeout)

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
                pass

    def _wait_for_completed(self, timeout: float | None) -> None:
        self._completed_wait.wait(timeout=timeout)


class UringProactor(ProactorBase):
    """io_uring-backed proactor.

    Default mode starts Python completion service threads that call
    ``ring.serve_completions()`` and deliver via ``Ring.callback``. The
    scheduler's ``wait()`` parks on ``ring.wait_idle()`` until workers deliver
    and ``wake_wait()`` → ``ring.break_wait()`` opens that park.

    With ``completion_threads=0`` (see also ``SyncUringProactor``), there are no
    service threads: ``wait()`` blocks in ``ring.wait()`` and runs the same
    completion processor on the calling thread. ``wake_wait()`` still calls
    ``break_wait()`` (internal NOP) so a blocked inline reaper can return.
    """

    def __init__(
        self,
        entries: int = 8,
        flags: int = 0,
        *,
        ring_factory: _UringRingFactory | None = None,
        completion_threads: int = _DEFAULT_URING_COMPLETION_THREADS,
        completion_thread_nice: int | None = _DEFAULT_URING_COMPLETION_THREAD_NICE,
        op_pool_max: int = _DEFAULT_URING_OP_POOL_MAX,
    ) -> None:
        if completion_threads < 0:
            raise ValueError("completion_threads must be non-negative")
        if ring_factory is None:
            ring_factory = _default_uring_ring_factory
        super().__init__()
        self._op_pool = _UringOpPool(op_pool_max)
        self._ring = ring_factory(entries, flags)
        try:
            self._capabilities = uring_api.probe(entries=entries, flags=flags)
        except (OSError, RuntimeError, NotImplementedError):
            self._capabilities = {}
        self._send_zc_supported = self._capabilities.get("IORING_OP_SEND_ZC", False)
        self._sendmsg_zc_supported = self._capabilities.get("IORING_OP_SENDMSG_ZC", False)
        if self._capabilities.get("IORING_RECV_MULTISHOT", False):
            self.recv_multishot: _RecvMultishotImpl = self._recv_multishot
        else:
            self.recv_multishot = self._recv_multishot_fallback
        if self._capabilities.get("IORING_ACCEPT_MULTISHOT", False):
            self.accept_multishot: _AcceptMultishotImpl = self._accept_multishot
        else:
            self.accept_multishot = self._accept_multishot_fallback
        # continuous *many ops prefer kernel multishot when probed; otherwise they
        # emulate the stream by resubmitting the matching one-shot opcode after
        # each completion (see the *_oneshot delivery handlers below).
        self._completion_thread_nice = completion_thread_nice
        # Unfinished uring ops for this proactor only (list length = count).
        self._pending_operations: list[None] = []
        # Serialise deferred queue with cancel until issuer-only mutation is complete
        # (see docs/URING_DEFERRED_SUBMIT.md). Drain runs on submit (FIFO); workers
        # do not drain. RLock: arm under the lock may re-enter submit on the same
        # thread; nested drain exits via _retrying_deferred_submissions.
        self._deferred_lock = threading.RLock()
        self._deferred_submissions: list[_UringOp] = []
        self._retrying_deferred_submissions = False
        self._submit_queue_full = 0
        self._deferred_queue_peak = 0
        # IORING_BUF_RING is 5.19; IORING_RECV_MULTISHOT is 6.0 and requires it.
        # Synthetic pools are only for kernels without buf rings — never for multishot.
        self._provided_buffers_supported = bool(self._capabilities.get("IORING_BUF_RING", False))
        # inline: driver thread reaps via ring.wait() (callback delivers in-process).
        # threaded: workers serve_completions() → same callback off the driver;
        # driver parks on ring.wait_idle() until wake_wait → break_wait.
        self._inline_completions = completion_threads == 0
        self._ring.callback = self._deliver_uring_completion
        # reverse-link before kernel submit (avoids post-return install races)
        self._ring.pre_submit = self._on_uring_pre_submit
        # bind once: avoid a mode check on every scheduler wait()
        self.wait = self._wait_inline if self._inline_completions else self._wait_workers
        self._service_threads = [
            threading.Thread(target=self._service_thread_main, name=f"tealetio-uring-{index}")
            for index in range(completion_threads)
        ]
        if self._inline_completions:
            return
        try:
            for thread in self._service_threads:
                thread.start()
            self._wait_until_service_started()
        except BaseException:
            self._ring.stop_serving()
            for thread in self._service_threads:
                if thread.is_alive():
                    thread.join()
            # drop proactor ↔ ring cycles (bound methods / hooks) before close
            self._ring.pre_submit = None
            self._ring.callback = None
            self._ring.exception_handler = None
            self._ring.close()
            raise

    def set_delivery_exception_handler(
        self,
        handler: Callable[[dict[str, Any]], object] | None,
    ) -> None:
        """Wire ring and proactor handlers for continuous delivery failures."""

        super().set_delivery_exception_handler(handler)
        self._ring.exception_handler = handler

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
        """Return observed submission-queue backpressure counters."""

        return UringSubmissionStats(
            submit_queue_full=self._submit_queue_full,
            deferred_queue_peak=self._deferred_queue_peak,
        )

    def reset_submission_stats(self) -> None:
        """Reset submission backpressure counters to zero."""

        self._submit_queue_full = 0
        self._deferred_queue_peak = 0

    def _note_submit_queue_full(self) -> None:
        self._submit_queue_full += 1

    def _enqueue_deferred_operation(self, operation: _UringOp) -> None:
        with self._deferred_lock:
            self._enqueue_deferred_operation_locked(operation)

    def _enqueue_deferred_operation_locked(self, operation: _UringOp) -> None:
        self._deferred_submissions.append(operation)
        deferred_count = len(self._deferred_submissions)
        if deferred_count > self._deferred_queue_peak:
            self._deferred_queue_peak = deferred_count

    def _prepare_uring_op(
        self,
        operation: _UringOp,
        complete: _UringOpComplete,
        cq0: object = None,
        cq1: object = None,
        cq2: object = None,
        cq3: object = None,
        *,
        poll_remove: bool = False,
    ) -> _UringOp:
        """Attach stable complete handler, optional cq context, and ring-leg flags.

        ``complete`` is an unbound ``UringProactor`` method (no per-submit lambda).
        ``cq0``… are positional completion-side context (buffers, offsets, …).
        ``poll_remove`` stays keyword-only (rare).
        """

        operation.complete = complete
        operation.poll_remove = poll_remove
        operation.completion = None
        operation.cq0 = cq0
        operation.cq1 = cq1
        operation.cq2 = cq2
        operation.cq3 = cq3
        return operation

    def _arm_sq(
        self,
        operation: _UringOp,
        impl: _UringSqImpl,
        sq0: object = None,
        sq1: object = None,
        sq2: object = None,
        sq3: object = None,
        sq4: object = None,
    ) -> None:
        """Install a stable submit recipe on ``operation`` (safe for deferred retry)."""

        operation.sq_impl = impl
        operation.sq0 = sq0
        operation.sq1 = sq1
        operation.sq2 = sq2
        operation.sq3 = sq3
        operation.sq4 = sq4

    def _stop_uring_poll_many_oneshot_locked(self, operation: _UringOp) -> None:
        """Stop a one-shot ``poll_many`` fallback without ``submit_cancel`` on poll.

        Caller holds ``_deferred_lock``.
        """

        self._cancel_deferred_operation_locked(operation)
        if operation.completion is not None:
            self._deactivate_uring_op(operation)
        # deferred cancel already cleared completion; nothing else to drop

    def cancel(self, operation: SupportsOperation[Any]) -> SupportsOperation[None]:
        # Waitables never leave this proactor; every cancel target is a uring op.
        #
        # Thread contract (submit vs cancel):
        #   - Submit and cancel are issuer-thread only: the ring owner / scheduler
        #     driver. Callers must not cancel from a delivery thread, and must not
        #     race two issuer threads on the same proactor.
        #   - Completion delivery may run on worker threads concurrently; those
        #     paths deliver CQEs and may re-queue the next leg. They are not a
        #     second submit/cancel issuer.
        #   - ``_deferred_lock`` serialises deferred-queue claim vs cancel remove
        #     (and deferred retry submit), not concurrent issuer submit/cancel.
        # Under that model, ``Ring.pre_submit`` installing ``operation.completion``
        # before ``io_uring_submit`` is enough for cancel to see a live ring handle
        # as soon as the op is kernel-visible — there is no first-submit vs cancel
        # race between two issuer threads.
        op = cast(_UringOp, operation)
        if op.done():
            return self._completed_cancel_operation("cancel", op)

        # Under _deferred_lock: either remove a deferred claim (safe to terminalise)
        # or snapshot op.completion for ring cancel. Retry holds the same lock
        # across deferred submit so these cannot race.
        #
        # Multishot poll_many (poll_remove=True): post stop_poll, then terminalise
        # the consumer-facing op immediately. Late poll CQEs may still race stop
        # (including after POLL_REMOVE completes — the kernel/API does not forbid
        # that, and delivery threads race Python). poll_many is never freelisted.
        # Armed recv/accept legs still wait for the target CQE (ASYNC_CANCEL).

        immediate_terminalise = True
        cancel_op: Operation[None] | None = None
        ring_cancel: tuple[_UringCompletion, str] | None
        ring_cancel = None
        with self._deferred_lock:
            completion = op.completion
            if completion is None:
                # not yet armed on the ring (deferred queue, or pre_submit not run)
                self._cancel_deferred_operation_locked(op)
                self._deactivate_uring_op(op)
                cancel_op = self._completed_cancel_operation("cancel", op)
            elif op.poll_remove:
                ring_cancel = (completion, "poll_remove")
            elif op.kind == "poll_many":
                self._stop_uring_poll_many_oneshot_locked(op)
                cancel_op = self._completed_cancel_operation("poll_remove", op)
            else:
                immediate_terminalise = False
                ring_cancel = (completion, "cancel")

        if ring_cancel is not None:
            completion, kind = ring_cancel
            cancel_op = self._submit_cancel_op(completion, kind=kind)
            if kind == "poll_remove":
                # Drop our handle; freelist never reuses poll_many waitables.
                self._deactivate_uring_op(op)
        assert cancel_op is not None
        if immediate_terminalise:
            self._terminalise_cancelled(op)
        return cancel_op

    def _submit_cancel_op(
        self,
        target_completion: _UringCompletion,
        *,
        kind: str,
    ) -> Operation[None]:
        cancel_operation = self._acquire_uring_op(kind, target_completion)
        self._prepare_uring_op(cancel_operation, UringProactor._complete_uring_cancel)
        sq_impl = _sq_poll_remove if kind == "poll_remove" else _sq_cancel
        self._arm_sq(cancel_operation, sq_impl, target_completion)
        self._submit_uring_op(cancel_operation)
        return cancel_operation

    def _complete_uring_cancel(self, op: _UringOp, completion: _UringCompletion) -> Operation[Any] | None:
        # Teardown ack only; never terminalises or freelists the cancel target.
        operation = cast(Operation[None], op)
        operation.deliver(self, result=None)
        return operation

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> RecvBufferPool:
        """Create a provided-buffer group, or synthetic pool without ``IORING_BUF_RING``.

        Gated by probe ``IORING_BUF_RING`` (5.19). That pre-dates
        ``IORING_RECV_MULTISHOT`` (6.0), so multishot never uses synthetic pools.
        """

        if self._provided_buffers_supported:
            return self._ring.create_buf_group(buffer_size, buffer_count)
        # no buf rings => no multishot either; one-shot fallback may still use synthetic
        assert not self._capabilities.get("IORING_RECV_MULTISHOT", False)
        return SyntheticRecvBufferPool(buffer_size, buffer_count)

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
        """Return True if unfinished operations or deferred ring submissions remain."""

        return bool(self._pending_operations) or bool(self._deferred_submissions)

    def close(self) -> None:
        """Close the owned `io_uring` ring."""

        if self._closed:
            return
        self._closed = True
        self._op_pool.clear()
        self._clear_shared_recv_buffer_pool()
        if self._service_threads:
            self._ring.stop_serving()
            for thread in self._service_threads:
                thread.join()
        else:
            # interrupt a driver blocked in ring.wait() before close
            try:
                self._ring.break_wait()
            except (OSError, RuntimeError, ValueError):
                pass
        self._deferred_submissions.clear()
        self.wake_wait()
        # drop proactor ↔ ring cycles (bound methods / hooks) before close
        self._ring.pre_submit = None
        self._ring.callback = None
        self._ring.exception_handler = None
        self._ring.close()

    def _acquire_uring_op(self, kind: str, fileobj: object | None = None) -> UringOperation[Any]:
        return self._op_pool.acquire_one_shot(self, kind, fileobj)

    def recycle_operation(self, operation: SupportsOperation[Any]) -> None:
        """Return a finished waitable to the freelist when safe.

        Only ``poll_many`` is never pooled (late CQEs after stop). Other ops
        recycle when terminal and not ring-live. ``IOWaiter`` calls this on
        ``wait()`` / ``forget()``.
        """

        if self._closed:
            return
        self._op_pool.release(operation)

    def _acquire_uring_continuous_op(
        self,
        kind: str,
        fileobj: object | None = None,
        result_callback: Callable[[MultishotDelivery], object] | None = None,
    ) -> UringContinuousOperation[Any]:
        return self._op_pool.acquire_continuous(self, kind, fileobj, result_callback)

    @property
    def op_pool_stats(self) -> dict[str, int]:
        """Return freelist counters for microbenchmarks (hits/misses/releases/drops/size)."""

        return self._op_pool.stats()

    def wake_wait(self) -> None:
        """Unblock sync/async ``wait`` via ``ring.break_wait()``.

        Opens ``wait_idle`` immediately. The ring best-effort submits an
        internal NOP only when completion service is idle (inline ``ring.wait()``
        on an empty CQ); with service workers the NOP is skipped.
        """

        try:
            self._ring.break_wait()
        except (OSError, RuntimeError, ValueError):
            pass

    def _wait_inline(self, deadline: float | None = None) -> None:
        """Block in ``ring.wait``; delivery runs via the registered ring callback.

        Wait after ``close()`` is undefined (misuse), not a recovery path — no
        ``_check_open()`` here so the hot park stays lean.
        """

        # deadline==0: one non-blocking harvest (selector wait(0) analogue)
        # callback mode: wait delivers non-empty batches and returns None
        self._ring.wait(self._timeout_until_deadline(deadline))

    def _wait_workers(self, deadline: float | None = None) -> None:
        """Park on ``ring.wait_idle`` while completion workers own CQ reaping.

        The ring idle park allows many ``wake_wait`` / ``break_wait`` signallers
        but only one concurrent waiter — the proactor driver. Do not park a
        second host (or dual ``wait`` / ``wait_async`` threads) on the same ring.

        Wait after ``close()`` is undefined (misuse); same as ``_wait_inline``.
        """

        if deadline == 0:
            return

        timeout = self._timeout_until_deadline(deadline)
        if timeout == 0:
            return
        self._ring.wait_idle(timeout)

    async def wait_async(self, deadline: float | None = None) -> None:
        """Wait asynchronously until completed operations are signalled.

        Parks in an executor on the same ``wait`` binding as the sync path so
        ring reaping / idle park stays off the asyncio loop thread.
        """

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

        operation = self._acquire_uring_op("recv", sock)
        if n == 0:
            operation.deliver(self, result=b"")
            return operation
        data = memoryview(bytearray(n))
        entry = self._prepare_uring_op(
            operation,
            UringProactor._complete_uring_recv,
            data,
        )
        self._arm_sq(entry, _sq_recv, sock.fileno(), data)
        self._submit_uring_op(entry)
        return operation

    def _complete_uring_recv(self, op: _UringOp, completion: _UringCompletion) -> Operation[bytes]:
        data = cast(memoryview, op.cq0)
        operation = cast(Operation[bytes], op)
        operation.deliver(self, result=data[: completion.res].tobytes())
        return operation

    def recv_into(self, sock: socket.socket, buf: Any) -> Operation[int]:
        """Submit a socket receive-into operation."""

        operation = self._acquire_uring_op("recv_into", sock)
        entry = self._prepare_uring_op(
            operation,
            UringProactor._complete_uring_recv_into,
        )
        self._arm_sq(entry, _sq_recv, sock.fileno(), buf)
        self._submit_uring_op(entry)
        return operation

    def _complete_uring_recv_into(self, op: _UringOp, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], op)
        operation._finish(result=completion.res)
        return operation

    def recvfrom(self, sock: socket.socket, bufsize: int) -> Operation[tuple[bytes, Any]]:
        """Submit a datagram receive operation."""

        operation = self._acquire_uring_op("recvfrom", sock)
        data = memoryview(bytearray(bufsize))
        self._submit_recvmsg(
            sock,
            operation,
            data,
            UringProactor._complete_uring_recvfrom,
            data,
        )
        return operation

    def _complete_uring_recvfrom(self, op: _UringOp, completion: _UringCompletion) -> Operation[tuple[bytes, Any]]:
        data = cast(memoryview, op.cq0)
        operation = cast(Operation[tuple[bytes, Any]], op)
        operation._finish(result=(data[: completion.res].tobytes(), completion.result))
        return operation

    def recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> Operation[tuple[int, Any]]:
        """Submit a datagram receive-into operation."""

        operation = self._acquire_uring_op("recvfrom_into", sock)
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
            UringProactor._complete_uring_recvfrom_into,
        )
        return operation

    def _complete_uring_recvfrom_into(
        self,
        op: _UringOp,
        completion: _UringCompletion,
    ) -> Operation[tuple[int, Any]]:
        operation = cast(Operation[tuple[int, Any]], op)
        operation._finish(result=(completion.res, completion.result))
        return operation

    def send(
        self,
        sock: socket.socket,
        data: Any,
        progress: _ProgressCallback | None = None,
    ) -> Operation[None]:
        """Submit a stream send that drains ``data`` before completing."""

        operation = self._acquire_uring_op("send", sock)
        payload = memoryview(data)
        if not payload:
            self._check_open()
            operation.deliver(self, result=None)
            return operation
        self._submit_sendall(sock, operation, payload, 0, progress)
        return operation

    def _complete_uring_sendall(
        self,
        op: _UringOp,
        completion: _UringCompletion,
    ) -> Operation[None] | None:
        data = cast(memoryview, op.cq0)
        offset = cast(int, op.cq1)
        progress = cast(_ProgressCallback | None, op.cq2)
        operation = cast(Operation[None], op)
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
        # Same drain: only advance offset + remaining slice; keep complete/sq recipe.
        self._resubmit_sendall_remainder(op, data, offset)
        return None

    def sendto(self, sock: socket.socket, data: Any, address: Any) -> Operation[int]:
        """Submit a datagram send operation."""

        operation = self._acquire_uring_op("sendto", sock)
        payload = memoryview(data)
        entry = self._prepare_uring_op(
            operation,
            UringProactor._complete_uring_sendto,
        )
        if self._sendmsg_zc_supported and sock.family != socket.AF_UNIX:
            self._arm_sq(entry, _sq_sendmsg_zc, sock.fileno(), payload, address)
        else:
            self._arm_sq(entry, _sq_sendto, sock.fileno(), payload, address)
        self._submit_uring_op(entry)
        return operation

    def _complete_uring_sendto(self, op: _UringOp, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], op)
        operation._finish(result=completion.res)
        return operation

    def accept(self, sock: socket.socket) -> Operation[socket.socket]:
        """Submit a socket accept operation."""

        operation = self._acquire_uring_op("accept", sock)
        entry = self._prepare_uring_op(
            operation,
            UringProactor._complete_uring_accept,
        )
        self._arm_sq(entry, _sq_accept, sock.fileno(), _DEFAULT_ACCEPT_FLAGS)
        self._submit_uring_op(entry)
        return operation

    def _complete_uring_accept(self, op: _UringOp, completion: _UringCompletion) -> Operation[socket.socket]:
        conn = socket_from_uring_fd(completion.res)
        operation = cast(Operation[socket.socket], op)
        operation._finish(result=conn)
        return operation

    def shutdown(self, sock: socket.socket, how: int) -> Operation[None]:
        """Submit ``socket.shutdown(how)`` for ``sock``."""

        operation = self._acquire_uring_op("shutdown", sock)
        if sock.fileno() == -1:
            operation.deliver(self, exception=OSError(errno.EBADF, "Bad file descriptor"))
            return operation
        entry = self._prepare_uring_op(
            operation,
            UringProactor._complete_uring_void_op,
        )
        self._arm_sq(entry, _sq_shutdown, sock.fileno(), how)
        self._submit_uring_op(entry)
        return operation

    def close_socket(self, sock: socket.socket) -> Operation[None]:
        """Submit socket close and release the Python wrapper fd."""

        operation = self._acquire_uring_op("close_socket", sock)
        if sock.fileno() == -1:
            operation.deliver(self, result=None)
            return operation
        fd = sock.detach()
        entry = self._prepare_uring_op(
            operation,
            UringProactor._complete_uring_void_op,
        )
        self._arm_sq(entry, _sq_close, fd)
        self._submit_uring_op(entry)
        return operation

    def close_fd(self, fd: int) -> Operation[None]:
        """Submit raw fd close for caller-owned descriptors (for example from ``openat``)."""

        operation = self._acquire_uring_op("close_fd", fd)
        if fd < 0:
            operation.deliver(self, result=None)
            return operation
        entry = self._prepare_uring_op(
            operation,
            UringProactor._complete_uring_void_op,
        )
        self._arm_sq(entry, _sq_close, fd)
        self._submit_uring_op(entry)
        return operation

    def _complete_uring_void_op(self, op: _UringOp, completion: _UringCompletion) -> Operation[None]:
        operation = cast(Operation[None], op)
        res = completion.res
        if res < 0:
            self._deactivate_uring_op(op)
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
        *,
        base_sequence: int = 0,
    ) -> ContinuousOperation[AcceptManyResult]:
        """Accept connections and deliver each via the result callback.

        Uses multishot accept when the runtime probe accepts it; otherwise
        submits one ``submit_accept()``, emits the connection, and finishes so
        callers re-arm. `callback` may run on any uring completion service thread.

        Each accepted connection is delivered as the accepted ``socket``. Call
        ``socket.getpeername()`` when the peer address is needed. Use
        ``ProactorIOManager.accept_many`` for accept-time reads and richer
        delivery shapes.

        ``base_sequence`` seeds multishot ``completion.sequence`` (or the single
        oneshot delivery index) so continuous arms can continue after eager accepts.
        """

        return self.accept_multishot(sock, callback, base_sequence=base_sequence)

    def _accept_multishot(
        self,
        sock: socket.socket,
        callback: _AcceptManyCallback,
        *,
        base_sequence: int = 0,
    ) -> ContinuousOperation[AcceptManyResult]:
        operation = self._acquire_uring_continuous_op(
            "accept_many",
            sock,
            callback,
        )
        # one multishot accept stays armed until F_MORE clears or we cancel.
        entry = self._prepare_uring_op(
            operation,
            UringProactor._deliver_uring_accept_many,
        )
        self._arm_sq(entry, _sq_accept_multishot, sock.fileno(), _DEFAULT_ACCEPT_FLAGS, base_sequence)
        self._submit_uring_op(entry)
        return operation

    def _accept_multishot_fallback(
        self,
        sock: socket.socket,
        callback: _AcceptManyCallback,
        *,
        base_sequence: int = 0,
    ) -> ContinuousOperation[AcceptManyResult]:
        # emulated accept_many: one accept, emit, finish; callers re-arm (for example StreamServer).
        operation = self._acquire_uring_continuous_op(
            "accept_many",
            sock,
            self._guard_delivery_callback(callback),
        )
        entry = self._prepare_uring_op(
            operation,
            UringProactor._deliver_uring_accept_many_oneshot,
            base_sequence,
        )
        self._arm_sq(entry, _sq_accept, sock.fileno(), _DEFAULT_ACCEPT_FLAGS)
        self._submit_uring_op(entry)
        return operation

    def _deliver_uring_accept_many_oneshot(
        self,
        op: _UringOp,
        completion: _UringCompletion,
    ) -> Operation[Any] | None:
        operation = cast(ContinuousOperation[AcceptManyResult], op)
        base_sequence = cast(int, op.cq0)
        res = completion.res
        if res < 0:
            self._deactivate_uring_op(op)
            # emulated accept_many: soft errors finish without exception so
            # callers re-arm (same policy as SelectorProactor.accept_many).
            if _is_soft_accept_errno(-res):
                operation._finish_with_terminal_delivery(
                    _soft_accept_terminal_delivery(index=base_sequence),
                )
            else:
                operation._finish_with_terminal_delivery(
                    _continuous_error_delivery(_uring_cqe_oserror(res), index=base_sequence),
                )
            return operation
        conn = socket_from_uring_fd(completion.res)
        _handoff_accept_many(operation, conn, more=False, index=base_sequence)
        self._deactivate_uring_op(op)
        return operation

    def _deliver_uring_accept_many(
        self,
        op: _UringOp,
        completion: _UringCompletion,
    ) -> Operation[Any] | None:
        operation = cast(ContinuousOperation[AcceptManyResult], op)
        res = completion.res
        index = int(completion.sequence)
        if res < 0:
            self._deactivate_uring_op(op)
            # keep completion.sequence (including ECANCELED): uring-api assigns the
            # next multishot leg index; default index=0 would stall reorder buffers
            # after any more=True accepts.
            operation._finish_with_terminal_delivery(
                _continuous_error_delivery(_uring_cqe_oserror(res), index=index),
            )
            return operation
        conn = socket_from_uring_fd(completion.res)
        more = bool(completion.flags & uring_api.IORING_CQE_F_MORE)
        _handoff_accept_many(operation, conn, more=more, index=index)
        if not more:
            self._deactivate_uring_op(op)
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
            operation = self._acquire_uring_op("create_socket", (family, type, proto))
            entry = self._prepare_uring_op(
                operation,
                UringProactor._complete_uring_create_socket,
            )
            self._arm_sq(entry, _sq_socket, family, socket_type, proto, 0)
            self._submit_uring_op(entry)
            return operation

        operation = self._acquire_uring_op("create_socket", (family, type, proto))
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

        operation = self._acquire_uring_op("connect", sock)
        entry = self._prepare_uring_op(
            operation,
            UringProactor._complete_uring_connect,
        )
        self._arm_sq(entry, _sq_connect, sock.fileno(), address)
        self._submit_uring_op(entry)
        return operation

    def _complete_uring_connect(self, op: _UringOp, completion: _UringCompletion) -> Operation[None]:
        operation = cast(Operation[None], op)
        operation.deliver(self, result=None)
        return operation

    def _complete_uring_create_socket(
        self,
        op: _UringOp,
        completion: _UringCompletion,
    ) -> Operation[socket.socket]:
        operation = cast(Operation[socket.socket], op)
        operation.deliver(self, result=socket_from_uring_fd(completion.res))
        return operation

    def openat(self, path: str, flags: int, mode: int = 0, *, dfd: int = _DEFAULT_OPENAT_DFD) -> Operation[int]:
        """Submit an io_uring openat operation and return the opened fd on success."""

        operation = self._acquire_uring_op("openat", path)
        entry = self._prepare_uring_op(
            operation,
            UringProactor._complete_uring_openat,
        )
        self._arm_sq(entry, _sq_openat, path, flags, mode, dfd)
        self._submit_uring_op(entry)
        return operation

    def _complete_uring_openat(self, op: _UringOp, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], op)
        operation._finish(result=completion.res)
        return operation

    def read(self, fd: int, n: int, offset: int) -> Operation[bytes]:
        """Submit a positioned file read that completes with the bytes read."""

        operation = self._acquire_uring_op("read", fd)
        data = memoryview(bytearray(n))
        entry = self._prepare_uring_op(
            operation,
            UringProactor._complete_uring_read,
            data,
        )
        self._arm_sq(entry, _sq_read, fd, data, offset)
        self._submit_uring_op(entry)
        return operation

    def _complete_uring_read(self, op: _UringOp, completion: _UringCompletion) -> Operation[bytes]:
        data = cast(memoryview, op.cq0)
        operation = cast(Operation[bytes], op)
        operation._finish(result=data[: completion.res].tobytes())
        return operation

    def read_into(self, fd: int, buf: Any, offset: int) -> Operation[int]:
        """Submit a positioned file read into a caller-provided buffer."""

        operation = self._acquire_uring_op("read_into", fd)
        entry = self._prepare_uring_op(
            operation,
            UringProactor._complete_uring_read_into,
        )
        self._arm_sq(entry, _sq_read, fd, buf, offset)
        self._submit_uring_op(entry)
        return operation

    def _complete_uring_read_into(self, op: _UringOp, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], op)
        operation._finish(result=completion.res)
        return operation

    def write(self, fd: int, data: Any, offset: int) -> Operation[int]:
        """Submit a positioned file write and return the byte count written."""

        operation = self._acquire_uring_op("write", fd)
        payload = memoryview(data)
        entry = self._prepare_uring_op(
            operation,
            UringProactor._complete_uring_write,
        )
        self._arm_sq(entry, _sq_write, fd, payload, offset)
        self._submit_uring_op(entry)
        return operation

    def _complete_uring_write(self, op: _UringOp, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], op)
        operation._finish(result=completion.res)
        return operation

    def stat(self, path: str = "", *, fd: int = -1) -> Operation[os.stat_result]:
        """Return file metadata via io_uring statx when probed, else blocking ``os.stat``."""

        self._check_open()
        if fd < 0 and not path:
            raise ValueError("stat() requires fd >= 0 or a non-empty path")
        if not self._capabilities.get("IORING_OP_STATX", False) or not hasattr(self._ring, "submit_statx"):
            return super().stat(path, fd=fd)

        operation = self._acquire_uring_op("stat", fd if fd >= 0 else path)
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
        entry = self._prepare_uring_op(
            operation,
            UringProactor._complete_uring_stat,
            stat_buf,
        )
        self._arm_sq(entry, _sq_statx, dfd, stat_path, stat_flags, uring_api.STATX_BASIC_STATS, buf)
        self._submit_uring_op(entry)
        return operation

    def _complete_uring_stat(self, op: _UringOp, completion: _UringCompletion) -> Operation[os.stat_result]:
        data = cast(memoryview, op.cq0)
        operation = cast(Operation[os.stat_result], op)
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

        operation = self._acquire_uring_op("stat_fdsize", fd)
        entry = self._prepare_uring_op(
            operation,
            UringProactor._complete_uring_stat_fdsize,
        )
        self._arm_sq(entry, _sq_statx_fdsize, fd)
        self._submit_uring_op(entry)
        return operation

    def _complete_uring_stat_fdsize(self, op: _UringOp, completion: _UringCompletion) -> Operation[int]:
        # Rare statx_fdsize parse miss: recover with blocking fstat on this thread.
        operation = cast(Operation[int], op)
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

        When multishot receive is unavailable but ``buf_group`` is a real
        provided-buffer pool (``IORING_BUF_RING`` without multishot: 5.19–5.x),
        the proactor submits one ``submit_recv_buf()`` and delivers a leased
        ``BufView`` per leg. With a ``SyntheticRecvBufferPool`` (no buf rings;
        also no multishot), it falls back to ``submit_recv()`` and leases
        copied chunks against the synthetic pool before delivery.

        ``buf_group`` must be a provided-buffer pool from
        ``create_recv_buffer_pool()`` or ``shared_recv_buffer_pool()``.
        """

        return self.recv_multishot(
            sock,
            callback,
            buf_group=buf_group,
            base_sequence=base_sequence,
        )

    def _recv_multishot(
        self,
        sock: socket.socket,
        callback: _RecvManyCallback,
        *,
        buf_group: RecvBufferPool,
        base_sequence: int = 0,
    ) -> ContinuousOperation[_RecvManyValue]:
        operation = self._acquire_uring_continuous_op(
            "recv_many",
            sock,
            callback,
        )
        uring_group = cast(_UringBufGroup, buf_group)
        entry = self._prepare_uring_op(
            operation,
            UringProactor._deliver_uring_recv_many,
        )
        self._arm_sq(entry, _sq_recv_multishot, sock.fileno(), uring_group, 0, base_sequence)
        self._submit_uring_op(entry)
        return operation

    def _recv_multishot_fallback(
        self,
        sock: socket.socket,
        callback: _RecvManyCallback,
        *,
        buf_group: RecvBufferPool,
        base_sequence: int = 0,
    ) -> ContinuousOperation[_RecvManyValue]:
        operation = self._acquire_uring_continuous_op(
            "recv_many",
            sock,
            self._guard_delivery_callback(callback),
        )
        if _is_synthetic_recv_buffer_pool(buf_group):
            if _synthetic_recv_pool_is_full(buf_group):
                return _complete_recv_many_enobufs(operation, index=base_sequence)
            buffer = bytearray(_DEFAULT_SELECTOR_RECV_MANY_CHUNK_SIZE)
            synthetic_pool = cast(SyntheticRecvBufferPool, buf_group)
            entry = self._prepare_uring_op(
                operation,
                UringProactor._deliver_uring_recv_oneshot,
                buffer,
                base_sequence,
                synthetic_pool,
            )
            self._arm_sq(entry, _sq_recv, sock.fileno(), buffer)
            self._submit_uring_op(entry)
            return operation

        uring_group = cast(_UringBufGroup, buf_group)
        entry = self._prepare_uring_op(
            operation,
            UringProactor._deliver_uring_recv_buf,
            base_sequence,
        )
        self._arm_sq(entry, _sq_recv_buf, sock.fileno(), uring_group)
        self._submit_uring_op(entry)
        return operation

    def _recv_many_chunk_view(
        self,
        buffer: bytearray,
        res: int,
        *,
        synthetic_pool: SyntheticRecvBufferPool | None,
    ) -> memoryview:
        if res == 0:
            return memoryview(b"")
        data = bytes(buffer[:res])
        if synthetic_pool is None:
            return memoryview(data)
        return _leased_synthetic_memoryview(data, synthetic_pool)

    def _deliver_uring_recv_oneshot(
        self,
        op: _UringOp,
        completion: _UringCompletion,
    ) -> Operation[Any] | None:
        buffer = cast(bytearray, op.cq0)
        base_sequence = cast(int, op.cq1)
        synthetic_pool = cast(SyntheticRecvBufferPool | None, op.cq2)
        operation = cast(ContinuousOperation[_RecvManyValue], op)
        res = completion.res
        if res < 0:
            self._deactivate_uring_op(op)
            operation._finish_with_terminal_delivery(
                _recv_many_error_delivery(index=base_sequence, res=res),
            )
            return operation
        operation._emit_result(
            self._recv_many_chunk_view(buffer, res, synthetic_pool=synthetic_pool),
            index=base_sequence,
            more=False,
        )
        self._deactivate_uring_op(op)
        return operation

    def _deliver_uring_recv_buf(
        self,
        op: _UringOp,
        completion: _UringCompletion,
    ) -> Operation[Any] | None:
        base_sequence = cast(int, op.cq0)
        operation = cast(ContinuousOperation[_RecvManyValue], op)
        res = completion.res
        if res < 0:
            self._deactivate_uring_op(op)
            operation._finish_with_terminal_delivery(
                _recv_many_error_delivery(index=base_sequence, res=res),
            )
            return operation
        if res == 0:
            payload = completion.result
            chunk = memoryview(b"") if payload is None else memoryview(cast(Any, payload))
        else:
            chunk = memoryview(cast(Any, completion.result))
        operation._emit_result(chunk, index=base_sequence, more=False)
        self._deactivate_uring_op(op)
        return operation

    def poll(self, fd: int, mask: int) -> Operation[int]:
        """Submit a one-shot io_uring poll operation."""

        # mask and fd go straight to io_uring; bad values show up as CQE errors.
        # selector validates masks (select() fd lists) and fd>=0; no per-fd exclusivity.
        operation = self._acquire_uring_op("poll", fd)
        entry = self._prepare_uring_op(
            operation,
            UringProactor._complete_uring_poll,
        )
        self._arm_sq(entry, _sq_poll, fd, mask)
        self._submit_uring_op(entry)
        return operation

    def _complete_uring_poll(self, op: _UringOp, completion: _UringCompletion) -> Operation[int]:
        operation = cast(Operation[int], op)
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
        operation = self._acquire_uring_continuous_op(
            "poll_many",
            fd,
            self._guard_delivery_callback(callback),
        )
        if self._capabilities.get("IORING_POLL_MULTISHOT", False):
            # kernel keeps the poll armed; cancel via submit_poll_remove().
            entry = self._prepare_uring_op(
                operation,
                UringProactor._deliver_uring_poll_many,
                poll_remove=True,
            )
            self._arm_sq(entry, _sq_poll_multishot, fd, mask)
            self._submit_uring_op(entry)
            return operation

        # fallback: one-shot submit_poll per readiness event.
        next_index = [0]
        entry = self._prepare_uring_op(
            operation,
            UringProactor._deliver_uring_poll_many_oneshot,
            next_index,
        )
        self._arm_sq(entry, _sq_poll, fd, mask)
        self._submit_uring_op(entry)
        return operation

    def _deliver_uring_poll_many_oneshot(
        self,
        op: _UringOp,
        completion: _UringCompletion,
    ) -> Operation[Any] | None:
        # Emit the mask; try to arm the next one-shot leg immediately (no deferred
        # queue from delivery). On SQ-full, terminalise more=False and swallow.
        next_index = cast(list[int], op.cq0)
        operation = cast(ContinuousOperation[int], op)
        res = completion.res
        index = next_index[0]
        if res < 0:
            self._deactivate_uring_op(op)
            operation._finish_with_terminal_delivery(
                _continuous_error_delivery(_uring_cqe_oserror(res), index=index),
            )
            return operation
        # This CQE is done; drop reverse link before arming the next leg.
        self._deactivate_uring_op(op)
        next_index[0] = index + 1
        if operation.done():
            operation._emit_result(res, more=False, index=index)
            return operation
        try:
            impl = operation.sq_impl
            assert impl is not None
            impl(self, operation)
        except uring_api.SubmissionQueueFull:
            # SQ pressure is not a stream error — terminal leg, caller may re-arm.
            self._note_submit_queue_full()
            operation._emit_result(res, more=False, index=index)
            return operation
        except Exception as exc:
            operation._finish_with_terminal_delivery(
                _continuous_error_delivery(exc, index=index),
            )
            return operation
        operation._emit_result(res, more=True, index=index)
        return None

    def _deliver_uring_poll_many(self, op: _UringOp, completion: _UringCompletion) -> Operation[Any] | None:
        operation = cast(ContinuousOperation[int], op)
        res = completion.res
        index = int(completion.sequence)
        if res < 0:
            self._deactivate_uring_op(op)
            operation._finish_with_terminal_delivery(
                _continuous_error_delivery(_uring_cqe_oserror(res), index=index),
            )
            return operation
        more = bool(completion.flags & uring_api.IORING_CQE_F_MORE)
        operation._emit_result(res, more=more, index=index)
        if not more:
            self._deactivate_uring_op(op)
        return operation

    def _deliver_uring_recv_many(
        self,
        op: _UringOp,
        completion: _UringCompletion,
    ) -> Operation[Any] | None:
        operation = cast(ContinuousOperation[_RecvManyValue], op)
        res = completion.res
        index = int(completion.sequence)

        if res < 0:
            if res == -errno.ENOBUFS:
                operation._emit_delivery(_recv_many_enobufs_delivery(index=index))
                self._deactivate_uring_op(op)
                return operation
            self._deactivate_uring_op(op)
            operation._finish_with_terminal_delivery(_recv_many_error_delivery(index=index, res=res))
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

        if not more:
            self._deactivate_uring_op(op)
        return operation

    def _deactivate_uring_op(self, operation: _UringOp) -> None:
        # Drop the pending Completion handle so op <-> completion.user_data
        # cycles do not linger after this leg is done.
        operation.completion = None

    def _fail_uring_op(self, operation: _UringOp, exc: BaseException) -> None:
        self._deactivate_uring_op(operation)
        operation.deliver(self, exception=exc)

    def _deliver_uring_completion(self, completions: list[_UringCompletion]) -> None:
        # Single pass. Cancel / poll_remove CQEs only finish their teardown
        # waitables (never the cancel target). Multishot poll_many is terminalised
        # when stop_poll is posted; POLL_REMOVE only acks the teardown waitable.
        completed_operation: Operation[Any] | None = None
        for completion in completions:
            if completion.kind in (
                uring_api.COMPLETION_KIND_POLL_REMOVE,
                uring_api.COMPLETION_KIND_CANCEL,
            ):
                op = cast(_UringOp, completion.user_data)
                if completion.kind == uring_api.COMPLETION_KIND_CANCEL:
                    if op.kind not in ("cancel", "poll_remove"):
                        continue
                elif op.kind != "poll_remove":
                    continue
            result = self._complete_uring_operation(completion)
            if result is not None:
                completed_operation = result
        # Deferred drain is issuer submit-path only (see _submit_uring_op).
        # threaded mode: workers deliver off the driver; open wait_idle via break_wait.
        # inline mode: the driver is already inside wait() processing this batch.
        if not self._inline_completions and completed_operation is None and not self.has_pending_operations():
            self.wake_wait()

    @staticmethod
    def _on_uring_pre_submit(completion: _UringCompletion) -> None:
        """``Ring.pre_submit``: reverse-link before the op is visible to the kernel."""

        cast(_UringOp, completion.user_data).completion = completion

    def _submit_uring_op(self, operation: _UringOp) -> bool:
        """Submit an armed op. ``pre_submit`` installs ``operation.completion``.

        If the deferred SQ queue is non-empty, append ``operation`` and drain FIFO
        until SQ-full or empty (new work never jumps older deferred legs). When
        the queue is empty, arm immediately; on ``SubmissionQueueFull`` enqueue
        as the sole deferred head.
        """

        failures: list[tuple[_UringOp, BaseException]] = []
        with self._deferred_lock:
            if self._deferred_submissions:
                self._enqueue_deferred_operation_locked(operation)
                failures = self._drain_deferred_locked()
                armed = operation.completion is not None
            else:
                try:
                    impl = operation.sq_impl
                    assert impl is not None
                    impl(self, operation)
                    armed = True
                except uring_api.SubmissionQueueFull:
                    assert operation.completion is None
                    self._note_submit_queue_full()
                    self._enqueue_deferred_operation_locked(operation)
                    armed = False
                except BaseException as exc:
                    self._fail_uring_op(operation, exc)
                    raise
        for failed_op, exc in failures:
            self._fail_uring_op(failed_op, exc)
        return armed

    def _drain_deferred_locked(self) -> list[tuple[_UringOp, BaseException]]:
        """Arm deferred heads under ``_deferred_lock``. Caller holds the lock.

        On SQ-full leave the head in place (no re-enqueue). Hard errors pop and
        are returned for failure outside the lock.
        """

        failures: list[tuple[_UringOp, BaseException]] = []
        if self._retrying_deferred_submissions:
            return failures
        self._retrying_deferred_submissions = True
        try:
            while self._deferred_submissions:
                operation = self._deferred_submissions[0]
                try:
                    impl = operation.sq_impl
                    assert impl is not None
                    impl(self, operation)
                except uring_api.SubmissionQueueFull:
                    assert operation.completion is None
                    self._note_submit_queue_full()
                    break
                except Exception as exc:
                    del self._deferred_submissions[0]
                    operation.completion = None
                    failures.append((operation, exc))
                    continue
                # reverse link live; remove so cancel uses ASYNC_CANCEL
                del self._deferred_submissions[0]
        finally:
            self._retrying_deferred_submissions = False
        return failures

    def _retry_deferred_submissions(self) -> None:
        """Drain deferred SQ submissions (issuer path; holds ``_deferred_lock``)."""

        with self._deferred_lock:
            failures = self._drain_deferred_locked()
        for operation, exc in failures:
            self._fail_uring_op(operation, exc)

    def _cancel_deferred_operation(self, operation: Operation[Any]) -> bool:
        with self._deferred_lock:
            return self._cancel_deferred_operation_locked(operation)

    def _cancel_deferred_operation_locked(self, operation: Operation[Any]) -> bool:
        for index, deferred in enumerate(self._deferred_submissions):
            if deferred is operation:
                del self._deferred_submissions[index]
                deferred.completion = None
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
        """First leg of a sendall drain: install complete recipe and submit."""

        entry = self._prepare_uring_op(
            operation,
            UringProactor._complete_uring_sendall,
            data,
            offset,
            progress,
        )
        chunk = data[offset:]
        if self._send_zc_supported and sock.family != socket.AF_UNIX:
            self._arm_sq(entry, _sq_send_zc, sock.fileno(), chunk)
        else:
            self._arm_sq(entry, _sq_send, sock.fileno(), chunk)
        self._submit_uring_op(entry)

    def _resubmit_sendall_remainder(self, op: _UringOp, data: memoryview, offset: int) -> None:
        """Continue a sendall drain after a partial CQE.

        ``complete``, base ``data`` (cq0), ``progress`` (cq2), fd (sq0), and
        ``sq_impl`` are already set from the first leg. Only the byte offset and
        remaining slice change.
        """

        op.cq1 = offset
        op.sq1 = data[offset:]
        self._submit_uring_op(op)

    def _submit_recvmsg(
        self,
        sock: socket.socket,
        operation: "UringOperation[Any]",
        data: memoryview,
        complete: _UringOpComplete,
        cq0: object = None,
        cq1: object = None,
        cq2: object = None,
        cq3: object = None,
    ) -> None:
        entry = self._prepare_uring_op(operation, complete, cq0, cq1, cq2, cq3)
        self._arm_sq(entry, _sq_recvmsg, sock.fileno(), data)
        self._submit_uring_op(entry)

    def _complete_uring_operation(
        self,
        completion: _UringCompletion,
    ) -> Operation[Any] | None:
        op = cast(_UringOp, completion.user_data)
        res = completion.res
        # Continuous legs (multishot and emulated oneshot) own error shaping in
        # their complete handlers — e.g. soft accept errors that finish cleanly.
        if completion.multishot or isinstance(op, ContinuousOperation):
            assert op.complete is not None
            return op.complete(self, op, completion)
        has_more = bool(completion.flags & uring_api.IORING_CQE_F_MORE)
        if not has_more:
            self._deactivate_uring_op(op)
        if res < 0:
            op.deliver(
                self,
                exception=OSError(-res, errno.errorcode.get(-res, "io_uring operation failed")),
            )
            return op
        assert op.complete is not None
        return op.complete(self, op, completion)

    def _raise_unsupported(self, operation: str) -> NoReturn:
        self._check_open()
        raise NotImplementedError(f"UringProactor does not yet support {operation} operations")


def _default_proactor_factory() -> Proactor:
    if uring_api.is_available():
        # concrete backends satisfy Proactor structurally; ty does not always prove it
        return cast(Proactor, UringProactor())
    return cast(Proactor, SelectorProactor())


class SyncUringProactor(UringProactor):
    """Single-threaded ``UringProactor``: ``wait()`` is ``ring.wait`` + deliver.

    Intended for benchmarks and debugging against the threaded default. Same
    submit path and Operation model; no completion service threads and no
    cross-thread delivery hop on the sync driver.
    """

    def __init__(
        self,
        entries: int = 8,
        flags: int = 0,
        *,
        ring_factory: _UringRingFactory | None = None,
        completion_thread_nice: int | None = _DEFAULT_URING_COMPLETION_THREAD_NICE,
    ) -> None:
        super().__init__(
            entries,
            flags,
            ring_factory=ring_factory,
            completion_threads=0,
            completion_thread_nice=completion_thread_nice,
        )


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
        self._proactor.set_delivery_exception_handler(self.call_exception_handler)
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
        self._proactor.wake_wait()

    def _break_wait(self) -> None:
        self._proactor.wake_wait()

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

    def _lazy_bind_running_loop(self) -> None:
        if self._wakeup_loop is None:
            self.bind_loop(_asyncio.get_running_loop())

    def _before_arun(self) -> None:
        self._lazy_bind_running_loop()

    def close(self) -> None:
        """Close proactor and scheduler-owned resources."""

        super().close()

    async def _driver_wait(self) -> None:
        self._lazy_bind_running_loop()
        deadline = self._next_timer_deadline()
        await self._proactor.wait_async(deadline)
