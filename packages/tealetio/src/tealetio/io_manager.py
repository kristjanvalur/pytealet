from __future__ import annotations

import errno
import os
import socket
import sys
import threading
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Sequence
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, runtime_checkable

from .delivery import (
    AcceptDelivery,
    AcceptRecvErrorCallback,
    AcceptStreamsDelivery,
    DeliveryCallback,
    MultishotDelivery,
    OpHandle,
    ReorderBuffer,
    finalize_accept_recv_error,
    normalize_accept_recv_size,
)
from .files import IOFile, ProactorFile, parse_open_mode
from .happy_eyeballs import HAPPY_EYEBALLS_DELAY
from .io_buffers import RecvIterBuffer, SendBuffer, open_recv_iter_buffer, open_send_buffer
from .io_waiter import (
    IOHandle,
    IOWaitable,
    IOWaiter,
    IOWaiterSync,
    IOWaitGroup,
    IOWaitGroupChild,
)
from .socket_helpers import abortive_close, configure_scheduler_socket, real_socket_module
from .types import IoExpect, RecvResult, SocketSendBuffer

if TYPE_CHECKING:
    from .proactor import Proactor, RecvBufferPool
    from .scheduler import BaseScheduler

T = TypeVar("T")

# idle recv buffer pools kept for reuse; excess free pools are not cached.
# one size only — matches pooled_default_stream_factory defaults.
DEFAULT_MAX_FREE_RECV_BUFFER_POOLS = 1024
DEFAULT_RECV_POOL_BUFFER_SIZE = 16 * 1024
DEFAULT_RECV_POOL_BUFFER_COUNT = 4


def _group_unregistered(pool: RecvBufferPool) -> bool:
    """True when a uring group has already freed its buf ring.

    Synthetic pools have no ``group_id``. ``0`` is the freed id.
    """

    return getattr(pool, "group_id", None) == 0


def _env_sock_close_nowait() -> bool:
    raw = os.environ.get("TEALETIO_SOCK_CLOSE", "nowait")
    return raw.strip().lower() not in ("stdlib", "socket", "blocking")


def _create_scheduler_socket(
    family: int,
    type: int,
    proto: int = 0,
    *,
    flags: int = 0,
) -> socket.socket:
    # Blocking socket() is faster than creating through io_uring; leave
    # Proactor.create_socket / IORING_OP_SOCKET available for direct proactor use.
    # ``flags`` are socket *type* flags only (e.g. SOCK_NONBLOCK / SOCK_CLOEXEC
    # where the platform accepts them in the type argument). Not uring-only
    # IORING_OP_SOCKET bits; those stay on Proactor.create_socket.
    return configure_scheduler_socket(real_socket_module().socket(family, type | flags, proto))


def _recv_result_bytes(result: RecvResult) -> bytes:
    """``sock_recv`` waits to payload bytes; ``IoMore`` stays on ``proactor.recv``."""

    return result.data


def _finish_or_close_socket(group: IOWaitGroup[Any], sock: socket.socket, result: Any) -> None:
    if not group.finish(result):
        abortive_close(sock)


# Linux 4.11. This interpreter may omit the name; the number is stable there.
_raw_fastopen_connect = getattr(socket, "TCP_FASTOPEN_CONNECT", None)
_TCP_FASTOPEN_CONNECT: int | None = (
    _raw_fastopen_connect if isinstance(_raw_fastopen_connect, int) else (30 if sys.platform == "linux" else None)
)
del _raw_fastopen_connect


def _enable_tcp_fastopen_connect(sock: socket.socket) -> None:
    """Set ``TCP_FASTOPEN_CONNECT`` so the next write can carry the SYN.

    ``EOPNOTSUPP``, ``ENOTSUP``, and ``ENOPROTOOPT`` are ignored. A Unix
    socket and a datagram socket still connect and send. Any other error
    raises before connect is submitted.
    """

    option = _TCP_FASTOPEN_CONNECT
    if option is None:
        return
    try:
        sock.setsockopt(socket.IPPROTO_TCP, option, 1)
    except OSError as exc:
        if exc.errno in (
            errno.EOPNOTSUPP,
            errno.ENOPROTOOPT,
            getattr(errno, "ENOTSUP", errno.EOPNOTSUPP),
        ):
            return
        raise


def _bind_local_addrs(sock: socket.socket, addrs: Sequence[Any]) -> None:
    """Bind the first address that accepts. Raise the last bind error."""

    error: OSError | None = None
    for laddr in addrs:
        try:
            sock.bind(laddr)
        except OSError as exc:
            detail = exc.strerror.lower() if exc.strerror else str(exc)
            error = OSError(exc.errno, f"error while attempting to bind on address {laddr!r}: {detail}")
        else:
            return
    assert error is not None
    raise error


_ProgressCallback = Callable[[int], object]
_RecvProgressCallback = Callable[[bytes], object]
_RecvIterYield = tuple[int, memoryview]

# sockaddr shapes vary by family; tighten when accept/connect types are unified.
SocketAddress = Any

IO_UNSUPPORTED_ERROR = "operation requires a scheduler with IO support"
SELECTOR_IO_UNSUPPORTED_ERROR = (
    "stream helpers require a proactor scheduler; selector schedulers "
    "use scheduler.sock_* until SelectorIOManager is available"
)

__all__ = [
    "FileIO",
    "IOFile",
    "PollIO",
    "ProactorAccess",
    "ProactorFile",
    "ProactorIOManager",
    "ProactorSocketIO",
    "ServerIO",
    "SocketIO",
    "SupportsProactorIO",
]


class SupportsProactorIO(Protocol):
    """Scheduler that exposes a proactor-backed ``scheduler.io`` facade.

    Use for static typing after narrowing (for example ``isinstance(scheduler,
    ProactorScheduler)``). Do not rely on ``isinstance(..., SupportsProactorIO)``
    at runtime: schedulers without a real IO backend may still define an ``io``
    property that raises.
    """

    @property
    def io(self) -> ProactorIOManager: ...


class ProactorAccess(Protocol):
    """IO facade with access to proactor submission (``accept_many``, …)."""

    @property
    def proactor(self) -> Proactor: ...


@runtime_checkable
class SocketIO(Protocol):
    """Asyncio-shaped socket helpers.

    Plain oneshots return ``IOWaiter`` (callback + opaque ``OpHandle``).
    ``sock_sendall`` and ``sock_shutdown`` are ``IOWaiter``.
    ``sock_send_nowait``, ``sock_shutdown_nowait``, and ``sock_close`` are
    nowait (``None``). Compose helpers (``sock_accept`` with preread,
    ``sock_connect`` with ``initial``, ``sock_fastopen``,
    ``sock_create`` with ``connect_to``) return ``IOWaitable`` (waiter or group).
    ``connect_tcp`` blocks the calling tealet and returns the connected socket.
    """

    def sock_recv(self, sock: socket.socket, n: int) -> IOWaiter[bytes]: ...

    def sock_recv_into(self, sock: socket.socket, buf: Any) -> IOWaiter[int]: ...

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> IOWaiter[tuple[bytes, Any]]: ...

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> IOWaiter[tuple[int, Any]]: ...

    def sock_send(self, sock: socket.socket, data: SocketSendBuffer) -> IOWaiter[int]: ...

    def sock_sendall(
        self,
        sock: socket.socket,
        data: SocketSendBuffer,
        progress: _ProgressCallback | None = None,
    ) -> IOWaiter[None]: ...

    def sock_send_nowait(
        self,
        sock: socket.socket,
        data: SocketSendBuffer,
        callback: Callable[[object, BaseException | None], object] | None = None,
        *,
        expect: IoExpect = IoExpect.READY,
    ) -> None: ...

    def sock_send_close(
        self,
        sock: socket.socket,
        data: SocketSendBuffer,
        *,
        expect: IoExpect = IoExpect.READY,
    ) -> None: ...

    def sock_send_iter(
        self,
        sock: socket.socket,
        chunks: Iterable[SocketSendBuffer],
    ) -> None: ...

    def sock_sendto(self, sock: socket.socket, data: SocketSendBuffer, address: Any) -> IOWaiter[int]: ...

    def sock_accept(
        self,
        sock: socket.socket,
        n: int | None = None,
    ) -> IOWaitable[AcceptDelivery]: ...

    def sock_connect(
        self,
        sock: socket.socket,
        address: Any,
        *,
        initial: SocketSendBuffer | None = None,
        fastopen: bool = False,
    ) -> IOWaitable[None]: ...

    def sock_fastopen(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
        connect_to: Any,
        data: SocketSendBuffer,
    ) -> IOWaitable[tuple[socket.socket, int]]: ...

    def sock_create(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
        connect_to: Any | None = None,
        initial_data: SocketSendBuffer | None = None,
        fastopen: bool = False,
    ) -> IOWaitable[socket.socket]: ...

    def connect_tcp(
        self,
        addr: tuple[str, int],
        *,
        family: int = socket.AF_UNSPEC,
        proto: int = 0,
        happy_eyeballs: bool = True,
        happy_eyeballs_delay: float | None = HAPPY_EYEBALLS_DELAY,
        interleave: int | None = None,
        source_address: tuple[Any, ...] | None = None,
        initial: SocketSendBuffer | None = None,
        fastopen: bool = False,
    ) -> socket.socket: ...

    def sock_recv_iter(
        self, sock: socket.socket, buffer_pool: RecvBufferPool | None = None
    ) -> Iterator[_RecvIterYield]: ...

    def sock_recvall(
        self,
        sock: socket.socket,
        progress: _RecvProgressCallback | None = None,
        *,
        buffer_pool: RecvBufferPool | None = None,
    ) -> bytes: ...

    def sock_shutdown(self, sock: socket.socket, how: int) -> IOWaiter[None]: ...

    def sock_shutdown_nowait(self, sock: socket.socket, how: int) -> None: ...

    def sock_close(self, sock: socket.socket) -> None: ...

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> RecvBufferPool: ...

    def acquire_recv_buffer_pool(self) -> RecvBufferPool: ...

    def release_recv_buffer_pool(self, pool: RecvBufferPool) -> None: ...

    def shared_recv_buffer_pool(self) -> RecvBufferPool: ...

    def set_shared_recv_buffer_pool(self, pool: RecvBufferPool) -> None: ...


# Stream-facing protocol slices live in ``streams.open`` / ``streams.writer`` as
# ``StreamOpenIO`` and ``StreamWriterIO``. ``ProactorIOManager`` satisfies both
# structurally without inheriting those protocols.


@runtime_checkable
class PollIO(Protocol):
    """Poll helpers over a scheduler IO backend."""

    def poll(self, fd: int, mask: int) -> IOWaiter[int]: ...

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Callable[[MultishotDelivery], object],
    ) -> IOHandle: ...


@runtime_checkable
class FileIO(Protocol):
    """Positioned binary file open helper over a scheduler IO backend."""

    def open(self, path: str, mode: str = "rb") -> IOWaiter[IOFile]: ...


class ServerIO(SocketIO, ProactorAccess, Protocol):
    """Socket IO plus proactor submission for stream servers.

    Static typing only: ``proactor`` is a property (same limitation as ``IOFile``).
    At runtime use ``isinstance(io, SocketIO)`` and ``io.proactor``; do not rely
    on ``isinstance(io, ServerIO)`` or ``isinstance(io, ProactorSocketIO)``.
    """

    def accept_sockets(
        self,
        sock: socket.socket,
        thread_handler: Callable[[socket.socket], object],
    ) -> IOWaiter[None]: ...

    def accept_many(
        self,
        sock: socket.socket,
        callback: Callable[[AcceptDelivery], object],
        *,
        recv_size: int | None = None,
        recv_timeout: float | None = None,
        on_recv_error: AcceptRecvErrorCallback | None = None,
    ) -> IOWaiter[None]: ...

    def accept_many_connections(
        self,
        sock: socket.socket,
        callback: Callable[[Any], object],
        *,
        pool: Any,
    ) -> IOWaiter[None]: ...

    def sock_create_streams(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        connect_to: Any,
        flags: int = 0,
        initial_data: SocketSendBuffer | None = None,
        fastopen: bool = False,
        limit: int = 2**16,
        stream_factory: Any | None = None,
        async_: bool = False,
    ) -> IOWaitable[AcceptStreamsDelivery]: ...


ProactorSocketIO = ServerIO


class RecvBufferPoolCache:
    """One-size FIFO of receive buffer pools.

    HTTP streams use one BufGroup size (``DEFAULT_RECV_POOL_BUFFER_SIZE`` ×
    ``DEFAULT_RECV_POOL_BUFFER_COUNT``). Other ``(buffer_size, buffer_count)``
    pairs allocate uncached pools. ``pool.close()`` destroys a group.
    ``release`` hands a checked-out group back here.

    Pools sit on one deque, appended at the back and taken from the front.
    A group just returned may still be in use: a receive is armed, or a
    caller still holds a buffer. The front is not handed out while
    ``in_use()`` is true. It is appended again and the new front is tried.
    That pass is capped at the number of groups queued when it started, so
    a group rotated to the back is not popped again. If every one of them
    is in use, checkout allocates.
    The idle cap (default 1024, ``None`` unlimited) applies to this deque.
    An armed pool is queued too and the ring stays registered. A second
    ``release`` of a pool already on the deque does nothing. A return past
    the cap, or after the cache is closed, calls ``pool.close()``.
    """

    def __init__(
        self,
        create: Callable[[int, int], RecvBufferPool],
        *,
        buffer_size: int = DEFAULT_RECV_POOL_BUFFER_SIZE,
        buffer_count: int = DEFAULT_RECV_POOL_BUFFER_COUNT,
        max_free: int | None = DEFAULT_MAX_FREE_RECV_BUFFER_POOLS,
    ) -> None:
        self._create = create
        self._buffer_size = buffer_size
        self._buffer_count = buffer_count
        self._max_free = max_free
        self._free: deque[RecvBufferPool] = deque()
        self._parked: set[int] = set()
        self._checked_out: set[int] = set()
        # popped for an in_use() check and not yet re-queued or handed out
        self._away = 0
        self._closed = False
        # in_use() runs outside this lock so shutdown can close while a
        # checkout is asking, then the rotate path drains that group.
        self._mu = threading.Lock()

    @property
    def max_free(self) -> int | None:
        return self._max_free

    @property
    def free_count(self) -> int:
        return len(self._free)

    @property
    def buffer_size(self) -> int:
        return self._buffer_size

    @property
    def buffer_count(self) -> int:
        return self._buffer_count

    def _at_cap(self) -> bool:
        return self._max_free is not None and len(self._free) + self._away >= self._max_free

    def _allocate(self) -> RecvBufferPool:
        with self._mu:
            if self._closed:
                raise RuntimeError("receive buffer pool cache is closed") from None
        pool = self._create(self._buffer_size, self._buffer_count)
        with self._mu:
            if self._closed:
                pool.close()
                raise RuntimeError("receive buffer pool cache is closed") from None
            self._checked_out.add(id(pool))
        return pool

    def acquire(self) -> RecvBufferPool:
        """Checkout the front group, or allocate one.

        An in-use front is moved to the back. Each group already queued is
        tried once. If the cache is closed during that append, the queue is
        drained and checkout fails.
        """

        with self._mu:
            if self._closed:
                raise RuntimeError("receive buffer pool cache is closed")
            pending = len(self._free)
        for _ in range(pending):
            with self._mu:
                try:
                    pool = self._free.popleft()
                except IndexError:
                    break
                self._away += 1
            if _group_unregistered(pool):
                with self._mu:
                    self._away -= 1
                    self._parked.discard(id(pool))
                continue
            try:
                busy = pool.in_use()
            except BaseException:
                with self._mu:
                    self._away -= 1
                    self._parked.add(id(pool))
                    self._free.append(pool)
                    # the group was off the deque, so an earlier close missed it
                    if self._closed:
                        self._drain_locked()
                raise
            if busy:
                with self._mu:
                    self._away -= 1
                    self._parked.add(id(pool))
                    self._free.append(pool)
                    if self._closed:
                        self._drain_locked()
                        raise RuntimeError("receive buffer pool cache is closed")
                continue
            with self._mu:
                self._away -= 1
                if self._closed:
                    self._parked.add(id(pool))
                    self._free.append(pool)
                    self._drain_locked()
                    raise RuntimeError("receive buffer pool cache is closed")
                self._parked.discard(id(pool))
                self._checked_out.add(id(pool))
            return pool
        return self._allocate()

    def release(self, pool: RecvBufferPool) -> None:
        """Return a checked-out pool to the FIFO, or destroy it.

        A pool already on the deque is left there. A pool this cache did
        not hand out is closed. An armed pool is queued too; checkout will
        not hand it out. Over-cap and closed-cache returns are closed.
        """

        key = id(pool)
        if _group_unregistered(pool):
            with self._mu:
                self._checked_out.discard(key)
            return
        with self._mu:
            if key in self._parked:
                return
            checked_out = key in self._checked_out
            if checked_out:
                self._checked_out.discard(key)
            if checked_out and not self._closed and not self._at_cap():
                self._parked.add(key)
                self._free.append(pool)
                return
        pool.close()

    def close(self) -> None:
        """Destroy idle pools and reject further caching."""

        with self._mu:
            self._closed = True
            self._drain_locked()

    def _drain_locked(self) -> None:
        while True:
            try:
                pool = self._free.pop()
            except IndexError:
                break
            self._parked.discard(id(pool))
            pool.close()


class ProactorIOManager:
    """IO facade over a ``Proactor`` backend.

    One-shot helpers return ``IOWaiter`` (callback + opaque ``OpHandle``)
    or ``IOWaiterSync`` for cheap local work (create). Call ``wait()`` to
    block the current tealet when needed. Accept, recv, and send always go
    to the proactor — this manager does not branch on backend type.
    Continuous ``accept_many`` returns ``IOWaiter[None]`` (``wait()`` until
    the stream ends). Continuous ``poll_many`` returns ``IOHandle``
    (``close()`` to stop; deliveries are callback-only). ``sock_recv_iter``
    remains a blocking iterator over receive chunks. Always owned by a
    proactor scheduler.

    Structurally implements ``StreamOpenIO`` and ``StreamWriterIO`` (defined in
    ``streams.open`` / ``streams.writer``) for stream-pair construction.
    """

    def __init__(
        self,
        scheduler: BaseScheduler,
        proactor: Proactor,
        *,
        max_free_recv_buffer_pools: int | None = DEFAULT_MAX_FREE_RECV_BUFFER_POOLS,
    ) -> None:
        self._scheduler: BaseScheduler | None = scheduler
        self._proactor: Proactor | None = proactor
        self._closed = False
        self._close_nowait = _env_sock_close_nowait()
        self._recv_pool_cache = RecvBufferPoolCache(
            proactor.create_recv_buffer_pool,
            max_free=max_free_recv_buffer_pools,
        )

    @property
    def proactor(self) -> Proactor:
        self._check_open()
        proactor = self._proactor
        assert proactor is not None
        return proactor

    def close(self) -> None:
        """Release scheduler ownership; called from ``ProactorScheduler.close()``."""

        self._closed = True
        self._recv_pool_cache.close()
        self._scheduler = None
        self._proactor = None

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("IO manager is closed")

    def _marshal_on_scheduler(self, thunk: Callable[[], object]) -> None:
        assert self._scheduler is not None
        self._scheduler.call_on_scheduler(thunk)

    def _thread_reorder_helper(
        self,
        delivery_callback: DeliveryCallback,
        *,
        start: int = 0,
    ) -> Callable[[MultishotDelivery], None]:
        buffer = ReorderBuffer(delivery_callback, start=start)

        def on_thread_delivery(delivery: MultishotDelivery) -> None:
            assert self._scheduler is not None
            self._scheduler.call_on_scheduler(lambda: buffer.deliver(delivery))

        return on_thread_delivery

    def sock_recv(self, sock: socket.socket, n: int) -> IOWaiter[bytes]:
        """Receive up to ``n`` bytes via the proactor (no manager-side first try)."""

        waiter: IOWaiter[bytes] = IOWaiter(self, map_result=_recv_result_bytes)
        return waiter.bind(self.proactor.recv(sock, n, waiter.complete))

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> RecvBufferPool:
        """Allocate a new receive buffer pool (not taken from the idle stack)."""

        return self.proactor.create_recv_buffer_pool(buffer_size, buffer_count)

    def acquire_recv_buffer_pool(self) -> RecvBufferPool:
        """Checkout a pool of the manager's idle-stack size (16 KiB × 4)."""

        self._check_open()
        return self._recv_pool_cache.acquire()

    def release_recv_buffer_pool(self, pool: RecvBufferPool) -> None:
        """Return a checked-out pool to the idle stack.

        A pool this cache did not hand out is closed. A second call while
        the pool sits on the stack does nothing. ``pool.close()`` destroys
        a group and does not return it here.
        """

        self._recv_pool_cache.release(pool)

    def shared_recv_buffer_pool(self) -> RecvBufferPool:
        return self.proactor.shared_recv_buffer_pool()

    def set_shared_recv_buffer_pool(self, pool: RecvBufferPool) -> None:
        self.proactor.set_shared_recv_buffer_pool(pool)

    def _resolve_recv_buffer_pool(self, buffer_pool: RecvBufferPool | None) -> RecvBufferPool:
        if buffer_pool is None:
            return self.proactor.shared_recv_buffer_pool()
        return buffer_pool

    def _open_sock_recv_iter(
        self,
        sock: socket.socket,
        buffer_pool: RecvBufferPool | None = None,
        *,
        owns_pool: bool = False,
        start: bool = True,
        pool_factory: Callable[[], RecvBufferPool] | None = None,
    ) -> RecvIterBuffer:
        """Open a ``RecvIterBuffer`` for ``sock``.

        ``buffer_pool`` of ``None`` uses the proactor shared pool and never
        transfers ownership (``owns_pool`` is forced false), unless
        ``pool_factory`` is set. The factory is called on the first
        ``recv_many``, not at construction, and ``owns_pool`` applies to the
        pool it returns. When a pool is passed, ``owns_pool`` defaults false:
        the caller still owns acquire / release. Only paths that check out a
        pool for this buffer's lifetime (for example
        ``pooled_default_stream_factory``) should pass ``owns_pool=True``.
        That hands the group to ``release_recv_buffer_pool`` when the buffer
        closes. ``start=False`` skips the initial ``recv_many`` so
        ``feed_initial`` can stash a oneshot prefix; the leg is posted when a
        later ``take_next`` needs another chunk.
        """

        if pool_factory is not None:
            pool = buffer_pool
        elif buffer_pool is None:
            pool = self.proactor.shared_recv_buffer_pool()
            owns_pool = False
        else:
            pool = buffer_pool
        scheduler = self._scheduler
        assert scheduler is not None
        return open_recv_iter_buffer(
            sock,
            # Proactor structurally matches _RecvIterProactor (ty protocol check is strict)
            proactor=self.proactor,  # ty: ignore[invalid-argument-type]
            buffer_pool=pool,
            scheduler=scheduler,
            owns_pool=owns_pool,
            # RecvBufferPool is the cache's pool; the buffer types it as _BufGroupLike.
            release_pool=self.release_recv_buffer_pool if owns_pool else None,  # ty: ignore[invalid-argument-type]
            start=start,
            pool_factory=pool_factory,
        )

    def sock_recv_iter(
        self, sock: socket.socket, buffer_pool: RecvBufferPool | None = None
    ) -> Iterator[_RecvIterYield]:
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
        if progress is None:
            process = bytes
        else:

            def process(chunk: memoryview) -> bytes:
                cargo = bytes(chunk)
                if cargo:
                    progress(cargo)
                return cargo

        return b"".join(process(chunk) for _index, chunk in self.sock_recv_iter(sock, buffer_pool))

    def _recv_many(
        self,
        sock: socket.socket,
        callback: Callable[[MultishotDelivery], object],
        *,
        buf_group: RecvBufferPool | None = None,
        base_sequence: int = 0,
    ) -> OpHandle:
        """``proactor.recv_many`` with the manager's pool resolution.

        Same shape as ``Proactor.recv_many``. Used by ``RecvIterBuffer`` so
        cancel still goes through the proactor. Returns an opaque
        ``OpHandle`` (not waitable). No manager-side non-blocking drain — ready
        data is the proactor's job (uring provided buffers).
        """

        pool = self._resolve_recv_buffer_pool(buf_group)
        return self.proactor.recv_many(
            sock,
            callback,
            buf_group=pool,
            base_sequence=base_sequence,
        )

    def sock_recv_into(self, sock: socket.socket, buf: Any) -> IOWaiter[int]:
        waiter: IOWaiter[int] = IOWaiter(self)
        return waiter.bind(self.proactor.recv_into(sock, buf, waiter.complete))

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> IOWaiter[tuple[bytes, Any]]:
        waiter: IOWaiter[tuple[bytes, Any]] = IOWaiter(self)
        return waiter.bind(self.proactor.recvfrom(sock, bufsize, waiter.complete))

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> IOWaiter[tuple[int, Any]]:
        waiter: IOWaiter[tuple[int, Any]] = IOWaiter(self)
        return waiter.bind(self.proactor.recvfrom_into(sock, buf, waiter.complete, nbytes))

    def sock_send(self, sock: socket.socket, data: Any) -> IOWaiter[int]:
        """One ``send(2)`` via ``proactor.send``. The result is the count.

        A short count is success. This does not drain the buffer. Empty
        ``data`` completes with 0. A socket ``send`` waits on this.
        """

        waiter: IOWaiter[int] = IOWaiter(self)
        return waiter.bind(self.proactor.send(sock, data, waiter.complete))

    def sock_sendall(
        self,
        sock: socket.socket,
        data: Any,
        progress: _ProgressCallback | None = None,
    ) -> IOWaiter[None]:
        """Drain ``data`` via ``proactor.send_all`` (uring ``send_all``).

        No manager-side stdlib ``send``. Empty payloads still go to the
        proactor (immediate complete).
        """

        waiter: IOWaiter[None] = IOWaiter(self)
        return waiter.bind(self.proactor.send_all(sock, data, waiter.complete, progress, expect=IoExpect.READY))

    def sock_send_nowait(
        self,
        sock: socket.socket,
        data: Any,
        callback: Callable[[object, BaseException | None], object] | None = None,
        *,
        expect: IoExpect = IoExpect.READY,
    ) -> None:
        """Pass-through to ``proactor.send_nowait`` (fire-and-forget sendall)."""

        self.proactor.send_nowait(sock, data, callback, expect=expect)

    def sock_send_close(
        self,
        sock: socket.socket,
        data: Any,
        *,
        expect: IoExpect = IoExpect.READY,
    ) -> None:
        """Pass-through to ``proactor.send_close_nowait`` (sendall then close, no waitable)."""

        self.proactor.send_close_nowait(sock, data, expect=expect)

    def _open_send_buffer(self, sock: socket.socket) -> SendBuffer:
        return open_send_buffer(sock, io=self, scheduler=self._scheduler)

    def sock_send_iter(
        self,
        sock: socket.socket,
        chunks: Iterable[bytes | bytearray | memoryview],
    ) -> None:
        for chunk in chunks:
            if not chunk:
                continue
            self.sock_sendall(sock, chunk).wait()

    def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> IOWaiter[int]:
        waiter: IOWaiter[int] = IOWaiter(self)
        return waiter.bind(self.proactor.sendto(sock, data, address, waiter.complete))

    def sock_shutdown(self, sock: socket.socket, how: int) -> IOWaiter[None]:
        """Submit ``shutdown`` and return a waiter. ``wait()`` raises ``OSError``.

        ``forget()`` drops the waiter and leaves the completion in place.
        ``sock_shutdown_nowait`` is the direct nowait submit.
        """

        waiter: IOWaiter[None] = IOWaiter(self)
        return waiter.bind(self.proactor.shutdown(sock, how, waiter.complete))

    def sock_shutdown_nowait(self, sock: socket.socket, how: int) -> None:
        """Pass-through to ``proactor.shutdown_nowait`` (uring nowait shutdown)."""

        self.proactor.shutdown_nowait(sock, how)

    def sock_close(self, sock: socket.socket) -> None:
        """Close ``sock`` without waiting for a completion.

        Uses ``Proactor.close_socket_nowait``: uring detaches and prepares a
        nowait ring close (same lazy flush as ``close_socket``); selector
        calls ``sock.close()``. Raises ``OSError`` from detach or stdlib
        close. Cancel outstanding proactor ops on this socket first.
        ``Proactor.close_socket`` remains for ordered ring teardown.
        Uring nowait close failures after submit go to the ring
        ``nowait_error_handler`` (not this call).
        """

        if self._close_nowait:
            self.proactor.close_socket_nowait(sock)
            return
        if sock.fileno() != -1:
            sock.close()

    def cancel_nowait(self, handle: OpHandle, *, no_deliver_multi: bool = False) -> None:
        """Cancel an opaque proactor ``handle`` without a teardown waitable.

        Stream recv close uses this so teardown does not allocate a cancel
        waitable. An ``IOWaiter`` unwraps its own handle and calls this.
        ``no_deliver_multi`` is passed to the proactor only when set: uring then
        suppresses later CQEs of a ``recv_multishot``. Accept, poll, and
        oneshot targets are still delivered.
        """

        if no_deliver_multi:
            self.proactor.cancel_nowait(handle, no_deliver_multi=True)
            return
        self.proactor.cancel_nowait(handle)

    def sock_accept(
        self,
        sock: socket.socket,
        n: int | None = None,
    ) -> IOWaitable[AcceptDelivery]:
        """Accept one connection via the proactor. Optional ``n`` composes a recv."""

        normalized_recv_size = normalize_accept_recv_size(n)

        if normalized_recv_size is None:
            waiter: IOWaiter[AcceptDelivery] = IOWaiter(
                self,
                map_result=lambda accepted: (accepted, None),
            )
            return waiter.bind(self.proactor.accept(sock, waiter.complete))

        group = IOWaitGroup(self)

        def advance_accept(child: IOWaitGroupChild[socket.socket]) -> None:
            accepted = child.value()
            waiter: IOWaiter[bytes] = IOWaiter(self, map_result=_recv_result_bytes)

            def advance_recv(recv_child: IOWaitGroupChild[bytes]) -> None:
                _finish_or_close_socket(group, accepted, (accepted, recv_child.value()))

            try:
                waiter.bind(self.proactor.recv(accepted, normalized_recv_size, waiter.complete))
                group.attach(
                    waiter,
                    on_cleanup=lambda fail, _value: abortive_close(accepted) if fail else None,
                    advance=advance_recv,
                )
            except BaseException:
                abortive_close(accepted)
                raise

        accept_waiter: IOWaiter[socket.socket] = IOWaiter(self)
        accept_waiter.bind(self.proactor.accept(sock, accept_waiter.complete))
        group.attach(
            accept_waiter,
            advance=advance_accept,
        )
        return group

    def _attach_sock_sendall(
        self,
        group: IOWaitGroup[Any],
        sock: socket.socket,
        data: Any,
        *,
        on_cleanup: Callable[[bool, Any], object] | None = None,
        on_done: Callable[[], object],
    ) -> None:
        """Chain ``sock_sendall`` into ``group``.

        Used after connect for ``initial`` / ``initial_data``.
        """

        waiter = self.sock_sendall(sock, data)
        group.attach(
            waiter,
            on_cleanup=on_cleanup,
            advance=lambda _child: on_done(),
        )

    def sock_connect(
        self,
        sock: socket.socket,
        address: Any,
        *,
        initial: SocketSendBuffer | None = None,
        fastopen: bool = False,
    ) -> IOWaitable[None]:
        """Connect ``sock``. Non-empty ``initial`` is written with ``sock_sendall``.

        ``fastopen=True`` sets ``TCP_FASTOPEN_CONNECT`` before connect. The
        default leaves the option unset. An unsupported option is ignored.
        The caller owns ``sock``.
        """

        if not initial:
            waiter: IOWaiter[None] = IOWaiter(self)
            return waiter.bind(self.proactor.connect(sock, address, waiter.complete))

        if fastopen:
            _enable_tcp_fastopen_connect(sock)
        group = IOWaitGroup(self)

        def advance_connect(_child: IOWaitGroupChild[None]) -> None:
            self._attach_sock_sendall(
                group,
                sock,
                initial,
                on_done=lambda: _finish_or_close_socket(group, sock, None),
            )

        connect_waiter: IOWaiter[None] = IOWaiter(self)
        connect_waiter.bind(self.proactor.connect(sock, address, connect_waiter.complete))
        group.attach(connect_waiter, advance=advance_connect)
        return group

    def connect_tcp(
        self,
        addr: tuple[str, int],
        *,
        family: int = socket.AF_UNSPEC,
        proto: int = 0,
        happy_eyeballs: bool = True,
        happy_eyeballs_delay: float | None = HAPPY_EYEBALLS_DELAY,
        interleave: int | None = None,
        source_address: tuple[Any, ...] | None = None,
        initial: SocketSendBuffer | None = None,
        fastopen: bool = False,
    ) -> socket.socket:
        """Block until a TCP socket is connected. See ``tealetio.connect_tcp``."""

        self._check_open()
        scheduler = self._scheduler
        assert scheduler is not None
        from .happy_eyeballs import _connect_with_io

        return _connect_with_io(
            self,
            scheduler,
            addr,
            family=family,
            proto=proto,
            happy_eyeballs=happy_eyeballs,
            happy_eyeballs_delay=happy_eyeballs_delay,
            interleave=interleave,
            source_address=source_address,
            initial=initial,
            fastopen=fastopen,
        )

    def sock_create(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
        connect_to: Any | None = None,
        initial_data: SocketSendBuffer | None = None,
        fastopen: bool = False,
        local_addrs: Sequence[Any] | None = None,
    ) -> IOWaitable[socket.socket]:
        """Create a socket via stdlib; optionally connect and send ``initial_data``.

        ``flags`` are socket type flags OR'd into ``type`` (e.g. ``SOCK_NONBLOCK`` /
        ``SOCK_CLOEXEC`` where valid). Not io_uring-only socket-create bits.
        Non-empty ``initial_data`` is written with ``sock_sendall``.
        ``fastopen=True`` sets ``TCP_FASTOPEN_CONNECT`` before connect. The
        default leaves the option unset. An unsupported option is ignored
        and the bytes still go out. ``local_addrs`` binds the first address
        that accepts, before connect.
        """

        if initial_data is not None and connect_to is None:
            raise ValueError("initial_data requires connect_to")

        try:
            sock = _create_scheduler_socket(family, type, proto, flags=flags)
        except OSError as exc:
            return IOWaiterSync.failed(exc)
        if local_addrs:
            try:
                _bind_local_addrs(sock, local_addrs)
            except OSError as exc:
                abortive_close(sock)
                return IOWaiterSync.failed(exc)

        if connect_to is None:
            return IOWaiterSync(sock)

        group = IOWaitGroup(self)

        def close_on_fail(fail: bool, _value: Any) -> None:
            if fail:
                abortive_close(sock)

        def finish_connected(_connect_child: IOWaitGroupChild[None]) -> None:
            if not initial_data:
                _finish_or_close_socket(group, sock, sock)
                return
            self._attach_sock_sendall(
                group,
                sock,
                initial_data,
                on_cleanup=close_on_fail,
                on_done=lambda: _finish_or_close_socket(group, sock, sock),
            )

        # sock is local until attach registers close_on_fail; close if submit fails first
        try:
            if initial_data and fastopen:
                _enable_tcp_fastopen_connect(sock)
            connect_waiter: IOWaiter[None] = IOWaiter(self)
            connect_waiter.bind(self.proactor.connect(sock, connect_to, connect_waiter.complete))
            group.attach(
                connect_waiter,
                on_cleanup=close_on_fail,
                advance=finish_connected,
            )
        except BaseException:
            abortive_close(sock)
            raise
        return group

    def sock_fastopen(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
        connect_to: Any,
        data: SocketSendBuffer,
        local_addrs: Sequence[Any] | None = None,
    ) -> IOWaitable[tuple[socket.socket, int]]:
        """Create a socket, connect, and post one ``sock_send`` of ``data``.

        Same shape as ``sock_create`` with a payload, except the send is one
        ``send(2)`` and the result is ``(sock, n_sent)``. ``n_sent`` is the
        count that call accepted. Sets ``TCP_FASTOPEN_CONNECT`` first, so a
        cached cookie can put that send on the SYN. Does not write the rest.
        Empty ``data`` raises ``ValueError``.
        """

        if not data:
            raise ValueError("sock_fastopen requires data")

        try:
            sock = _create_scheduler_socket(family, type, proto, flags=flags)
        except OSError as exc:
            return IOWaiterSync.failed(exc)
        if local_addrs:
            try:
                _bind_local_addrs(sock, local_addrs)
            except OSError as exc:
                abortive_close(sock)
                return IOWaiterSync.failed(exc)

        group: IOWaitGroup[tuple[socket.socket, int]] = IOWaitGroup(self)

        def close_on_fail(fail: bool, _value: Any) -> None:
            if fail:
                abortive_close(sock)

        def advance_connected(_connect_child: IOWaitGroupChild[None]) -> None:
            # The connect submit's try does not cover this. A prepare error
            # here runs before the send leg's close_on_fail is registered.
            try:
                waiter = self.sock_send(sock, data)

                def advance_sent(child: IOWaitGroupChild[int]) -> None:
                    _finish_or_close_socket(group, sock, (sock, child.value()))

                group.attach(waiter, on_cleanup=close_on_fail, advance=advance_sent)
            except BaseException:
                abortive_close(sock)
                raise

        try:
            _enable_tcp_fastopen_connect(sock)
            connect_waiter: IOWaiter[None] = IOWaiter(self)
            connect_waiter.bind(self.proactor.connect(sock, connect_to, connect_waiter.complete))
            group.attach(connect_waiter, on_cleanup=close_on_fail, advance=advance_connected)
        except BaseException:
            abortive_close(sock)
            raise
        return group

    def poll(self, fd: int, mask: int) -> IOWaiter[int]:
        waiter: IOWaiter[int] = IOWaiter(self)
        return waiter.bind(self.proactor.poll(fd, mask, waiter.complete))

    def read(self, fd: int, n: int, offset: int) -> IOWaiter[bytes]:
        waiter: IOWaiter[bytes] = IOWaiter(self)
        return waiter.bind(self.proactor.read(fd, n, offset, waiter.complete))

    def read_into(self, fd: int, buf: Any, offset: int) -> IOWaiter[int]:
        waiter: IOWaiter[int] = IOWaiter(self)
        return waiter.bind(self.proactor.read_into(fd, buf, offset, waiter.complete))

    def write(self, fd: int, data: Any, offset: int) -> IOWaiter[int]:
        waiter: IOWaiter[int] = IOWaiter(self)
        return waiter.bind(self.proactor.write(fd, data, offset, waiter.complete))

    def stat_fdsize(self, fd: int) -> IOWaiter[int]:
        waiter: IOWaiter[int] = IOWaiter(self)
        return waiter.bind(self.proactor.stat_fdsize(fd, waiter.complete))

    def close_fd(self, fd: int) -> IOWaiter[None]:
        waiter: IOWaiter[None] = IOWaiter(self)
        return waiter.bind(self.proactor.close_fd(fd, waiter.complete))

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Callable[[MultishotDelivery], object],
    ) -> IOHandle:
        """Start continuous poll; return a closeable handle (not a waitable).

        Readiness and terminal cancel/error deliveries go to ``callback``
        (scheduler thread, ordered). Call ``handle.close()`` to stop (maps to
        ``proactor.stop_poll``). ``handle.closed`` is true after the stream
        finishes (terminal ``!MORE``).
        """

        io_handle = IOHandle(self)

        def on_ordered_delivery(delivery: MultishotDelivery) -> None:
            try:
                callback(delivery)
            finally:
                if not delivery.more:
                    io_handle._mark_closed()

        token = self.proactor.poll_many(
            fd,
            mask,
            self._thread_reorder_helper(on_ordered_delivery),
        )
        io_handle.bind(token)
        return io_handle

    def accept_sockets(
        self,
        sock: socket.socket,
        thread_handler: Callable[[socket.socket], object],
    ) -> IOWaiter[None]:
        """Call ``thread_handler(accepted)`` on the thread that delivers the completion.

        That thread is a completion worker, or the scheduler thread when
        completions are inline. ``thread_handler`` owns that socket. A
        failure while it still owns the socket is the handler's to close; a
        failure after it has handed the socket on (for example the scheduler
        rejects the marshal) must not be closed here. The exception is
        reported on the scheduler and does not settle or fail the waiter.

        ``wait()`` completes when this arm is disarmed (``more`` is false),
        with that delivery's exception. A successful accept does not settle
        the waiter, so the loop can re-arm as soon as the kernel request is
        done. Completions already produced for this arm still run
        ``thread_handler``.

        The handler may prepare further IO (the connection's first recv, a
        send from the accept callback). The driver can already be inside its
        idle park, and a completion worker does not submit. After the
        handler returns, an off-scheduler call wakes that park so the driver
        submits those SQEs.
        """

        waiter: IOWaiter[None] = IOWaiter(self)

        def report(exc: BaseException) -> None:
            def raise_error(error: BaseException = exc) -> None:
                raise error

            assert self._scheduler is not None
            self._scheduler.call_on_scheduler(raise_error)

        def on_worker_delivery(delivery: MultishotDelivery) -> None:
            # disarm is the signal the accept loop needs. do this before the
            # thread handler so a slow handoff does not hold the re-arm.
            if not delivery.more:
                waiter.complete(None, delivery.exception)
            accepted = delivery.value
            if not isinstance(accepted, socket.socket):
                return
            try:
                thread_handler(accepted)
            except BaseException as exc:
                report(exc)
            finally:
                self._wake_accept_driver()

        return waiter.bind(self.proactor.accept_many(sock, on_worker_delivery))

    def _wake_accept_driver(self) -> None:
        """Wake a driver parked in idle wait so it can submit worker-prepared SQEs.

        No-op on the scheduler thread: that stack submits on the way back
        into the park, and a self-wake would only spin the selector.
        """

        scheduler = self._scheduler
        if scheduler is None:
            return
        # only a running driver records the thread parked in idle wait
        owner = getattr(scheduler, "_owner_thread", None)
        if owner is None or owner == threading.get_ident():
            return
        self.proactor.wake_wait()

    def accept_many(
        self,
        sock: socket.socket,
        callback: Callable[[AcceptDelivery], object],
        *,
        recv_size: int | None = None,
        recv_timeout: float | None = None,
        on_recv_error: AcceptRecvErrorCallback | None = None,
    ) -> IOWaiter[None]:
        """Accept connections via ``accept_sockets``.

        User ``callback`` is per-connection only (``(conn, initial_data)``),
        marshalled onto the scheduler in completion order, not index order.
        Stream-end (cancel, accept ``OSError`` including transient
        ``EMFILE`` / ``ECONNABORTED``) never goes to that callback.
        ``wait()`` returns when the accept arm is disarmed (``more`` is
        false): ``None`` on a clean end, or the stored exception. Accept-time
        ``recv`` and the scheduler callback may still be in flight. There is
        no manager-side non-blocking ``accept`` drain — ready backlog is the
        proactor's job (a selector backend can first-try internally). The
        proactor handle is an ``OpHandle``; this waitable is the accept-arm
        supervisor park (re-arm after oneshot, join on close). Transient
        accept errors are the accept loop's to ignore, pause, or die on.

        **Shutdown and late deliveries.** Cancelling this ``IOWaitable`` or the
        hosting accept-loop tealet does **not** cancel accept-time ``recv`` legs
        started when ``recv_size`` is set. Late accepts and preread completions
        can still reach ``callback`` after shutdown unless the application
        discards them — close listening sockets, check a shutdown flag in the
        accept callback, and ignore or close unwanted connections (``StreamServer``
        uses ``_closed`` for this). ``recv_timeout`` (requires ``recv_size``)
        is passed to ``proactor.recv``. It does not replace listener close
        or callback-side discard.

        Recv failures invoke ``on_recv_error(conn, exc)`` when provided; the
        socket is always closed afterwards. With no ``on_recv_error``, recv
        failures close the socket silently.

        When ``recv_timeout`` is set, the preread is
        ``proactor.recv(..., timeout=recv_timeout)``. Uring arms a link
        timeout; the selector arms its scheduler timer. Uring finishes that
        recv with ``ECANCELED`` if it was still waiting, or ``EINTR`` if it
        had already entered the syscall. The selector timer uses
        ``ECANCELED``. ``EINTR`` is not ``is_io_cancellation``. The merged
        ``(conn, recv_error)`` leg is posted to the scheduler;
        ``finalize_accept_recv_error`` runs there and the user accept
        callback is skipped.

        ``wait()`` ends the accept arm only. On non-multishot backends that is
        after each accept. Re-arm in a loop when more accepts are needed.
        """

        normalized_recv_size = normalize_accept_recv_size(recv_size)
        if recv_timeout is not None:
            if normalized_recv_size is None:
                raise ValueError("recv_timeout requires recv_size")
            if recv_timeout <= 0:
                raise ValueError("recv_timeout must be positive when provided")

        def deliver(
            conn: socket.socket,
            initial_data: bytes | None,
            recv_error: BaseException | None,
        ) -> None:
            if recv_error is not None:
                finalize_accept_recv_error(conn, recv_error, on_recv_error)
                return
            try:
                callback((conn, initial_data))
            except BaseException:
                abortive_close(conn)
                raise

        def thread_handler(accepted: socket.socket) -> None:
            if normalized_recv_size is None:
                self._marshal_on_scheduler(lambda: deliver(accepted, None, None))
                return

            def on_recv(result: RecvResult | None, exception: BaseException | None) -> None:
                if exception is not None:
                    self._marshal_on_scheduler(lambda: deliver(accepted, None, exception))
                    return
                assert result is not None
                data = result.data
                self._marshal_on_scheduler(lambda: deliver(accepted, data, None))

            self.proactor.recv(accepted, normalized_recv_size, on_recv, timeout=recv_timeout)

        return self.accept_sockets(sock, thread_handler)

    def accept_many_connections(
        self,
        sock: socket.socket,
        callback: Callable[[Any], object],
        *,
        pool: Any,
    ) -> IOWaiter[None]:
        """Accept ``Connection`` objects via ``accept_sockets``.

        ``pool`` is the server's oneshot recv buffer cache (``acquire`` /
        ``release`` / ``buffer_size``). On the delivery thread each accepted
        socket posts ``recv_into`` from that pool, then the ``Connection`` is
        marshalled onto the scheduler. The first recv may complete before or
        after the user ``callback``. ``wait()`` matches ``accept_sockets``:
        the arm ends when it is disarmed, while the scheduler callback may
        still be in flight.
        """

        from .connections import Connection

        def thread_handler(accepted: socket.socket) -> None:
            try:
                conn = Connection.start(self, accepted, pool=pool)
            except BaseException as exc:
                abortive_close(accepted)

                def reraise(error: BaseException = exc) -> None:
                    raise error

                assert self._scheduler is not None
                self._scheduler.call_on_scheduler(reraise)
                return

            def deliver() -> None:
                try:
                    callback(conn)
                except BaseException:
                    conn.close()
                    raise

            self._marshal_on_scheduler(deliver)

        return self.accept_sockets(sock, thread_handler)

    def sock_create_streams(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        connect_to: Any,
        flags: int = 0,
        initial_data: SocketSendBuffer | None = None,
        fastopen: bool = False,
        limit: int = 2**16,
        stream_factory: Any | None = None,
        async_: bool = False,
    ) -> IOWaitable[AcceptStreamsDelivery]:
        """Create a socket, connect, and return stream endpoints.

        ``initial_data`` is sent on the wire after connect, before streams open.
        ``fastopen=True`` sets ``TCP_FASTOPEN_CONNECT`` before connect. The
        default leaves the option unset. An unsupported option is ignored.
        Socket creation is direct (stdlib); connect goes through the proactor;
        the optional initial send uses ``sock_sendall``. ``flags`` are socket
        type flags only (same contract as ``sock_create``).
        """

        try:
            sock = _create_scheduler_socket(family, type, proto, flags=flags)
        except OSError as exc:
            return IOWaiterSync.failed(exc)

        group = IOWaitGroup(self)

        def close_on_fail(fail: bool, _value: Any) -> None:
            if fail:
                abortive_close(sock)

        def open_and_finish() -> None:
            try:
                streams = open_streams(
                    self,
                    sock,
                    limit=limit,
                    stream_factory=stream_factory,
                    async_=async_,
                )
            except BaseException:
                abortive_close(sock)
                raise
            if not group.finish(streams):
                _reader, writer = streams
                writer.close()

        def finish_connected(_connect_child: IOWaitGroupChild[None]) -> None:
            if not initial_data:
                open_and_finish()
                return
            self._attach_sock_sendall(
                group,
                sock,
                initial_data,
                on_cleanup=close_on_fail,
                on_done=open_and_finish,
            )

        # sock is local until attach registers close_on_fail; close if submit fails first
        try:
            if initial_data and fastopen:
                _enable_tcp_fastopen_connect(sock)
            connect_waiter: IOWaiter[None] = IOWaiter(self)
            connect_waiter.bind(self.proactor.connect(sock, connect_to, connect_waiter.complete))
            group.attach(
                connect_waiter,
                on_cleanup=close_on_fail,
                advance=finish_connected,
            )
        except BaseException:
            abortive_close(sock)
            raise
        return group

    def open(self, path: str, mode: str = "rb") -> IOWaiter[IOFile]:
        flags, file_mode = parse_open_mode(mode)

        def make_file(fd: int) -> IOFile:
            try:
                return ProactorFile(
                    self,
                    fd,
                    path=path,
                    flags=flags,
                    append="a" in mode,
                )
            except BaseException:
                try:
                    self.close_fd(fd).wait()
                except OSError:
                    pass
                raise

        waiter: IOWaiter[IOFile] = IOWaiter(self, map_result=make_file)
        try:
            waiter.bind(self.proactor.openat(path, flags, waiter.complete, file_mode))
        except NotImplementedError as exc:
            raise NotImplementedError("file I/O requires a proactor with openat support") from exc
        return waiter


from .streams.open import open_streams
