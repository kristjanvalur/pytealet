from __future__ import annotations

import socket
from collections.abc import Callable, Iterable, Iterator
from typing import TYPE_CHECKING, Any, Protocol, TypeVar, cast, runtime_checkable

from .files import IOFile, ProactorFile, parse_open_mode
from .continuous_callbacks import (
    AcceptDelivery,
    AcceptReadResult,
    AcceptRecvErrorCallback,
    AcceptStreamsDelivery,
    DeliveryCallback,
    ReorderBuffer,
    finalize_accept_recv_error,
    finish_continuous_delivery,
    is_cancellation_delivery,
    normalize_accept_recv_size,
)
from .io_waiter import (
    IOOperation,
    IOWaiter,
    IOWaiterSync,
    IOWaitGroup,
    IOWaitGroupChild,
    IOWaitGroupChildProtocol,
    IOWaitable,
)


from .io_buffers import RecvIterBuffer, SendBuffer, _RecvIterProactor, open_recv_iter_buffer, open_send_buffer
from .operations import (
    ContinuousOperation,
    MultishotDelivery,
    Operation,
    SupportsContinuousOperation,
    SupportsOperation,
)
from .socket_helpers import abortive_close, configure_scheduler_socket
from .types import SocketSendBuffer

if TYPE_CHECKING:
    from .proactor import Proactor, RecvBufferPool
    from .scheduler import BaseScheduler, TimerHandle

T = TypeVar("T")


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
    return configure_scheduler_socket(socket.socket(family, type | flags, proto))


def _accept_ready_connection(sock: socket.socket) -> socket.socket | None:
    """Non-blocking accept of one ready connection, or ``None`` if would block.

    Raises ``OSError`` for hard accept errors (not ``BlockingIOError`` /
    ``InterruptedError``). Applies the scheduler socket contract on success.
    """

    while True:
        try:
            conn, _address = sock.accept()
        except BlockingIOError:
            return None
        except InterruptedError:
            continue
        return configure_scheduler_socket(conn)


def _recv_ready_chunk(sock: socket.socket, n: int) -> bytes | None:
    """Non-blocking ``recv(n)``: data, empty EOF ``b""``, or ``None`` if would block.

    Raises ``OSError`` for hard receive errors (not ``BlockingIOError`` /
    ``InterruptedError``).
    """

    while True:
        try:
            return sock.recv(n)
        except BlockingIOError:
            return None
        except InterruptedError:
            continue


def _send_ready_bytes(sock: socket.socket, data: memoryview) -> int | None:
    """One non-blocking ``send``: bytes written, or ``None`` if would block.

    A zero-byte return is treated as would-block (same as proactor sendall).
    Raises ``OSError`` for hard send errors (not ``BlockingIOError`` /
    ``InterruptedError``).
    """

    while True:
        try:
            sent = sock.send(data)
        except BlockingIOError:
            return None
        except InterruptedError:
            continue
        if sent == 0:
            return None
        return sent


def _recv_pool_is_full(pool: RecvBufferPool) -> bool:
    """True when no free delivery slots remain (synthetic or provided-buffer)."""

    return pool.leased_count >= pool.buffer_count


def _eager_recv_chunk_view(data: bytes, pool: RecvBufferPool) -> memoryview:
    """Wrap eager ``recv`` bytes for delivery; lease when the pool supports it.

    Synthetic pools may lease (and then throttle via ``_recv_pool_is_full`` so
    continuous can still surface ENOBUFS). Provided-buffer / uring ``BufGroup``
    pools return a plain view of the copied bytes: eager startup does **not**
    apply pool backpressure — data already in the socket buffer may as well sit
    in user memory as in kernel memory until the consumer drains it.
    """

    if not data:
        return memoryview(b"")
    lease = getattr(pool, "lease_delivery_chunk", None)
    if lease is not None:
        return cast(memoryview, lease(data))
    return memoryview(data)


def _finish_or_close_socket(group: IOWaitGroup[Any], sock: socket.socket, result: Any) -> None:
    if not group.finish(result):
        abortive_close(sock)


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
    "IO_UNSUPPORTED_ERROR",
    "IOFile",
    "IOOperation",
    "IOWaiter",
    "IOWaiterSync",
    "IOWaitGroup",
    "IOWaitGroupChild",
    "IOWaitGroupChildProtocol",
    "IOWaitable",
    "PollIO",
    "ProactorAccess",
    "ProactorIOManager",
    "ProactorSocketIO",
    "SELECTOR_IO_UNSUPPORTED_ERROR",
    "ServerIO",
    "SocketAddress",
    "SocketSendBuffer",
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
    def io(self) -> "ProactorIOManager": ...


class ProactorAccess(Protocol):
    """IO facade with access to proactor submission (``accept_many``, …)."""

    @property
    def proactor(self) -> "Proactor": ...


@runtime_checkable
class SocketIO(Protocol):
    """Asyncio-shaped socket helpers; one-shot methods return ``IOWaitable``.

    Eager paths may resolve as ``IOWaiterSync``; otherwise ``IOWaiter`` wraps a
    proactor ``Operation``. Continuous helpers use ``IOWaitable[None]``.
    """

    def sock_recv(self, sock: socket.socket, n: int) -> IOWaitable[bytes]: ...

    def sock_recv_into(self, sock: socket.socket, buf: Any) -> IOWaiter[int]: ...

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> IOWaiter[tuple[bytes, Any]]: ...

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> IOWaiter[tuple[int, Any]]: ...

    def sock_sendall(
        self,
        sock: socket.socket,
        data: SocketSendBuffer,
        progress: _ProgressCallback | None = None,
    ) -> IOWaitable[None]: ...

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
    ) -> IOWaitable[None]: ...

    def sock_create(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
        connect_to: Any | None = None,
        initial_data: SocketSendBuffer | None = None,
    ) -> IOWaitable[socket.socket]: ...

    def sock_recv_iter(
        self, sock: socket.socket, buffer_pool: "RecvBufferPool | None" = None
    ) -> Iterator[_RecvIterYield]: ...

    def sock_recvall(
        self,
        sock: socket.socket,
        progress: _RecvProgressCallback | None = None,
        *,
        buffer_pool: "RecvBufferPool | None" = None,
    ) -> bytes: ...

    def sock_shutdown(self, sock: socket.socket, how: int) -> IOWaitable[None]: ...

    def sock_close(self, sock: socket.socket) -> IOWaitable[None]: ...

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> "RecvBufferPool": ...

    def shared_recv_buffer_pool(self) -> "RecvBufferPool": ...

    def set_shared_recv_buffer_pool(self, pool: "RecvBufferPool") -> None: ...


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
    ) -> IOWaitable[None]: ...


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

    def accept_many(
        self,
        sock: socket.socket,
        callback: Callable[[AcceptDelivery], object],
        *,
        recv_size: int | None = None,
        recv_timeout: float | None = None,
        on_recv_error: AcceptRecvErrorCallback | None = None,
    ) -> IOWaitable[None]: ...

    def accept_many_streams(
        self,
        sock: socket.socket,
        callback: Callable[[AcceptStreamsDelivery], object],
        *,
        limit: int = 2**16,
        stream_factory: Any | None = None,
        async_: bool = False,
    ) -> IOWaitable[None]: ...

    def sock_create_streams(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        connect_to: Any,
        flags: int = 0,
        initial_data: SocketSendBuffer | None = None,
        limit: int = 2**16,
        stream_factory: Any | None = None,
        async_: bool = False,
    ) -> IOWaitable[AcceptStreamsDelivery]: ...


ProactorSocketIO = ServerIO


class ProactorIOManager:
    """IO facade over a ``Proactor`` backend.

    One-shot helpers return ``IOWaitable``: ``IOWaiterSync`` when the op finishes
    on the eager non-blocking path, otherwise ``IOWaiter`` over a proactor
    ``Operation``. Call ``wait()`` to block the current tealet when needed.
    Continuous helpers (``accept_many``, ``poll_many``) return
    ``IOWaitable[None]``; call ``wait()`` to block until the stream ends.
    ``sock_recv_iter`` remains a blocking iterator over receive chunks.
    Always owned by a proactor scheduler.

    Structurally implements ``StreamOpenIO`` and ``StreamWriterIO`` (defined in
    ``streams.open`` / ``streams.writer``) for stream-pair construction.
    """

    def __init__(self, scheduler: BaseScheduler, proactor: Proactor) -> None:
        self._scheduler = scheduler
        self._proactor = proactor
        self._closed = False

    @property
    def proactor(self) -> Proactor:
        self._check_open()
        return self._proactor

    def close(self) -> None:
        """Release scheduler ownership; called from ``ProactorScheduler.close()``."""

        self._closed = True
        self._scheduler = None

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("IO manager is closed")

    def _marshal_on_scheduler(self, thunk: Callable[[], object]) -> None:
        assert self._scheduler is not None
        self._scheduler.call_soon_threadsafe(thunk, immediate=True)

    def _thread_reorder_helper(
        self,
        delivery_callback: DeliveryCallback,
        *,
        start: int = 0,
        flush_heap_on_unsequenced_terminal: bool = False,
    ) -> Callable[[MultishotDelivery], None]:
        buffer = ReorderBuffer(delivery_callback, start=start)

        def deliver_on_scheduler(delivery: MultishotDelivery) -> None:
            # accept/poll: local cancel uses index=None and would otherwise leave
            # OOO sockets on the heap. recv_many keeps the default (no flush) so
            # cancel cannot surface gap-skipped stream data.
            if flush_heap_on_unsequenced_terminal and delivery.index is None:
                buffer.flush_pending()
            buffer.deliver(delivery)

        def on_thread_delivery(delivery: MultishotDelivery) -> None:
            assert self._scheduler is not None
            self._scheduler.call_soon_threadsafe(lambda: deliver_on_scheduler(delivery), immediate=True)

        return on_thread_delivery

    def _wrap_continuous_delivery(
        self,
        operation: SupportsContinuousOperation[Any],
        deliver: Callable[[MultishotDelivery], object],
    ) -> Callable[[MultishotDelivery], None]:
        """Marshal ``deliver`` for tests and paths that hold ``operation`` out-of-band."""

        def on_ordered_delivery(delivery: MultishotDelivery) -> None:
            try:
                deliver(delivery)
            finally:
                finish_continuous_delivery(delivery)

        on_thread_delivery = self._thread_reorder_helper(on_ordered_delivery)

        def on_delivery(delivery: MultishotDelivery) -> None:
            if delivery.operation is None:
                delivery = delivery._replace(operation=operation)
            on_thread_delivery(delivery)

        return on_delivery

    def _cancel_operation(self, operation: SupportsOperation[Any]) -> IOWaitable[None]:
        """Cancel ``operation`` and return a waitable for its teardown leg.

        Internal helper for io_manager composition paths that hold raw
        waitable handles (for example accept-time ``recv``). Returns a
        teardown ``IOWaitable``; call ``wait()`` to block until ring cancel
        settles, or ``forget()`` when only the target's terminal state matters.
        The waitable is already complete when the target was done or produced no
        async backend teardown operation.
        """

        return IOWaiter(self, self._proactor.cancel(operation))

    def _recv_if_ready(self, sock: socket.socket, n: int) -> bytes | None:
        """Non-blocking ``recv(n)``: data/EOF when ready, ``None`` if would block.

        Raises ``OSError`` for hard receive errors. Shared by ``sock_recv`` and
        accept-time preread.
        """

        return _recv_ready_chunk(sock, n)

    def sock_recv(self, sock: socket.socket, n: int) -> IOWaitable[bytes]:
        """Receive up to ``n`` bytes; try a direct non-blocking ``recv`` first.

        When data (or EOF) is already available, returns ``IOWaiterSync`` without
        a proactor submit. Would-block **and** eager ``OSError`` fall through to
        ``proactor.recv`` — the happy path does not classify errors; the proactor
        is the canonical error path.
        """

        try:
            data = self._recv_if_ready(sock, n)
        except OSError:
            data = None
        if data is not None:
            return IOWaiterSync(data)
        return IOWaiter(self, self._proactor.recv(sock, n))

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> RecvBufferPool:
        return self._proactor.create_recv_buffer_pool(buffer_size, buffer_count)

    def shared_recv_buffer_pool(self) -> RecvBufferPool:
        return self._proactor.shared_recv_buffer_pool()

    def set_shared_recv_buffer_pool(self, pool: RecvBufferPool) -> None:
        self._proactor.set_shared_recv_buffer_pool(pool)

    def _resolve_recv_buffer_pool(self, buffer_pool: RecvBufferPool | None) -> RecvBufferPool:
        if buffer_pool is None:
            return self._proactor.shared_recv_buffer_pool()
        return buffer_pool

    def _open_sock_recv_iter(self, sock: socket.socket, buffer_pool: RecvBufferPool | None) -> RecvIterBuffer:
        pool = self._resolve_recv_buffer_pool(buffer_pool)
        # eager drain via _recv_many; cancel unfinished legs on the real proactor
        return open_recv_iter_buffer(
            sock,
            # Proactor structurally matches; cast for Protocol vs concrete signatures
            proactor=cast(_RecvIterProactor, self._proactor),
            buf_group=pool,
            scheduler=self._scheduler,
            recv_many=self._recv_many,
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
    ) -> ContinuousOperation[memoryview]:
        """Eager non-blocking drain, then ``proactor.recv_many``.

        Same shape as ``Proactor.recv_many``: returns a ``ContinuousOperation`` and
        invokes ``callback`` for each leg. No marshal, reorder, or finish wrapping.
        Used by ``RecvIterBuffer`` so it does not call ``proactor.recv_many``
        directly (cancel still uses the proactor).

        Intermediate eager chunks are delivered with ``operation=None`` (known).
        Pure-eager EOF finishes a synthetic ``ContinuousOperation`` already done
        (EOF is a normal terminal, not an error path). Would-block and any
        ``OSError`` during the eager drain fall through to ``proactor.recv_many``
        — error handling lives on that canonical path only.

        Eager startup drains ready socket data without provided-buffer pool
        backpressure (see ``_eager_recv_chunk_view``). Synthetic pools still stop
        when fully leased so the continuous path can report ENOBUFS.
        """

        pool = self._resolve_recv_buffer_pool(buf_group)
        chunk_size = pool.buffer_size
        index = base_sequence
        try:
            while True:
                if _recv_pool_is_full(pool):
                    # fully leased: stop eager so continuous can surface ENOBUFS
                    break
                data = _recv_ready_chunk(sock, chunk_size)
                if data is None:
                    break
                if not data:
                    # EOF: finish the stream without arming continuous
                    terminal = ContinuousOperation[memoryview](kind="recv_many", fileobj=sock)
                    delivery = MultishotDelivery(
                        index=index,
                        value=memoryview(b""),
                        more=False,
                        operation=terminal,
                    )
                    callback(delivery)
                    finish_continuous_delivery(delivery)
                    return terminal
                chunk = _eager_recv_chunk_view(data, pool)
                # intermediate: operation=None until terminal synthetic or proactor return
                callback(MultishotDelivery(index=index, value=chunk, more=True))
                index += 1
        except OSError:
            # Happy path has no error classification: stop eager, arm proactor.
            pass

        return self._proactor.recv_many(
            sock,
            callback,
            buf_group=pool,
            base_sequence=index,
        )

    def sock_recv_into(self, sock: socket.socket, buf: Any) -> IOWaiter[int]:
        return IOWaiter(self, self._proactor.recv_into(sock, buf))

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> IOWaiter[tuple[bytes, Any]]:
        return IOWaiter(self, self._proactor.recvfrom(sock, bufsize))

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> IOWaiter[tuple[int, Any]]:
        return IOWaiter(self, self._proactor.recvfrom_into(sock, buf, nbytes))

    def sock_sendall(
        self, sock: socket.socket, data: Any, progress: _ProgressCallback | None = None
    ) -> IOWaitable[None]:
        """Drain ``data``; try one non-blocking ``send`` before the proactor.

        When the full buffer is accepted immediately, returns ``IOWaiterSync``
        without a submit. Partial progress is reported via ``progress`` (if any)
        and the remainder is handed to ``proactor.send``, which continues the
        drain. Empty payloads go straight to the proactor (immediate complete).

        Exactly one eager ``send`` is intentional: a cheap ready-now try, then
        the proactor owns the rest. ``UringProactor`` completes that remainder
        via io_uring only (no multi-send stdlib drain on the manager path).

        If ``progress`` raises after a partial write, the remainder is not
        submitted: the waitable fails with that exception and the short write
        stays on the wire (same as a proactor mid-drain progress failure).
        Retrying the full original buffer can duplicate already-sent bytes.
        """

        view = memoryview(data)
        if not view:
            return IOWaiter(self, self._proactor.send(sock, data, progress))

        try:
            sent = _send_ready_bytes(sock, view)
        except OSError as exc:
            return IOWaiterSync.failed(exc)
        if sent is None:
            return IOWaiter(self, self._proactor.send(sock, data, progress))

        if progress is not None:
            try:
                progress(sent)
            except BaseException as exc:
                return IOWaiterSync.failed(exc)
        if sent >= len(view):
            return IOWaiterSync(None)

        remainder = view[sent:]
        if progress is None:
            return IOWaiter(self, self._proactor.send(sock, remainder, None))

        base = sent

        def progress_wrap(n: int) -> object:
            return progress(base + n)

        return IOWaiter(self, self._proactor.send(sock, remainder, progress_wrap))

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
            self.sock_sendall(sock, memoryview(chunk)).wait()

    def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> IOWaiter[int]:
        return IOWaiter(self, self._proactor.sendto(sock, data, address))

    def sock_shutdown(self, sock: socket.socket, how: int) -> IOWaitable[None]:
        """``socket.shutdown(how)`` on the calling thread (no proactor submit).

        Matches asyncio stream teardown: shutdown is a quick local syscall.
        ``Proactor.shutdown`` remains for direct proactor callers.
        """

        try:
            sock.shutdown(how)
        except OSError as exc:
            return IOWaiterSync.failed(exc)
        return IOWaiterSync(None)

    def sock_close(self, sock: socket.socket) -> IOWaitable[None]:
        """``socket.close()`` on the calling thread (no proactor submit).

        Matches asyncio stream teardown: close releases the Python wrapper and
        fd immediately. Cancel outstanding proactor ops on this socket first —
        a concurrent uring leg may still hold the fd (``detach`` + ring close
        via ``Proactor.close_socket``). ``Proactor.close_socket`` remains for
        ordered ring teardown when the caller owns that lifecycle.
        """

        try:
            sock.close()
        except OSError as exc:
            return IOWaiterSync.failed(exc)
        return IOWaiterSync(None)

    def sock_accept(
        self,
        sock: socket.socket,
        n: int | None = None,
    ) -> IOWaitable[AcceptDelivery]:
        """Accept one connection, trying a direct non-blocking accept first.

        When the listen socket is already readable, ``accept()`` runs without a
        proactor submit. Would-block and eager ``OSError`` fall through to
        ``proactor.accept`` (canonical error path). Optional ``n`` composes an
        accept-time ``recv``.
        """

        normalized_recv_size = normalize_accept_recv_size(n)

        try:
            conn = _accept_ready_connection(sock)
        except OSError:
            conn = None

        if conn is not None:
            if normalized_recv_size is None:
                return IOWaiterSync((conn, None))
            return self._sock_accept_preread(conn, normalized_recv_size)

        if normalized_recv_size is None:
            return IOWaiter(
                self,
                self._proactor.accept(sock),
                map_result=lambda accepted: (accepted, None),
            )

        group = IOWaitGroup(self)

        def advance_accept(child: IOWaitGroupChildProtocol[socket.socket]) -> None:
            accepted = child.value()
            try:
                data = self._recv_if_ready(accepted, normalized_recv_size)
            except OSError as exc:
                abortive_close(accepted)
                group._complete_error(exc)
                return
            if data is not None:
                _finish_or_close_socket(group, accepted, (accepted, data))
                return

            def advance_recv(recv_child: IOWaitGroupChildProtocol[bytes]) -> None:
                data = recv_child.value()
                _finish_or_close_socket(group, accepted, (accepted, data))

            try:
                group.attach(
                    self._proactor.recv(accepted, normalized_recv_size),
                    on_cleanup=lambda fail, _value: abortive_close(accepted) if fail else None,
                    advance=advance_recv,
                )
            except BaseException:
                abortive_close(accepted)
                raise

        group.attach(
            self._proactor.accept(sock),
            advance=advance_accept,
        )
        return group

    def _sock_accept_preread(self, conn: socket.socket, recv_size: int) -> IOWaitable[AcceptDelivery]:
        """Compose accept-time ``recv`` for an already-accepted connection."""

        try:
            data = self._recv_if_ready(conn, recv_size)
        except OSError as exc:
            abortive_close(conn)
            return IOWaiterSync.failed(exc)
        if data is not None:
            return IOWaiterSync((conn, data))

        group = IOWaitGroup(self)

        def advance_recv(recv_child: IOWaitGroupChildProtocol[bytes]) -> None:
            data = recv_child.value()
            _finish_or_close_socket(group, conn, (conn, data))

        try:
            group.attach(
                self._proactor.recv(conn, recv_size),
                on_cleanup=lambda fail, _value: abortive_close(conn) if fail else None,
                advance=advance_recv,
            )
        except BaseException:
            abortive_close(conn)
            raise
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
        """Chain ``sock_sendall`` into ``group`` (eager try, then proactor remainder).

        Used after connect for ``initial`` / ``initial_data``. Sync success runs
        ``on_done`` immediately; sync failure completes the group with the error.
        """

        waiter = self.sock_sendall(sock, data)
        if isinstance(waiter, IOWaiterSync):
            exc = waiter.exception()
            if exc is not None:
                if on_cleanup is not None:
                    on_cleanup(True, None)
                group._complete_error(exc)
                return
            on_done()
            return
        # sock_sendall returns IOWaiterSync or IOWaiter only
        assert isinstance(waiter, IOWaiter)
        operation = waiter.operation
        assert operation is not None
        group.attach(
            operation,
            on_cleanup=on_cleanup,
            advance=lambda _child: on_done(),
        )

    def sock_connect(
        self,
        sock: socket.socket,
        address: Any,
        *,
        initial: SocketSendBuffer | None = None,
    ) -> IOWaitable[None]:
        if initial is None:
            return IOWaiter(self, self._proactor.connect(sock, address))

        payload = memoryview(initial)
        if not payload:
            return IOWaiter(self, self._proactor.connect(sock, address))

        group = IOWaitGroup(self)

        def advance_connect(_child: IOWaitGroupChildProtocol[None]) -> None:
            self._attach_sock_sendall(
                group,
                sock,
                payload,
                on_done=lambda: _finish_or_close_socket(group, sock, None),
            )

        group.attach(self._proactor.connect(sock, address), advance=advance_connect)
        return group

    def sock_create(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
        connect_to: Any | None = None,
        initial_data: SocketSendBuffer | None = None,
    ) -> IOWaitable[socket.socket]:
        """Create a socket via stdlib; optionally connect and send ``initial_data``.

        ``flags`` are socket type flags OR'd into ``type`` (e.g. ``SOCK_NONBLOCK`` /
        ``SOCK_CLOEXEC`` where valid). Not io_uring-only socket-create bits.
        """

        if initial_data is not None and connect_to is None:
            raise ValueError("initial_data requires connect_to")

        try:
            sock = _create_scheduler_socket(family, type, proto, flags=flags)
        except OSError as exc:
            return IOWaiterSync.failed(exc)

        if connect_to is None:
            return IOWaiterSync(sock)

        group = IOWaitGroup(self)
        payload = memoryview(initial_data) if initial_data is not None else None

        def close_on_fail(fail: bool, _value: Any) -> None:
            if fail:
                abortive_close(sock)

        def finish_connected(_connect_child: IOWaitGroupChildProtocol[None]) -> None:
            if payload is None or not payload:
                _finish_or_close_socket(group, sock, sock)
                return
            self._attach_sock_sendall(
                group,
                sock,
                payload,
                on_cleanup=close_on_fail,
                on_done=lambda: _finish_or_close_socket(group, sock, sock),
            )

        # sock is local until attach registers close_on_fail; close if submit fails first
        try:
            group.attach(
                self._proactor.connect(sock, connect_to),
                on_cleanup=close_on_fail,
                advance=finish_connected,
            )
        except BaseException:
            abortive_close(sock)
            raise
        return group

    def poll(self, fd: int, mask: int) -> IOWaiter[int]:
        return IOWaiter(self, self._proactor.poll(fd, mask))

    def read(self, fd: int, n: int, offset: int) -> IOWaiter[bytes]:
        return IOWaiter(self, self._proactor.read(fd, n, offset))

    def read_into(self, fd: int, buf: Any, offset: int) -> IOWaiter[int]:
        return IOWaiter(self, self._proactor.read_into(fd, buf, offset))

    def write(self, fd: int, data: Any, offset: int) -> IOWaiter[int]:
        return IOWaiter(self, self._proactor.write(fd, data, offset))

    def stat_fdsize(self, fd: int) -> IOWaiter[int]:
        return IOWaiter(self, self._proactor.stat_fdsize(fd))

    def close_fd(self, fd: int) -> IOWaiter[None]:
        return IOWaiter(self, self._proactor.close_fd(fd))

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Callable[[MultishotDelivery], object],
    ) -> IOWaitable[None]:
        def on_ordered_delivery(delivery: MultishotDelivery) -> None:
            try:
                callback(delivery)
            finally:
                finish_continuous_delivery(delivery)

        operation = self._proactor.poll_many(
            fd,
            mask,
            self._thread_reorder_helper(
                on_ordered_delivery,
                flush_heap_on_unsequenced_terminal=True,
            ),
        )
        return IOWaiter(self, operation)

    def _schedule_accept_recv_timeout(
        self,
        recv_op: Operation[bytes],
        timer_box: list[TimerHandle | None],
        *,
        timeout: float,
    ) -> None:
        """Arm a scheduler timer that cancels ``recv_op`` when it fires."""

        def arm() -> None:
            if recv_op.done():
                return

            def on_timeout() -> None:
                if not recv_op.done():
                    self._cancel_operation(recv_op).forget()

            assert self._scheduler is not None
            timer_box[0] = self._scheduler.call_later(timeout, on_timeout)

        self._marshal_on_scheduler(arm)

    def _cancel_accept_recv_timeout(self, timer_box: list[TimerHandle | None]) -> None:
        def cancel() -> None:
            handle = timer_box[0]
            if handle is not None:
                handle.cancel()
                timer_box[0] = None

        self._marshal_on_scheduler(cancel)

    def _accept_preread_on_worker(
        self,
        delivery: MultishotDelivery,
        on_thread_delivery: Callable[[MultishotDelivery], None],
        *,
        recv_size: int,
        recv_timeout: float | None = None,
    ) -> None:
        """Schedule accept-time ``recv`` on the worker thread and post the merged leg.

        Tries a direct non-blocking ``recv`` first (same policy as ``sock_recv``)
        so ready first-bytes skip a proactor submit; falls through when would-block.
        """

        conn = delivery.value
        assert isinstance(conn, socket.socket)
        try:
            data = self._recv_if_ready(conn, recv_size)
        except OSError as exc:
            on_thread_delivery(delivery._replace(value=(conn, None, exc)))
            return
        if data is not None:
            on_thread_delivery(delivery._replace(value=(conn, data, None)))
            return

        recv_op = self._proactor.recv(conn, recv_size)
        timer_box: list[TimerHandle | None] = [None]

        if recv_timeout is not None:
            self._schedule_accept_recv_timeout(
                recv_op,
                timer_box,
                timeout=recv_timeout,
            )

        def on_recv_complete(op: Operation[bytes]) -> None:
            self._cancel_accept_recv_timeout(timer_box)
            exc = op.exception()
            if exc is not None:
                on_thread_delivery(delivery._replace(value=(conn, None, exc)))
                return
            on_thread_delivery(delivery._replace(value=(conn, op.result(), None)))

        recv_op.add_done_callback(on_recv_complete)

    def accept_many(
        self,
        sock: socket.socket,
        callback: Callable[[AcceptDelivery], object],
        *,
        recv_size: int | None = None,
        recv_timeout: float | None = None,
        on_recv_error: AcceptRecvErrorCallback | None = None,
    ) -> IOWaitable[None]:
        """Accept connections: direct drain while ready, then continuous proactor.

        Ready connections are accepted with non-blocking ``accept()`` and posted
        through the same reorder/marshal path as continuous legs
        (``call_soon_threadsafe(..., immediate=True)``), so ``callback`` runs on
        the scheduler thread whether the accept was eager or proactor-backed.
        When the listen socket would block, ``proactor.accept_many`` is armed
        and its continuous waitable is returned.

        **Eager errors.** The drain loop does not classify ``OSError``: any
        mid-drain failure (soft or hard) stops eager only and still arms
        continuous accept. Error handling lives on the proactor path (and the
        host loop). Already-delivered connections stay valid.

        **Shutdown and late deliveries.** Cancelling this ``IOWaitable`` or the
        hosting accept-loop tealet does **not** cancel accept-time ``recv`` legs
        started when ``recv_size`` is set. Late accepts and preread completions
        can still reach ``callback`` after shutdown unless the application
        discards them — close listening sockets, check a shutdown flag in the
        accept callback, and ignore or close unwanted connections (``StreamServer``
        uses ``_closed`` for this). ``recv_timeout`` (requires ``recv_size``)
        bounds each accept-time preread cooperatively; it does not replace
        listener close or callback-side discard.

        Recv failures invoke ``on_recv_error(conn, exc)`` when provided; the
        socket is always closed afterwards. With no ``on_recv_error``, recv
        failures close the socket silently.

        When ``recv_timeout`` is set, each accept-time ``recv`` is cancelled if
        it has not completed by then. Timeout cancel is cooperative/best-effort
        like other cancel paths: the merged ``(conn, recv_error)`` leg is posted
        to the scheduler reorder buffer and disposition runs there via
        ``finalize_accept_recv_error`` (or the user accept callback is skipped).

        ``wait()`` on the returned ``IOWaitable`` ends the accept **stream leg**
        only. On non-multishot backends the stream finishes after each accept;
        accept-time ``recv`` and scheduler-marshalled deliveries may still be in
        flight. Re-arm in a loop when more accepts are needed.

        """

        normalized_recv_size = normalize_accept_recv_size(recv_size)
        if recv_timeout is not None:
            if normalized_recv_size is None:
                raise ValueError("recv_timeout requires recv_size")
            if recv_timeout <= 0:
                raise ValueError("recv_timeout must be positive when provided")

        def deliver_wrapped(result: AcceptReadResult) -> None:
            conn, initial_data, recv_error = result
            if recv_error is not None:
                finalize_accept_recv_error(conn, recv_error, on_recv_error)
                return
            try:
                callback((conn, initial_data))
            except BaseException:
                abortive_close(conn)
                raise

        def on_ordered_delivery(delivery: MultishotDelivery) -> None:
            if is_cancellation_delivery(delivery):
                finish_continuous_delivery(delivery)
                return
            if delivery.exception is not None:
                finish_continuous_delivery(delivery)
                raise delivery.exception
            if delivery.value is None:
                finish_continuous_delivery(delivery)
                return
            try:
                deliver_wrapped(delivery.value)
            finally:
                finish_continuous_delivery(delivery)

        on_thread_delivery = self._thread_reorder_helper(
            on_ordered_delivery,
            flush_heap_on_unsequenced_terminal=True,
        )

        def on_worker_delivery(delivery: MultishotDelivery) -> None:
            if is_cancellation_delivery(delivery):
                on_thread_delivery(delivery)
                return
            if delivery.exception is not None:
                on_thread_delivery(delivery)
                return
            if delivery.value is None:
                on_thread_delivery(delivery)
                return
            if normalized_recv_size is not None:
                self._accept_preread_on_worker(
                    delivery,
                    on_thread_delivery,
                    recv_size=normalized_recv_size,
                    recv_timeout=recv_timeout,
                )
                return
            on_thread_delivery(delivery._replace(value=(delivery.value, None, None)))

        # Happy path: drain ready accepts only. No error classification here —
        # any OSError stops eager and we arm proactor.accept_many (canonical path).
        # Indices continue into multishot via base_sequence.
        eager_count = 0
        try:
            while True:
                conn = _accept_ready_connection(sock)
                if conn is None:
                    break
                index = eager_count
                eager_count += 1
                if normalized_recv_size is not None:
                    # more=True so finish_continuous is a no-op for the stream leg
                    self._accept_preread_on_worker(
                        MultishotDelivery(index=index, value=conn, more=True),
                        on_thread_delivery,
                        recv_size=normalized_recv_size,
                        recv_timeout=recv_timeout,
                    )
                else:
                    on_thread_delivery(
                        MultishotDelivery(index=index, value=(conn, None, None), more=True),
                    )
        except OSError:
            pass

        operation = self._proactor.accept_many(sock, on_worker_delivery, base_sequence=eager_count)
        return IOWaiter(self, operation)

    def accept_many_streams(
        self,
        sock: socket.socket,
        callback: Callable[[AcceptStreamsDelivery], object],
        *,
        limit: int = 2**16,
        stream_factory: Any | None = None,
        async_: bool = False,
    ) -> IOWaitable[None]:
        """Accept stream pairs: direct drain while ready, then continuous proactor.

        Ready connections are accepted and opened as streams; user ``callback``
        runs on the scheduler via the same reorder/marshal path as continuous
        legs (``immediate=True``). When the listen socket would block,
        ``proactor.accept_many`` is armed; further accepts open streams on the
        delivery thread before marshalling the callback. Receive begins as soon
        as streams open; a silent peer leaves ``recv_many`` pending without
        withholding the pair from the handler. Idle or slow-client policy
        belongs in the handler (read timeouts, early close, etc.).

        Eager mid-drain ``OSError`` stops the try only and still arms continuous
        accept (same policy as ``accept_many()``).

        See ``accept_many()`` for ``wait()`` / accept-stream semantics and the
        shutdown discard responsibilities (close listeners; check a flag in the
        accept callback).
        """

        def open_and_deliver(conn: socket.socket) -> AcceptStreamsDelivery:
            try:
                return open_streams(
                    self,
                    conn,
                    limit=limit,
                    stream_factory=stream_factory,
                    async_=async_,
                )
            except BaseException:
                abortive_close(conn)
                raise

        def deliver_streams(streams: AcceptStreamsDelivery) -> None:
            reader, writer = streams
            try:
                callback((reader, writer))
            except BaseException:
                try:
                    writer.close()
                except BaseException:
                    abortive_close(writer.get_extra_info("socket"))
                raise

        def on_ordered_delivery(delivery: MultishotDelivery) -> None:
            if is_cancellation_delivery(delivery):
                finish_continuous_delivery(delivery)
                return
            if delivery.exception is not None:
                finish_continuous_delivery(delivery)
                raise delivery.exception
            if delivery.value is None:
                finish_continuous_delivery(delivery)
                return

            try:
                deliver_streams(delivery.value)
            finally:
                finish_continuous_delivery(delivery)

        on_thread_delivery = self._thread_reorder_helper(
            on_ordered_delivery,
            flush_heap_on_unsequenced_terminal=True,
        )

        def on_worker_delivery(delivery: MultishotDelivery) -> None:
            if is_cancellation_delivery(delivery):
                on_thread_delivery(delivery)
                return
            if delivery.exception is not None:
                on_thread_delivery(delivery)
                return
            conn = delivery.value
            if conn is None:
                on_thread_delivery(delivery)
                return

            try:
                streams = open_and_deliver(conn)
            except BaseException as exc:
                on_thread_delivery(delivery._replace(value=None, exception=exc))
                return

            on_thread_delivery(delivery._replace(value=streams))

        # Happy path drain only; OSError → arm continuous (same as accept_many).
        eager_count = 0
        try:
            while True:
                conn = _accept_ready_connection(sock)
                if conn is None:
                    break
                index = eager_count
                eager_count += 1
                try:
                    streams = open_and_deliver(conn)
                except BaseException as exc:
                    on_thread_delivery(
                        MultishotDelivery(index=index, value=None, exception=exc, more=True),
                    )
                else:
                    on_thread_delivery(
                        MultishotDelivery(index=index, value=streams, more=True),
                    )
        except OSError:
            pass

        operation = self._proactor.accept_many(sock, on_worker_delivery, base_sequence=eager_count)
        return IOWaiter(self, operation)

    def sock_create_streams(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        connect_to: Any,
        flags: int = 0,
        initial_data: SocketSendBuffer | None = None,
        limit: int = 2**16,
        stream_factory: Any | None = None,
        async_: bool = False,
    ) -> IOWaitable[AcceptStreamsDelivery]:
        """Create a socket, connect, and return stream endpoints.

        ``initial_data`` is sent on the wire after connect, before streams open.
        Socket creation is direct (stdlib); connect goes through the proactor;
        the optional initial send uses ``sock_sendall`` (eager try).
        ``flags`` are socket type flags only (same contract as ``sock_create``).
        """

        try:
            sock = _create_scheduler_socket(family, type, proto, flags=flags)
        except OSError as exc:
            return IOWaiterSync.failed(exc)

        group = IOWaitGroup(self)
        payload = memoryview(initial_data) if initial_data is not None else None

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

        def finish_connected(_connect_child: IOWaitGroupChildProtocol[None]) -> None:
            if payload is None or not payload:
                open_and_finish()
                return
            self._attach_sock_sendall(
                group,
                sock,
                payload,
                on_cleanup=close_on_fail,
                on_done=open_and_finish,
            )

        # sock is local until attach registers close_on_fail; close if submit fails first
        try:
            group.attach(
                self._proactor.connect(sock, connect_to),
                on_cleanup=close_on_fail,
                advance=finish_connected,
            )
        except BaseException:
            abortive_close(sock)
            raise
        return group

    def open(self, path: str, mode: str = "rb") -> IOWaiter[IOFile]:
        flags, file_mode = parse_open_mode(mode)
        try:
            operation = self._proactor.openat(path, flags, file_mode)
        except NotImplementedError as exc:
            raise NotImplementedError("file I/O requires a proactor with openat support") from exc

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

        return IOWaiter(self, operation, map_result=make_file)


from .streams.open import open_streams  # noqa: E402
