"""IOCP-shaped asyncio proactor backed by ``uring_api.Ring``.

CQE shaping and send-all / accept-fd wrapping follow the same contracts as
``tealetio.UringProactor``; this module does not import tealetio.
"""

from __future__ import annotations

import errno
import socket
import time
from asyncio import Future
from asyncio.events import AbstractEventLoop
from asyncio.exceptions import SendfileNotAvailableError
from collections.abc import Callable, Mapping
from typing import Any

import uring_api

DEFAULT_SQ_ENTRIES = 256
DEFAULT_CQ_ENTRIES = 1024

_ACCEPT_FLAGS = getattr(socket, "SOCK_NONBLOCK", 0) | getattr(socket, "SOCK_CLOEXEC", 0)
_RESET_ERRNOS = frozenset({errno.ECONNRESET, errno.EPIPE, errno.ENOTCONN})


class UringUnavailableError(RuntimeError):
    """Raised when an asyncio uring proactor cannot create an ``io_uring`` ring."""


class _UringFuture(Future[Any]):
    """Future whose ``cancel()`` posts ``ASYNC_CANCEL`` for the armed Completion."""

    __slots__ = ("_completion", "_proactor")

    def __init__(self, proactor: UringProactor, *, loop: AbstractEventLoop) -> None:
        super().__init__(loop=loop)
        self._proactor = proactor
        self._completion: uring_api.Completion | None = None

    def cancel(self, msg: Any = None) -> bool:
        self._cancel_uring()
        return super().cancel(msg)

    def _cancel_uring(self) -> None:
        completion = self._completion
        proactor = self._proactor
        self._completion = None
        if completion is not None:
            proactor._cancel_completion(completion)


class _Op:
    __slots__ = ("extra", "future", "shaper")

    def __init__(self, future: _UringFuture, shaper: Callable[..., Any], extra: Any) -> None:
        self.future = future
        self.shaper = shaper
        self.extra = extra


def _oserror(res: int) -> OSError:
    err = -res
    if err in _RESET_ERRNOS:
        return ConnectionResetError(err, errno.errorcode.get(err, "connection reset"))
    if err == errno.ECONNABORTED:
        return ConnectionAbortedError(err, errno.errorcode.get(err, "connection aborted"))
    if err == errno.ETIMEDOUT:
        return TimeoutError(err, errno.errorcode.get(err, "timed out"))
    return OSError(err, errno.errorcode.get(err, "io_uring operation failed"))


def _shape_res(completion: uring_api.Completion, _extra: Any) -> int:
    res = completion.res
    if res < 0:
        raise _oserror(res)
    return res


def _shape_bytes(completion: uring_api.Completion, buf: Any) -> bytes:
    res = completion.res
    if res < 0:
        raise _oserror(res)
    return bytes(buf[:res])


def _shape_send_all(completion: uring_api.Completion, _extra: Any) -> int:
    res = completion.res
    if res < 0:
        raise _oserror(res)
    total = completion.result
    if isinstance(total, int):
        return total
    return res


def _shape_void(completion: uring_api.Completion, _extra: Any) -> None:
    res = completion.res
    if res < 0:
        raise _oserror(res)


def _socket_from_uring_fd(fd: int) -> socket.socket:
    # SOCK_NONBLOCK|SOCK_CLOEXEC were requested on accept; setblocking(False)
    # syncs the wrapper without changing already-set fd flags.
    sock = socket.socket(fileno=fd)
    sock.setblocking(False)
    return sock


def _shape_accept(completion: uring_api.Completion, listener: socket.socket) -> tuple[socket.socket, Any]:
    res = completion.res
    if res < 0:
        raise _oserror(res)
    conn = _socket_from_uring_fd(res)
    try:
        peername = conn.getpeername()
    except OSError:
        peername = None
    return conn, peername


def _shape_recvfrom(completion: uring_api.Completion, buf: Any) -> tuple[bytes, Any]:
    res = completion.res
    if res < 0:
        raise _oserror(res)
    return bytes(buf[:res]), completion.result


def _shape_recvfrom_into(completion: uring_api.Completion, _extra: Any) -> tuple[int, Any]:
    res = completion.res
    if res < 0:
        raise _oserror(res)
    return res, completion.result


def _choose_ring_flags(requested: int | None) -> int:
    if requested is not None:
        return requested
    combo = uring_api.IORING_SETUP_SINGLE_ISSUER | uring_api.IORING_SETUP_DEFER_TASKRUN
    try:
        probe = uring_api.probe(entries=2, flags=combo)
    except (OSError, RuntimeError, NotImplementedError):
        probe = {}
    if probe.get("available"):
        return combo
    return 0


class UringProactor:
    """Asyncio proactor duck-typed after ``IocpProactor``, using ``uring_api``."""

    def __init__(
        self,
        entries: int = DEFAULT_SQ_ENTRIES,
        flags: int | None = None,
        *,
        cq_entries: int | None = None,
    ) -> None:
        if not uring_api.is_available():
            raise UringUnavailableError("io_uring is not available on this system")
        ring_flags = _choose_ring_flags(flags)
        if cq_entries is None:
            cq_entries = max(DEFAULT_CQ_ENTRIES, entries * 2)
        try:
            self._ring = uring_api.Ring(entries=entries, flags=ring_flags, cq_entries=cq_entries)
        except (OSError, RuntimeError, NotImplementedError) as exc:
            raise UringUnavailableError("failed to create an io_uring ring") from exc
        try:
            self._capabilities = uring_api.probe(entries=entries, flags=ring_flags)
        except (OSError, RuntimeError, NotImplementedError):
            self._capabilities = {}
        self._recv_flags = (
            uring_api.IORING_RECVSEND_POLL_FIRST if self._capabilities.get("IORING_RECVSEND_POLL_FIRST") else 0
        )
        self._loop: AbstractEventLoop | None = None
        self._closed = False
        self._ops: set[uring_api.Completion] = set()
        self._stopped_serving: set[int] = set()
        self._ring.callback = self._deliver
        self._ring.exception_handler = self._on_ring_exception

    def set_loop(self, loop: AbstractEventLoop) -> None:
        """Bind the asyncio loop that owns Futures created by this proactor."""

        self._loop = loop

    def get_map(self) -> Mapping[Any, Any]:
        """Return an empty selector map for BaseEventLoop compatibility."""

        return {}

    def select(self, timeout: float | None = None) -> list[object]:
        """Wait for ring completions; Futures are completed from ``Ring.callback``."""

        if self._closed:
            return []
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative or None")
        self._ring.wait(timeout)
        return []

    def recv(self, conn: socket.socket, nbytes: int, flags: int = 0) -> Future[bytes]:
        """Receive up to ``nbytes`` bytes. Completes with ``bytes`` (empty at EOF)."""

        if flags:
            return self._failed(ValueError("recv flags are not supported"))
        if nbytes == 0:
            return self._completed(b"")
        buf = bytearray(nbytes)
        return self._submit(
            self._ring.prepare_recv, conn.fileno(), buf, self._recv_flags, shaper=_shape_bytes, extra=buf
        )

    def recv_into(self, conn: socket.socket, buf: Any, flags: int = 0) -> Future[int]:
        """Receive into ``buf``. Completes with the number of bytes written."""

        if flags:
            return self._failed(ValueError("recv_into flags are not supported"))
        return self._submit(self._ring.prepare_recv, conn.fileno(), buf, self._recv_flags, shaper=_shape_res)

    def recvfrom(self, conn: socket.socket, nbytes: int, flags: int = 0) -> Future[tuple[bytes, Any]]:
        """Receive a datagram. Completes with ``(data, address)``."""

        if flags:
            return self._failed(ValueError("recvfrom flags are not supported"))
        buf = bytearray(nbytes)
        return self._submit(
            self._ring.prepare_recvmsg,
            conn.fileno(),
            buf,
            self._recv_flags,
            shaper=_shape_recvfrom,
            extra=buf,
        )

    def recvfrom_into(self, conn: socket.socket, buf: Any, flags: int = 0) -> Future[tuple[int, Any]]:
        """Receive a datagram into ``buf``. Completes with ``(nbytes, address)``."""

        if flags:
            return self._failed(ValueError("recvfrom_into flags are not supported"))
        return self._submit(
            self._ring.prepare_recvmsg,
            conn.fileno(),
            buf,
            self._recv_flags,
            shaper=_shape_recvfrom_into,
        )

    def send(self, conn: socket.socket, buf: Any, flags: int = 0) -> Future[int]:
        """Send the whole buffer (``send_all``). Completes with the byte count."""

        if flags:
            return self._failed(ValueError("send flags are not supported"))
        if not buf:
            return self._completed(0)
        return self._submit(
            self._ring.prepare_send_all,
            conn.fileno(),
            buf,
            0,
            shaper=_shape_send_all,
        )

    def sendto(self, conn: socket.socket, buf: Any, flags: int = 0, addr: Any = None) -> Future[int]:
        """Send a datagram. Completes with the number of bytes sent."""

        if flags:
            return self._failed(ValueError("sendto flags are not supported"))
        return self._submit(
            self._ring.prepare_sendto,
            conn.fileno(),
            buf,
            addr,
            self._recv_flags,
            shaper=_shape_res,
        )

    def accept(self, listener: socket.socket) -> Future[tuple[socket.socket, Any]]:
        """Accept one connection. Completes with ``(conn, peername)``."""

        fd = listener.fileno()
        if fd in self._stopped_serving:
            return self._failed(OSError(errno.EBADF, "listener is no longer serving"))
        return self._submit(
            self._ring.prepare_accept,
            fd,
            _ACCEPT_FLAGS,
            shaper=_shape_accept,
            extra=listener,
        )

    def connect(self, conn: socket.socket, address: Any) -> Future[None]:
        """Connect ``conn`` to ``address``."""

        return self._submit(self._ring.prepare_connect, conn.fileno(), address, shaper=_shape_void)

    def sendfile(self, sock: socket.socket, file: Any, offset: int, count: int) -> Future[int]:
        """Report that native sendfile is not available (asyncio copy fallback)."""

        del sock, file, offset, count
        return self._failed(SendfileNotAvailableError("UringProactor does not support sendfile"))

    def _stop_serving(self, sock: socket.socket) -> None:
        # the loop cancels the accept Future (which posts ASYNC_CANCEL);
        # remember the fd so a late CQE cannot be treated as a live accept.
        self._stopped_serving.add(sock.fileno())

    def close(self) -> None:
        """Cancel in-flight ops, reap their CQEs, then close the ring."""

        if self._closed:
            return
        self._closed = True
        for completion in list(self._ops):
            self._cancel_completion(completion)
        deadline = time.monotonic() + 1.0
        try:
            while self._ops and time.monotonic() < deadline:
                remaining = max(0.0, deadline - time.monotonic())
                self._ring.wait(remaining)
        except (OSError, RuntimeError, ValueError):
            pass
        self._ring.callback = None
        self._ring.exception_handler = None
        self._ring.close()
        self._loop = None
        self._ops.clear()

    def _submit(
        self,
        prepare: Callable[..., uring_api.Completion],
        *args: Any,
        shaper: Callable[..., Any],
        extra: Any = None,
    ) -> Future[Any]:
        loop = self._require_loop()
        self._check_open()
        fut = _UringFuture(self, loop=loop)
        op = _Op(fut, shaper, extra)
        try:
            completion = prepare(*args, op)
        except BaseException as exc:
            if not fut.done():
                fut.set_exception(exc)
            return fut
        fut._completion = completion
        self._ops.add(completion)
        return fut

    def _deliver(self, completion: uring_api.Completion) -> None:
        op = completion.take_user_data()
        self._ops.discard(completion)
        if not isinstance(op, _Op):
            return
        fut = op.future
        fut._completion = None
        if fut.done():
            return
        res = completion.res
        if res == -errno.ECANCELED:
            fut.cancel()
            return
        try:
            result = op.shaper(completion, op.extra)
        except BaseException as exc:
            if not fut.done():
                fut.set_exception(exc)
            return
        if not fut.done():
            if isinstance(result, tuple) and result and isinstance(result[0], socket.socket):
                listener = op.extra
                if isinstance(listener, socket.socket):
                    try:
                        fd = listener.fileno()
                    except OSError:
                        fd = -1
                    if fd in self._stopped_serving:
                        result[0].close()
                        fut.cancel()
                        return
            fut.set_result(result)

    def _cancel_completion(self, completion: uring_api.Completion) -> None:
        try:
            if self._ring.closed:
                return
        except (AttributeError, RuntimeError):
            return
        try:
            self._ring.prepare_cancel_nowait(completion)
        except (OSError, RuntimeError, ValueError):
            pass

    def _on_ring_exception(self, context: dict[str, Any]) -> None:
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        loop.call_exception_handler(context)

    def _completed(self, value: Any) -> Future[Any]:
        fut = self._require_loop().create_future()
        fut.set_result(value)
        return fut

    def _failed(self, exc: BaseException) -> Future[Any]:
        fut = self._require_loop().create_future()
        fut.set_exception(exc)
        return fut

    def _require_loop(self) -> AbstractEventLoop:
        loop = self._loop
        if loop is None:
            raise RuntimeError("UringProactor is not bound to an asyncio loop")
        return loop

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("proactor is closed")
