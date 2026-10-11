"""Stdlib-shaped sockets that park the current tealet instead of the OS thread.

``TealetSocket`` wraps a real socket. Parking methods wait on the running
scheduler's ``io`` helpers (``sock_recv``, ``sock_sendall``, and the rest).
The OS fd stays non-blocking for the proactor. ``gettimeout`` /
``setblocking`` describe the wrapper, not that fd.

``import tealetio`` does not install this class on the ``socket`` module.
``tealetio.ext.socket.patch`` does that.
"""

from __future__ import annotations

import asyncio
import errno
import io
import itertools
import socket
from typing import Any, NoReturn

from .io_manager import ProactorIOManager, SupportsProactorIO
from .io_waiter import IOWaitable
from .locks import Event, TimeoutError, timeout
from .scheduler import get_running_scheduler
from .socket_helpers import configure_scheduler_socket, real_socket_module
from .taskgroups import ExceptionGroup, TaskGroup

# Same stagger as ``streams.connect``. Duplicated so this module does not
# import the stream stack.
HAPPY_EYEBALLS_DELAY = 0.25

_AddrInfo = tuple[int, int, int, str, tuple[Any, ...]]
_GLOBAL_DEFAULT_TIMEOUT = getattr(socket, "_GLOBAL_DEFAULT_TIMEOUT", object())
_BLOCKING_ERRNOS = {errno.EAGAIN, errno.EWOULDBLOCK, errno.EDEADLK}

__all__ = [
    "HAPPY_EYEBALLS_DELAY",
    "TealetSocket",
    "create_connection",
    "create_server",
    "fromfd",
    "getaddrinfo",
    "gethostbyaddr",
    "gethostbyname",
    "getnameinfo",
    "socketpair",
]


def _running_io() -> ProactorIOManager:
    from .proactor import ProactorScheduler

    scheduler = get_running_scheduler()
    if isinstance(scheduler, ProactorScheduler):
        narrowed: SupportsProactorIO = scheduler
        return narrowed.io
    raise RuntimeError("socket operations require a proactor scheduler")


def _is_default_timeout(value: object) -> bool:
    if value is _GLOBAL_DEFAULT_TIMEOUT:
        return True
    return value is getattr(socket, "_GLOBAL_DEFAULT_TIMEOUT", None)


class _SocketRaw(io.RawIOBase):
    """Raw stream over a ``TealetSocket``. Holds one makefile reference."""

    def __init__(self, sock: TealetSocket, mode: str) -> None:
        super().__init__()
        self._sock: TealetSocket | None = sock
        if "b" not in mode:
            mode += "b"
        self._mode = mode
        self._reading = "r" in mode
        self._writing = "w" in mode
        self._timeout_occurred = False
        sock._io_refs += 1

    def readable(self) -> bool:
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        return self._reading

    def writable(self) -> bool:
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        return self._writing

    def seekable(self) -> bool:
        return False

    def fileno(self) -> int:
        self._checkClosed()
        assert self._sock is not None
        return self._sock.fileno()

    def readinto(self, b: Any) -> int | None:
        self._checkClosed()
        if not self.readable():
            raise io.UnsupportedOperation("File or stream is not readable")
        if self._timeout_occurred:
            raise OSError("cannot read from timed out object")
        assert self._sock is not None
        try:
            return self._sock.recv_into(b)
        except TimeoutError:
            self._timeout_occurred = True
            raise
        except OSError as exc:
            if exc.errno in _BLOCKING_ERRNOS:
                return None
            raise

    def write(self, b: Any) -> int | None:
        self._checkClosed()
        if not self.writable():
            raise io.UnsupportedOperation("File or stream is not writable")
        assert self._sock is not None
        try:
            return self._sock.send(b)
        except OSError as exc:
            if exc.errno in _BLOCKING_ERRNOS:
                return None
            raise

    def close(self) -> None:
        if self.closed:
            return
        super().close()
        sock = self._sock
        self._sock = None
        if sock is not None:
            sock._decref_socketios()


class TealetSocket:
    """Socket whose blocking methods park the current tealet.

    Construct it with the same arguments as ``socket.socket``. The wrapped
    fd is non-blocking and close-on-exec. The wrapper starts in the default
    timeout mode (``socket.getdefaulttimeout()``), which is blocking when
    that default is ``None``.
    """

    def __init__(self, family: int = -1, type: int = -1, proto: int = -1, fileno: int | None = None) -> None:
        real = real_socket_module()
        if fileno is None:
            if family == -1:
                family = socket.AF_INET
            if type == -1:
                type = socket.SOCK_STREAM
            if proto == -1:
                proto = 0
            sock = real.socket(family, type, proto)
        else:
            sock = real.socket(family, type, proto, fileno)
        self._init_from_sock(sock)

    @classmethod
    def from_real(cls, sock: socket.socket) -> TealetSocket:
        """Wrap an existing real socket. A ``TealetSocket`` is returned as-is."""

        if isinstance(sock, cls):
            return sock
        wrapped = object.__new__(cls)
        wrapped._init_from_sock(sock)
        return wrapped

    def _init_from_sock(self, sock: socket.socket) -> None:
        configure_scheduler_socket(sock)
        self._sock = sock
        self._closed = False
        self._io_refs = 0
        default = real_socket_module().getdefaulttimeout()
        self._timeout: float | None = None if default is None else float(default)

    def __repr__(self) -> str:
        closed = " [closed]" if self._closed else ""
        try:
            fd = self.fileno()
        except OSError:
            fd = -1
        return f"<tealetio.TealetSocket{closed} fd={fd} family={self.family} type={self.type} proto={self.proto}>"

    def __enter__(self) -> TealetSocket:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def io(self) -> ProactorIOManager:
        """The running scheduler's IO manager. Resolved on each use."""

        return _running_io()

    @property
    def family(self) -> int:
        return self._sock.family

    @property
    def type(self) -> int:
        return self._sock.type

    @property
    def proto(self) -> int:
        return self._sock.proto

    def fileno(self) -> int:
        return self._sock.fileno()

    def gettimeout(self) -> float | None:
        return self._timeout

    def settimeout(self, value: float | None) -> None:
        if value is None:
            self._timeout = None
            return
        value = float(value)
        if value < 0:
            raise ValueError("timeout must be non-negative")
        self._timeout = value

    def getblocking(self) -> bool:
        return self._timeout != 0

    def setblocking(self, flag: bool) -> None:
        self.settimeout(None if flag else 0)

    def bind(self, address: Any) -> None:
        self._sock.bind(address)

    def listen(self, backlog: int | None = None) -> None:
        if backlog is None:
            self._sock.listen()
            return
        self._sock.listen(backlog)

    def getsockname(self) -> Any:
        return self._sock.getsockname()

    def getpeername(self) -> Any:
        return self._sock.getpeername()

    def getsockopt(self, level: int, optname: int, buflen: int | None = None) -> Any:
        if buflen is None:
            return self._sock.getsockopt(level, optname)
        return self._sock.getsockopt(level, optname, buflen)

    def setsockopt(self, level: int, optname: int, value: Any, optlen: int | None = None) -> None:
        if optlen is None:
            self._sock.setsockopt(level, optname, value)
            return
        self._sock.setsockopt(level, optname, value, optlen)

    def detach(self) -> int:
        self._closed = True
        return self._sock.detach()

    def dup(self) -> TealetSocket:
        copied = TealetSocket.from_real(self._sock.dup())
        copied.settimeout(self._timeout)
        return copied

    def close(self) -> None:
        """Release the socket. Idempotent. Does not wait for a completion.

        A makefile that is still open keeps the fd alive until that file
        closes, matching the stdlib ``_io_refs`` contract. Off a running
        scheduler (GC, ``__del__``) this falls back to the real ``close``.
        """

        self._closed = True
        if self._io_refs > 0:
            return
        self._real_close()

    def _decref_socketios(self) -> None:
        if self._io_refs > 0:
            self._io_refs -= 1
        if self._closed:
            self._real_close()

    def _real_close(self) -> None:
        sock = self._sock
        if sock.fileno() == -1:
            return
        try:
            io_manager = _running_io()
        except RuntimeError:
            sock.close()
            return
        io_manager.sock_close(sock)

    def makefile(
        self,
        mode: str = "r",
        buffering: int | None = None,
        *,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> Any:
        """Return a buffered stream (or a text wrapper) over this socket."""

        if not set(mode) <= {"r", "w", "b"}:
            raise ValueError(f"invalid mode {mode!r} (only r, w, b allowed)")
        writing = "w" in mode
        reading = "r" in mode or not writing
        binary = "b" in mode
        rawmode = ""
        if reading:
            rawmode += "r"
        if writing:
            rawmode += "w"
        raw = _SocketRaw(self, rawmode)
        if buffering is None:
            buffering = -1
        if buffering < 0:
            buffering = io.DEFAULT_BUFFER_SIZE
        if buffering == 0:
            if not binary:
                raw.close()
                raise ValueError("unbuffered streams must be binary")
            return raw
        if reading and writing:
            buffer: Any = io.BufferedRWPair(raw, raw, buffering)
        elif reading:
            buffer = io.BufferedReader(raw, buffering)
        else:
            buffer = io.BufferedWriter(raw, buffering)
        if binary:
            return buffer
        # text_encoding exists from 3.10; the stub ty uses does not list it.
        chosen = io.text_encoding(encoding)  # ty: ignore[unresolved-attribute]
        text = io.TextIOWrapper(buffer, chosen, errors, newline)
        text.mode = mode  # ty: ignore[invalid-assignment]
        return text

    def recv(self, bufsize: int, flags: int = 0) -> bytes:
        self._check_flags(flags)
        if self._timeout == 0:
            return self._sock.recv(bufsize)
        return self._wait(self.io.sock_recv(self._sock, bufsize))

    def recv_into(self, buffer: Any, nbytes: int = 0, flags: int = 0) -> int:
        self._check_flags(flags)
        if self._timeout == 0:
            return self._sock.recv_into(buffer, nbytes)
        view = memoryview(buffer)
        try:
            target = view[:nbytes] if nbytes else view
            try:
                return self._wait(self.io.sock_recv_into(self._sock, target))
            finally:
                if target is not view:
                    target.release()
        finally:
            view.release()

    def recvfrom(self, bufsize: int, flags: int = 0) -> tuple[bytes, Any]:
        self._check_flags(flags)
        if self._timeout == 0:
            return self._sock.recvfrom(bufsize)
        return self._wait(self.io.sock_recvfrom(self._sock, bufsize))

    def recvfrom_into(self, buffer: Any, nbytes: int = 0, flags: int = 0) -> tuple[int, Any]:
        self._check_flags(flags)
        if self._timeout == 0:
            return self._sock.recvfrom_into(buffer, nbytes)
        return self._wait(self.io.sock_recvfrom_into(self._sock, buffer, nbytes))

    def send(self, data: bytes | bytearray | memoryview, flags: int = 0) -> int:
        """Send all of ``data`` and return its length.

        The IO manager has no short ``send``. A caller that loops until the
        count is exhausted finishes in one pass.
        """

        self._check_flags(flags)
        view = memoryview(data)
        try:
            nbytes = view.nbytes
        finally:
            view.release()
        if self._timeout == 0:
            return self._sock.send(data)
        self._wait(self.io.sock_sendall(self._sock, data))
        return nbytes

    def sendall(self, data: bytes | bytearray | memoryview, flags: int = 0) -> None:
        self._check_flags(flags)
        if self._timeout == 0:
            self._sock.sendall(data)
            return
        self._wait(self.io.sock_sendall(self._sock, data))

    def sendto(self, data: bytes | bytearray | memoryview, *args: Any) -> int:
        if len(args) == 1:
            flags = 0
            address = args[0]
        elif len(args) == 2:
            flags, address = args
        else:
            raise TypeError("sendto() takes 2 or 3 arguments")
        self._check_flags(int(flags))
        if self._timeout == 0:
            return self._sock.sendto(data, address)
        return self._wait(self.io.sock_sendto(self._sock, data, address))

    def accept(self) -> tuple[TealetSocket, Any]:
        if self._timeout == 0:
            conn, addr = self._sock.accept()
            return TealetSocket.from_real(conn), addr
        accepted, _preread = self._wait(self.io.sock_accept(self._sock))
        try:
            address = accepted.getpeername()
        except OSError:
            address = ""
        return TealetSocket.from_real(accepted), address

    def connect(self, address: Any) -> None:
        """Connect to one sockaddr. Happy eyeballs lives on ``create_connection``."""

        if self._timeout == 0:
            self._sock.connect(address)
            return
        self._wait(self.io.sock_connect(self._sock, address))

    def connect_ex(self, address: Any) -> int:
        try:
            self.connect(address)
        except OSError as exc:
            if isinstance(exc, TimeoutError) or exc.errno is None:
                raise
            return exc.errno
        return 0

    def shutdown(self, how: int) -> None:
        self._wait(self.io.sock_shutdown_wait(self._sock, how))

    def _wait(self, waiter: IOWaitable[Any]) -> Any:
        if self._timeout is None:
            return waiter.wait()
        assert self._timeout != 0
        try:
            with timeout(self._timeout):
                return waiter.wait()
        except asyncio.TimeoutError as exc:
            raise TimeoutError("timed out") from exc

    @staticmethod
    def _check_flags(flags: int) -> None:
        if flags:
            raise ValueError("non-zero socket flags are not supported")


def socketpair(
    family: int | None = None,
    type: int = socket.SOCK_STREAM,
    proto: int = 0,
) -> tuple[TealetSocket, TealetSocket]:
    """Return a connected pair. ``family=None`` lets the platform pick."""

    real = real_socket_module()
    if family is None:
        left, right = real.socketpair()
    else:
        left, right = real.socketpair(family, type, proto)
    return TealetSocket.from_real(left), TealetSocket.from_real(right)


def fromfd(fd: int, family: int, type: int, proto: int = 0) -> TealetSocket:
    """Duplicate ``fd`` and wrap it. Matches ``socket.fromfd``."""

    return TealetSocket.from_real(real_socket_module().fromfd(fd, family, type, proto))


def getaddrinfo(
    host: str | bytes | None,
    port: str | int | bytes | None,
    family: int = 0,
    type: int = 0,
    proto: int = 0,
    flags: int = 0,
) -> list[_AddrInfo]:
    """Resolve on the scheduler executor and park the current tealet."""

    return get_running_scheduler().getaddrinfo(
        host,
        port,
        family=family,
        type=type,
        proto=proto,
        flags=flags,
    )


def getnameinfo(sockaddr: tuple[Any, ...], flags: int = 0) -> tuple[str, str]:
    """Reverse-resolve on the scheduler executor and park the current tealet."""

    return get_running_scheduler().getnameinfo(sockaddr, flags)


def gethostbyname(name: str) -> str:
    """Return an IPv4 address string. The lookup runs on the executor."""

    real = real_socket_module()
    return get_running_scheduler().run_in_executor(None, real.gethostbyname, name).wait()


def gethostbyaddr(host: str) -> tuple[str, list[str], list[str]]:
    """Reverse-lookup ``host`` on the executor and park the current tealet."""

    real = real_socket_module()
    return get_running_scheduler().run_in_executor(None, real.gethostbyaddr, host).wait()


def _interleave_addrinfos(addrinfos: list[_AddrInfo]) -> list[_AddrInfo]:
    """Alternate address families, one result from each family at a time."""

    by_family: dict[int, list[_AddrInfo]] = {}
    for info in addrinfos:
        by_family.setdefault(info[0], []).append(info)
    columns = list(by_family.values())
    return [item for item in itertools.chain.from_iterable(itertools.zip_longest(*columns)) if item is not None]


def _connect_one(info: _AddrInfo, timeout_value: float | None, source_address: Any) -> TealetSocket:
    family, socktype, proto, _canon, sockaddr = info
    sock = TealetSocket(family, socktype, proto)
    try:
        if timeout_value is not None:
            sock.settimeout(timeout_value)
        if source_address:
            sock.bind(source_address)
        sock.connect(sockaddr)
    except BaseException:
        sock.close()
        raise
    return sock


def _connect_sequential(
    infos: list[_AddrInfo],
    timeout_value: float | None,
    source_address: Any,
) -> tuple[TealetSocket | None, list[OSError]]:
    errors: list[OSError] = []
    for info in infos:
        try:
            return _connect_one(info, timeout_value, source_address), errors
        except OSError as exc:
            errors.append(exc)
    return None, errors


def _connect_happy_eyeballs(
    infos: list[_AddrInfo],
    timeout_value: float | None,
    source_address: Any,
    delay: float,
) -> tuple[TealetSocket | None, list[OSError]]:
    winner: TealetSocket | None = None
    errors: list[OSError] = []
    with TaskGroup() as group:
        for index, info in enumerate(infos):
            if winner is not None:
                break
            finished = Event()

            def attempt(info: _AddrInfo = info, finished: Event = finished) -> None:
                nonlocal winner
                sock: TealetSocket | None = None
                try:
                    sock = _connect_one(info, timeout_value, source_address)
                except OSError as exc:
                    errors.append(exc)
                    sock = None
                else:
                    if winner is None:
                        winner = sock
                        sock = None
                        group.cancel()
                finally:
                    if sock is not None:
                        sock.close()
                    finished.set()

            group.spawn(attempt)
            if winner is not None or index + 1 >= len(infos):
                break
            try:
                with timeout(delay):
                    finished.swait()
            except TimeoutError:
                pass
        if winner is not None:
            group.cancel()
    return winner, errors


def _raise_connect_errors(errors: list[OSError], *, all_errors: bool) -> NoReturn:
    if not errors:
        raise OSError("getaddrinfo returns an empty list")
    if all_errors:
        raise ExceptionGroup("create_connection failed", errors)
    raise errors[-1]


def create_connection(
    address: tuple[Any, Any],
    timeout: object = _GLOBAL_DEFAULT_TIMEOUT,
    source_address: Any = None,
    *,
    all_errors: bool = False,
    happy_eyeballs_delay: float | None = HAPPY_EYEBALLS_DELAY,
) -> TealetSocket:
    """Connect to ``(host, port)`` and return a ``TealetSocket``.

    Addresses are tried with a happy-eyeballs stagger (default 0.25s).
    ``happy_eyeballs_delay=None`` tries them one at a time in
    ``getaddrinfo`` order. ``timeout`` is the per-attempt socket timeout,
    separate from the stagger. The default timeout sentinel uses
    ``socket.getdefaulttimeout()``.
    """

    if happy_eyeballs_delay is not None and happy_eyeballs_delay < 0:
        raise ValueError("happy_eyeballs_delay must be None or >= 0")
    host, port = address
    scheduler = get_running_scheduler()
    infos = scheduler.ensure_resolved((host, port), family=0, type=socket.SOCK_STREAM, proto=0)
    if not infos:
        raise OSError("getaddrinfo returns an empty list")
    timeout_value: float | None
    if _is_default_timeout(timeout):
        default = real_socket_module().getdefaulttimeout()
        timeout_value = None if default is None else float(default)
    elif timeout is None:
        timeout_value = None
    else:
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            raise TypeError("timeout must be a number or None")
        timeout_value = float(timeout)
        if timeout_value < 0:
            raise ValueError("timeout must be non-negative")
    if happy_eyeballs_delay is not None and len(infos) > 1:
        infos = _interleave_addrinfos(infos)
        winner, errors = _connect_happy_eyeballs(infos, timeout_value, source_address, happy_eyeballs_delay)
    else:
        winner, errors = _connect_sequential(infos, timeout_value, source_address)
    if winner is not None:
        return winner
    _raise_connect_errors(errors, all_errors=all_errors)


def create_server(
    address: Any,
    *,
    family: int = socket.AF_INET,
    backlog: int | None = None,
    reuse_port: bool = False,
    dualstack_ipv6: bool = False,
) -> TealetSocket:
    """Bind and listen. ``dualstack_ipv6=True`` is not supported."""

    if dualstack_ipv6:
        raise ValueError("dualstack_ipv6 is not supported")
    if reuse_port and not hasattr(socket, "SO_REUSEPORT"):
        raise ValueError("SO_REUSEPORT not supported on this platform")
    sock = TealetSocket(family, socket.SOCK_STREAM)
    try:
        if hasattr(socket, "SO_REUSEADDR"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            except OSError:
                pass
        if reuse_port:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(address)
        sock.listen(backlog)
    except BaseException:
        sock.close()
        raise
    return sock
