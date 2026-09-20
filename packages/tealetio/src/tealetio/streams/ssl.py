"""MemoryBIO TLS wrap for native tealetio streams.

Userspace ``ssl.SSLObject`` sits above a ``ReadStream`` / ``WriteStream`` pair.
The proactor sees ciphertext on the fd. ``open_connection`` / ``start_server``
take asyncio-shaped ``ssl=`` and call ``WriteStream.handshake()`` on a
scheduler tealet.
"""

from __future__ import annotations

import asyncio
import os
import ssl
from collections.abc import Callable, Iterable
from typing import Any, TypeVar

from ..locks import Condition
from ..locks import timeout as timeout_cm
from .open import NativeStreamPair, StreamFactory, StreamOpenIO, default_stream_factory
from .reader import ReadStream
from .util import DEFAULT_LIMIT
from .writer import WriteStream, shutdown_stream_writer

__all__ = [
    "SSL_HANDSHAKE_TIMEOUT",
    "SSLStream",
    "ssl_server_context",
    "ssl_stream_factory",
    "start_tls",
    "wrap_ssl",
]

# asyncio.constants.SSL_HANDSHAKE_TIMEOUT (Nginx-matched default)
SSL_HANDSHAKE_TIMEOUT = 60.0

# one TLS record is 16KiB plus a small header; never use reader.read(-1) here —
# that waits for TCP EOF, but OpenSSL only needs the next ciphertext chunk.
_TLS_IO_SIZE = 16384
_COMPACT_PREFIX = 4096

_WantRead = (ssl.SSLWantReadError, ssl.SSLSyscallError)

T = TypeVar("T")


def wrap_ssl(
    reader: ReadStream,
    writer: WriteStream,
    sslcontext: ssl.SSLContext,
    *,
    server_side: bool = False,
    server_hostname: str | None = None,
    limit: int = DEFAULT_LIMIT,
) -> tuple[SSLStream, SSLStream]:
    """Wrap an already-connected stream pair as TLS.

    Returns ``(stream, stream)``: one ``SSLStream`` that implements both
    ``ReadStream`` and ``WriteStream``. Handshake is not done yet.
    """

    stream = SSLStream(
        reader,
        writer,
        sslcontext,
        server_side=server_side,
        server_hostname=server_hostname,
        limit=limit,
    )
    return stream, stream


def start_tls(
    writer: WriteStream,
    sslcontext: ssl.SSLContext,
    *,
    server_side: bool = False,
    server_hostname: str | None = None,
    ssl_handshake_timeout: float | None = None,
    limit: int | None = None,
) -> tuple[SSLStream, SSLStream]:
    """Drain plaintext, wrap the writer's paired reader, and handshake.

    Returns ``(stream, stream)``. Must run on the owning scheduler tealet.
    ``limit`` defaults to the inner reader's ``limit``.
    """

    writer.drain()
    stream_reader, stream_writer = wrap_ssl(
        writer.reader,
        writer,
        sslcontext,
        server_side=server_side,
        server_hostname=server_hostname,
        limit=writer.reader.limit if limit is None else limit,
    )
    try:
        stream_writer.handshake(timeout=ssl_handshake_timeout)
    except BaseException:
        shutdown_stream_writer(stream_writer, best_effort=True)
        raise
    return stream_reader, stream_writer


def ssl_stream_factory(
    sslcontext: ssl.SSLContext,
    *,
    server_side: bool = False,
    server_hostname: str | None = None,
    inner: StreamFactory | None = None,
) -> StreamFactory:
    """Wrap an inner native ``StreamFactory`` with TLS.

    Construction only: this runs on the accept/connect completion worker
    (``accept_many_streams`` / ``sock_create_streams``), so it must not park.
    Handshake happens later on the owning scheduler tealet via
    ``WriteStream.handshake()`` (``open_connection`` after connect, the
    ``start_server`` handler tealet before the user callback).
    """

    inner_factory = default_stream_factory if inner is None else inner

    def factory(io: StreamOpenIO, sock: Any, *, limit: int = DEFAULT_LIMIT) -> NativeStreamPair:
        reader, writer = inner_factory(io, sock, limit=limit)
        return wrap_ssl(
            reader,
            writer,
            sslcontext,
            server_side=server_side,
            server_hostname=server_hostname,
            limit=limit,
        )

    return factory


def ssl_server_context(
    certfile: str | bytes | os.PathLike[str] | os.PathLike[bytes],
    keyfile: str | bytes | os.PathLike[str] | os.PathLike[bytes] | None = None,
    *,
    password: Callable[[], str | bytes | bytearray] | str | bytes | bytearray | None = None,
) -> ssl.SSLContext:
    """Build a server ``SSLContext`` with a certificate chain.

    Uses ``ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)`` (the stdlib
    server-side purpose) and ``load_cert_chain``. Pass the result as
    ``start_server(..., ssl=...)``. Client connections use ``ssl=True`` or an
    ``SSLContext`` from ``ssl.create_default_context()``.
    """

    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile, keyfile, password=password)
    return context


def _client_ssl_params(
    ssl_arg: ssl.SSLContext | bool | None,
    *,
    server_hostname: str | None,
    host: str | None,
) -> tuple[ssl.SSLContext | None, str | None]:
    """Resolve asyncio-shaped ``ssl=`` / ``server_hostname=`` for a client."""

    if server_hostname is not None and not ssl_arg:
        raise ValueError("server_hostname is only meaningful with ssl")
    if not ssl_arg:
        return None, None
    if isinstance(ssl_arg, bool):
        context = ssl.create_default_context()
    else:
        context = ssl_arg
    if server_hostname is None:
        if not host:
            raise ValueError("You must set server_hostname when using ssl without a host")
        server_hostname = host
    return context, server_hostname


def _server_ssl_context(ssl_arg: ssl.SSLContext | bool | None) -> ssl.SSLContext | None:
    if ssl_arg is None:
        return None
    if isinstance(ssl_arg, bool):
        raise TypeError("ssl argument must be an SSLContext or None")
    return ssl_arg


def _require_native_ssl(*, async_: bool) -> None:
    if async_:
        raise TypeError("ssl= is not supported with async_=True")


def handshake_timeout_delay(ssl_handshake_timeout: float | None) -> float:
    """Resolve asyncio-shaped ``ssl_handshake_timeout`` (``None`` → 60s)."""

    if ssl_handshake_timeout is None:
        return SSL_HANDSHAKE_TIMEOUT
    if ssl_handshake_timeout <= 0:
        raise ValueError(f"ssl_handshake_timeout should be a positive number, got {ssl_handshake_timeout}")
    return ssl_handshake_timeout


def check_ssl_handshake_timeout(
    ssl_arg: ssl.SSLContext | bool | None,
    ssl_handshake_timeout: float | None,
) -> None:
    if ssl_handshake_timeout is not None and not ssl_arg:
        raise ValueError("ssl_handshake_timeout is only meaningful with ssl")
    if ssl_arg:
        handshake_timeout_delay(ssl_handshake_timeout)


class SSLStream:
    """Blocking TLS record stream implementing ``ReadStream`` and ``WriteStream``.

    One ``SSLObject`` owns the bidirectional record layer. Application read and
    write may run on two different tealets (at most one of each). Inner
    ciphertext ``read`` / ``drain`` are muxed so a ``WantRead`` on write cannot
    steal the inner read from a ``WantRead`` on read.
    """

    def __init__(
        self,
        reader: ReadStream,
        writer: WriteStream,
        sslcontext: ssl.SSLContext,
        *,
        server_side: bool = False,
        server_hostname: str | None = None,
        limit: int = DEFAULT_LIMIT,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._sslcontext = sslcontext
        self._incoming = ssl.MemoryBIO()
        self._outgoing = ssl.MemoryBIO()
        self._sslobj = sslcontext.wrap_bio(
            self._incoming,
            self._outgoing,
            server_side=server_side,
            server_hostname=server_hostname,
        )
        self._limit = limit
        self._buffer = bytearray()
        self._buffer_pos = 0
        self._tls_eof = False
        self._handshake_done = False
        self._closing = False
        self._closed = False
        self._unwrapped = False
        self._read_cond = Condition()
        self._write_cond = Condition()
        self._inner_reading = False
        self._inner_writing = False

    @property
    def reader(self) -> ReadStream:
        # public pair reader is this object; self._reader is the inner ciphertext stream
        return self

    @property
    def limit(self) -> int:
        return self._limit

    def handshake(self, timeout: float | None = None) -> None:
        """Run the TLS handshake, parking on ciphertext I/O as OpenSSL requests it.

        Idempotent. Must run on a scheduler tealet, not a completion worker.
        ``timeout`` is seconds; ``None`` uses ``SSL_HANDSHAKE_TIMEOUT``
        (60s, same as asyncio).
        """

        if self._handshake_done:
            return
        with timeout_cm(handshake_timeout_delay(timeout)):
            self._retry(self._sslobj.do_handshake)
            self._flush_outgoing()
            self._handshake_done = True

    def start_tls(
        self,
        sslcontext: ssl.SSLContext,
        *,
        server_side: bool = False,
        server_hostname: str | None = None,
        ssl_handshake_timeout: float | None = None,
        limit: int | None = None,
    ) -> tuple[SSLStream, SSLStream]:
        """Drain, wrap this pair as TLS, and handshake. Returns ``(stream, stream)``."""

        return start_tls(
            self,
            sslcontext,
            server_side=server_side,
            server_hostname=server_hostname,
            ssl_handshake_timeout=ssl_handshake_timeout,
            limit=limit,
        )

    @property
    def at_eof(self) -> bool:
        return self._tls_eof and not self._buffer_available()

    def read(self, n: int = -1) -> bytes:
        """Read decrypted application data.

        ``n < 0`` reads until TLS EOF (close_notify or incoming EOF). ``n >= 0``
        returns at most ``n`` bytes and may return fewer.
        """

        if n == 0:
            return b""
        if n < 0:
            while not self._tls_eof:
                if not self._append_next_chunk():
                    break
            payload = bytes(self._buffer[self._buffer_pos :])
            self._buffer.clear()
            self._buffer_pos = 0
            return payload
        if self._buffer_available():
            return self._take_bytes(min(n, self._buffer_available()))
        if self._tls_eof:
            return b""
        self._append_next_chunk()
        return self._take_bytes(min(n, self._buffer_available()))

    def readinto(self, b: Any) -> int:
        view = memoryview(b).cast("B")
        if not view.nbytes:
            return 0
        if self.at_eof:
            return 0
        nbytes = view.nbytes
        if self._buffer_available() < nbytes and not self._tls_eof:
            self._fill_buffer(nbytes)
        total = 0
        while total < nbytes and self._buffer_available():
            total += self._take_into(view, total, nbytes - total)
        return total

    def readexactly(self, n: int) -> bytes:
        if n < 0:
            raise ValueError("readexactly size must not be negative")
        if n == 0:
            return b""
        if self._buffer_available() < n and not self._tls_eof:
            self._fill_buffer(n)
        if self._buffer_available() < n:
            partial = bytes(self._buffer[self._buffer_pos :])
            self._buffer.clear()
            self._buffer_pos = 0
            raise asyncio.IncompleteReadError(partial, n)
        return self._take_bytes(n)

    def readline(self) -> bytes:
        while True:
            newline = self._buffer.find(b"\n", self._buffer_pos)
            if newline >= 0:
                return self._take_bytes(newline - self._buffer_pos + 1)
            if self._tls_eof:
                return self._take_bytes(self._buffer_available())
            if self._buffer_available() >= self._limit:
                raise asyncio.LimitOverrunError(
                    "Separator is not found, and chunk exceed the limit",
                    self._buffer_available(),
                )
            if not self._append_next_chunk():
                return self._take_bytes(self._buffer_available())

    def write(self, data: bytes | bytearray | memoryview) -> None:
        """Encrypt ``data`` and flush ciphertext through the inner writer."""

        if self._closing or self._closed:
            raise RuntimeError("SSLStream is closed")
        if not data:
            return
        view = memoryview(data).cast("B")
        while view.nbytes:
            written = self._retry(self._sslobj.write, view)
            assert written > 0
            view = view[written:]
            self._flush_outgoing()

    def writelines(self, lines: Iterable[bytes | bytearray | memoryview]) -> None:
        for line in lines:
            self.write(line)

    def drain(self) -> None:
        """Flush any pending outgoing BIO bytes and the inner writer send buffer."""

        self._flush_outgoing()

    def flush(self) -> None:
        self._flush_outgoing()
        self._writer.flush()

    def set_write_buffer_limits(self, high: int | None = None, low: int | None = None) -> None:
        self._writer.set_write_buffer_limits(high, low)

    def unwrap(self) -> None:
        """Send close_notify and complete the TLS shutdown handshake.

        Leaves the inner plaintext transport open.
        """

        if self._unwrapped:
            return
        if not self._handshake_done:
            return
        try:
            self._retry(self._sslobj.unwrap)
        except ssl.SSLZeroReturnError:
            pass
        self._flush_outgoing()
        self._unwrapped = True

    def close(self) -> None:
        """Best-effort close_notify without waiting for the peer, then close the inner pair."""

        if self._closing or self._closed:
            return
        self._closing = True
        if not self._unwrapped:
            try:
                # one-shot unwrap: send close_notify if OpenSSL can, but do not park
                # on the peer's reply — that deadlocks a sequential close of both ends
                self._sslobj.unwrap()
            except (ssl.SSLWantReadError, ssl.SSLSyscallError, ssl.SSLWantWriteError, ssl.SSLError):
                pass
            try:
                self._flush_outgoing()
            except OSError:
                pass
        self._reader.close()
        self._writer.close()

    def wait_closed(self) -> None:
        if self._closed:
            return
        if not self._closing:
            self.close()
        try:
            self._writer.wait_closed()
        except OSError:
            # peer already closed the fd; close_notify send is best-effort
            pass
        self._closed = True

    def is_closing(self) -> bool:
        return self._closing or self._closed

    def can_write_eof(self) -> bool:
        """TLS has no TCP-style half-close."""

        return False

    def write_eof(self) -> None:
        """Reject half-close; TLS uses close_notify via ``unwrap`` / ``close``."""

        raise NotImplementedError("TLS has no half-close; use unwrap() or close()")

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        """Return TLS or inner-transport extra info (``sslcontext``, ``peercert``, ``cipher``, ``socket``)."""

        if name == "sslcontext":
            return self._sslcontext
        if name == "ssl_object":
            return self._sslobj
        if name in ("peercert", "cipher", "compression"):
            if not self._handshake_done:
                return default
            if name == "peercert":
                return self._sslobj.getpeercert()
            if name == "cipher":
                return self._sslobj.cipher()
            return self._sslobj.compression()
        return self._writer.get_extra_info(name, default)

    def _retry(self, fn: Callable[..., T], *args: Any) -> T:
        while True:
            try:
                return fn(*args)
            except _WantRead:
                self._flush_outgoing()
                if self._incoming.eof:
                    raise
                self._feed_incoming()
            except ssl.SSLWantWriteError:
                self._flush_outgoing()

    def _flush_outgoing(self) -> None:
        """Drain the outgoing BIO through the inner writer, at most one tealet at a time."""

        with self._write_cond:
            if self._inner_writing:
                self._write_cond.swait()
                return
            self._inner_writing = True
        try:
            data = self._outgoing.read()
            if data:
                self._writer.write(data)
            self._writer.drain()
        finally:
            with self._write_cond:
                self._inner_writing = False
                self._write_cond.notify_all()

    def _feed_incoming(self) -> None:
        """Pull ciphertext into the incoming BIO, at most one tealet at a time.

        If another tealet already owns the inner read, wait for that read to
        finish and return so the caller retries the SSL op. Do not issue a
        second ``reader.read()``.
        """

        with self._read_cond:
            if self._inner_reading:
                self._read_cond.swait()
                return
            self._inner_reading = True
        try:
            chunk = self._reader.read(_TLS_IO_SIZE)
            if not chunk:
                self._incoming.write_eof()
            else:
                self._incoming.write(chunk)
        finally:
            with self._read_cond:
                self._inner_reading = False
                self._read_cond.notify_all()

    def _read_some(self, n: int) -> bytes:
        try:
            data = self._retry(self._sslobj.read, n)
        except ssl.SSLZeroReturnError:
            self._tls_eof = True
            self._flush_outgoing()
            return b""
        # a read can emit handshake/KeyUpdate bytes without WantWrite
        self._flush_outgoing()
        if not data:
            self._tls_eof = True
            return b""
        return data

    def _buffer_available(self) -> int:
        return len(self._buffer) - self._buffer_pos

    def _compact_buffer(self) -> None:
        if self._buffer_pos:
            del self._buffer[: self._buffer_pos]
            self._buffer_pos = 0

    def _maybe_compact_buffer(self) -> None:
        if self._buffer_pos >= _COMPACT_PREFIX and self._buffer_pos >= len(self._buffer) // 2:
            self._compact_buffer()

    def _append_next_chunk(self) -> bool:
        chunk = self._read_some(_TLS_IO_SIZE)
        if not chunk:
            return False
        self._buffer.extend(chunk)
        return True

    def _fill_buffer(self, min_bytes: int) -> None:
        while self._buffer_available() < min_bytes and not self._tls_eof:
            if not self._append_next_chunk():
                return

    def _take_bytes(self, n: int) -> bytes:
        available = self._buffer_available()
        count = min(n, available)
        if count == 0:
            return b""
        start = self._buffer_pos
        chunk = bytes(self._buffer[start : start + count])
        self._buffer_pos += count
        self._maybe_compact_buffer()
        return chunk

    def _take_into(self, view: memoryview, offset: int, n: int) -> int:
        available = self._buffer_available()
        count = min(n, available)
        if count == 0:
            return 0
        start = self._buffer_pos
        view[offset : offset + count] = self._buffer[start : start + count]
        self._buffer_pos += count
        self._maybe_compact_buffer()
        return count
