"""Experimental MemoryBIO TLS wrap for native tealetio streams.

Userspace ``ssl.SSLObject`` sits above ``StreamReader`` / ``StreamWriter``. The
proactor sees ciphertext on the fd. Handshake is explicit; this module is not
hooked into ``open_connection`` / ``start_server``.
"""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import Callable
from typing import Any, TypeVar

from .reader import StreamReader
from .writer import StreamWriter

__all__ = ["SSLStream", "wrap_ssl"]

# one TLS record is 16KiB plus a small header; never use reader.read(-1) here —
# that waits for TCP EOF, but OpenSSL only needs the next ciphertext chunk.
_TLS_IO_SIZE = 16384

_WantRead = (ssl.SSLWantReadError, ssl.SSLSyscallError)

T = TypeVar("T")


def wrap_ssl(
    reader: StreamReader,
    writer: StreamWriter,
    sslcontext: ssl.SSLContext,
    *,
    server_side: bool = False,
    server_hostname: str | None = None,
) -> SSLStream:
    """Wrap an already-connected plaintext stream pair. Handshake is not done yet."""

    return SSLStream(
        reader,
        writer,
        sslcontext,
        server_side=server_side,
        server_hostname=server_hostname,
    )


class SSLStream:
    """Blocking TLS record stream over a plaintext ``StreamReader`` / ``StreamWriter`` pair.

    One ``SSLObject`` owns the bidirectional record layer. The stream is owned
    by a single tealet, same as the inner reader and writer.
    """

    def __init__(
        self,
        reader: StreamReader,
        writer: StreamWriter,
        sslcontext: ssl.SSLContext,
        *,
        server_side: bool = False,
        server_hostname: str | None = None,
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
        self._handshake_done = False
        self._closed = False
        self._unwrapped = False

    def do_handshake(self) -> None:
        """Run the TLS handshake, parking on ciphertext I/O as OpenSSL requests it."""

        self._retry(self._sslobj.do_handshake)
        self._flush_outgoing()
        self._handshake_done = True

    def read(self, n: int = -1) -> bytes:
        """Read decrypted application data.

        ``n < 0`` reads until TLS EOF (close_notify or incoming EOF). ``n >= 0``
        returns at most ``n`` bytes and may return fewer.
        """

        if n == 0:
            return b""
        if n < 0:
            return self._read_until_eof()
        return self._read_some(n)

    def readexactly(self, n: int) -> bytes:
        """Read exactly ``n`` decrypted bytes, or raise ``asyncio.IncompleteReadError``."""

        if n < 0:
            raise ValueError("readexactly size must not be negative")
        if n == 0:
            return b""
        buf = bytearray()
        while len(buf) < n:
            chunk = self._read_some(n - len(buf))
            if not chunk:
                raise asyncio.IncompleteReadError(bytes(buf), n)
            buf.extend(chunk)
        return bytes(buf)

    def write(self, data: bytes | bytearray | memoryview) -> None:
        """Encrypt ``data`` and flush ciphertext through the inner writer."""

        if not data:
            return
        view = memoryview(data).cast("B")
        while view.nbytes:
            written = self._retry(self._sslobj.write, view)
            assert written > 0
            view = view[written:]
            self._flush_outgoing()

    def drain(self) -> None:
        """Flush any pending outgoing BIO bytes and the inner writer send buffer."""

        self._flush_outgoing()

    def unwrap(self) -> None:
        """Send close_notify and complete the TLS shutdown handshake.

        Leaves the inner plaintext transport open.
        """

        if self._unwrapped:
            return
        try:
            self._retry(self._sslobj.unwrap)
        except ssl.SSLZeroReturnError:
            pass
        self._flush_outgoing()
        self._unwrapped = True

    def close(self) -> None:
        """Best-effort close_notify without waiting for the peer, then close the inner stream."""

        if self._closed:
            return
        self._closed = True
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
        self._writer.close()
        try:
            self._writer.wait_closed()
        except OSError:
            # peer already closed the fd; close_notify send is best-effort
            pass

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
        data = self._outgoing.read()
        if data:
            self._writer.write(data)
        self._writer.drain()

    def _feed_incoming(self) -> None:
        chunk = self._reader.read(_TLS_IO_SIZE)
        if not chunk:
            self._incoming.write_eof()
        else:
            self._incoming.write(chunk)

    def _read_some(self, n: int) -> bytes:
        try:
            data = self._retry(self._sslobj.read, n)
        except ssl.SSLZeroReturnError:
            return b""
        return data or b""

    def _read_until_eof(self) -> bytes:
        chunks: list[bytes] = []
        while True:
            chunk = self._read_some(_TLS_IO_SIZE)
            if not chunk:
                return b"".join(chunks)
            chunks.append(chunk)
