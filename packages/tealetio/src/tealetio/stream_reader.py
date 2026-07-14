"""Stream reader cores and public reader types."""

from __future__ import annotations

import asyncio
from typing import Any

from .io_buffers import RECV_MANY_BUFFER_PRESSURE, RecvIterBuffer
from .stream_constants import DEFAULT_LIMIT


class ReaderCore:
    _COMPACT_PREFIX = 4096

    def __init__(self, *, recv_buffer: RecvIterBuffer, limit: int = DEFAULT_LIMIT) -> None:
        self._recv_buffer = recv_buffer
        self._limit = limit
        self._buffer = bytearray()
        self._buffer_pos = 0
        self._eof = False

    @property
    def at_eof(self) -> bool:
        return self._eof and not self._buffer_available()

    def close(self) -> None:
        self._recv_buffer.close()

    def _buffer_available(self) -> int:
        return len(self._buffer) - self._buffer_pos

    def _compact_buffer(self) -> None:
        if self._buffer_pos:
            del self._buffer[: self._buffer_pos]
            self._buffer_pos = 0

    def _maybe_compact_buffer(self) -> None:
        if self._buffer_pos >= self._COMPACT_PREFIX and self._buffer_pos >= len(self._buffer) // 2:
            self._compact_buffer()

    def _append_next_chunk(self) -> bool:
        while True:
            item = self._recv_buffer.take_next()
            if item is None:
                self._eof = True
                return False
            index, chunk = item
            if index == RECV_MANY_BUFFER_PRESSURE:
                continue
            if chunk.nbytes:
                self._buffer.extend(chunk)
                chunk.release()
                return True
            self._eof = True
            return False

    def _fill_buffer(self, min_bytes: int) -> None:
        while self._buffer_available() < min_bytes and not self._eof:
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

    def read(self, n: int = -1) -> bytes:
        if n == 0:
            return b""
        if n < 0:
            while not self._eof:
                if not self._append_next_chunk():
                    break
            payload = bytes(self._buffer[self._buffer_pos :])
            self._buffer.clear()
            self._buffer_pos = 0
            return payload
        if self._buffer_available():
            return self._take_bytes(min(n, self._buffer_available()))
        if self._eof:
            return b""
        self._append_next_chunk()
        return self._take_bytes(min(n, self._buffer_available()))

    def readinto(self, b: Any) -> int:
        view = memoryview(b).cast("B")
        if not view.nbytes:
            return 0
        if self._eof and not self._buffer_available():
            return 0

        nbytes = view.nbytes
        if self._buffer_available() < nbytes and not self._eof:
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

        if self._buffer_available() < n and not self._eof:
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
            if self._eof:
                return self._take_bytes(self._buffer_available())
            if self._buffer_available() >= self._limit:
                raise asyncio.LimitOverrunError(
                    "Separator is not found, and chunk exceed the limit",
                    self._buffer_available(),
                )
            if not self._append_next_chunk():
                return self._take_bytes(self._buffer_available())


class StreamReader:
    """Native tealet stream reader with synchronous methods."""

    def __init__(
        self,
        *,
        limit: int = DEFAULT_LIMIT,
        recv_buffer: RecvIterBuffer,
    ) -> None:
        self._core = ReaderCore(recv_buffer=recv_buffer, limit=limit)

    @property
    def at_eof(self) -> bool:
        return self._core.at_eof

    def close(self) -> None:
        self._core.close()

    def read(self, n: int = -1) -> bytes:
        return self._core.read(n)

    def readinto(self, b: Any) -> int:
        return self._core.readinto(b)

    def readexactly(self, n: int) -> bytes:
        return self._core.readexactly(n)

    def readline(self) -> bytes:
        return self._core.readline()


class AsyncStreamReader:
    """Asyncio-shaped stream reader backed by tealet-blocking socket I/O."""

    def __init__(
        self,
        *,
        limit: int = DEFAULT_LIMIT,
        recv_buffer: RecvIterBuffer,
    ) -> None:
        self._core = ReaderCore(recv_buffer=recv_buffer, limit=limit)

    @property
    def at_eof(self) -> bool:
        return self._core.at_eof

    def close(self) -> None:
        self._core.close()

    async def read(self, n: int = -1) -> bytes:
        return self._core.read(n)

    async def readinto(self, b: Any) -> int:
        return self._core.readinto(b)

    async def readexactly(self, n: int) -> bytes:
        return self._core.readexactly(n)

    async def readline(self) -> bytes:
        return self._core.readline()
