from __future__ import annotations

import errno
import io
import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .proactor import Proactor, ProactorScheduler

_DEFAULT_CREAT_MODE = 0o666
_READ_CHUNK = 64 * 1024


def parse_open_mode(mode: str) -> tuple[int, int]:
    """Map a binary open mode string to ``(flags, mode)`` for ``openat``."""

    if not mode or "b" not in mode:
        raise ValueError("ProactorFile requires a binary mode such as 'rb' or 'wb'")
    if "t" in mode:
        raise ValueError("text mode is not supported; use io.TextIOWrapper on a binary ProactorFile")

    if "+" in mode:
        flags = os.O_RDWR
    elif "w" in mode:
        flags = os.O_WRONLY
    elif "r" in mode:
        flags = os.O_RDONLY
    else:
        raise ValueError(f"invalid mode: {mode!r}")

    append = "a" in mode
    if "w" in mode or append or "+" in mode:
        flags |= os.O_CREAT
    if "w" in mode and not append:
        flags |= os.O_TRUNC
    if append:
        flags |= os.O_APPEND

    return flags, _DEFAULT_CREAT_MODE


class ProactorFile(io.RawIOBase):
    """Unbuffered positioned file I/O backed by a proactor scheduler.

    Tracks a logical file position in userspace and forwards reads and writes to
    positioned proactor operations. ``readinto()`` uses ``read_into`` so
    ``io.BufferedReader`` can fill caller buffers without an extra copy through
    ``read()``.
    """

    def __init__(
        self,
        scheduler: ProactorScheduler,
        proactor: Proactor,
        fd: int,
        *,
        path: str,
        flags: int,
        append: bool = False,
    ) -> None:
        super().__init__()
        self._scheduler = scheduler
        self._proactor = proactor
        self._fd = fd
        self._path = path
        self._flags = flags
        self._append = append
        access = flags & os.O_ACCMODE
        self._readable = access in (os.O_RDONLY, os.O_RDWR)
        self._writable = access in (os.O_WRONLY, os.O_RDWR)
        self._pos = 0
        if append:
            self._pos = os.fstat(fd).st_size

    @property
    def name(self) -> str:
        return self._path

    def readable(self) -> bool:
        return self._readable and not self.closed

    def writable(self) -> bool:
        return self._writable and not self.closed

    def seekable(self) -> bool:
        return not self.closed

    def fileno(self) -> int:
        self._checkClosed()
        return self._fd

    def tell(self) -> int:
        self._checkClosed()
        return self._pos

    def seek(self, pos: int, whence: int = os.SEEK_SET) -> int:
        self._checkClosed()
        if whence == os.SEEK_SET:
            self._pos = pos
        elif whence == os.SEEK_CUR:
            self._pos += pos
        elif whence == os.SEEK_END:
            self._pos = os.fstat(self._fd).st_size + pos
        else:
            raise ValueError("invalid whence")
        if self._pos < 0:
            raise OSError(errno.EINVAL, "Invalid argument")
        return self._pos

    def read(self, size: int = -1) -> bytes:
        self._checkClosed()
        if not self._readable:
            raise OSError(errno.EBADF, "File is not readable")
        if size == 0:
            return b""
        if size < 0:
            parts: list[bytes] = []
            while True:
                chunk = self._read_chunk(_READ_CHUNK)
                if not chunk:
                    break
                parts.append(chunk)
            return b"".join(parts)
        return self._read_chunk(size)

    def readinto(self, buffer: Any) -> int | None:
        self._checkClosed()
        if not self._readable:
            raise OSError(errno.EBADF, "File is not readable")
        view = memoryview(buffer).cast("B")
        if not view:
            return 0
        nbytes = self._scheduler.wait_operation(self._proactor.read_into(self._fd, view, self._pos))
        self._pos += nbytes
        return nbytes

    def write(self, b: Any) -> int | None:
        self._checkClosed()
        if not self._writable:
            raise OSError(errno.EBADF, "File is not writable")
        if self._append:
            self._pos = os.fstat(self._fd).st_size
        nbytes = self._scheduler.wait_operation(self._proactor.write(self._fd, b, self._pos))
        self._pos += nbytes
        return nbytes

    def close(self) -> None:
        if self.closed:
            return
        fd = self._fd
        self._fd = -1
        super().close()
        if fd < 0:
            return
        try:
            os.close(fd)
        except OSError as exc:
            if exc.errno != errno.EBADF:
                raise

    def _read_chunk(self, size: int) -> bytes:
        data = self._scheduler.wait_operation(self._proactor.read(self._fd, size, self._pos))
        self._pos += len(data)
        return data
