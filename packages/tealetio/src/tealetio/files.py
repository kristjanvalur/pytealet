from __future__ import annotations

import errno
import io
import os
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

from .operations import Operation

if TYPE_CHECKING:
    from .proactor import Proactor

T = TypeVar("T")

__all__ = [
    "IOFile",
    "OperationWaiter",
    "ProactorFile",
    "parse_open_mode",
]

_DEFAULT_CREAT_MODE = 0o666
_READ_CHUNK = 64 * 1024
_SUPPORTED_BINARY_OPEN_MODES = frozenset(
    {
        "rb",
        "wb",
        "ab",
        "r+b",
        "rb+",
        "w+b",
        "wb+",
        "a+b",
        "ab+",
    }
)


class OperationWaiter(Protocol):
    """Block the current tealet until a submitted ``Operation`` completes."""

    def wait_operation(self, operation: Operation[T]) -> T: ...


class IOFile(Protocol):
    """Positioned binary file handle returned by ``FileIO.open()``.

    Static typing only: not ``@runtime_checkable`` because ``name`` and
    ``closed`` are properties, which breaks ``isinstance`` on Python 3.10–3.11.
    Import from ``tealetio`` (or ``tealetio.proactor``), not only
    ``tealetio.files``.
    """

    @property
    def name(self) -> str: ...

    @property
    def closed(self) -> bool: ...

    def readable(self) -> bool: ...

    def writable(self) -> bool: ...

    def seekable(self) -> bool: ...

    def fileno(self) -> int: ...

    def tell(self) -> int: ...

    def seek(self, pos: int, whence: int = os.SEEK_SET) -> int: ...

    def read(self, size: int = -1) -> bytes: ...

    def readinto(self, buffer: Any) -> int | None: ...

    def write(self, b: Any) -> int | None: ...

    def close(self) -> None: ...


def parse_open_mode(mode: str) -> tuple[int, int]:
    """Map a binary open mode string to ``(flags, mode)`` for ``openat``."""

    if not mode:
        raise ValueError("ProactorFile requires a binary mode such as 'rb' or 'wb'")
    if "t" in mode:
        raise ValueError("text mode is not supported; use io.TextIOWrapper on a binary ProactorFile")
    if "x" in mode:
        raise ValueError("exclusive create modes such as 'xb' are not supported")
    if mode not in _SUPPORTED_BINARY_OPEN_MODES:
        raise ValueError(f"unsupported mode: {mode!r}")

    append = "a" in mode
    if "+" in mode:
        flags = os.O_RDWR
    elif "w" in mode:
        flags = os.O_WRONLY
    elif "r" in mode:
        flags = os.O_RDONLY
    elif append:
        flags = os.O_WRONLY
    else:
        raise ValueError(f"invalid mode: {mode!r}")

    if "w" in mode or append:
        flags |= os.O_CREAT
    if "w" in mode and not append:
        flags |= os.O_TRUNC
    if append:
        flags |= os.O_APPEND
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC

    return flags, _DEFAULT_CREAT_MODE


class ProactorFile(io.RawIOBase):
    """Unbuffered positioned file I/O backed by a proactor and IO waiter.

    Tracks a logical file position in userspace and forwards reads and writes to
    positioned proactor operations. ``readinto()`` uses ``read_into`` so
    ``io.BufferedReader`` can fill caller buffers without an extra copy through
    ``read()``.

    Append mode tracks ``_pos_at_eof`` so writes can skip ``stat_fdsize()`` while
    the handle has only extended the file from the tail. ``seek()`` (except
    ``seek(SEEK_END, 0)`` when already at EOF) and reads clear the flag; the next
    append write looks up file size again. Concurrent writers can still race.

    ``fileno()`` returns the raw OS descriptor. Reads and writes through this
    handle use the tracked logical offset; direct ``os.read`` / ``os.write`` on
    that fd bypass position tracking and can desynchronise ``tell()`` and later
    proactor I/O.
    """

    def __init__(
        self,
        waiter: OperationWaiter,
        proactor: Proactor,
        fd: int,
        *,
        path: str,
        flags: int,
        append: bool = False,
    ) -> None:
        super().__init__()
        self._io = waiter
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
            self._pos = self._file_size()
            self._pos_at_eof = True

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
        """Return the OS fd; direct syscalls on it bypass logical position tracking."""

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
            if not (self._append and self._pos_at_eof and pos == 0):
                self._pos = self._file_size() + pos
        else:
            raise ValueError("invalid whence")
        if self._pos < 0:
            raise OSError(errno.EINVAL, "Invalid argument")
        if self._append:
            self._pos_at_eof = whence == os.SEEK_END and pos == 0
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
            # empty buffer is a no-op; keep append EOF tracking unchanged
            return 0
        nbytes = self._io.wait_operation(self._proactor.read_into(self._fd, view, self._pos))
        self._pos += nbytes
        if self._append:
            self._pos_at_eof = False
        return nbytes

    def write(self, b: Any) -> int | None:
        self._checkClosed()
        if not self._writable:
            raise OSError(errno.EBADF, "File is not writable")
        if self._append and not self._pos_at_eof:
            self._pos = self._file_size()
        nbytes = self._io.wait_operation(self._proactor.write(self._fd, b, self._pos))
        self._pos += nbytes
        if self._append:
            self._pos_at_eof = True
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
            self._io.wait_operation(self._proactor.close_fd(fd))
        except OSError as exc:
            if exc.errno != errno.EBADF:
                raise

    def _file_size(self) -> int:
        return self._io.wait_operation(self._proactor.stat_fdsize(self._fd))

    def _read_chunk(self, size: int) -> bytes:
        data = self._io.wait_operation(self._proactor.read(self._fd, size, self._pos))
        self._pos += len(data)
        if self._append:
            self._pos_at_eof = False
        return data
