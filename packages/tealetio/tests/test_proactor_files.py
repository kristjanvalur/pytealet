from __future__ import annotations

import errno
import os
from typing import Any

import pytest

from io_fakes import StubScheduler
import tealetio.files as files_module
from tealetio.files import ProactorFile, parse_open_mode
from tealetio.io_manager import ProactorIOManager
from tealetio.operations import Operation

_TEST_FD = 901

if hasattr(os, "O_CLOEXEC"):
    _CLOEXEC = os.O_CLOEXEC
else:
    _CLOEXEC = 0


class _MemoryProactor:
    def recycle_operation(self, operation: object) -> None:
        return

    def __init__(self, store: dict[int, bytearray]) -> None:
        self._store = store
        self.read_calls: list[tuple[int, int, int]] = []
        self.write_calls: list[tuple[int, bytes, int]] = []
        self.read_into_calls: list[tuple[int, int]] = []
        self.close_fd_calls: list[int] = []

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> object:
        from tealetio.proactor import SyntheticRecvBufferPool

        return SyntheticRecvBufferPool(buffer_size, buffer_count)

    def read(self, fd: int, n: int, offset: int) -> Operation[bytes]:
        self.read_calls.append((fd, n, offset))
        data = bytes(self._store.get(fd, b"")[offset : offset + n])
        operation = Operation[bytes](kind="read", fileobj=fd)
        operation._finish(result=data)
        return operation

    def write(self, fd: int, data: Any, offset: int) -> Operation[int]:
        payload = bytes(data)
        self.write_calls.append((fd, payload, offset))
        buf = self._store.setdefault(fd, bytearray())
        end = offset + len(payload)
        if end > len(buf):
            buf.extend(b"\x00" * (end - len(buf)))
        buf[offset:end] = payload
        operation = Operation[int](kind="write", fileobj=fd)
        operation._finish(result=len(payload))
        return operation

    def read_into(self, fd: int, buf: Any, offset: int) -> Operation[int]:
        self.read_into_calls.append((fd, offset))
        view = memoryview(buf).cast("B")
        payload = self._store.get(fd, b"")[offset : offset + len(view)]
        nbytes = min(len(view), len(payload))
        if nbytes:
            view[:nbytes] = payload[:nbytes]
        operation = Operation[int](kind="read_into", fileobj=fd)
        operation._finish(result=nbytes)
        return operation

    def stat_fdsize(self, fd: int) -> Operation[int]:
        operation = Operation[int](kind="stat_fdsize", fileobj=fd)
        operation._finish(result=len(self._store.get(fd, b"")))
        return operation

    def close_fd(self, fd: int) -> Operation[None]:
        self.close_fd_calls.append(fd)
        self._store.pop(fd, None)
        operation = Operation[None](kind="close_fd", fileobj=fd)
        operation._finish(result=None)
        return operation


def _make_file(
    *,
    data: bytes = b"",
    flags: int = os.O_RDWR,
    append: bool = False,
) -> tuple[ProactorFile, _MemoryProactor, dict[int, bytearray]]:
    store: dict[int, bytearray] = {_TEST_FD: bytearray(data)}
    proactor = _MemoryProactor(store)
    io = ProactorIOManager(StubScheduler(), proactor)  # type: ignore[arg-type]
    handle = ProactorFile(
        io,
        _TEST_FD,
        path="/tmp/memory.txt",
        flags=flags,
        append=append,
    )
    return handle, proactor, store


@pytest.mark.parametrize(
    ("mode", "expected_flags"),
    [
        ("rb", os.O_RDONLY | _CLOEXEC),
        ("wb", os.O_WRONLY | os.O_CREAT | os.O_TRUNC | _CLOEXEC),
        ("ab", os.O_WRONLY | os.O_CREAT | os.O_APPEND | _CLOEXEC),
        ("r+b", os.O_RDWR | _CLOEXEC),
        ("rb+", os.O_RDWR | _CLOEXEC),
        ("w+b", os.O_RDWR | os.O_CREAT | os.O_TRUNC | _CLOEXEC),
        ("wb+", os.O_RDWR | os.O_CREAT | os.O_TRUNC | _CLOEXEC),
        ("a+b", os.O_RDWR | os.O_CREAT | os.O_APPEND | _CLOEXEC),
        ("ab+", os.O_RDWR | os.O_CREAT | os.O_APPEND | _CLOEXEC),
    ],
)
def test_parse_open_mode_maps_supported_binary_modes(mode: str, expected_flags: int) -> None:
    flags, creat_mode = parse_open_mode(mode)
    assert flags == expected_flags
    assert creat_mode == 0o666


@pytest.mark.parametrize(
    "mode",
    ["", "rt", "xb", "x+b", "u", "abr"],
)
def test_parse_open_mode_rejects_unsupported_modes(mode: str) -> None:
    with pytest.raises(ValueError):
        parse_open_mode(mode)


def test_proactor_file_exposes_iofile_surface() -> None:
    handle, _proactor, _store = _make_file(data=b"hello")
    try:
        for attr in (
            "name",
            "closed",
            "readable",
            "writable",
            "seekable",
            "fileno",
            "tell",
            "seek",
            "read",
            "readinto",
            "write",
            "close",
        ):
            assert hasattr(handle, attr)
    finally:
        handle.close()


def test_seek_and_tell_track_logical_position() -> None:
    handle, proactor, _store = _make_file(data=b"hello")
    try:
        assert handle.tell() == 0
        assert handle.seek(2) == 2
        assert handle.read(2) == b"ll"
        assert handle.tell() == 4
        assert proactor.read_calls[-1] == (_TEST_FD, 2, 2)
    finally:
        handle.close()


def test_read_all_chunks_until_eof(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(files_module, "_READ_CHUNK", 3)
    handle, proactor, _store = _make_file(data=b"hello")
    try:
        assert handle.read() == b"hello"
        assert [call[1] for call in proactor.read_calls] == [3, 3, 3]
    finally:
        handle.close()


def test_close_is_idempotent() -> None:
    handle, proactor, _store = _make_file()
    handle.close()
    handle.close()
    assert handle.closed
    assert proactor.close_fd_calls == [_TEST_FD]


def test_close_delegates_to_proactor_close_fd() -> None:
    handle, proactor, store = _make_file(data=b"hi")
    try:
        handle.close()
        assert proactor.close_fd_calls == [_TEST_FD]
        assert _TEST_FD not in store
    finally:
        handle.close()


def test_append_readinto_empty_buffer_preserves_eof_flag() -> None:
    handle, proactor, store = _make_file(data=b"hi", flags=os.O_RDWR | os.O_APPEND, append=True)
    try:
        assert handle.tell() == 2
        assert handle.readinto(bytearray()) == 0
        assert handle.tell() == 2
        assert proactor.read_into_calls == []
        handle.write(b"!")
        assert bytes(store[_TEST_FD]) == b"hi!"
        assert proactor.write_calls[-1][2] == 2
    finally:
        handle.close()


def test_writeonly_handle_rejects_read() -> None:
    handle, _proactor, _store = _make_file(flags=os.O_WRONLY)
    try:
        with pytest.raises(OSError) as excinfo:
            handle.read(1)
        assert excinfo.value.errno == errno.EBADF
    finally:
        handle.close()