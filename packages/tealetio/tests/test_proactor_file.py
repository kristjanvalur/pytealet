from __future__ import annotations

import errno
import os
from typing import Any
from unittest.mock import patch

import pytest

import tealetio.files as files_module
from tealetio.files import ProactorFile
from tealetio.operations import Operation

_TEST_FD = 901


class _ImmediateWaiter:
    def wait_operation(self, operation: Operation[Any]) -> Any:
        return operation.result()


class _MemoryProactor:
    def __init__(self, store: dict[int, bytearray]) -> None:
        self._store = store
        self.read_calls: list[tuple[int, int, int]] = []
        self.write_calls: list[tuple[int, bytes, int]] = []
        self.read_into_calls: list[tuple[int, int]] = []

    def read(self, fd: int, n: int, offset: int) -> Operation[bytes]:
        self.read_calls.append((fd, n, offset))
        data = bytes(self._store.get(fd, b"")[offset : offset + n])
        operation = Operation[bytes](kind="read", fileobj=fd)
        operation._set_result(data)
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
        operation._set_result(len(payload))
        return operation

    def read_into(self, fd: int, buf: Any, offset: int) -> Operation[int]:
        self.read_into_calls.append((fd, offset))
        view = memoryview(buf).cast("B")
        payload = self._store.get(fd, b"")[offset : offset + len(view)]
        nbytes = min(len(view), len(payload))
        if nbytes:
            view[:nbytes] = payload[:nbytes]
        operation = Operation[int](kind="read_into", fileobj=fd)
        operation._set_result(nbytes)
        return operation

    def stat_fdsize(self, fd: int) -> Operation[int]:
        operation = Operation[int](kind="stat_fdsize", fileobj=fd)
        operation._set_result(len(self._store.get(fd, b"")))
        return operation


def _make_file(
    *,
    data: bytes = b"",
    flags: int = os.O_RDWR,
    append: bool = False,
) -> tuple[ProactorFile, _MemoryProactor, dict[int, bytearray]]:
    store: dict[int, bytearray] = {_TEST_FD: bytearray(data)}
    proactor = _MemoryProactor(store)
    waiter = _ImmediateWaiter()
    handle = ProactorFile(
        waiter,
        proactor,  # type: ignore[arg-type]
        _TEST_FD,
        path="/tmp/memory.txt",
        flags=flags,
        append=append,
    )
    return handle, proactor, store


@pytest.fixture(autouse=True)
def _noop_os_close():
    with patch("tealetio.files.os.close"):
        yield


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
    handle, _proactor, _store = _make_file()
    handle.close()
    handle.close()
    assert handle.closed


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