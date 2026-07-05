"""Shared io_uring fakes for tealetio proactor and streams tests."""

from __future__ import annotations

import errno
import os
import select
import socket
import struct
import threading
from types import SimpleNamespace
from typing import Any

import pytest
import uring_api

def _pack_fake_statx_buffer(
    buf: bytearray | memoryview,
    *,
    size: int,
    mode: int = 0o100644,
    ino: int = 1,
    atime_sec: int = 0,
    mtime_sec: int = 0,
    ctime_sec: int = 0,
    dev_major: int = 1,
    dev_minor: int = 0,
    rdev_major: int = 0,
    rdev_minor: int = 0,
) -> None:
    view = memoryview(buf)
    mask = uring_api.STATX_BASIC_STATS
    struct.pack_into("<IIQ", view, 0, mask, 4096, 0)
    struct.pack_into("<IIIH", view, 16, 1, os.getuid(), os.getgid(), mode)
    struct.pack_into("<QQQ", view, 32, ino, size, (size + 511) // 512)
    struct.pack_into("<Q", view, 56, 0)
    struct.pack_into("<qi", view, 64, atime_sec, 0)
    struct.pack_into("<qi", view, 96, ctime_sec, 0)
    struct.pack_into("<qi", view, 112, mtime_sec, 0)
    struct.pack_into("<IIII", view, 128, rdev_major, rdev_minor, dev_major, dev_minor)


def _native_uring_extension_imported() -> bool:
    return getattr(uring_api, "_native_import_error", None) is None

def _default_uring_capabilities(**overrides: bool) -> dict[str, bool]:
    capabilities = {
        "available": _native_uring_extension_imported(),
        "IORING_ACCEPT_MULTISHOT": True,
        "IORING_RECV_MULTISHOT": True,
        "IORING_POLL_MULTISHOT": True,
        "IORING_OP_SEND_ZC": True,
        "IORING_OP_SENDMSG_ZC": True,
        "IORING_OP_STATX": True,
    }
    capabilities.update(overrides)
    return capabilities


def _patch_uring_capabilities(monkeypatch: pytest.MonkeyPatch, **overrides: bool) -> None:
    monkeypatch.setattr(
        uring_api,
        "probe",
        lambda *args, **kwargs: _default_uring_capabilities(**overrides),
    )


def _force_uring_multishot_probes(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_uring_capabilities(monkeypatch)


class _FakeBufGroup:
    def __init__(self, ring: "_FakeUringRing", buffer_size: int, buffer_count: int) -> None:
        self.ring = ring
        self.buffer_size = buffer_size
        self.buffer_count = buffer_count
        self.leased_count = 0

    def note_chunk_released(self) -> None:
        if self.leased_count:
            self.leased_count -= 1


def _fake_multishot_recv_payload(data: bytes) -> memoryview:
    # fake-ring completions use owned views; do not consult uring_api.is_available()
    # because TestUringProactor patches probe() to enable multishot opcodes.
    return memoryview(bytearray(data))


class _FakeUringRing:
    def __init__(self, entries: int, flags: int) -> None:
        self.entries = entries
        self.flags = flags
        self.fd = 99
        self.features = 123
        self.sq_entries = entries
        self.cq_entries = entries * 2
        self.closed = False
        self.running = False
        self.callback = None
        self.serve_count = 0
        self.stop_serving_count = 0
        self._stop_serving_event = threading.Event()
        self.break_count = 0
        self.completions: list[SimpleNamespace] = []
        self.accepted_peers: list[socket.socket] = []
        self.submitted_recv: list[tuple[int, object, object]] = []
        self.submitted_recv_multishot: list[tuple[int, _FakeBufGroup, object]] = []
        self.buf_groups: list[_FakeBufGroup] = []
        self.submitted_recvmsg: list[tuple[int, object, object]] = []
        self.submitted_send: list[tuple[int, object, object]] = []
        self.submitted_sendto: list[tuple[int, object, object, object]] = []
        self.submitted_accept: list[tuple[int, object, int]] = []
        self.submitted_accept_multishot: list[tuple[int, object, int]] = []
        self.submitted_connect: list[tuple[int, object, object]] = []
        self.submitted_cancel: list[object] = []
        self.submitted_poll: list[tuple[int, int, object]] = []
        self.submitted_poll_multishot: list[tuple[int, int, object]] = []
        self.submitted_poll_remove: list[object] = []
        self.submitted_openat: list[tuple[str, int, int, object, int]] = []
        self.submitted_statx: list[tuple[int, str, int, int, object, object]] = []
        self.submitted_statx_fdsize: list[tuple[int, object]] = []
        self.submitted_read: list[tuple[int, object, int, object]] = []
        self.submitted_write: list[tuple[int, bytes, int, object]] = []
        self.open_fds: dict[int, bytes] = {}
        self.next_open_fd = 200
        self.pending_recv: list[SimpleNamespace] = []
        self.pending_recv_multishot: list[SimpleNamespace] = []
        self.pending_accept_multishot: list[SimpleNamespace] = []
        self.pending_poll_multishot: list[SimpleNamespace] = []
        self.pending_poll_oneshot: list[SimpleNamespace] = []
        self.pending_accept_oneshot: list[SimpleNamespace] = []
        self.pending_recv_oneshot: list[SimpleNamespace] = []
        self.recv_multishot_sequence = 0

    def _completion(
        self,
        user_data: object,
        kind: int = uring_api.COMPLETION_KIND_RECV,
        res: int = 0,
        flags: int = 0,
        result: object = None,
        sequence: int = 0,
        *,
        multishot: bool = False,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            user_data=user_data,
            kind=kind,
            res=res,
            flags=flags,
            result=result,
            sequence=sequence,
            multishot=multishot,
        )

    def close(self) -> None:
        self.stop_serving()
        for peer in self.accepted_peers:
            peer.close()
        self.accepted_peers.clear()
        self.closed = True

    def serve_completions(self) -> None:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.running = True
        self.serve_count += 1
        self._stop_serving_event.wait()
        self.running = False

    def stop_serving(self) -> None:
        self._stop_serving_event.set()
        self.stop_serving_count += 1

    def reset_serving(self) -> None:
        self._stop_serving_event.clear()

    def break_wait(self) -> None:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.break_count += 1

    def _recv_buffer_for_entry(self, entry: object) -> memoryview:
        for _fd, buf, user_data in reversed(self.submitted_recv):
            if user_data is entry:
                return memoryview(buf)
        raise RuntimeError("recv buffer not found for entry")

    def submit_recv(self, fd: int, buf: Any, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        view = memoryview(buf)
        operation = getattr(user_data, "operation", None)
        kind = getattr(operation, "kind", None)
        self.submitted_recv.append((fd, buf, user_data))
        if kind == "recv_many":
            completion = self._completion(user_data, res=0, result=0)
            self.pending_recv_oneshot.append(completion)
            return completion
        payload = b"world" if kind == "recv_into" else b"hello"
        if len(view) >= len(payload):
            view[: len(payload)] = payload
        completion = self._completion(user_data, res=len(payload), result=len(payload))
        self.pending_recv.append(completion)
        self._deliver(completion)
        return completion

    def complete_recv_oneshot(self, data: bytes) -> None:
        completion = self.pending_recv_oneshot.pop(0)
        entry = completion.user_data
        view = self._recv_buffer_for_entry(entry)
        if data:
            view[: len(data)] = data
            completion.res = len(data)
            completion.result = len(data)
        else:
            completion.res = 0
            completion.result = 0
        self._deliver(completion)

    def create_buf_group(self, buffer_size: int, buffer_count: int) -> _FakeBufGroup:
        if self.closed:
            raise RuntimeError("ring is closed")
        buf_group = _FakeBufGroup(self, buffer_size, buffer_count)
        self.buf_groups.append(buf_group)
        return buf_group

    def submit_recv_multishot(
        self,
        fd: int,
        buf_group: _FakeBufGroup,
        user_data: object = None,
        flags: int = 0,
    ) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_recv_multishot.append((fd, buf_group, user_data))
        self.recv_multishot_sequence = 0
        completion = self._completion(user_data, kind=uring_api.COMPLETION_KIND_RECV_MULTISHOT, multishot=True)
        self.pending_recv_multishot.append(completion)
        return completion

    def complete_recv_multishot_enobufs(self, *, sequence: int | None = None) -> None:
        pending = self.pending_recv_multishot[-1]
        _, buf_group, _ = self.submitted_recv_multishot[-1]
        buf_group.leased_count = buf_group.buffer_count
        if sequence is None:
            sequence = self.recv_multishot_sequence
            self.recv_multishot_sequence += 1
        completion = self._completion(
            pending.user_data,
            kind=uring_api.COMPLETION_KIND_RECV_MULTISHOT,
            res=-errno.ENOBUFS,
            flags=0,
            result=None,
            sequence=sequence,
            multishot=True,
        )
        self._deliver(completion)

    def complete_recv_multishot(self, data: bytes, *, more: bool = True, sequence: int | None = None) -> None:
        pending = self.pending_recv_multishot[-1]
        _, buf_group, _ = self.submitted_recv_multishot[-1]
        if sequence is None:
            sequence = self.recv_multishot_sequence
            self.recv_multishot_sequence += 1
        if data:
            buf_group.leased_count += 1
        if not data:
            payload = None
            res = 0
        else:
            payload = _fake_multishot_recv_payload(data)
            res = len(data)
        completion = self._completion(
            pending.user_data,
            kind=uring_api.COMPLETION_KIND_RECV_MULTISHOT,
            res=res,
            flags=uring_api.IORING_CQE_F_MORE if more else 0,
            result=payload,
            sequence=sequence,
            multishot=True,
        )
        self._deliver(completion)

    def submit_send(self, fd: int, data: Any, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        payload = bytes(data)
        self.submitted_send.append((fd, data, user_data))
        completion = self._completion(
            user_data, kind=uring_api.COMPLETION_KIND_SEND, res=len(payload), result=len(payload)
        )
        self._deliver(completion)
        return completion

    def submit_recvmsg(self, fd: int, buf: Any, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        payload = b"again" if getattr(getattr(user_data, "operation", None), "kind", None) == "recvfrom" else b"hello"
        memoryview(buf)[: len(payload)] = payload
        self.submitted_recvmsg.append((fd, buf, user_data))
        completion = self._completion(
            user_data,
            kind=uring_api.COMPLETION_KIND_RECVMSG,
            res=len(payload),
            result=("127.0.0.1", 54321),
        )
        self._deliver(completion)
        return completion

    def submit_sendto(self, fd: int, data: Any, address: Any, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        payload = bytes(data)
        self.submitted_sendto.append((fd, data, address, user_data))
        completion = self._completion(
            user_data,
            kind=uring_api.COMPLETION_KIND_SENDTO,
            res=len(payload),
            result=len(payload),
        )
        self._deliver(completion)
        return completion

    def submit_send_zc(self, fd: int, data: Any, user_data: object = None) -> SimpleNamespace:
        return self.submit_send(fd, data, user_data)

    def submit_sendmsg_zc(
        self, fd: int, data: Any, address: Any, user_data: object = None, flags: int = 0
    ) -> SimpleNamespace:
        return self.submit_sendto(fd, data, address, user_data)

    def submit_accept(self, fd: int, user_data: object = None, flags: int = 0) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        conn, peer = socket.socketpair()
        self.accepted_peers.append(peer)
        self.submitted_accept.append((fd, user_data, flags))
        completion = self._completion(
            user_data,
            kind=uring_api.COMPLETION_KIND_ACCEPT,
            res=conn.fileno(),
            result=(conn.detach(), "peer"),
        )
        operation = getattr(user_data, "operation", None)
        if getattr(operation, "kind", None) == "accept_many":
            self.pending_accept_oneshot.append(completion)
            return completion
        self._deliver(completion)
        return completion

    def complete_accept_oneshot(self) -> None:
        completion = self.pending_accept_oneshot.pop(0)
        self._deliver(completion)

    def submit_accept_multishot(self, fd: int, user_data: object = None, flags: int = 0) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_accept_multishot.append((fd, user_data, flags))
        self.accept_multishot_sequence = 0
        completion = self._completion(user_data, kind=uring_api.COMPLETION_KIND_ACCEPT, multishot=True)
        self.pending_accept_multishot.append(completion)
        return completion

    def complete_accept_multishot(
        self,
        address: object = "peer",
        *,
        more: bool = True,
        sequence: int | None = None,
    ) -> None:
        pending = self.pending_accept_multishot[-1]
        if sequence is None:
            sequence = getattr(self, "accept_multishot_sequence", 0)
            self.accept_multishot_sequence = sequence + 1
        conn, peer = socket.socketpair()
        self.accepted_peers.append(peer)
        completion = self._completion(
            pending.user_data,
            kind=uring_api.COMPLETION_KIND_ACCEPT,
            res=conn.fileno(),
            flags=uring_api.IORING_CQE_F_MORE if more else 0,
            result=(conn.detach(), address),
            sequence=sequence,
            multishot=True,
        )
        self._deliver(completion)

    def submit_connect(self, fd: int, address: Any, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_connect.append((fd, address, user_data))
        completion = self._completion(user_data, kind=uring_api.COMPLETION_KIND_CONNECT, res=0, result=None)
        self._deliver(completion)
        return completion

    def submit_cancel(self, completion: SimpleNamespace) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_cancel.append(completion)
        cancel_completion = self._completion(completion, kind=uring_api.COMPLETION_KIND_CANCEL, res=0, result=None)
        self._deliver(cancel_completion)
        return cancel_completion

    def submit_poll(self, fd: int, mask: int, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_poll.append((fd, mask, user_data))
        completion = self._completion(user_data, kind=uring_api.COMPLETION_KIND_POLL, res=mask, result=mask)
        operation = getattr(user_data, "operation", None)
        if getattr(operation, "kind", None) == "poll_many":
            self.pending_poll_oneshot.append(completion)
            return completion
        self._deliver(completion)
        return completion

    def complete_poll_oneshot(self, res: int = select.POLLIN) -> None:
        completion = self.pending_poll_oneshot.pop(0)
        completion.res = res
        completion.result = res
        self._deliver(completion)

    def submit_poll_multishot(self, fd: int, mask: int, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_poll_multishot.append((fd, mask, user_data))
        completion = self._completion(user_data, kind=uring_api.COMPLETION_KIND_POLL_MULTISHOT, multishot=True)
        self.pending_poll_multishot.append(completion)
        return completion

    def complete_poll_multishot(
        self,
        res: int = select.POLLIN,
        *,
        more: bool = True,
        sequence: int = 0,
    ) -> None:
        pending = self.pending_poll_multishot[-1]
        completion = self._completion(
            pending.user_data,
            kind=uring_api.COMPLETION_KIND_POLL_MULTISHOT,
            res=res,
            flags=uring_api.IORING_CQE_F_MORE if more else 0,
            sequence=sequence,
            multishot=True,
        )
        self._deliver(completion)

    def submit_poll_remove(self, completion: SimpleNamespace) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_poll_remove.append(completion)
        remove_completion = self._completion(completion, kind=uring_api.COMPLETION_KIND_POLL_REMOVE, res=0)
        self._deliver(remove_completion)
        return remove_completion

    def submit_statx(
        self,
        dfd: int,
        path: str,
        flags: int,
        mask: int,
        buf: Any,
        user_data: object = None,
    ) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_statx.append((dfd, path, flags, mask, buf, user_data))
        size = len(self.open_fds.get(dfd, b""))
        _pack_fake_statx_buffer(buf, size=size)
        completion = self._completion(
            user_data,
            kind=uring_api.COMPLETION_KIND_STATX,
            res=0,
            result=0,
        )
        self._deliver(completion)
        return completion

    def submit_statx_fdsize(self, fd: int, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_statx_fdsize.append((fd, user_data))
        size = len(self.open_fds.get(fd, b""))
        completion = self._completion(
            user_data,
            kind=uring_api.COMPLETION_KIND_STATX_FDSIZE,
            res=0,
            result=size,
        )
        self._deliver(completion)
        return completion

    def submit_openat(
        self,
        path: str,
        flags: int,
        mode: int = 0,
        user_data: object = None,
        dfd: int = -100,
    ) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_openat.append((path, flags, mode, user_data, dfd))
        fd = self.next_open_fd
        self.next_open_fd += 1
        self.open_fds[fd] = b""
        completion = self._completion(
            user_data,
            kind=uring_api.COMPLETION_KIND_OPENAT,
            res=fd,
            result=fd,
        )
        self._deliver(completion)
        return completion

    def submit_write(self, fd: int, data: Any, offset: int, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        payload = bytes(memoryview(data))
        self.submitted_write.append((fd, payload, offset, user_data))
        existing = self.open_fds.get(fd, b"")
        if offset == len(existing):
            updated = existing + payload
        else:
            buf = bytearray(existing)
            end = offset + len(payload)
            if end > len(buf):
                buf.extend(b"\x00" * (end - len(buf)))
            buf[offset:end] = payload
            updated = bytes(buf)
        self.open_fds[fd] = updated
        completion = self._completion(
            user_data,
            kind=uring_api.COMPLETION_KIND_WRITE,
            res=len(payload),
            result=len(payload),
        )
        self._deliver(completion)
        return completion

    def submit_read(self, fd: int, buf: Any, offset: int, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_read.append((fd, buf, offset, user_data))
        view = memoryview(buf)
        payload = self.open_fds.get(fd, b"hello")[offset:]
        nbytes = min(len(view), len(payload))
        if nbytes:
            view[:nbytes] = payload[:nbytes]
        completion = self._completion(
            user_data,
            kind=uring_api.COMPLETION_KIND_READ,
            res=nbytes,
            result=nbytes,
        )
        self._deliver(completion)
        return completion

    def wait(self, timeout: float | None = None) -> SimpleNamespace | None:
        if not self.completions:
            return None
        return self.completions.pop(0)

    def _deliver(self, completion: SimpleNamespace) -> None:
        if self.running and self.callback is not None:
            self.callback(completion)
        else:
            self.completions.append(completion)
