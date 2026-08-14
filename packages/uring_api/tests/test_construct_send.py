"""construct_* + prepare: bind VIEW args without taking an SQE."""

from __future__ import annotations

import os
import select
import socket
import tempfile

import pytest

import uring_api
from helpers import require_uring, wait_one


def test_construct_send_is_not_kernel_visible_until_prepare():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            token = object()
            pending = ring.construct_send(writer.fileno(), b"hi", token)
            assert pending.kind == uring_api.COMPLETION_KIND_SEND
            assert pending.user_data is token
            assert pending.prepared is False

            # no SQE yet: wait times out and the peer has nothing
            assert ring.wait(0.05) == []
            with pytest.raises(BlockingIOError):
                reader.recv(2)

            n = ring.prepare(pending)
            assert n == 1
            assert pending.prepared is True

            completion = wait_one(ring, 1.0)
            assert completion is pending
            assert completion.res == 2
            assert reader.recv(2) == b"hi"
    finally:
        reader.close()
        writer.close()


def test_prepare_batch_and_reverse_link_before_sqe():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            first_waitable = {"completion": None}
            second_waitable = {"completion": None}
            first = ring.construct_send(writer.fileno(), b"ab", first_waitable)
            second = ring.construct_send(writer.fileno(), b"cd", second_waitable)
            first_waitable["completion"] = first
            second_waitable["completion"] = second
            assert first.prepared is False
            assert second.prepared is False

            n = ring.prepare([first, second])
            assert n == 2
            assert first.prepared is True
            assert second.prepared is True
            assert first_waitable["completion"] is first
            assert second_waitable["completion"] is second

            seen = []
            for _ in range(10):
                seen.extend(ring.wait(0.2))
                if first in seen and second in seen:
                    break
            assert first in seen and second in seen
            assert reader.recv(4) == b"abcd"
    finally:
        reader.close()
        writer.close()


def test_construct_send_zc_prepares_like_send():
    require_uring()
    if not uring_api.probe().get("IORING_OP_SEND_ZC"):
        pytest.skip("IORING_OP_SEND_ZC not available")

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        # AF_UNIX often returns -EOPNOTSUPP for ZC; still exercises construct/prepare.
        with uring_api.Ring() as ring:
            pending = ring.construct_send_zc(writer.fileno(), b"zc")
            assert pending.kind == uring_api.COMPLETION_KIND_SEND_ZC
            assert pending.prepared is False
            assert ring.prepare(pending) == 1
            assert pending.prepared is True
            completion = wait_one(ring, 1.0)
            assert completion is pending
    finally:
        reader.close()
        writer.close()


def test_submit_send_is_construct_plus_prepare():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            token = object()
            pending = ring.submit_send(writer.fileno(), b"xyz", token)
            assert pending.prepared is True
            assert pending.user_data is token
            completion = wait_one(ring, 1.0)
            assert completion is pending
            assert completion.res == 3
            assert reader.recv(3) == b"xyz"
    finally:
        reader.close()
        writer.close()


def test_prepare_rejects_already_prepared_and_wrong_kind():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            send = ring.construct_send(writer.fileno(), b"x")
            ring.prepare(send)
            with pytest.raises(ValueError, match="already prepared"):
                ring.prepare(send)
            assert wait_one(ring, 1.0) is send

            poll = ring.submit_poll(reader.fileno(), select.POLLIN)
            with pytest.raises(ValueError, match="constructed completions"):
                ring.prepare(poll)
            ring.submit_poll_remove_nowait(poll)
            ring.wait(0.2)
    finally:
        reader.close()
        writer.close()


def test_unprepared_completion_can_be_dropped():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            pending = ring.construct_send(writer.fileno(), b"nope")
            del pending
            assert ring.wait(0.05) == []
            with pytest.raises(BlockingIOError):
                reader.recv(4)
    finally:
        reader.close()
        writer.close()


def test_prepare_empty_sequence():
    require_uring()

    with uring_api.Ring() as ring:
        assert ring.prepare([]) == 0
        assert ring.prepare(()) == 0


def test_construct_recv_is_not_kernel_visible_until_prepare():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        writer.send(b"hi")
        with uring_api.Ring() as ring:
            token = object()
            buf = bytearray(2)
            pending = ring.construct_recv(reader.fileno(), buf, token)
            assert pending.kind == uring_api.COMPLETION_KIND_RECV
            assert pending.user_data is token
            assert pending.prepared is False

            assert ring.wait(0.05) == []
            assert bytes(buf) == b"\x00\x00"

            n = ring.prepare(pending)
            assert n == 1
            assert pending.prepared is True

            completion = wait_one(ring, 1.0)
            assert completion is pending
            assert completion.res == 2
            assert bytes(buf) == b"hi"
    finally:
        reader.close()
        writer.close()


def test_submit_recv_is_construct_plus_prepare():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        writer.send(b"xy")
        with uring_api.Ring() as ring:
            buf = bytearray(2)
            pending = ring.submit_recv(reader.fileno(), buf)
            assert pending.prepared is True
            completion = wait_one(ring, 1.0)
            assert completion is pending
            assert completion.res == 2
            assert bytes(buf) == b"xy"
    finally:
        reader.close()
        writer.close()


def test_construct_read_write_file():
    require_uring()

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        path = tmp.name
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT)
        try:
            with uring_api.Ring() as ring:
                write = ring.construct_write(fd, b"hello", 0)
                assert write.kind == uring_api.COMPLETION_KIND_WRITE
                assert write.prepared is False
                assert ring.prepare(write) == 1
                assert write.prepared is True
                assert wait_one(ring, 1.0) is write
                assert write.res == 5

                buf = bytearray(5)
                read = ring.construct_read(fd, buf, 0)
                assert read.kind == uring_api.COMPLETION_KIND_READ
                assert read.prepared is False
                assert ring.prepare(read) == 1
                assert wait_one(ring, 1.0) is read
                assert read.res == 5
                assert bytes(buf) == b"hello"
        finally:
            os.close(fd)
    finally:
        os.unlink(path)


def test_construct_read_write_reject_negative_offset():
    require_uring()

    with uring_api.Ring() as ring:
        with pytest.raises(ValueError, match="offset must be non-negative"):
            ring.construct_read(0, bytearray(1), -1)
        with pytest.raises(ValueError, match="offset must be non-negative"):
            ring.construct_write(0, b"x", -1)


def test_prepare_mixed_view_kinds():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            send = ring.construct_send(writer.fileno(), b"ab")
            buf = bytearray(2)
            recv = ring.construct_recv(reader.fileno(), buf)
            assert ring.prepare([send, recv]) == 2
            seen = []
            for _ in range(10):
                seen.extend(ring.wait(0.2))
                if send in seen and recv in seen:
                    break
            assert send in seen and recv in seen
            assert bytes(buf) == b"ab"
    finally:
        reader.close()
        writer.close()
