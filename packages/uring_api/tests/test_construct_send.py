"""construct_send + prepare: bind send args without taking an SQE."""

from __future__ import annotations

import socket

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
            assert pending.ring is ring

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
            assert pending.ring is ring
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

            buf = bytearray(1)
            recv = ring.submit_recv(reader.fileno(), buf)
            with pytest.raises(ValueError, match="constructed send"):
                ring.prepare(recv)
            assert wait_one(ring, 1.0) is recv
            assert buf == b"x"
    finally:
        reader.close()
        writer.close()


def test_prepare_rejects_other_ring():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring_a, uring_api.Ring() as ring_b:
            pending = ring_a.construct_send(writer.fileno(), b"x")
            with pytest.raises(ValueError, match="not constructed on this ring"):
                ring_b.prepare(pending)
            ring_a.prepare(pending)
            wait_one(ring_a, 1.0)
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
