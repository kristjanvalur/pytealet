"""Link timeout on prepare.

Oneshot recv is the original user. The same Completion.timeout is linked
for every kind. CQ order is not part of the contract. Assertions compare
the set of results and include the raw tuple so a failure shows which
order this kernel used.
"""

import errno
import select
import socket
import time

import pytest

import uring_api

from conftest import require_uring


def _wait_linked(ring: uring_api.Ring, timeout: float = 2.0) -> uring_api.Completion:
    deadline = time.monotonic() + timeout
    got = None
    while got is None and time.monotonic() < deadline:
        batch = ring.wait(0.2)
        if not batch:
            continue
        assert len(batch) == 1
        got = batch[0]
    assert got is not None
    while ring.pending_count() and time.monotonic() < deadline:
        batch = ring.wait(0.2)
        assert not batch
    assert ring.pending_count() == 0
    return got


def _arm(ring: uring_api.Ring, fd: int, buf: bytearray, via: str, timeout: float) -> uring_api.Completion:
    if via == "prepare":
        return ring.prepare_recv(fd, buf, timeout=timeout)
    if via == "attribute":
        completion = ring.construct_recv(fd, buf)
        assert completion.timeout is None
        completion.timeout = timeout
    else:
        completion = ring.construct_recv(fd, buf, timeout=timeout)
    assert completion.timeout == timeout
    assert completion.link_cqes == ()
    assert ring.prepare(completion) == 1
    return completion


def test_link_timeout_arms_on_construct():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        with uring_api.Ring() as ring:
            assert ring.construct_recv(reader.fileno(), bytearray(1)).link_cqes is None
            assert ring.construct_recv(reader.fileno(), bytearray(1), timeout=None).link_cqes is None
            plain = ring.construct_recv(reader.fileno(), bytearray(1))
            assert plain.timeout is None
            plain.timeout = 1.5
            assert plain.timeout == 1.5
            plain.timeout = None
            assert plain.timeout is None
            assert plain.link_cqes is None
            with pytest.raises(OverflowError, match="timeout"):
                plain.timeout = float(1 << 63)
            armed = ring.construct_recv(reader.fileno(), bytearray(1), timeout=0)
            assert armed.timeout == 0.0
            assert armed.link_cqes == ()
            assert armed.timed_out is False
            assert armed.prepared is False
            armed.timeout = None
            assert armed.timeout is None
            with pytest.raises(ValueError, match="timeout"):
                ring.construct_recv(reader.fileno(), bytearray(1), timeout=-1)
            send = ring.construct_send(writer.fileno(), b"x")
            send.timeout = 1
            assert send.timeout == 1.0
            assert send.link_cqes == ()
            send.timeout = None
            assert send.timeout is None
            prepared = ring.construct_recv(reader.fileno(), bytearray(1))
            assert ring.prepare(prepared) == 1
            with pytest.raises(ValueError, match="after prepare"):
                prepared.timeout = 1
            with pytest.raises(ValueError, match="timeout"):
                ring.construct_recv(reader.fileno(), bytearray(1), timeout=float("nan"))
            with pytest.raises(TypeError):
                ring.construct_recv(reader.fileno(), bytearray(1), 0, None, 1)
    finally:
        reader.close()
        writer.close()


@pytest.mark.parametrize("via", ["prepare", "construct", "attribute"])
def test_recv_link_timeout_data_disarms_timer(via: str):
    require_uring()

    payload = b"xyz"
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        writer.sendall(payload)
        buf = bytearray(len(payload))
        with uring_api.Ring() as ring:
            before = ring.stats()["sqe"]
            completion = _arm(ring, reader.fileno(), buf, via, 1.0)
            assert ring.stats()["sqe"] == before + 2
            assert ring.pending_count() == 2
            got = _wait_linked(ring)
            assert got is completion
            assert completion.res == len(payload)
            assert bytes(buf) == payload
            assert completion.timed_out is False
            assert len(completion.link_cqes) == 2
            assert len(payload) in completion.link_cqes
            timer = [res for res in completion.link_cqes if res != len(payload)]
            assert timer == [-errno.ECANCELED] or timer == [-errno.ENOENT], completion.link_cqes
    finally:
        reader.close()
        writer.close()


def test_recv_link_timeout_fires():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        buf = bytearray(8)
        with uring_api.Ring() as ring:
            completion = ring.prepare_recv(reader.fileno(), buf, timeout=0.05)
            assert ring.pending_count() == 2
            got = _wait_linked(ring)
            assert got is completion
            assert completion.timed_out is True
            assert completion.res == -errno.ECANCELED
            assert buf == bytearray(8)
            assert set(completion.link_cqes) == {-errno.ECANCELED, -errno.ETIME}, completion.link_cqes
    finally:
        reader.close()
        writer.close()


def test_send_link_timeout_data_disarms_timer():
    require_uring()

    payload = b"x"
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            before = ring.stats()["sqe"]
            completion = ring.construct_send(writer.fileno(), payload)
            completion.timeout = 1.0
            assert ring.prepare(completion) == 1
            assert ring.stats()["sqe"] == before + 2
            assert ring.pending_count() == 2
            got = _wait_linked(ring)
            assert got is completion
            assert completion.res == len(payload)
            assert completion.timed_out is False
            assert len(completion.link_cqes) == 2
            timer = [res for res in completion.link_cqes if res != len(payload)]
            assert timer == [-errno.ECANCELED] or timer == [-errno.ENOENT], completion.link_cqes
    finally:
        reader.close()
        writer.close()


def test_poll_link_timeout_fires():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            completion = ring.construct_poll(reader.fileno(), select.POLLIN)
            completion.timeout = 0.05
            assert ring.prepare(completion) == 1
            assert ring.pending_count() == 2
            got = _wait_linked(ring)
            assert got is completion
            assert completion.timed_out is True
            assert completion.res in (-errno.ECANCELED, -errno.EINTR)
            assert -errno.ETIME in completion.link_cqes or -errno.EALREADY in completion.link_cqes
    finally:
        reader.close()
        writer.close()


def test_recv_link_timeout_reserves_two_sq_slots():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring(entries=8, auto_submit=False) as ring:
            assert ring.sq_entries >= 2
            filled = ring.sq_entries - 1
            for _ in range(filled):
                ring.prepare_recv(reader.fileno(), bytearray(1))
            assert ring.stats()["sqe"] == filled
            assert ring.pending_count() == filled
            with pytest.raises(uring_api.SubmissionQueueFull, match="no submission queue entries available"):
                ring.prepare_recv(reader.fileno(), bytearray(1), timeout=1.0)
            assert ring.stats()["sqe"] == filled
            assert ring.pending_count() == filled
            ring.prepare_recv(reader.fileno(), bytearray(1))
            assert ring.stats()["sqe"] == filled + 1
            assert ring.pending_count() == filled + 1
    finally:
        reader.close()
        writer.close()
