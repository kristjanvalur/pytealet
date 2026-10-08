"""Link timeout on prepare.

Oneshot recv is the original user. The same Completion.timeout is linked
for every kind. The timer CQE is discarded. A fired timer is -ECANCELED
or -EINTR on the operation, the same as a cancel. A positive res is
success even if the timer loses the race.
"""

import errno
import gc
import os
import select
import socket
import time

import pytest

import uring_api

from conftest import require_uring



def _wait_op(ring: uring_api.Ring, timeout: float = 2.0) -> uring_api.Completion:
    deadline = time.monotonic() + timeout
    got = None
    while got is None and time.monotonic() < deadline:
        batch = ring.wait(0.2)
        if not batch:
            continue
        assert len(batch) == 1
        got = batch[0]
    assert got is not None
    assert ring.pending_count() == 0
    # a timer that lands after the op is swallowed, not delivered.
    assert not ring.wait(0.05)
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
    assert ring.prepare(completion) == 1
    return completion


def test_link_timeout_arms_on_construct():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        with uring_api.Ring() as ring:
            assert ring.construct_recv(reader.fileno(), bytearray(1)).timeout is None
            assert ring.construct_recv(reader.fileno(), bytearray(1), timeout=None).timeout is None
            plain = ring.construct_recv(reader.fileno(), bytearray(1))
            assert plain.timeout is None
            plain.timeout = 1.5
            assert plain.timeout == 1.5
            plain.timeout = None
            assert plain.timeout is None
            with pytest.raises(OverflowError, match="timeout"):
                plain.timeout = float(1 << 63)
            armed = ring.construct_recv(reader.fileno(), bytearray(1), timeout=0)
            assert armed.timeout == 0.0
            assert armed.prepared is False
            armed.timeout = None
            assert armed.timeout is None
            with pytest.raises(ValueError, match="timeout"):
                ring.construct_recv(reader.fileno(), bytearray(1), timeout=-1)
            send = ring.construct_send(writer.fileno(), b"x")
            send.timeout = 1
            assert send.timeout == 1.0
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
            assert ring.pending_count() == 1
            got = _wait_op(ring)
            assert got is completion
            assert completion.res == len(payload)
            assert bytes(buf) == payload
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
            assert ring.pending_count() == 1
            got = _wait_op(ring)
            assert got is completion
            assert completion.res in (-errno.ECANCELED, -errno.EINTR)
            assert buf == bytearray(8)
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
            assert ring.pending_count() == 1
            got = _wait_op(ring)
            assert got is completion
            assert completion.res == len(payload)
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
            assert ring.pending_count() == 1
            got = _wait_op(ring)
            assert got is completion
            assert completion.res in (-errno.ECANCELED, -errno.EINTR)
    finally:
        reader.close()
        writer.close()


def test_skip_all_timeout_outlives_the_completion():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        fd = os.dup(writer.fileno())
        with uring_api.Ring(auto_submit=False) as ring:
            completion = ring.construct_close(fd)
            completion.skip_all = True
            completion.timeout = 1.0
            assert ring.prepare(completion) == 1
            before = ring.stats()["cqe"]
            del completion
            gc.collect()
            # reuse the freed completion before submit. the timeout SQE must
            # not still point at its link_ts field.
            junk = [ring.construct_close(fd) for _ in range(32)]
            del junk
            gc.collect()
            assert ring.submit() == 2
            deadline = time.monotonic() + 2.0
            while ring.stats()["cqe"] < before + 1 and time.monotonic() < deadline:
                assert not ring.wait(0.1)
            assert ring.stats()["cqe"] >= before + 1
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


def test_send_all_timeout_parks_when_pair_does_not_fit():
    """A timed continuation parks when two SQ slots are not free.

    auto_submit is off, so that shortfall is backpressure, not a failed wait.
    The filler recvs sit on another socket so draining the payload does not
    race them.
    """
    require_uring()

    reader, writer = socket.socketpair()
    idle_r, idle_w = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        writer.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
        reader.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        payload = b"x" * (256 * 1024)
        with uring_api.Ring(entries=2, auto_submit=False) as ring:
            pending = ring.construct_send_all(writer.fileno(), payload)
            pending.timeout = 30.0
            assert ring.prepare(pending) == 1
            assert ring.submit() == 2
            for _ in range(ring.sq_entries):
                ring.prepare_recv(idle_r.fileno(), bytearray(1))
            parked = ring.stats()["next_leg_park"]
            deadline = time.monotonic() + 2.0
            while (
                time.monotonic() < deadline
                and pending.result is None
                and ring.stats()["next_leg_park"] == parked
            ):
                try:
                    ring.wait(0.05)
                except uring_api.SubmissionQueueFull:
                    pytest.fail("timed send_all continuation failed instead of parking")
            if pending.result == len(payload):
                pytest.skip("send_all finished before a continuation was needed")
            assert ring.stats()["next_leg_park"] == parked + 1
            assert pending.result is None
            # the parked send_all plus the filler recvs. a failed pair must
            # not take a second in-flight ref.
            assert ring.pending_count() == 1 + ring.sq_entries
            deadline = time.monotonic() + 2.0
            while pending.result is None and time.monotonic() < deadline:
                try:
                    reader.recv(8192)
                except BlockingIOError:
                    pass
                ring.submit()
                ring.wait(0)
            assert pending.result == len(payload)
            assert ring.pending_count() == ring.sq_entries
    finally:
        reader.close()
        writer.close()
        idle_r.close()
        idle_w.close()
