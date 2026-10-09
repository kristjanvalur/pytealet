import errno
import os
import socket

import pytest

import uring_api

from helpers import (
    wait_one,
)
from conftest import require_uring

def test_ring_lifecycle_when_available():
    require_uring()

    with uring_api.Ring() as ring:
        assert ring.fd >= 0
        assert ring.sq_entries > 0
        assert ring.cq_entries > 0
        assert not ring.closed

    assert ring.fd == -1
    assert ring.closed


def test_ring_cq_entries_defaults_to_about_twice_sq():
    require_uring()

    with uring_api.Ring(entries=8) as ring:
        assert ring.sq_entries == 8
        assert ring.cq_entries == 16


def test_ring_cq_entries_cqsize():
    require_uring()

    with uring_api.Ring(entries=8, cq_entries=64) as ring:
        assert ring.sq_entries == 8
        assert ring.cq_entries >= 64
        assert ring.cq_entries & (ring.cq_entries - 1) == 0


def test_ring_cq_entries_must_exceed_sq():
    require_uring()

    with pytest.raises(ValueError, match="greater than SQ"):
        uring_api.Ring(entries=8, cq_entries=8)
    with pytest.raises(ValueError, match="greater than SQ"):
        uring_api.Ring(entries=8, cq_entries=4)


def test_ring_pending_count_tracks_in_flight_waitables():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            assert ring.pending_count() == 0
            constructed = ring.construct_recv(reader.fileno(), bytearray(4), 0, object())
            assert ring.pending_count() == 0
            ring.prepare(constructed)
            assert ring.pending_count() == 1
            second = ring.prepare_recv(reader.fileno(), bytearray(4), 0, object())
            assert ring.pending_count() == 2
            writer.send(b"abcd")
            first = wait_one(ring, 1.0)
            assert first is not None
            assert ring.pending_count() == 1
            writer.send(b"efgh")
            done = wait_one(ring, 1.0)
            assert done is not None
            assert ring.pending_count() == 0
    finally:
        reader.close()
        writer.close()


def test_ring_pending_count_nowait_and_multishot():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            assert ring.pending_count() == 0
            ring.prepare_close_nowait(os.dup(reader.fileno()))
            assert ring.pending_count() == 0
            try:
                buf_group = ring.create_buf_group(8, 4)
                handle = ring.prepare_recv_multishot(reader.fileno(), buf_group, 0, object())
            except OSError as exc:
                if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    pytest.skip(f"recv multishot buffers are not supported: errno {exc.errno}")
                raise
            assert ring.pending_count() == 1
            writer.send(b"hello")
            first = wait_one(ring, 1.0)
            assert first is not None
            if first.res < 0:
                pytest.skip(f"recv multishot is not supported: errno {-first.res}")
            assert first is not handle
            assert ring.pending_count() == 1
            writer.close()
            writer = None
            terminal = wait_one(ring, 1.0)
            assert terminal is handle
            assert ring.pending_count() == 0
    finally:
        reader.close()
        if writer is not None:
            writer.close()


def test_ring_rejects_invalid_entries():
    with pytest.raises(ValueError):
        uring_api.Ring(0)

def test_ring_raises_oserror_or_initializes():
    try:
        ring = uring_api.Ring(2)
    except OSError as exc:
        assert exc.errno in {errno.ENOSYS, errno.EPERM, errno.EOPNOTSUPP, errno.ENOMEM, errno.EMFILE, errno.ENFILE}
    else:
        ring.close()

