import errno
import fcntl
import gc
import importlib.util
import mmap
import os
import select
import shlex
import shutil
import socket
import subprocess
import sys
import sysconfig
import tempfile
import threading
import time
import weakref
from importlib import resources
from pathlib import Path

import pytest

import _uring_api
import uring_api

from helpers import (
    wait_one,
    assert_fd_nonblocking_cloexec,
    build_c_api_client,
    collect_until_stable,
    connect_to_listener,
    connected_tcp_pair,
    kernel_version_at_least,
    oversized_file_buffer,
    require_setup_flags,
    wait_until_running,
)
from conftest import require_uring, require_uring_capability

def test_ring_lifecycle_when_available():
    require_uring()

    with uring_api.Ring() as ring:
        assert ring.fd >= 0
        assert ring.sq_entries > 0
        assert ring.cq_entries > 0
        assert not ring.closed

    assert ring.fd == -1
    assert ring.closed


def test_ring_pending_count_tracks_in_flight_waitables():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            assert ring.pending_count() == 0
            constructed = ring.construct_recv(reader.fileno(), bytearray(4), object())
            assert ring.pending_count() == 0
            ring.prepare(constructed)
            assert ring.pending_count() == 1
            second = ring.prepare_recv(reader.fileno(), bytearray(4), object())
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

