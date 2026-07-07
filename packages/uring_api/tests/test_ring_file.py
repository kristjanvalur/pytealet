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

def test_ring_submit_read_rejects_negative_offset():
    require_uring()

    with uring_api.Ring() as ring:
        with pytest.raises(ValueError, match="offset must be non-negative"):
            ring.submit_read(0, bytearray(1), -1)

def test_ring_submit_write_rejects_negative_offset():
    require_uring()

    with uring_api.Ring() as ring:
        with pytest.raises(ValueError, match="offset must be non-negative"):
            ring.submit_write(0, b"x", -1)

def test_ring_submit_read_rejects_buffer_length_above_uint_max():
    require_uring()

    buf = oversized_file_buffer()
    try:
        with uring_api.Ring() as ring:
            with pytest.raises(ValueError, match="buffer length must fit in uint32_t"):
                ring.submit_read(0, buf, 0)
    finally:
        buf.close()

def test_ring_submit_write_rejects_buffer_length_above_uint_max():
    require_uring()

    buf = oversized_file_buffer()
    try:
        with uring_api.Ring() as ring:
            with pytest.raises(ValueError, match="buffer length must fit in uint32_t"):
                ring.submit_write(0, buf, 0)
    finally:
        buf.close()

def test_ring_file_read_write_completion_when_available():
    require_uring()

    token = {"operation": "file"}
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        path = tmp.name
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT)
        try:
            with uring_api.Ring() as ring:
                write_handle = ring.submit_write(fd, b"hello", 0, token)
                write_completion = wait_one(ring, 1.0)
                assert write_completion is write_handle
                assert write_completion.kind == uring_api.COMPLETION_KIND_WRITE
                assert write_completion.user_data is token
                assert write_completion.res == 5
                assert write_completion.result == 5

                buf = bytearray(5)
                read_handle = ring.submit_read(fd, buf, 0, token)
                read_completion = wait_one(ring, 1.0)
                assert read_completion is read_handle
                assert read_completion.kind == uring_api.COMPLETION_KIND_READ
                assert read_completion.user_data is token
                assert read_completion.res == 5
                assert read_completion.result == 5
                assert bytes(buf) == b"hello"
        finally:
            os.close(fd)
    finally:
        os.unlink(path)

def test_ring_submit_statx_rejects_small_buffer():
    require_uring()

    with uring_api.Ring() as ring:
        with pytest.raises(ValueError, match="256 bytes"):
            ring.submit_statx(0, "", 0, uring_api.STATX_SIZE, bytearray(32))

def test_ring_statx_path_returns_size_in_buffer_when_available():
    require_uring()

    token = {"operation": "statx-path"}
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        path = tmp.name
    try:
        with open(path, "wb") as handle:
            handle.write(b"hello")
        buf = bytearray(uring_api.STATX_BUFFER_SIZE)
        with uring_api.Ring() as ring:
            statx_handle = ring.submit_statx(uring_api.AT_FDCWD, path, 0, uring_api.STATX_SIZE, buf, token)
            completion = wait_one(ring, 1.0)
            assert completion is statx_handle
            assert completion.kind == uring_api.COMPLETION_KIND_STATX
            assert completion.user_data is token
            assert completion.res == 0
            assert completion.result is None
            assert uring_api.statx_st_size(buf) == 5
    finally:
        os.unlink(path)

def test_ring_statx_fd_submit_returns_size_in_buffer_when_available():
    require_uring()

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        path = tmp.name
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT)
        try:
            assert os.write(fd, b"hello") == 5
            buf = bytearray(uring_api.STATX_BUFFER_SIZE)
            with uring_api.Ring() as ring:
                handle = ring.submit_statx(
                    fd,
                    "",
                    uring_api.AT_EMPTY_PATH,
                    uring_api.STATX_SIZE,
                    buf,
                )
                completion = wait_one(ring, 1.0)
                assert completion is handle
                assert completion.kind == uring_api.COMPLETION_KIND_STATX
                assert completion.res == 0
                assert completion.result is None
                assert uring_api.statx_st_size(buf) == 5
        finally:
            os.close(fd)
    finally:
        os.unlink(path)

def test_ring_statx_without_size_mask_returns_none_result_when_available():
    require_uring()

    statx_nlink = 0x4
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        path = tmp.name
    try:
        with open(path, "wb") as handle:
            handle.write(b"hello")
        buf = bytearray(uring_api.STATX_BUFFER_SIZE)
        with uring_api.Ring() as ring:
            handle = ring.submit_statx(uring_api.AT_FDCWD, path, 0, statx_nlink, buf)
            completion = wait_one(ring, 1.0)
            assert completion is handle
            assert completion.kind == uring_api.COMPLETION_KIND_STATX
            assert completion.res == 0
            assert completion.result is None
    finally:
        os.unlink(path)

def test_ring_statx_fdsize_returns_size_in_completion_result_when_available():
    require_uring()

    token = {"operation": "statx-fdsize"}
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        path = tmp.name
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT)
        try:
            assert os.write(fd, b"hello") == 5
            with uring_api.Ring() as ring:
                handle = ring.submit_statx_fdsize(fd, token)
                completion = wait_one(ring, 1.0)
                assert completion is handle
                assert completion.kind == uring_api.COMPLETION_KIND_STATX_FDSIZE
                assert completion.user_data is token
                assert completion.res == 0
                assert completion.result == 5
        finally:
            os.close(fd)
    finally:
        os.unlink(path)

def test_ring_statx_fdsize_fails_for_invalid_fd():
    require_uring()

    with uring_api.Ring() as ring:
        handle = ring.submit_statx_fdsize(-1)
        completion = wait_one(ring, 1.0)
        assert completion is handle
        assert completion.kind == uring_api.COMPLETION_KIND_STATX_FDSIZE
        assert completion.res < 0
        assert completion.result is None

def test_ring_statx_fails_for_nonexistent_path():
    require_uring()

    buf = bytearray(uring_api.STATX_BUFFER_SIZE)
    with uring_api.Ring() as ring:
        handle = ring.submit_statx(
            uring_api.AT_FDCWD,
            "/nonexistent/uring-api-statx-missing",
            0,
            uring_api.STATX_SIZE,
            buf,
        )
        completion = wait_one(ring, 1.0)
        assert completion is handle
        assert completion.kind == uring_api.COMPLETION_KIND_STATX
        assert completion.res < 0

def test_ring_openat_read_write_round_trip_when_available():
    require_uring()

    token = {"operation": "openat"}
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "openat-test.txt")
        with uring_api.Ring() as ring:
            open_handle = ring.submit_openat(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644, token)
            open_completion = wait_one(ring, 1.0)
            assert open_completion is open_handle
            assert open_completion.kind == uring_api.COMPLETION_KIND_OPENAT
            assert open_completion.user_data is token
            assert open_completion.res >= 0
            assert open_completion.result == open_completion.res
            fd = open_completion.res

            write_handle = ring.submit_write(fd, b"hello", 0, token)
            write_completion = wait_one(ring, 1.0)
            assert write_completion is write_handle
            assert write_completion.res == 5

            buf = bytearray(5)
            read_handle = ring.submit_read(fd, buf, 0, token)
            read_completion = wait_one(ring, 1.0)
            assert read_completion is read_handle
            assert read_completion.res == 5
            assert bytes(buf) == b"hello"

            close_handle = ring.submit_close(fd, token)
            close_completion = wait_one(ring, 1.0)
            assert close_completion is close_handle
            assert close_completion.res == 0

