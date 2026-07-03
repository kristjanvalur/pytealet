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

def test_ring_poll_completion_when_available():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        token = {"operation": "poll"}
        with uring_api.Ring() as ring:
            handle = ring.submit_poll(reader.fileno(), select.POLLIN, token)
            writer.send(b"x")
            completion = ring.wait(1.0)
            assert completion is not None
            assert completion is handle
            assert completion.kind == uring_api.COMPLETION_KIND_POLL
            assert completion.user_data is token
            assert completion.res & select.POLLIN
            assert completion.result == completion.res
    finally:
        reader.close()
        writer.close()

def test_ring_poll_multishot_completion_when_available():
    require_uring_capability("IORING_POLL_MULTISHOT")

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        token = {"operation": "poll-multishot"}
        with uring_api.Ring() as ring:
            handle = ring.submit_poll_multishot(reader.fileno(), select.POLLIN, token)
            writer.send(b"a")
            first = ring.wait(1.0)
            writer.send(b"b")
            second = ring.wait(1.0)

            assert first is not None
            assert second is not None
            assert handle.result is None
            for sequence, completion in ((0, first), (1, second)):
                assert completion is not handle
                assert completion.kind == uring_api.COMPLETION_KIND_POLL_MULTISHOT
                assert completion.user_data is token
                assert completion.sequence == sequence
                assert completion.res & select.POLLIN
                assert completion.result == completion.res
                if sequence == 0:
                    assert completion.flags & uring_api.IORING_CQE_F_MORE
    finally:
        reader.close()
        writer.close()

def test_ring_poll_remove_rejects_wrong_completion_kind():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        with uring_api.Ring() as ring:
            recv_handle = ring.submit_recv(reader.fileno(), bytearray(1))
            with pytest.raises(ValueError, match="poll or poll_multishot"):
                ring.submit_poll_remove(recv_handle)
    finally:
        reader.close()
        writer.close()

def test_ring_poll_remove_rejects_delivered_poll_copy_when_available():
    require_uring_capability("IORING_POLL_MULTISHOT")

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        with uring_api.Ring() as ring:
            handle = ring.submit_poll_multishot(reader.fileno(), select.POLLIN, {"operation": "poll-remove-invalid"})
            writer.send(b"a")
            delivered = ring.wait(1.0)
            assert delivered is not None
            assert delivered is not handle
            assert delivered.kind == uring_api.COMPLETION_KIND_POLL_MULTISHOT
            with pytest.raises(ValueError, match="original submit handle"):
                ring.submit_poll_remove(delivered)
    finally:
        reader.close()
        writer.close()

def test_ring_poll_remove_stops_multishot_poll_when_available():
    require_uring_capability("IORING_POLL_MULTISHOT")

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        token = {"operation": "poll-remove"}
        with uring_api.Ring() as ring:
            handle = ring.submit_poll_multishot(reader.fileno(), select.POLLIN, token)
            writer.send(b"a")
            first = ring.wait(1.0)
            assert first is not None
            assert first is not handle
            assert first.kind == uring_api.COMPLETION_KIND_POLL_MULTISHOT

            remove_handle = ring.submit_poll_remove(handle)
            assert remove_handle.kind == uring_api.COMPLETION_KIND_POLL_REMOVE
            removed = False
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                completion = ring.wait(0.0)
                if completion is None:
                    continue
                if completion is remove_handle:
                    removed = True
                    break
            assert removed
    finally:
        reader.close()
        writer.close()

