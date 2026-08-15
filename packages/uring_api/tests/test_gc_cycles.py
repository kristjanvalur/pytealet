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

def test_completion_user_data_cycles_are_collectable():
    require_uring()

    class Marker:
        pass

    reader, writer = connected_tcp_pair()
    try:
        ring = uring_api.Ring(entries=4)
        try:
            marker = Marker()
            marker_ref = weakref.ref(marker)
            user_data = [marker]
            completion = ring.prepare_recv(reader.fileno(), bytearray(8), user_data=user_data)
            user_data.append(completion)
            writer.send(b"x")
            assert wait_one(ring, 1.0).res == 1
        finally:
            ring.close()
    finally:
        reader.close()
        writer.close()

    del completion
    del user_data
    del marker
    gc.collect()

    assert marker_ref() is None


def test_clear_user_data_on_idle_handle_is_immediate():
    require_uring()

    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4) as ring:
            token = object()
            pending = ring.prepare_recv(reader.fileno(), bytearray(4), user_data=token)
            assert pending.user_data is token
            pending.clear_user_data()
            assert pending.user_data is None
            writer.send(b"abcd")
            done = wait_one(ring, 1.0)
            assert done is pending
            assert done.user_data is None
    finally:
        reader.close()
        writer.close()


def test_completion_user_data_is_settable_and_clearable():
    require_uring()

    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4) as ring:
            token = {"op": "recv"}
            pending = ring.prepare_recv(reader.fileno(), bytearray(4), user_data=token)
            assert pending.user_data is token
            pending.user_data = {"replaced": True}
            assert pending.user_data == {"replaced": True}
            writer.send(b"abcd")
            done = wait_one(ring, 1.0)
            assert done is pending
            assert done.user_data == {"replaced": True}
            done.clear_user_data()
            assert done.user_data is None
            del done.user_data
            assert done.user_data is None
    finally:
        reader.close()
        writer.close()


def test_clearing_user_data_breaks_waitable_cycle():
    """Clearing user_data after delivery allows GC without dropping reverse first."""

    require_uring()

    class Marker:
        pass

    reader, writer = connected_tcp_pair()
    try:
        ring = uring_api.Ring(entries=4)
        try:
            marker = Marker()
            marker_ref = weakref.ref(marker)
            # waitable-shaped cycle: list holds marker and completion
            payload = [marker]
            completion = ring.prepare_recv(reader.fileno(), bytearray(8), user_data=payload)
            payload.append(completion)
            writer.send(b"y")
            assert wait_one(ring, 1.0).res == 1
            # nerf without del completion
            completion.clear_user_data()
            del payload
            del marker
            gc.collect()
            assert marker_ref() is None
        finally:
            ring.close()
    finally:
        reader.close()
        writer.close()

def test_ring_callback_cycles_are_collectable():
    require_uring()

    class Marker:
        pass

    def make_cycle():
        ring = uring_api.Ring(entries=4)
        marker = Marker()
        marker_ref = weakref.ref(marker)

        def callback(_completion):
            marker
            ring.closed

        ring.callback = callback
        return marker_ref

    marker_ref = make_cycle()
    gc.collect()

    assert marker_ref() is None


def test_buf_group_callback_cycles_are_collectable():
    require_uring()

    class Marker:
        pass

    def make_cycle():
        ring = uring_api.Ring(entries=4)
        buf_group = ring.create_buf_group(16, 4)
        marker = Marker()
        marker_ref = weakref.ref(marker)

        def callback(_completion):
            marker
            buf_group.buffer_size
            ring.closed

        ring.callback = callback
        return marker_ref

    marker_ref = make_cycle()
    gc.collect()

    assert marker_ref() is None

