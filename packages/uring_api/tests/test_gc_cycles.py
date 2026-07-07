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
            completion = ring.submit_recv(reader.fileno(), bytearray(8), user_data=user_data)
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

