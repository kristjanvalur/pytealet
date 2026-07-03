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

def test_ring_lifecycle_when_available():
    require_uring()

    with uring_api.Ring() as ring:
        assert ring.fd >= 0
        assert ring.sq_entries > 0
        assert ring.cq_entries > 0
        assert not ring.closed

    assert ring.fd == -1
    assert ring.closed

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

