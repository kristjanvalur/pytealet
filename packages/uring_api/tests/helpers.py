import errno
import fcntl
import gc
import importlib.util
import mmap
import os
import shlex
import socket
import subprocess
import sys
import sysconfig
import tempfile
import time
from pathlib import Path

import pytest

import _uring_api
import uring_api

from conftest import EXTENSION_C_COMPILE_ARGS, require_uring

UINT_MAX = (1 << 32) - 1


def collect_until_stable() -> None:
    """Run GC until a pass collects nothing.

    Used by BufGroup ID recycling tests that require extension teardown
    before the next allocation. Stricter than one ``gc.collect()`` but still
    assumes a tracing GC like CPython's. Scenarios that must not depend on
    GC (for example ``ring.close()`` resetting the allocator) live in
    separate tests.
    """

    while gc.collect():
        pass

def wait_until_running(ring: uring_api.Ring) -> None:
    deadline = time.monotonic() + 1.0
    while not ring.running and time.monotonic() < deadline:
        time.sleep(0.01)
    assert ring.running

def assert_fd_nonblocking_cloexec(fd: int) -> None:
    assert fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_NONBLOCK
    assert fcntl.fcntl(fd, fcntl.F_GETFD) & fcntl.FD_CLOEXEC

def connect_to_listener(server: socket.socket) -> socket.socket:
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.setblocking(False)
    err = client.connect_ex(server.getsockname())
    assert err in {0, errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY}
    return client

def connected_tcp_pair() -> tuple[socket.socket, socket.socket]:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        writer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        writer.connect(server.getsockname())
        reader, _address = server.accept()
        reader.setblocking(False)
        writer.setblocking(False)
        return reader, writer
    finally:
        server.close()

def kernel_version_component(value: str) -> int:
    digits = []
    for char in value:
        if char.isdigit():
            digits.append(char)
        else:
            break
    return int("".join(digits)) if digits else 0

def kernel_version_at_least(release: str, major: int, minor: int, patch: int = 0) -> bool:
    parts = release.split("-", 1)[0].split(".")
    if len(parts) < 2:
        return False
    parsed = [
        kernel_version_component(parts[0]),
        kernel_version_component(parts[1]),
        kernel_version_component(parts[2]) if len(parts) > 2 else 0,
    ]
    if parsed[0] != major:
        return parsed[0] > major
    if parsed[1] != minor:
        return parsed[1] > minor
    return parsed[2] >= patch

def require_setup_flags(flags: int) -> None:
    require_uring()
    if not uring_api.probe(flags=flags):
        pytest.skip("setup flags are not accepted")

def build_c_api_client():
    include_dir = Path(uring_api.get_include())
    python_include = sysconfig.get_path("include")
    extension_suffix = sysconfig.get_config_var("EXT_SUFFIX")
    cc = sysconfig.get_config_var("CC") or "cc"
    source_path = Path(__file__).parent / "capi_client" / "uring_api_capi_client.c"

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        extension_path = temp_path / f"_uring_api_capi_test_client{extension_suffix}"
        subprocess.run(
            [
                *shlex.split(cc),
                *EXTENSION_C_COMPILE_ARGS,
                "-shared",
                "-fPIC",
                "-I",
                python_include,
                "-I",
                str(include_dir),
                str(source_path),
                _uring_api.__file__,
                "-o",
                str(extension_path),
            ],
            check=True,
        )
        spec = importlib.util.spec_from_file_location("_uring_api_capi_test_client", extension_path)
        assert spec is not None
        assert spec.loader is not None
        client = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(client)
        return client

def oversized_file_buffer():
    tmp = tempfile.NamedTemporaryFile(delete=False)
    try:
        os.ftruncate(tmp.fileno(), UINT_MAX + 1)
        buf = mmap.mmap(tmp.fileno(), UINT_MAX + 1, access=mmap.ACCESS_WRITE)
    except OSError:
        tmp.close()
        os.unlink(tmp.name)
        pytest.skip("cannot create oversized buffer for bounds test")
    tmp.close()
    os.unlink(tmp.name)
    return buf

