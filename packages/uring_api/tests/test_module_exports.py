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

from conftest import EXTENSION_C_COMPILE_ARGS

def test_package_is_marked_as_typed():
    assert resources.files("uring_api").joinpath("py.typed").is_file()

def test_uring_api_get_include_points_to_header_dir():
    include_dir = Path(uring_api.get_include())
    header = include_dir / "uring_api_capi.h"

    assert include_dir.is_dir()
    assert header.is_file()

def test_public_capi_header_compiles_without_liburing_headers():
    cc = os.environ.get("CC") or sysconfig.get_config_var("CC") or "cc"
    cc_argv = shlex.split(cc)
    if not cc_argv or not shutil.which(cc_argv[0]):
        pytest.skip("C compiler is not available")

    include_dir = Path(uring_api.get_include())
    python_include = Path(sysconfig.get_paths()["include"])
    if not python_include.joinpath("Python.h").is_file():
        pytest.skip("Python development headers are not available")
    source = (
        '#include "uring_api_capi.h"\n'
        "#include \"uring_api_completion_kinds.h\"\n"
        "static const unsigned int abi = URING_API_CAPI_ABI_VERSION;\n"
        "static const int recv_kind = URING_API_COMPLETION_KIND_RECV;\n"
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        source_path = Path(temp_dir) / "check_uring_api_capi.c"
        object_path = Path(temp_dir) / "check_uring_api_capi.o"
        source_path.write_text(source, encoding="utf-8")
        subprocess.run(
            [
                *cc_argv,
                *EXTENSION_C_COMPILE_ARGS,
                "-c",
                str(source_path),
                "-o",
                str(object_path),
                "-I",
                str(include_dir),
                "-I",
                str(python_include),
            ],
            check=True,
        )

def test_native_module_exports_c_api_constants():
    assert uring_api.C_API_ABI_VERSION == 2
    assert uring_api.C_API_FEATURE_CORE == 1 << 0
    assert uring_api.C_API_FEATURES & uring_api.C_API_FEATURE_CORE

def test_native_module_exports_submission_queue_full_exception():
    assert issubclass(uring_api.SubmissionQueueFull, RuntimeError)

def test_native_module_exports_setup_flag_constants():
    assert uring_api.IORING_SETUP_CQSIZE == 1 << 3
    assert uring_api.IORING_SETUP_CLAMP == 1 << 4
    assert uring_api.IORING_SETUP_COOP_TASKRUN == 1 << 8
    assert uring_api.IORING_SETUP_TASKRUN_FLAG == 1 << 9
    assert uring_api.IORING_SETUP_SINGLE_ISSUER == 1 << 12
    assert uring_api.IORING_SETUP_DEFER_TASKRUN == 1 << 13

def test_native_module_exports_cqe_flag_constants():
    assert uring_api.IORING_CQE_F_MORE == 1 << 1
    assert uring_api.IORING_CQE_F_NOTIF == 1 << 3

def test_native_module_exports_zero_copy_send_constants():
    assert uring_api.IORING_SEND_ZC_REPORT_USAGE == 1 << 3
    assert uring_api.IORING_NOTIF_USAGE_ZC_COPIED == 1 << 31

def test_completion_kind_enum_matches_module_constants():
    assert uring_api.CompletionKind.RECV == uring_api.COMPLETION_KIND_RECV
    assert uring_api.CompletionKind.SENDMSG_ZC == uring_api.COMPLETION_KIND_SENDMSG_ZC
    assert uring_api.CompletionKind(uring_api.COMPLETION_KIND_ACCEPT) is uring_api.CompletionKind.ACCEPT
    assert uring_api.CompletionKind.RECV_BUF == uring_api.COMPLETION_KIND_RECV_BUF
    assert uring_api.CompletionKind.RECV_MULTISHOT == uring_api.COMPLETION_KIND_RECV_MULTISHOT
    assert uring_api.CompletionKind.STATX == uring_api.COMPLETION_KIND_STATX
    assert uring_api.CompletionKind.STATX_FDSIZE == uring_api.COMPLETION_KIND_STATX_FDSIZE

def test_statx_st_size_is_native_helper():
    import _uring_api

    assert uring_api.statx_st_size is _uring_api.statx_st_size

def test_native_module_exports_statx_constants():
    assert uring_api.AT_FDCWD == -100
    assert uring_api.AT_EMPTY_PATH == 0x1000
    assert uring_api.STATX_BASIC_STATS == 0x000007FF
    assert uring_api.STATX_SIZE == 0x00000200
    assert uring_api.STATX_BUFFER_SIZE == 256
    assert uring_api.STATX_STX_SIZE_OFFSET == 40

def test_native_module_exports_completion_kind_constants():
    assert uring_api.COMPLETION_KIND_RECV == 1
    assert uring_api.COMPLETION_KIND_SEND == 2
    assert uring_api.COMPLETION_KIND_WAKE == 3
    assert uring_api.COMPLETION_KIND_SENDTO == 4
    assert uring_api.COMPLETION_KIND_RECVMSG == 5
    assert uring_api.COMPLETION_KIND_ACCEPT == 6
    assert uring_api.COMPLETION_KIND_CONNECT == 7
    assert uring_api.COMPLETION_KIND_CANCEL == 8
    assert uring_api.COMPLETION_KIND_SHUTDOWN == 9
    assert uring_api.COMPLETION_KIND_CLOSE == 10
    assert uring_api.COMPLETION_KIND_SENDMSG == 11
    assert uring_api.COMPLETION_KIND_SOCKET == 12
    assert uring_api.COMPLETION_KIND_RECV_MULTISHOT == 13
    assert uring_api.COMPLETION_KIND_SEND_ZC == 14
    assert uring_api.COMPLETION_KIND_SENDMSG_ZC == 15
    assert uring_api.COMPLETION_KIND_RECV_BUF == 16
    assert uring_api.COMPLETION_KIND_POLL == 17
    assert uring_api.COMPLETION_KIND_POLL_MULTISHOT == 18
    assert uring_api.COMPLETION_KIND_POLL_REMOVE == 19
    assert uring_api.COMPLETION_KIND_READ == 20
    assert uring_api.COMPLETION_KIND_WRITE == 21
    assert uring_api.COMPLETION_KIND_OPENAT == 22
    assert uring_api.COMPLETION_KIND_STATX == 23
    assert uring_api.COMPLETION_KIND_STATX_FDSIZE == 24

def test_public_star_exports_include_completion_kind_sendmsg_zc():
    namespace: dict[str, object] = {}

    exec("from uring_api import *", namespace)

    assert namespace["COMPLETION_KIND_SENDMSG_ZC"] == uring_api.COMPLETION_KIND_SENDMSG_ZC
    assert namespace["CompletionKind"] is uring_api.CompletionKind

def test_import_succeeds_when_native_extension_is_unavailable():
    script = """
import builtins
import errno
import sys

original_import = builtins.__import__

def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
    if name == "_uring_api":
        raise ImportError("simulated missing native extension")
    return original_import(name, globals, locals, fromlist, level)

builtins.__import__ = blocked_import
sys.modules.pop("uring_api", None)
sys.modules.pop("_uring_api", None)

import uring_api

probe = uring_api.probe()
assert probe == {}
assert uring_api.is_available() is False
try:
    uring_api.Ring()
except RuntimeError as exc:
    assert "native extension is unavailable" in str(exc)
else:
    raise AssertionError("Ring unexpectedly initialized")
"""
    subprocess.run([sys.executable, "-c", script], check=True)

def test_statx_try_read_st_size_graceful_degradation():
    """Mirror submit_statx_fdsize completion.result when res == 0 but size is absent."""
    client = build_c_api_client()
    buf_with_size = bytearray(uring_api.STATX_BUFFER_SIZE)
    buf_with_size[0:4] = uring_api.STATX_SIZE.to_bytes(4, "little")
    buf_with_size[uring_api.STATX_STX_SIZE_OFFSET : uring_api.STATX_STX_SIZE_OFFSET + 8] = (5).to_bytes(
        8, "little"
    )
    assert client.statx_try_read_st_size(buf_with_size) == 5

    buf_without_size = bytearray(uring_api.STATX_BUFFER_SIZE)
    assert client.statx_try_read_st_size(buf_without_size) is None

def test_statx_st_size_rejects_short_buffer():
    with pytest.raises(ValueError, match="STATX_BUFFER_SIZE"):
        uring_api.statx_st_size(bytearray(32))

def test_statx_st_size_rejects_buffer_without_size_mask():
    buf = bytearray(uring_api.STATX_BUFFER_SIZE)
    with pytest.raises(ValueError, match="STATX_SIZE"):
        uring_api.statx_st_size(buf)

