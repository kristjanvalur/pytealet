import errno
import importlib.util
from importlib import resources
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import sysconfig
import tempfile
import time
import threading

import pytest

import uring_api


def require_uring():
    probe = uring_api.probe()
    if not probe.available:
        pytest.skip(f"io_uring is not available: errno={probe.errno} message={probe.message}")


def test_package_is_marked_as_typed():
    assert resources.files("uring_api").joinpath("py.typed").is_file()


def test_uring_api_get_include_points_to_header_dir():
    include_dir = Path(uring_api.get_include())
    header = include_dir / "uring_api_capi.h"

    assert include_dir.is_dir()
    assert header.is_file()


def test_native_module_exports_c_api_constants():
    assert uring_api.C_API_ABI_VERSION == 2
    assert uring_api.C_API_FEATURE_PROBE == 1 << 0
    assert uring_api.C_API_FEATURE_RING == 1 << 1
    assert uring_api.C_API_FEATURE_C_CALLBACK == 1 << 2
    assert uring_api.C_API_FEATURE_COMPLETION == 1 << 3
    assert uring_api.C_API_FEATURES & uring_api.C_API_FEATURE_PROBE
    assert uring_api.C_API_FEATURES & uring_api.C_API_FEATURE_RING
    assert uring_api.C_API_FEATURES & uring_api.C_API_FEATURE_C_CALLBACK
    assert uring_api.C_API_FEATURES & uring_api.C_API_FEATURE_COMPLETION


def test_probe_returns_structured_result():
    probe = uring_api.probe()

    assert isinstance(probe.available, bool)
    assert isinstance(probe.features, int)
    assert isinstance(probe.sq_entries, int)
    assert isinstance(probe.cq_entries, int)
    assert probe.liburing_version
    assert probe.compiled_liburing_version == uring_api.__compiled_liburing_version__
    assert probe.compiled_liburing_version == uring_api.__liburing_version__
    assert probe.compiled_liburing_version_info == uring_api.__compiled_liburing_version_info__
    assert len(probe.compiled_liburing_version_info) == 2
    assert all(isinstance(part, int) for part in probe.compiled_liburing_version_info)
    assert probe.compiled_liburing_version_info >= (2, 4)
    if probe.available:
        assert probe.errno is None
        assert probe.message is None
        assert probe.sq_entries > 0
        assert probe.cq_entries > 0
    else:
        assert probe.errno is not None
        assert probe.message


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
assert probe.available is False
assert probe.errno == errno.ENOSYS
assert probe.message and "simulated missing native extension" in probe.message
assert probe.compiled_liburing_version_info == (0, 0)
assert uring_api.is_available() is False
try:
    uring_api.Ring()
except RuntimeError as exc:
    assert "native extension is unavailable" in str(exc)
else:
    raise AssertionError("Ring unexpectedly initialized")
"""
    subprocess.run([sys.executable, "-c", script], check=True)


def test_c_api_client_can_import_capsule_and_probe():
    client = build_c_api_client()

    abi_version, struct_size, feature_flags, major, minor = client.metadata()
    probe = client.probe()

    assert abi_version == uring_api.C_API_ABI_VERSION
    assert struct_size > 0
    assert feature_flags & uring_api.C_API_FEATURE_PROBE
    assert feature_flags & uring_api.C_API_FEATURE_RING
    assert feature_flags & uring_api.C_API_FEATURE_C_CALLBACK
    assert feature_flags & uring_api.C_API_FEATURE_COMPLETION
    assert (major, minor) == uring_api.__compiled_liburing_version_info__
    assert isinstance(probe["available"], bool)


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
                "-shared",
                "-fPIC",
                "-I",
                python_include,
                "-I",
                str(include_dir),
                str(source_path),
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


def test_c_api_client_can_create_ring_when_available():
    require_uring()

    client = build_c_api_client()
    is_ring, fd, features, sq_entries, cq_entries, closed, running = client.ring_summary()

    assert is_ring == 1
    assert fd >= 0
    assert features >= 0
    assert sq_entries > 0
    assert cq_entries > 0
    assert closed == 0
    assert running == 0


def test_c_api_callback_is_preferred_over_python_callback_when_available():
    require_uring()

    client = build_c_api_client()
    c_deliveries = []
    python_deliveries = []
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            client.set_c_callback(ring, c_deliveries)
            ring.callback = python_deliveries.append
            ring.start()
            try:
                buf = bytearray(4)
                ring.submit_recv(reader.fileno(), buf, 220)
                ring.submit_send(writer.fileno(), b"pong", 221)
                deadline = time.monotonic() + 2.0
                while len(c_deliveries) < 2 and time.monotonic() < deadline:
                    time.sleep(0.01)
            finally:
                ring.stop()
                client.clear_c_callback(ring)

        by_user_data = {completion.user_data: completion for completion in c_deliveries}
        assert client.completion_summary(by_user_data[220]) == (220, 4, 0, 4)
        assert client.completion_summary(by_user_data[221]) == (221, 4, 0, 4)
        assert by_user_data[220].res == 4
        assert by_user_data[220].result == 4
        assert bytes(buf) == b"pong"
        assert by_user_data[221].res == 4
        assert by_user_data[221].result == 4
        assert python_deliveries == []
    finally:
        reader.close()
        writer.close()


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


def test_probe_rejects_invalid_entries():
    with pytest.raises(ValueError):
        uring_api.probe(0)


def test_ring_raises_oserror_or_initializes():
    try:
        ring = uring_api.Ring(2)
    except OSError as exc:
        assert exc.errno in {errno.ENOSYS, errno.EPERM, errno.EOPNOTSUPP, errno.ENOMEM, errno.EMFILE, errno.ENFILE}
    else:
        ring.close()


def test_ring_recv_completion_when_available():
    require_uring()

    token = object()
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        buf = bytearray(5)
        with uring_api.Ring() as ring:
            ring.submit_recv(reader.fileno(), buf, token)
            writer.send(b"hello")

            completion = ring.wait(1.0)

        assert completion is not None
        assert isinstance(completion, uring_api.Completion)
        assert completion.user_data is token
        assert completion.res == 5
        assert completion.flags == 0
        assert completion.result == 5
        assert bytes(buf) == b"hello"
    finally:
        reader.close()
        writer.close()


def test_ring_send_completion_when_available():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            token = {"operation": "send"}
            ring.submit_send(writer.fileno(), b"hello", token)

            completion = ring.wait(1.0)

        assert completion is not None
        assert completion.user_data is token
        assert completion.res == 5
        assert completion.result == 5
        assert reader.recv(5) == b"hello"
    finally:
        reader.close()
        writer.close()


def test_ring_socketpair_round_trip_when_available():
    require_uring()

    left, right = socket.socketpair()
    try:
        left.setblocking(False)
        right.setblocking(False)
        recv_buf = bytearray(4)
        with uring_api.Ring() as ring:
            ring.submit_recv(left.fileno(), recv_buf, 130)
            ring.submit_send(right.fileno(), b"ping", 131)

            completions = []
            while len(completions) < 2:
                completion = ring.wait(1.0)
                assert completion is not None
                completions.append(completion)

        by_user_data = {completion.user_data: completion for completion in completions}
        assert by_user_data[130].res == 4
        assert by_user_data[130].result == 4
        assert bytes(recv_buf) == b"ping"
        assert by_user_data[131].res == 4
        assert by_user_data[131].result == 4
    finally:
        left.close()
        right.close()


def test_ring_break_wait_interrupts_wait_when_available():
    require_uring()

    with uring_api.Ring() as ring:
        results: list[object] = []
        thread = threading.Thread(target=lambda: results.append(ring.wait(10.0)))
        thread.start()
        ring.break_wait()
        thread.join(1.0)
        if thread.is_alive():
            ring.break_wait()
            thread.join(1.0)

    assert thread.is_alive() is False
    assert results == [None]


def test_ring_rejects_concurrent_wait_when_available():
    require_uring()

    with uring_api.Ring() as ring:
        started = threading.Event()
        results: list[object] = []
        errors: list[BaseException] = []

        def wait_in_thread():
            started.set()
            try:
                results.append(ring.wait(10.0))
            except BaseException as exc:  # pragma: no cover - reported below
                errors.append(exc)

        thread = threading.Thread(target=wait_in_thread)
        thread.start()
        assert started.wait(1.0)

        for _ in range(10000):
            try:
                completion = ring.wait(0)
            except RuntimeError as exc:
                assert "another wait is already active" in str(exc)
                break
            assert completion is None
        else:
            pytest.fail("concurrent wait was not rejected")

        ring.break_wait()
        thread.join(1.0)
        if thread.is_alive():
            ring.break_wait()
            thread.join(1.0)

    assert thread.is_alive() is False
    assert errors == []
    assert results == [None]


def test_ring_delivery_thread_invokes_callback_when_available():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        delivered = threading.Event()
        completions: list[uring_api.Completion] = []

        with uring_api.Ring() as ring:
            ring.callback = lambda completion: (completions.append(completion), delivered.set())
            ring.start()
            assert ring.running

            with pytest.raises(RuntimeError, match="delivery thread is active"):
                ring.wait(0)

            buf = bytearray(5)
            ring.submit_recv(reader.fileno(), buf, 125)
            writer.send(b"hello")
            assert delivered.wait(1.0)

            ring.stop()
            assert not ring.running

            ring.start()
            assert ring.running
            ring.stop()
            assert not ring.running

        assert completions
        assert completions[0].user_data == 125
        assert completions[0].res == 5
        assert completions[0].result == 5
        assert bytes(buf) == b"hello"
    finally:
        reader.close()
        writer.close()


def test_ring_delivery_thread_delivers_socketpair_round_trip_when_available():
    require_uring()

    left, right = socket.socketpair()
    try:
        left.setblocking(False)
        right.setblocking(False)
        delivered = threading.Event()
        completions: list[uring_api.Completion] = []

        def callback(completion):
            completions.append(completion)
            if len(completions) == 2:
                delivered.set()

        with uring_api.Ring() as ring:
            ring.callback = callback
            ring.start()
            recv_buf = bytearray(4)
            ring.submit_recv(left.fileno(), recv_buf, 132)
            ring.submit_send(right.fileno(), b"pong", 133)

            assert delivered.wait(1.0)
            ring.stop()

        by_user_data = {completion.user_data: completion for completion in completions}
        assert by_user_data[132].res == 4
        assert by_user_data[132].result == 4
        assert bytes(recv_buf) == b"pong"
        assert by_user_data[133].res == 4
        assert by_user_data[133].result == 4
    finally:
        left.close()
        right.close()


def test_ring_delivery_thread_writes_unraisable_and_exits_when_callback_fails():
    require_uring()

    reader, writer = socket.socketpair()
    old_hook = sys.unraisablehook
    unraisable = threading.Event()
    reports: list[object] = []

    def hook(args):
        reports.append(args.object)
        unraisable.set()

    def fail_callback(completion):
        raise RuntimeError("callback failed")

    try:
        sys.unraisablehook = hook
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            ring.callback = fail_callback
            ring.start()
            ring.submit_recv(reader.fileno(), bytearray(1), 126)
            writer.send(b"x")

            assert unraisable.wait(1.0)
            ring.stop()
            assert not ring.running

        assert reports == [ring]
    finally:
        sys.unraisablehook = old_hook
        reader.close()
        writer.close()


def test_ring_callback_property_validation_when_available():
    require_uring()

    def callback(completion):
        return None

    with uring_api.Ring() as ring:
        assert ring.callback is None
        ring.callback = callback
        assert ring.callback is callback
        ring.callback = None
        assert ring.callback is None

        with pytest.raises(TypeError, match="callback must be callable or None"):
            ring.callback = object()
        with pytest.raises(TypeError, match="cannot delete callback"):
            del ring.callback


def test_ring_delivery_thread_requires_callback_when_available():
    require_uring()

    with uring_api.Ring() as ring:
        with pytest.raises(RuntimeError, match="delivery callback is not set"):
            ring.start()


def test_ring_rejects_callback_change_while_delivery_thread_runs_when_available():
    require_uring()

    with uring_api.Ring() as ring:
        ring.callback = lambda completion: None
        ring.start()
        try:
            with pytest.raises(RuntimeError, match="cannot change callback while delivery thread is running"):
                ring.callback = lambda completion: None
        finally:
            ring.stop()
