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


def wait_until_running(ring: uring_api.Ring) -> None:
    deadline = time.monotonic() + 1.0
    while not ring.running and time.monotonic() < deadline:
        time.sleep(0.01)
    assert ring.running


def connect_to_listener(server: socket.socket) -> socket.socket:
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.setblocking(False)
    err = client.connect_ex(server.getsockname())
    assert err in {0, errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY}
    return client


def test_package_is_marked_as_typed():
    assert resources.files("uring_api").joinpath("py.typed").is_file()


def test_uring_api_get_include_points_to_header_dir():
    include_dir = Path(uring_api.get_include())
    header = include_dir / "uring_api_capi.h"

    assert include_dir.is_dir()
    assert header.is_file()


def test_native_module_exports_c_api_constants():
    assert uring_api.C_API_ABI_VERSION == 4
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


def test_probe_returns_structured_result():
    probe = uring_api.probe()

    assert isinstance(probe.available, bool)
    assert isinstance(probe.features, int)
    assert probe.requested_flags == uring_api.DEFAULT_FLAGS
    assert isinstance(probe.active_flags, int)
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


def test_probe_reports_requested_setup_flags():
    flags = uring_api.IORING_SETUP_SINGLE_ISSUER
    probe = uring_api.probe(flags=flags)

    assert probe.requested_flags == flags
    if probe.available:
        assert probe.active_flags & flags == flags


def test_ring_accepts_setup_flags_when_probe_accepts_them():
    require_uring()
    flags = uring_api.IORING_SETUP_SINGLE_ISSUER
    probe = uring_api.probe(flags=flags)
    if not probe.available:
        pytest.skip(f"setup flags are not accepted: errno={probe.errno} message={probe.message}")

    with uring_api.Ring(entries=2, flags=flags) as ring:
        assert ring.sq_entries > 0
        assert ring.cq_entries > 0


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
    assert feature_flags & uring_api.C_API_FEATURE_CORE
    assert (major, minor) == uring_api.__compiled_liburing_version_info__
    assert isinstance(probe["available"], bool)


def test_c_api_ring_new_accepts_setup_flags_when_probe_accepts_them():
    require_uring()
    flags = uring_api.IORING_SETUP_SINGLE_ISSUER
    probe = uring_api.probe(flags=flags)
    if not probe.available:
        pytest.skip(f"setup flags are not accepted: errno={probe.errno} message={probe.message}")

    client = build_c_api_client()
    ring_check, fd, _features, sq_entries, cq_entries, closed, running = client.ring_summary(flags)

    assert ring_check == 1
    assert fd >= 0
    assert sq_entries > 0
    assert cq_entries > 0
    assert closed == 0
    assert running == 0


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
            thread = threading.Thread(target=client.serve_completions, args=(ring,))
            thread.start()
            try:
                buf = bytearray(4)
                ring.submit_recv(reader.fileno(), buf, 220)
                ring.submit_send(writer.fileno(), b"pong", 221)
                deadline = time.monotonic() + 2.0
                while len(c_deliveries) < 2 and time.monotonic() < deadline:
                    time.sleep(0.01)
            finally:
                client.stop_serving(ring)
                thread.join(1.0)
                assert not thread.is_alive()
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


def test_c_api_datagram_operations_when_available():
    require_uring()

    client = build_c_api_client()
    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sender.setblocking(False)
        receiver.setblocking(False)
        sender.bind(("127.0.0.1", 0))
        receiver.bind(("127.0.0.1", 0))
        with uring_api.Ring() as ring:
            buf = bytearray(5)
            client.submit_recvmsg(ring, receiver.fileno(), buf, 230)
            client.submit_sendto(ring, sender.fileno(), b"hello", receiver.getsockname(), 231)

            first = ring.wait(1.0)
            second = ring.wait(1.0)

        assert first is not None
        assert second is not None
        by_user_data = {first.user_data: first, second.user_data: second}
        recv_completion = by_user_data[230]
        send_completion = by_user_data[231]
        assert client.completion_summary(recv_completion) == (230, 5, 0, sender.getsockname())
        assert client.completion_summary(send_completion) == (231, 5, 0, 5)
        assert bytes(buf) == b"hello"
    finally:
        sender.close()
        receiver.close()


def test_c_api_accept_operation_when_available():
    require_uring()

    client_api = build_c_api_client()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client = None
    accepted = None
    try:
        server.setblocking(False)
        server.bind(("127.0.0.1", 0))
        server.listen()
        with uring_api.Ring() as ring:
            client_api.submit_accept(ring, server.fileno(), 240)
            client = connect_to_listener(server)

            completion = ring.wait(1.0)

        assert completion is not None
        user_data, res, flags, result = client_api.completion_summary(completion)
        accepted_fd, address = result
        accepted = socket.socket(fileno=accepted_fd)
        assert user_data == 240
        assert res == accepted_fd
        assert flags == 0
        assert address == client.getsockname()
    finally:
        if accepted is not None:
            accepted.close()
        if client is not None:
            client.close()
        server.close()


def test_c_api_connect_operation_when_available():
    require_uring()

    client_api = build_c_api_client()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    accepted = None
    try:
        server.setblocking(False)
        server.bind(("127.0.0.1", 0))
        server.listen()
        client.setblocking(False)
        with uring_api.Ring() as ring:
            client_api.submit_connect(ring, client.fileno(), server.getsockname(), 241)

            completion = ring.wait(1.0)

        assert completion is not None
        assert client_api.completion_summary(completion) == (241, 0, 0, None)
        accepted, _address = server.accept()
    finally:
        if accepted is not None:
            accepted.close()
        client.close()
        server.close()


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


def test_ring_accept_completion_when_available():
    require_uring()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client = None
    accepted = None
    try:
        server.setblocking(False)
        server.bind(("127.0.0.1", 0))
        server.listen()
        token = {"operation": "accept"}
        with uring_api.Ring() as ring:
            ring.submit_accept(server.fileno(), token)
            client = connect_to_listener(server)

            completion = ring.wait(1.0)

        assert completion is not None
        accepted_fd, address = completion.result
        accepted = socket.socket(fileno=accepted_fd)
        assert completion.user_data is token
        assert completion.res == accepted_fd
        assert completion.flags == 0
        assert address == client.getsockname()
    finally:
        if accepted is not None:
            accepted.close()
        if client is not None:
            client.close()
        server.close()


def test_ring_connect_completion_when_available():
    require_uring()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    accepted = None
    try:
        server.setblocking(False)
        server.bind(("127.0.0.1", 0))
        server.listen()
        client.setblocking(False)
        token = {"operation": "connect"}
        with uring_api.Ring() as ring:
            ring.submit_connect(client.fileno(), server.getsockname(), token)

            completion = ring.wait(1.0)

        assert completion is not None
        assert completion.user_data is token
        assert completion.res == 0
        assert completion.flags == 0
        assert completion.result is None
        accepted, _address = server.accept()
    finally:
        if accepted is not None:
            accepted.close()
        client.close()
        server.close()


def test_ring_sendto_completion_when_available():
    require_uring()

    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        receiver.bind(("127.0.0.1", 0))
        receiver.setblocking(False)
        sender.setblocking(False)
        token = {"operation": "sendto"}
        with uring_api.Ring() as ring:
            ring.submit_sendto(sender.fileno(), b"hello", receiver.getsockname(), token)

            completion = ring.wait(1.0)

        assert completion is not None
        assert completion.user_data is token
        assert completion.res == 5
        assert completion.result == 5
        data, address = receiver.recvfrom(5)
        assert data == b"hello"
        assert address[1] == sender.getsockname()[1]
    finally:
        sender.close()
        receiver.close()


def test_ring_recvmsg_completion_when_available():
    require_uring()

    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        receiver.bind(("127.0.0.1", 0))
        receiver.setblocking(False)
        sender.setblocking(False)
        buf = bytearray(5)
        token = {"operation": "recvmsg"}
        with uring_api.Ring() as ring:
            ring.submit_recvmsg(receiver.fileno(), buf, token)
            sender.sendto(b"world", receiver.getsockname())

            completion = ring.wait(1.0)

        assert completion is not None
        assert completion.user_data is token
        assert completion.res == 5
        assert completion.result[1] == sender.getsockname()[1]
        assert bytes(buf) == b"world"
    finally:
        sender.close()
        receiver.close()


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


def test_ring_serve_completions_invokes_callback_when_available():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        delivered = threading.Event()
        completions: list[uring_api.Completion] = []

        with uring_api.Ring() as ring:
            ring.callback = lambda completion: (completions.append(completion), delivered.set())
            thread = threading.Thread(target=ring.serve_completions)
            thread.start()
            wait_until_running(ring)

            with pytest.raises(RuntimeError, match="completion service is active"):
                ring.wait(0)

            buf = bytearray(5)
            ring.submit_recv(reader.fileno(), buf, 125)
            writer.send(b"hello")
            assert delivered.wait(1.0)

            ring.stop_serving()
            thread.join(1.0)
            assert not thread.is_alive()
            assert not ring.running

            ring.reset_serving()
            thread = threading.Thread(target=ring.serve_completions)
            thread.start()
            wait_until_running(ring)
            ring.stop_serving()
            thread.join(1.0)
            assert not thread.is_alive()
            assert not ring.running

        assert completions
        assert completions[0].user_data == 125
        assert completions[0].res == 5
        assert completions[0].result == 5
        assert bytes(buf) == b"hello"
    finally:
        reader.close()
        writer.close()


def test_ring_serve_completions_delivers_socketpair_round_trip_when_available():
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
            thread = threading.Thread(target=ring.serve_completions)
            thread.start()
            recv_buf = bytearray(4)
            ring.submit_recv(left.fileno(), recv_buf, 132)
            ring.submit_send(right.fileno(), b"pong", 133)

            assert delivered.wait(1.0)
            ring.stop_serving()
            thread.join(1.0)
            assert not thread.is_alive()

        by_user_data = {completion.user_data: completion for completion in completions}
        assert by_user_data[132].res == 4
        assert by_user_data[132].result == 4
        assert bytes(recv_buf) == b"pong"
        assert by_user_data[133].res == 4
        assert by_user_data[133].result == 4
    finally:
        left.close()
        right.close()


def test_ring_serving_workers_can_dispatch_while_another_callback_blocks_when_available():
    require_uring()

    left, right = socket.socketpair()
    try:
        left.setblocking(False)
        right.setblocking(False)
        first_callback_blocking = threading.Event()
        release_first_callback = threading.Event()
        delivered_two = threading.Event()
        completions: list[uring_api.Completion] = []
        lock = threading.Lock()

        def callback(completion):
            with lock:
                completions.append(completion)
                count = len(completions)
            if count == 1:
                first_callback_blocking.set()
                release_first_callback.wait(2.0)
            elif count == 2:
                delivered_two.set()
                release_first_callback.set()

        with uring_api.Ring() as ring:
            ring.callback = callback
            threads = [threading.Thread(target=ring.serve_completions) for _ in range(2)]
            for thread in threads:
                thread.start()
            first_buf = bytearray(1)
            second_buf = bytearray(1)
            ring.submit_recv(left.fileno(), first_buf, 140)
            ring.submit_recv(left.fileno(), second_buf, 141)
            right.send(b"xy")

            assert first_callback_blocking.wait(1.0)
            assert delivered_two.wait(1.0)
            ring.stop_serving()
            for thread in threads:
                thread.join(1.0)
                assert not thread.is_alive()

        by_user_data = {completion.user_data: completion for completion in completions}
        assert by_user_data[140].result == 1
        assert by_user_data[141].result == 1
        assert {bytes(first_buf), bytes(second_buf)} == {b"x", b"y"}
    finally:
        release_first_callback.set()
        left.close()
        right.close()


def test_ring_serve_completions_writes_unraisable_and_exits_when_callback_fails():
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
            thread = threading.Thread(target=ring.serve_completions)
            thread.start()
            ring.submit_recv(reader.fileno(), bytearray(1), 126)
            writer.send(b"x")

            assert unraisable.wait(1.0)
            thread.join(1.0)
            assert not thread.is_alive()
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


def test_ring_serve_completions_requires_callback_when_available():
    require_uring()

    with uring_api.Ring() as ring:
        with pytest.raises(RuntimeError, match="delivery callback is not set"):
            ring.serve_completions()


def test_ring_reset_serving_clears_stop_flag_when_available():
    require_uring()

    with uring_api.Ring() as ring:
        calls = 0

        def callback(completion):
            nonlocal calls
            calls += 1

        ring.callback = callback
        ring.stop_serving()
        ring.serve_completions()
        assert calls == 0
        ring.reset_serving()
        thread = threading.Thread(target=ring.serve_completions)
        thread.start()
        wait_until_running(ring)
        ring.stop_serving()
        thread.join(1.0)
        assert not thread.is_alive()


def test_ring_rejects_callback_change_while_completion_service_runs_when_available():
    require_uring()

    with uring_api.Ring() as ring:
        ring.callback = lambda completion: None
        thread = threading.Thread(target=ring.serve_completions)
        thread.start()
        try:
            with pytest.raises(RuntimeError, match="cannot change callback while completion service is active"):
                ring.callback = lambda completion: None
        finally:
            ring.stop_serving()
            thread.join(1.0)
            assert not thread.is_alive()
