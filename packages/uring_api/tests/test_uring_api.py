import errno
import fcntl
import gc
import importlib.util
import os
from importlib import resources
from pathlib import Path
import shutil
import shlex
import socket
import subprocess
import sys
import sysconfig
import tempfile
import threading
import time
import weakref

import pytest

import uring_api


def require_uring():
    probe = uring_api.probe()
    if not probe:
        pytest.skip("io_uring is not available")


def require_uring_capability(name: str) -> None:
    probe = uring_api.probe()
    if not probe:
        pytest.skip("io_uring is not available")
    if not probe.get(name, False):
        pytest.skip(f"{name} is not supported")


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
    source = '#include "uring_api_capi.h"\nstatic const unsigned int abi = URING_API_CAPI_ABI_VERSION;\n'

    with tempfile.TemporaryDirectory() as temp_dir:
        source_path = Path(temp_dir) / "check_uring_api_capi.c"
        object_path = Path(temp_dir) / "check_uring_api_capi.o"
        source_path.write_text(source, encoding="utf-8")
        subprocess.run(
            [*cc_argv, "-c", str(source_path), "-o", str(object_path), "-I", str(include_dir), "-I", str(python_include)],
            check=True,
        )


def test_native_module_exports_c_api_constants():
    assert uring_api.C_API_ABI_VERSION == 1
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


def test_public_star_exports_include_completion_kind_sendmsg_zc():
    namespace: dict[str, object] = {}

    exec("from uring_api import *", namespace)

    assert namespace["COMPLETION_KIND_SENDMSG_ZC"] == uring_api.COMPLETION_KIND_SENDMSG_ZC


def test_probe_returns_structured_result():
    probe = uring_api.probe()

    assert set(probe) == {
        "available",
        "IORING_ACCEPT_MULTISHOT",
        "IORING_RECV_MULTISHOT",
        "IORING_OP_SEND_ZC",
        "IORING_OP_SENDMSG_ZC",
        "IORING_OP_SOCKET",
    }
    assert probe["available"] is True
    assert isinstance(probe["IORING_ACCEPT_MULTISHOT"], bool)
    assert isinstance(probe["IORING_RECV_MULTISHOT"], bool)
    assert isinstance(probe["IORING_OP_SEND_ZC"], bool)
    assert isinstance(probe["IORING_OP_SENDMSG_ZC"], bool)
    assert isinstance(probe["IORING_OP_SOCKET"], bool)


def test_probe_reports_requested_setup_flags():
    flags = uring_api.IORING_SETUP_SINGLE_ISSUER
    probe = uring_api.probe(flags=flags)

    if probe:
        assert probe["available"] is True


def test_ring_accepts_setup_flags_when_probe_accepts_them():
    require_uring()
    flags = uring_api.IORING_SETUP_SINGLE_ISSUER
    probe = uring_api.probe(flags=flags)
    if not probe:
        pytest.skip("setup flags are not accepted")

    with uring_api.Ring(entries=2, flags=flags) as ring:
        assert ring.sq_entries > 0
        assert ring.cq_entries > 0


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
            assert ring.wait(timeout=1.0).res == 1
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


def test_c_api_client_can_import_capsule_and_probe():
    client = build_c_api_client()

    abi_version, struct_size, feature_flags, major, minor = client.metadata()
    probe = client.probe()

    assert abi_version == uring_api.C_API_ABI_VERSION
    assert struct_size > 0
    assert feature_flags & uring_api.C_API_FEATURE_CORE
    assert (major, minor) == uring_api.__compiled_liburing_version_info__
    assert probe == uring_api.probe()


def test_c_api_ring_new_accepts_setup_flags_when_probe_accepts_them():
    require_uring()
    flags = uring_api.IORING_SETUP_SINGLE_ISSUER
    probe = uring_api.probe(flags=flags)
    if not probe:
        pytest.skip("setup flags are not accepted")

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
            client.submit_sendto(ring, sender.fileno(), b"hello", receiver.getsockname(), 0, 231)

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


def test_c_api_sendmsg_operation_when_available():
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
            client.submit_sendmsg(ring, sender.fileno(), b"hello", receiver.getsockname(), 0, 244)
            completion = ring.wait(1.0)

        assert completion is not None
        assert client.completion_summary(completion) == (244, 5, 0, 5)
        assert client.completion_sequence(completion) == 0
        assert completion.kind == uring_api.COMPLETION_KIND_SENDMSG
        data, address = receiver.recvfrom(5)
        assert data == b"hello"
        assert address[1] == sender.getsockname()[1]
    finally:
        sender.close()
        receiver.close()


def test_c_api_sendmsg_zc_operation_when_available():
    require_uring_capability("IORING_OP_SENDMSG_ZC")

    client = build_c_api_client()
    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sender.setblocking(False)
        receiver.setblocking(False)
        sender.bind(("127.0.0.1", 0))
        receiver.bind(("127.0.0.1", 0))
        with uring_api.Ring() as ring:
            client.submit_sendmsg_zc(ring, sender.fileno(), b"hello", receiver.getsockname(), 0, 245)
            completion = ring.wait(1.0)
            notification = ring.wait(1.0)

        assert completion is not None
        assert client.completion_summary(completion) == (245, 5, completion.flags, 5)
        assert client.completion_sequence(completion) == 0
        assert completion.kind == uring_api.COMPLETION_KIND_SENDMSG_ZC
        assert not (completion.flags & uring_api.IORING_CQE_F_NOTIF)
        assert notification is None
        data, address = receiver.recvfrom(5)
        assert data == b"hello"
        assert address[1] == sender.getsockname()[1]
    finally:
        sender.close()
        receiver.close()


def test_c_api_send_zc_operation_when_available():
    require_uring_capability("IORING_OP_SEND_ZC")

    client = build_c_api_client()
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring() as ring:
            client.submit_send_zc(ring, writer.fileno(), b"hello", 0, 0, 246)
            completion = ring.wait(1.0)
            notification = ring.wait(1.0)

        assert completion is not None
        assert client.completion_summary(completion) == (246, 5, completion.flags, 5)
        assert completion.kind == uring_api.COMPLETION_KIND_SEND_ZC
        assert not (completion.flags & uring_api.IORING_CQE_F_NOTIF)
        assert notification is None
        assert reader.recv(5) == b"hello"
    finally:
        reader.close()
        writer.close()


def test_c_api_socket_operation_when_available():
    require_uring()

    client_api = build_c_api_client()
    sock = None
    with uring_api.Ring() as ring:
        client_api.submit_socket(ring, socket.AF_INET, socket.SOCK_STREAM, 0, 0, 245)
        completion = ring.wait(1.0)

    assert completion is not None
    assert completion.kind == uring_api.COMPLETION_KIND_SOCKET
    if completion.res < 0:
        errno_value = -completion.res
        if errno_value in {errno.ENOSYS, errno.EOPNOTSUPP, errno.EINVAL}:
            pytest.skip(f"IORING_OP_SOCKET is not supported: errno {errno_value}")
    assert completion.res >= 0
    try:
        user_data, res, flags, result = client_api.completion_summary(completion)
        assert user_data == 245
        assert res == completion.res
        assert flags == 0
        assert result == completion.res
        sock = socket.socket(fileno=completion.res)
        assert sock.family == socket.AF_INET
        assert sock.type & socket.SOCK_STREAM
    finally:
        if sock is not None:
            sock.close()
        elif completion.res >= 0:
            os.close(completion.res)


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
            client_api.submit_accept(ring, server.fileno(), 240, socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC)
            client = connect_to_listener(server)

            completion = ring.wait(1.0)

        assert completion is not None
        user_data, res, flags, result = client_api.completion_summary(completion)
        accepted_fd, address = result
        assert_fd_nonblocking_cloexec(accepted_fd)
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


def test_c_api_recv_multishot_operation_when_available():
    require_uring()

    client_api = build_c_api_client()
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            try:
                client_api.submit_recv_multishot(ring, reader.fileno(), 8, 4, 0, 246)
            except OSError as exc:
                if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    pytest.skip(f"recv multishot buffers are not supported: errno {exc.errno}")
                raise
            writer.send(b"hello")
            completion = ring.wait(1.0)

        assert completion is not None
        if completion.res < 0:
            errno_value = -completion.res
            if errno_value in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP, errno.ENOBUFS}:
                pytest.skip(f"recv multishot is not supported: errno {errno_value}")
        user_data, res, _flags, result = client_api.completion_summary(completion)
        assert user_data == 246
        assert res == len(result)
        assert result == b"hello"
        assert client_api.completion_sequence(completion) == 0
    finally:
        reader.close()
        writer.close()


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


def test_c_api_shutdown_operation_when_available():
    require_uring()

    client_api = build_c_api_client()
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            client_api.submit_shutdown(ring, writer.fileno(), socket.SHUT_WR, 242)
            completion = ring.wait(1.0)

        assert completion is not None
        assert client_api.completion_summary(completion) == (242, 0, 0, None)
        assert completion.kind == uring_api.COMPLETION_KIND_SHUTDOWN
        assert reader.recv(1) == b""
    finally:
        reader.close()
        writer.close()


def test_c_api_close_operation_when_available():
    require_uring()

    client_api = build_c_api_client()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fd = sock.detach()
    with uring_api.Ring() as ring:
        client_api.submit_close(ring, fd, 243)
        completion = ring.wait(1.0)

    assert completion is not None
    assert client_api.completion_summary(completion) == (243, 0, 0, None)
    assert completion.kind == uring_api.COMPLETION_KIND_CLOSE
    with pytest.raises(OSError) as excinfo:
        os.fstat(fd)
    assert excinfo.value.errno == errno.EBADF


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
            pending = ring.submit_recv(reader.fileno(), buf, token)
            writer.send(b"hello")

            assert isinstance(pending, uring_api.Completion)
            assert pending.user_data is token
            assert pending.kind == uring_api.COMPLETION_KIND_RECV
            assert pending.res == 0
            assert pending.flags == 0
            assert pending.result is None

            completion = ring.wait(1.0)

        assert completion is not None
        assert completion is pending
        assert isinstance(completion, uring_api.Completion)
        assert completion.user_data is token
        assert completion.res == 5
        assert completion.flags == 0
        assert completion.result == 5
        assert completion.sequence == 0
        assert bytes(buf) == b"hello"
    finally:
        reader.close()
        writer.close()


def test_c_api_completion_result_is_none_for_pending_completion_when_available():
    require_uring()

    client = build_c_api_client()
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        buf = bytearray(5)
        with uring_api.Ring() as ring:
            pending = ring.submit_recv(reader.fileno(), buf, 247)

            assert client.completion_summary(pending) == (247, 0, 0, None)

            writer.send(b"hello")
            completion = ring.wait(1.0)

        assert completion is pending
        assert client.completion_summary(completion) == (247, 5, 0, 5)
    finally:
        reader.close()
        writer.close()


def test_ring_recv_multishot_completion_when_available():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        token = {"operation": "recv-multishot"}
        with uring_api.Ring() as ring:
            try:
                handle = ring.submit_recv_multishot(reader.fileno(), 8, 4, token)
            except OSError as exc:
                if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    pytest.skip(f"recv multishot buffers are not supported: errno {exc.errno}")
                raise

            writer.send(b"hello")
            first = ring.wait(1.0)
            writer.send(b"world")
            second = ring.wait(1.0)

            assert first is not None
            assert second is not None
            assert handle.result is None
            for sequence, completion, expected in ((0, first, b"hello"), (1, second, b"world")):
                if completion.res < 0:
                    errno_value = -completion.res
                    if errno_value in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP, errno.ENOBUFS}:
                        pytest.skip(f"recv multishot is not supported: errno {errno_value}")
                assert completion is not handle
                assert completion.kind == uring_api.COMPLETION_KIND_RECV_MULTISHOT
                assert completion.user_data is token
                assert completion.sequence == sequence
                assert completion.result == expected
                assert completion.res == len(expected)
                assert completion.flags & uring_api.IORING_CQE_F_MORE

            ring.submit_cancel(handle)
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                completion = ring.wait(0.0)
                if completion is handle:
                    break
    finally:
        reader.close()
        writer.close()


def test_ring_recv_multishot_eof_returns_empty_bytes_when_available():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        token = {"operation": "recv-multishot-eof"}
        with uring_api.Ring() as ring:
            try:
                handle = ring.submit_recv_multishot(reader.fileno(), 8, 4, token)
            except OSError as exc:
                if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    pytest.skip(f"recv multishot buffers are not supported: errno {exc.errno}")
                raise

            writer.send(b"hello")
            first = ring.wait(1.0)
            writer.close()
            final = ring.wait(1.0)

        assert first is not None
        if first.res < 0:
            errno_value = -first.res
            if errno_value in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP, errno.ENOBUFS}:
                pytest.skip(f"recv multishot is not supported: errno {errno_value}")
        assert first is not handle
        assert first.sequence == 0
        assert first.result == b"hello"
        assert first.res == 5
        assert first.flags & uring_api.IORING_CQE_F_MORE

        assert final is handle
        assert final.kind == uring_api.COMPLETION_KIND_RECV_MULTISHOT
        assert final.user_data is token
        assert final.sequence == 1
        assert final.res == 0
        assert final.result == b""
        assert not (final.flags & uring_api.IORING_CQE_F_MORE)
    finally:
        reader.close()
        writer.close()


def test_ring_cancel_unknown_completion_reports_cancel_completion_when_available():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        buf = bytearray(5)
        with uring_api.Ring() as ring:
            target = ring.submit_recv(reader.fileno(), buf, "target")
            writer.send(b"hello")
            assert ring.wait(1.0) is target

            cancel = ring.submit_cancel(target)
            completion = ring.wait(1.0)

        assert completion is cancel
        assert cancel.user_data is target
        assert cancel.kind == uring_api.COMPLETION_KIND_CANCEL
        assert cancel.res < 0
        assert cancel.result is None
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


def test_ring_send_zc_completion_when_available():
    require_uring_capability("IORING_OP_SEND_ZC")

    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring() as ring:
            token = {"operation": "send_zc"}
            pending = ring.submit_send_zc(writer.fileno(), b"hello", token)

            completion = ring.wait(1.0)
            notification = ring.wait(1.0)

        assert completion is pending
        assert completion.user_data is token
        assert completion.kind == uring_api.COMPLETION_KIND_SEND_ZC
        assert completion.res == 5
        assert completion.result == 5
        assert not (completion.flags & uring_api.IORING_CQE_F_NOTIF)
        assert notification is None
        assert reader.recv(5) == b"hello"
    finally:
        reader.close()
        writer.close()


def test_ring_sendmsg_zc_completion_when_available():
    require_uring_capability("IORING_OP_SENDMSG_ZC")

    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sender.setblocking(False)
        receiver.setblocking(False)
        sender.bind(("127.0.0.1", 0))
        receiver.bind(("127.0.0.1", 0))
        with uring_api.Ring() as ring:
            token = {"operation": "sendmsg_zc"}
            pending = ring.submit_sendmsg_zc(sender.fileno(), b"hello", receiver.getsockname(), token)

            completion = ring.wait(1.0)
            notification = ring.wait(1.0)

        assert completion is pending
        assert completion.user_data is token
        assert completion.kind == uring_api.COMPLETION_KIND_SENDMSG_ZC
        assert completion.res == 5
        assert completion.result == 5
        assert not (completion.flags & uring_api.IORING_CQE_F_NOTIF)
        assert notification is None
        data, address = receiver.recvfrom(5)
        assert data == b"hello"
        assert address[1] == sender.getsockname()[1]
    finally:
        sender.close()
        receiver.close()


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
            ring.submit_accept(server.fileno(), token, flags=socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC)
            client = connect_to_listener(server)

            completion = ring.wait(1.0)

        assert completion is not None
        accepted_fd, address = completion.result
        assert_fd_nonblocking_cloexec(accepted_fd)
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


def test_ring_accept_multishot_completion_when_available():
    require_uring()
    if not uring_api.probe().get("IORING_ACCEPT_MULTISHOT", False):
        pytest.skip("IORING_ACCEPT_MULTISHOT is not available")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clients = []
    accepted = []
    try:
        server.setblocking(False)
        server.bind(("127.0.0.1", 0))
        server.listen()
        token = {"operation": "accept-multishot"}
        with uring_api.Ring() as ring:
            handle = ring.submit_accept_multishot(
                server.fileno(), token, flags=socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC
            )
            clients.append(connect_to_listener(server))
            first = ring.wait(1.0)
            clients.append(connect_to_listener(server))
            second = ring.wait(1.0)

            assert first is not None
            assert second is not None
            assert handle.result is None
            for sequence, completion, client in ((0, first, clients[0]), (1, second, clients[1])):
                if completion.res < 0:
                    errno_value = -completion.res
                    if errno_value in {errno.EINVAL, errno.EOPNOTSUPP, errno.ENOSYS}:
                        pytest.skip(f"IORING_ACCEPT_MULTISHOT is not supported: errno {errno_value}")
                assert completion is not handle
                assert completion.kind == uring_api.COMPLETION_KIND_ACCEPT
                assert completion.user_data is token
                assert completion.sequence == sequence
                assert completion.flags & uring_api.IORING_CQE_F_MORE
                accepted_fd, address = completion.result
                assert_fd_nonblocking_cloexec(accepted_fd)
                accepted.append(socket.socket(fileno=accepted_fd))
                assert completion.res == accepted_fd
                assert address == client.getsockname()

            ring.submit_cancel(handle)
            cancelled = False
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                completion = ring.wait(0.0)
                if completion is None:
                    continue
                if completion is handle:
                    cancelled = True
                    break
            assert cancelled
    finally:
        for sock in accepted:
            sock.close()
        for client in clients:
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


def test_ring_shutdown_completion_when_available():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        token = {"operation": "shutdown"}
        with uring_api.Ring() as ring:
            pending = ring.submit_shutdown(writer.fileno(), socket.SHUT_WR, token)
            completion = ring.wait(1.0)

        assert completion is pending
        assert completion.user_data is token
        assert completion.kind == uring_api.COMPLETION_KIND_SHUTDOWN
        assert completion.res == 0
        assert completion.flags == 0
        assert completion.result is None
        assert reader.recv(1) == b""
    finally:
        reader.close()
        writer.close()


def test_ring_close_completion_when_available():
    require_uring()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fd = sock.detach()
    token = {"operation": "close"}
    with uring_api.Ring() as ring:
        pending = ring.submit_close(fd, token)
        completion = ring.wait(1.0)

    assert completion is pending
    assert completion.user_data is token
    assert completion.kind == uring_api.COMPLETION_KIND_CLOSE
    assert completion.res == 0
    assert completion.flags == 0
    assert completion.result is None
    with pytest.raises(OSError) as excinfo:
        os.fstat(fd)
    assert excinfo.value.errno == errno.EBADF


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


def test_ring_sendmsg_completion_when_available():
    require_uring()

    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        receiver.bind(("127.0.0.1", 0))
        receiver.setblocking(False)
        sender.setblocking(False)
        token = {"operation": "sendmsg"}
        with uring_api.Ring() as ring:
            pending = ring.submit_sendmsg(sender.fileno(), b"hello", receiver.getsockname(), token)

            completion = ring.wait(1.0)

        assert completion is pending
        assert completion.user_data is token
        assert completion.kind == uring_api.COMPLETION_KIND_SENDMSG
        assert completion.res == 5
        assert completion.result == 5
        data, address = receiver.recvfrom(5)
        assert data == b"hello"
        assert address[1] == sender.getsockname()[1]
    finally:
        sender.close()
        receiver.close()


def test_ring_socket_completion_when_available():
    require_uring()

    sock = None
    token = {"operation": "socket"}
    with uring_api.Ring() as ring:
        pending = ring.submit_socket(socket.AF_INET, socket.SOCK_STREAM, user_data=token)

        completion = ring.wait(1.0)

    assert completion is pending
    assert completion.user_data is token
    assert completion.kind == uring_api.COMPLETION_KIND_SOCKET
    if completion.res < 0:
        errno_value = -completion.res
        if errno_value in {errno.ENOSYS, errno.EOPNOTSUPP, errno.EINVAL}:
            pytest.skip(f"IORING_OP_SOCKET is not supported: errno {errno_value}")
    assert completion.res >= 0
    assert completion.result == completion.res
    try:
        sock = socket.socket(fileno=completion.res)
        assert sock.family == socket.AF_INET
        assert sock.type & socket.SOCK_STREAM
    finally:
        if sock is not None:
            sock.close()
        elif completion.res >= 0:
            os.close(completion.res)


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
