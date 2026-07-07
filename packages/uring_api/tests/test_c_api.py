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

def test_c_api_client_can_import_capsule_and_probe():
    client = build_c_api_client()

    abi_version, struct_size, feature_flags, major, minor = client.metadata()
    probe = client.probe()

    assert abi_version == uring_api.C_API_ABI_VERSION
    assert struct_size == uring_api.C_API_STRUCT_SIZE
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
        assert client.completion_summary(by_user_data[220]) == (
            220,
            uring_api.COMPLETION_KIND_RECV,
            4,
            0,
            4,
        )
        assert client.completion_summary(by_user_data[221]) == (
            221,
            uring_api.COMPLETION_KIND_SEND,
            4,
            0,
            4,
        )
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
        assert client.completion_summary(recv_completion) == (
            230,
            uring_api.COMPLETION_KIND_RECVMSG,
            5,
            0,
            sender.getsockname(),
        )
        assert client.completion_summary(send_completion) == (
            231,
            uring_api.COMPLETION_KIND_SENDTO,
            5,
            0,
            5,
        )
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
        assert client.completion_summary(completion) == (
            244,
            uring_api.COMPLETION_KIND_SENDMSG,
            5,
            0,
            5,
        )
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
        assert client.completion_summary(completion) == (
            245,
            uring_api.COMPLETION_KIND_SENDMSG_ZC,
            5,
            completion.flags,
            5,
        )
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
        assert client.completion_summary(completion) == (
            246,
            uring_api.COMPLETION_KIND_SEND_ZC,
            5,
            completion.flags,
            5,
        )
        assert completion.kind == uring_api.COMPLETION_KIND_SEND_ZC
        assert not (completion.flags & uring_api.IORING_CQE_F_NOTIF)
        assert notification is None
        assert reader.recv(5) == b"hello"
    finally:
        reader.close()
        writer.close()

def test_c_api_poll_operation_when_available():
    require_uring()

    client = build_c_api_client()
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        with uring_api.Ring() as ring:
            client.submit_poll(ring, reader.fileno(), select.POLLIN, 250)
            writer.send(b"x")
            completion = ring.wait(1.0)

        assert completion is not None
        assert client.completion_summary(completion) == (
            250,
            uring_api.COMPLETION_KIND_POLL,
            completion.res,
            0,
            completion.res,
        )
        assert completion.res & select.POLLIN
    finally:
        reader.close()
        writer.close()

def test_c_api_poll_multishot_operation_when_available():
    require_uring_capability("IORING_POLL_MULTISHOT")

    client = build_c_api_client()
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        with uring_api.Ring() as ring:
            client.submit_poll_multishot(ring, reader.fileno(), select.POLLIN, 251)
            writer.send(b"a")
            first = ring.wait(1.0)
            writer.send(b"b")
            second = ring.wait(1.0)

        assert first is not None
        assert second is not None
        for sequence, completion in ((0, first), (1, second)):
            assert client.completion_summary(completion) == (
                251,
                uring_api.COMPLETION_KIND_POLL_MULTISHOT,
                completion.res,
                completion.flags,
                completion.res,
            )
            assert client.completion_sequence(completion) == sequence
            assert completion.res & select.POLLIN
            if sequence == 0:
                assert completion.flags & uring_api.IORING_CQE_F_MORE
    finally:
        reader.close()
        writer.close()

def test_c_api_poll_remove_operation_when_available():
    require_uring_capability("IORING_POLL_MULTISHOT")

    client = build_c_api_client()
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        with uring_api.Ring() as ring:
            handle = ring.submit_poll_multishot(reader.fileno(), select.POLLIN, 252)
            writer.send(b"a")
            first = ring.wait(1.0)
            assert first is not None
            assert first.kind == uring_api.COMPLETION_KIND_POLL_MULTISHOT

            client.submit_poll_remove(ring, handle)
            removed = False
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                completion = ring.wait(0.0)
                if completion is None:
                    continue
                if completion.kind == uring_api.COMPLETION_KIND_POLL_REMOVE:
                    removed = True
                    assert completion.user_data is handle
                    break
            assert removed
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
        user_data, kind, res, flags, result = client_api.completion_summary(completion)
        assert user_data == 245
        assert kind == uring_api.COMPLETION_KIND_SOCKET
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
        user_data, kind, res, flags, result = client_api.completion_summary(completion)
        accepted_fd = result
        assert accepted_fd == res
        assert_fd_nonblocking_cloexec(accepted_fd)
        accepted = socket.socket(fileno=accepted_fd)
        assert user_data == 240
        assert kind == uring_api.COMPLETION_KIND_ACCEPT
        assert flags == 0
        assert accepted.getpeername() == client.getsockname()
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
                buf_group = ring.create_buf_group(8, 4)
                client_api.submit_recv_multishot(ring, reader.fileno(), buf_group, 246, 0)
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
        user_data, kind, res, _flags, result = client_api.completion_summary(completion)
        assert user_data == 246
        assert completion.multishot is True
        assert kind == uring_api.COMPLETION_KIND_RECV_MULTISHOT
        assert isinstance(result, uring_api.BufView)
        assert res == result.length
        view = memoryview(result)
        try:
            assert bytes(view) == b"hello"
        finally:
            del view
        assert client_api.completion_sequence(completion) == 0
    finally:
        reader.close()
        writer.close()

def test_c_api_recv_buf_operation_when_available():
    require_uring()

    client_api = build_c_api_client()
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            try:
                buf_group = ring.create_buf_group(8, 4)
                client_api.submit_recv_buf(ring, reader.fileno(), buf_group, 247, 0)
            except OSError as exc:
                if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    pytest.skip(f"provided-buffer recv is not supported: errno {exc.errno}")
                raise
            writer.send(b"hello")
            completion = ring.wait(1.0)

        assert completion is not None
        if completion.res < 0:
            errno_value = -completion.res
            if errno_value in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP, errno.ENOBUFS}:
                pytest.skip(f"recv buf is not supported: errno {errno_value}")
        user_data, kind, res, _flags, result = client_api.completion_summary(completion)
        assert user_data == 247
        assert kind == uring_api.COMPLETION_KIND_RECV_BUF
        assert isinstance(result, uring_api.BufView)
        assert res == result.length
        view = memoryview(result)
        try:
            assert bytes(view) == b"hello"
        finally:
            del view
    finally:
        reader.close()
        writer.close()

def test_c_api_cancel_operation_when_available():
    require_uring()

    client_api = build_c_api_client()
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        buf = bytearray(5)
        with uring_api.Ring() as ring:
            target = ring.submit_recv(reader.fileno(), buf, "target")
            writer.send(b"hello")
            assert ring.wait(1.0) is target

            client_api.submit_cancel(ring, target)
            completion = ring.wait(1.0)

        assert completion is not None
        user_data, kind, res, _flags, result = client_api.completion_summary(completion)
        assert user_data is target
        assert kind == uring_api.COMPLETION_KIND_CANCEL
        assert res < 0
        assert result is None
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
        assert client_api.completion_summary(completion) == (
            241,
            uring_api.COMPLETION_KIND_CONNECT,
            0,
            0,
            None,
        )
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
        assert client_api.completion_summary(completion) == (
            242,
            uring_api.COMPLETION_KIND_SHUTDOWN,
            0,
            0,
            None,
        )
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
    assert client_api.completion_summary(completion) == (
        243,
        uring_api.COMPLETION_KIND_CLOSE,
        0,
        0,
        None,
    )
    assert completion.kind == uring_api.COMPLETION_KIND_CLOSE
    with pytest.raises(OSError) as excinfo:
        os.fstat(fd)
    assert excinfo.value.errno == errno.EBADF

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

            assert client.completion_summary(pending) == (
                247,
                uring_api.COMPLETION_KIND_RECV,
                0,
                0,
                None,
            )

            writer.send(b"hello")
            completion = ring.wait(1.0)

        assert completion is pending
        assert client.completion_summary(completion) == (
            247,
            uring_api.COMPLETION_KIND_RECV,
            5,
            0,
            5,
        )
    finally:
        reader.close()
        writer.close()

def test_c_api_statx_when_available():
    require_uring()

    client = build_c_api_client()
    token = 262
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        path = tmp.name
    try:
        with open(path, "wb") as handle:
            handle.write(b"hello")
        buf = bytearray(uring_api.STATX_BUFFER_SIZE)
        with uring_api.Ring() as ring:
            client.submit_statx(ring, -100, path, 0, uring_api.STATX_SIZE, buf, token)
            completion = ring.wait(1.0)

        assert completion is not None
        assert client.completion_summary(completion) == (
            token,
            uring_api.COMPLETION_KIND_STATX,
            0,
            0,
            None,
        )
        assert uring_api.statx_st_size(buf) == 5
    finally:
        os.unlink(path)

def test_c_api_statx_st_size_reads_buffer():
    client = build_c_api_client()
    buf = bytearray(uring_api.STATX_BUFFER_SIZE)
    buf[0:4] = uring_api.STATX_SIZE.to_bytes(4, "little")
    buf[uring_api.STATX_STX_SIZE_OFFSET : uring_api.STATX_STX_SIZE_OFFSET + 8] = (9).to_bytes(8, "little")
    assert client.statx_st_size(buf) == 9

def test_c_api_statx_fdsize_when_available():
    require_uring()

    client = build_c_api_client()
    token = 263
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        path = tmp.name
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT)
        try:
            assert os.write(fd, b"hello") == 5
            with uring_api.Ring() as ring:
                client.submit_statx_fdsize(ring, fd, token)
                completion = ring.wait(1.0)
            assert completion is not None
            assert client.completion_summary(completion) == (
                token,
                uring_api.COMPLETION_KIND_STATX_FDSIZE,
                0,
                0,
                5,
            )
        finally:
            os.close(fd)
    finally:
        os.unlink(path)

def test_c_api_openat_read_write_round_trip_when_available():
    require_uring()

    client = build_c_api_client()
    token = 261
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "openat-capi.txt")
        with uring_api.Ring() as ring:
            client.submit_openat(ring, -100, path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644, token)
            open_completion = ring.wait(1.0)
            assert open_completion is not None
            fd = open_completion.res
            assert fd >= 0

            client.submit_write(ring, fd, 0, b"hello", token)
            write_completion = ring.wait(1.0)
            buf = bytearray(5)
            client.submit_read(ring, fd, 0, buf, token)
            read_completion = ring.wait(1.0)
            client.submit_close(ring, fd, token)
            close_completion = ring.wait(1.0)

        assert client.completion_summary(open_completion) == (
            token,
            uring_api.COMPLETION_KIND_OPENAT,
            fd,
            0,
            fd,
        )
        assert write_completion is not None
        assert read_completion is not None
        assert close_completion is not None
        assert bytes(buf) == b"hello"

def test_c_api_file_read_write_operation_when_available():
    require_uring()

    client = build_c_api_client()
    token = 260
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        path = tmp.name
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT)
        try:
            with uring_api.Ring() as ring:
                client.submit_write(ring, fd, 0, b"hello", token)
                write_completion = ring.wait(1.0)
                buf = bytearray(5)
                client.submit_read(ring, fd, 0, buf, token)
                read_completion = ring.wait(1.0)

            assert write_completion is not None
            assert read_completion is not None
            assert client.completion_summary(write_completion) == (
                token,
                uring_api.COMPLETION_KIND_WRITE,
                5,
                0,
                5,
            )
            assert client.completion_summary(read_completion) == (
                token,
                uring_api.COMPLETION_KIND_READ,
                5,
                0,
                5,
            )
            assert bytes(buf) == b"hello"
        finally:
            os.close(fd)
    finally:
        os.unlink(path)

