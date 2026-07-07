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
    collect_completions,
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

            completion = wait_one(ring, 1.0)

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
            assert wait_one(ring, 1.0) is target

            cancel = ring.submit_cancel(target)
            completion = wait_one(ring, 1.0)

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

            completion = wait_one(ring, 1.0)

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

            completion = wait_one(ring, 1.0)
            notification = wait_one(ring, 1.0)

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

            completion = wait_one(ring, 1.0)
            notification = wait_one(ring, 1.0)

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

            completion = wait_one(ring, 1.0)

        assert completion is not None
        accepted_fd = completion.result
        assert accepted_fd == completion.res
        assert_fd_nonblocking_cloexec(accepted_fd)
        accepted = socket.socket(fileno=accepted_fd)
        assert completion.user_data is token
        assert completion.flags == 0
        assert accepted.getpeername() == client.getsockname()
    finally:
        if accepted is not None:
            accepted.close()
        if client is not None:
            client.close()
        server.close()

def test_ring_accept_multishot_batch_peer_addresses_when_available():
    require_uring()
    if not uring_api.probe().get("IORING_ACCEPT_MULTISHOT", False):
        pytest.skip("IORING_ACCEPT_MULTISHOT is not available")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clients = []
    try:
        server.setblocking(False)
        server.bind(("127.0.0.1", 0))
        server.listen()
        with uring_api.Ring() as ring:
            ring.submit_accept_multishot(server.fileno(), 170, flags=socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC)
            clients.append(connect_to_listener(server))
            clients.append(connect_to_listener(server))
            batch = collect_completions(ring, 1.0, 2)

        assert len(batch) == 2
        expected_peers = {client.getsockname() for client in clients}
        seen_peers = set()
        for completion in batch:
            if completion.res < 0:
                errno_value = -completion.res
                if errno_value in {errno.EINVAL, errno.EOPNOTSUPP, errno.ENOSYS}:
                    pytest.skip(f"IORING_ACCEPT_MULTISHOT is not supported: errno {errno_value}")
            accepted_sock = socket.socket(fileno=completion.result)
            try:
                peer = accepted_sock.getpeername()
            finally:
                accepted_sock.close()
            assert peer in expected_peers
            seen_peers.add(peer)
        assert seen_peers == expected_peers
    finally:
        for client in clients:
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
            first = wait_one(ring, 1.0)
            clients.append(connect_to_listener(server))
            second = wait_one(ring, 1.0)

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
                assert completion.multishot is True
                assert completion.user_data is token
                assert completion.sequence == sequence
                assert completion.flags & uring_api.IORING_CQE_F_MORE
                accepted_fd = completion.result
                assert accepted_fd == completion.res
                assert_fd_nonblocking_cloexec(accepted_fd)
                accepted_sock = socket.socket(fileno=accepted_fd)
                accepted.append(accepted_sock)
                assert accepted_sock.getpeername() == client.getsockname()

            ring.submit_cancel(handle)
            cancelled = False
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                for completion in ring.wait(0.0):
                    if completion is handle:
                        cancelled = True
                        break
                if cancelled:
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

            completion = wait_one(ring, 1.0)

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
            completion = wait_one(ring, 1.0)

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
        completion = wait_one(ring, 1.0)

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

            completion = wait_one(ring, 1.0)

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

            completion = wait_one(ring, 1.0)

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

            completion = wait_one(ring, 1.0)

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

        completion = wait_one(ring, 1.0)

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


def test_ring_socket_nonblock_cloexec_flags_when_available():
    require_uring()

    socket_flags = getattr(socket, "SOCK_NONBLOCK", 0) | getattr(socket, "SOCK_CLOEXEC", 0)
    if not socket_flags:
        pytest.skip("SOCK_NONBLOCK/SOCK_CLOEXEC are not available")

    sock = None
    with uring_api.Ring() as ring:
        pending = ring.submit_socket(
            socket.AF_INET,
            socket.SOCK_STREAM | socket_flags,
            user_data={"operation": "socket_flags"},
        )
        completion = wait_one(ring, 1.0)

    assert completion is pending
    assert completion.kind == uring_api.COMPLETION_KIND_SOCKET
    if completion.res < 0:
        errno_value = -completion.res
        if errno_value in {errno.ENOSYS, errno.EOPNOTSUPP, errno.EINVAL}:
            pytest.skip(f"IORING_OP_SOCKET is not supported: errno {errno_value}")
    assert completion.res >= 0
    try:
        assert_fd_nonblocking_cloexec(completion.res)
        sock = socket.socket(fileno=completion.res)
        assert sock.family == socket.AF_INET
        assert sock.type & socket.SOCK_STREAM
    finally:
        if sock is not None:
            sock.close()
        elif completion.res >= 0:
            os.close(completion.res)


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="AF_UNIX is not supported")
def test_ring_socket_unix_when_available():
    require_uring()

    sock = None
    socket_flags = getattr(socket, "SOCK_NONBLOCK", 0) | getattr(socket, "SOCK_CLOEXEC", 0)
    with uring_api.Ring() as ring:
        pending = ring.submit_socket(
            socket.AF_UNIX,
            socket.SOCK_STREAM | socket_flags,
            user_data={"operation": "socket_unix"},
        )
        completion = wait_one(ring, 1.0)

    assert completion is pending
    assert completion.kind == uring_api.COMPLETION_KIND_SOCKET
    if completion.res < 0:
        errno_value = -completion.res
        if errno_value in {errno.ENOSYS, errno.EOPNOTSUPP, errno.EINVAL}:
            pytest.skip(f"IORING_OP_SOCKET for AF_UNIX is not supported: errno {errno_value}")
    assert completion.res >= 0
    try:
        if socket_flags:
            assert_fd_nonblocking_cloexec(completion.res)
        sock = socket.socket(fileno=completion.res)
        assert sock.family == socket.AF_UNIX
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

            completions = collect_completions(ring, 1.0, 2)

        by_user_data = {completion.user_data: completion for completion in completions}
        assert by_user_data[130].res == 4
        assert by_user_data[130].result == 4
        assert bytes(recv_buf) == b"ping"
        assert by_user_data[131].res == 4
        assert by_user_data[131].result == 4
    finally:
        left.close()
        right.close()

