import errno
import os
import socket
import time

import pytest

import uring_api

from helpers import (
    collect_completions,
    wait_one,
    assert_fd_nonblocking_cloexec,
    connect_to_listener,
    connected_tcp_pair,
    so_linger,
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
            pending = ring.prepare_recv(reader.fileno(), buf, 0, token)
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


def test_ring_recv_poll_first_completes_when_available():
    require_uring()
    if not uring_api.probe().get("IORING_RECVSEND_POLL_FIRST", False):
        pytest.skip("IORING_RECVSEND_POLL_FIRST is not available")

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        buf = bytearray(5)
        with uring_api.Ring() as ring:
            pending = ring.prepare_recv(reader.fileno(), buf, uring_api.IORING_RECVSEND_POLL_FIRST, "poll-first")
            writer.send(b"hello")
            completion = wait_one(ring, 1.0)
        assert completion is pending
        assert completion.res == 5
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
            target = ring.prepare_recv(reader.fileno(), buf, 0, "target")
            writer.send(b"hello")
            assert wait_one(ring, 1.0) is target

            cancel = ring.prepare_cancel(target)
            completion = wait_one(ring, 1.0)

        assert completion is cancel
        assert cancel.user_data is None
        assert cancel.cancel_target is target
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
            ring.prepare_send(writer.fileno(), b"hello", 0, token)

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
            pending = ring.prepare_send_zc(writer.fileno(), b"hello", 0, 0, token)

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
            pending = ring.prepare_sendmsg_zc(sender.fileno(), b"hello", receiver.getsockname(), 0, token)

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

def test_accept_tcp_nodelay_flag_value():
    assert uring_api.ACCEPT_TCP_NODELAY == 1 << 31
    assert uring_api.ACCEPT_TCP_NODELAY & (socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC) == 0


def _accepted_socket(completion: uring_api.Completion) -> socket.socket:
    assert completion.res >= 0
    assert completion.result == completion.res
    return socket.socket(fileno=completion.result)


def test_ring_accept_tcp_nodelay_flag_when_available():
    require_uring()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clients = []
    accepted = []
    try:
        server.setblocking(False)
        server.bind(("127.0.0.1", 0))
        server.listen()
        plain = socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC
        with uring_api.Ring() as ring:
            ring.prepare_accept(server.fileno(), plain, None)
            clients.append(connect_to_listener(server))
            without = wait_one(ring, 1.0)
            ring.prepare_accept(server.fileno(), plain | uring_api.ACCEPT_TCP_NODELAY, None)
            clients.append(connect_to_listener(server))
            with_flag = wait_one(ring, 1.0)

        assert without is not None and with_flag is not None
        accepted.append(_accepted_socket(without))
        accepted.append(_accepted_socket(with_flag))
        assert accepted[0].getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY) == 0
        assert accepted[1].getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY) == 1
        assert_fd_nonblocking_cloexec(accepted[1].fileno())
    finally:
        for sock in accepted:
            sock.close()
        for client in clients:
            client.close()
        server.close()


def test_ring_accept_multishot_tcp_nodelay_when_available():
    require_uring()
    if not uring_api.probe().get("IORING_ACCEPT_MULTISHOT", False):
        pytest.skip("IORING_ACCEPT_MULTISHOT is not available")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client = None
    accepted = None
    try:
        server.setblocking(False)
        server.bind(("127.0.0.1", 0))
        server.listen()
        flags = socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC | uring_api.ACCEPT_TCP_NODELAY
        with uring_api.Ring() as ring:
            handle = ring.prepare_accept_multishot(server.fileno(), flags, None)
            client = connect_to_listener(server)
            completion = wait_one(ring, 1.0)
            assert completion is not None
            if completion.res < 0 and -completion.res in {errno.EINVAL, errno.EOPNOTSUPP, errno.ENOSYS}:
                pytest.skip(f"IORING_ACCEPT_MULTISHOT is not supported: errno {-completion.res}")
            assert completion is not handle
            assert completion.flags & uring_api.IORING_CQE_F_MORE
            accepted = _accepted_socket(completion)
            assert accepted.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY) == 1
            ring.prepare_cancel(handle)
            cancelled = False
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                for item in ring.wait(0.0):
                    if item is handle:
                        cancelled = True
                        break
                if cancelled:
                    break
            assert cancelled
    finally:
        if accepted is not None:
            accepted.close()
        if client is not None:
            client.close()
        server.close()


def test_ring_accept_tcp_nodelay_on_unix_still_delivers_when_available(tmp_path):
    require_uring()

    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    accepted = None
    try:
        server.bind(str(tmp_path / "accept.sock"))
        server.listen()
        server.setblocking(False)
        flags = socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC | uring_api.ACCEPT_TCP_NODELAY
        with uring_api.Ring() as ring:
            ring.prepare_accept(server.fileno(), flags, None)
            client.connect(server.getsockname())
            completion = wait_one(ring, 1.0)
        assert completion is not None
        accepted = _accepted_socket(completion)
        assert_fd_nonblocking_cloexec(accepted.fileno())
    finally:
        if accepted is not None:
            accepted.close()
        client.close()
        server.close()


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
            ring.prepare_accept(server.fileno(), socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC, token)
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
            ring.prepare_accept_multishot(server.fileno(), socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC, 170)
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
            handle = ring.prepare_accept_multishot(
                server.fileno(), socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC, token
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

            ring.prepare_cancel(handle)
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


def test_ring_accept_multishot_base_sequence_when_available():
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
        with uring_api.Ring() as ring:
            handle = ring.prepare_accept_multishot(
                server.fileno(),
                socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC,
                None,
                5,
            )
            assert handle.sequence == 5
            clients.append(connect_to_listener(server))
            first = wait_one(ring, 1.0)
            clients.append(connect_to_listener(server))
            second = wait_one(ring, 1.0)

            assert first is not None
            assert second is not None
            for sequence, completion in ((5, first), (6, second)):
                if completion.res < 0:
                    errno_value = -completion.res
                    if errno_value in {errno.EINVAL, errno.EOPNOTSUPP, errno.ENOSYS}:
                        pytest.skip(f"IORING_ACCEPT_MULTISHOT is not supported: errno {errno_value}")
                assert completion.sequence == sequence
                accepted.append(socket.socket(fileno=completion.result))
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
            ring.prepare_connect(client.fileno(), server.getsockname(), token)

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
            pending = ring.prepare_shutdown(writer.fileno(), socket.SHUT_WR, token)
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
        pending = ring.prepare_close(fd, token)
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


def _assert_fd_closed(fd: int) -> None:
    with pytest.raises(OSError) as excinfo:
        os.fstat(fd)
    assert excinfo.value.errno == errno.EBADF


def test_ring_close_abortive_resets_tcp_when_available():
    """abortive=True sets SO_LINGER {1, 0} at fill, then the close resets."""
    require_uring()

    reader, writer = connected_tcp_pair()
    fd = writer.detach()
    try:
        with uring_api.Ring(flags=0) as ring:
            pending = ring.construct_close(fd, abortive=True)
            assert so_linger(fd) == (0, 0)
            assert ring.prepare(pending) == 1
            assert so_linger(fd) == (1, 0)
            completion = wait_one(ring, 1.0)
        assert completion is pending
        assert completion.res == 0
        _assert_fd_closed(fd)
        with pytest.raises(ConnectionResetError):
            reader.recv(1)
    finally:
        reader.close()


def test_ring_close_default_is_graceful_when_available():
    require_uring()

    reader, writer = connected_tcp_pair()
    fd = writer.detach()
    try:
        with uring_api.Ring(flags=0) as ring:
            pending = ring.prepare_close(fd, user_data={"operation": "close"})
            assert so_linger(fd) == (0, 0)
            completion = wait_one(ring, 1.0)
        assert completion is pending
        assert completion.res == 0
        assert reader.recv(1) == b""
    finally:
        reader.close()


def test_ring_close_abortive_is_keyword_only():
    require_uring()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fd = sock.detach()
    try:
        with uring_api.Ring(flags=0) as ring:
            with pytest.raises(TypeError):
                ring.prepare_close(fd, None, True)
            assert so_linger(fd) == (0, 0)
            assert ring.prepare_close_nowait(fd, abortive=True) is None
            assert so_linger(fd) == (1, 0)
            assert ring.submit() >= 1
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                try:
                    os.fstat(fd)
                except OSError as exc:
                    assert exc.errno == errno.EBADF
                    break
                ring.wait(0)
            else:
                raise AssertionError("abortive nowait close did not close the fd")
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def test_abortive_nowait_close_leaves_linger_unset_when_the_sq_is_full():
    """A nowait close that cannot take a slot must not arm SO_LINGER."""
    require_uring()

    reader, _writer = socket.socketpair()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fd = sock.detach()
    try:
        reader.setblocking(False)
        with uring_api.Ring(entries=8, flags=0, auto_submit=False, make_room=False) as ring:
            for _ in range(ring.sq_entries):
                ring.prepare_recv(reader.fileno(), bytearray(1))
            with pytest.raises(uring_api.SubmissionQueueFull):
                ring.prepare_close_nowait(fd, abortive=True)
            assert so_linger(fd) == (0, 0)
    finally:
        reader.close()
        os.close(fd)


def test_abortive_close_link_timeout_sets_linger_only_after_prepare():
    """Linger follows a committed close SQE, including its link timeout."""
    require_uring()

    reader, writer = connected_tcp_pair()
    fd = writer.detach()
    try:
        with uring_api.Ring(entries=1, flags=0, auto_submit=False, make_room=False) as ring:
            pending = ring.construct_close(fd, abortive=True)
            pending.timeout = 1.0
            with pytest.raises(RuntimeError, match="sq_entries is 1"):
                ring.prepare(pending)
            assert pending.prepared is False
            assert so_linger(fd) == (0, 0)
        with uring_api.Ring(flags=0) as ring:
            pending = ring.construct_close(fd, abortive=True)
            pending.timeout = 5.0
            assert so_linger(fd) == (0, 0)
            assert ring.prepare(pending) == 1
            assert so_linger(fd) == (1, 0)
            completion = wait_one(ring, 1.0)
        assert completion is pending
        assert completion.res == 0
        _assert_fd_closed(fd)
    finally:
        reader.close()


def test_ring_close_abortive_ignores_setsockopt_failure_when_available():
    """A file rejects SO_LINGER. The close still completes."""
    require_uring()

    fd = os.open("/dev/null", os.O_RDONLY)
    with uring_api.Ring(flags=0) as ring:
        pending = ring.prepare_close(fd, abortive=True)
        completion = wait_one(ring, 1.0)
    assert completion is pending
    assert completion.res == 0
    _assert_fd_closed(fd)


def test_ring_shutdown_nowait_no_completion():
    require_uring()

    delivered: list[object] = []

    def on_complete(completion: object) -> None:
        delivered.append(completion)

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            ring.callback = on_complete
            assert ring.prepare_shutdown_nowait(writer.fileno(), socket.SHUT_WR) is None
            buf = bytearray(1)
            pending = ring.prepare_recv(reader.fileno(), buf, 0, object())
            assert ring.wait(1.0) is None
        assert delivered == [pending]
        assert reader.recv(1) == b""
    finally:
        reader.close()
        writer.close()


def test_ring_cancel_nowait_no_completion():
    require_uring()

    delivered: list[object] = []

    def on_complete(completion: object) -> None:
        delivered.append(completion)

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            ring.callback = on_complete
            pending = ring.prepare_recv(reader.fileno(), bytearray(4), 0, object())
            assert ring.prepare_cancel_nowait(pending) is None
            writer.send(b"xxxx")
            ring.wait(1.0)
        assert all(getattr(c, "kind", None) != uring_api.COMPLETION_KIND_CANCEL for c in delivered)
        assert pending in delivered or any(c is pending for c in delivered)
    finally:
        reader.close()
        writer.close()


def test_ring_cancel_nowait_lost_race_is_silent():
    """Cancel after the target already completed: -ENOENT/-EALREADY are not errors."""

    require_uring()

    calls: list[dict] = []

    def on_nowait_error(context: dict) -> None:
        calls.append(dict(context))

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            ring.nowait_error_handler = on_nowait_error
            pending = ring.prepare_recv(reader.fileno(), bytearray(5), 0, object())
            writer.send(b"hello")
            assert wait_one(ring, 1.0) is pending
            assert ring.prepare_cancel_nowait(pending) is None
            # force a drain so a nowait failure CQE would reach the handler
            token = object()
            pending2 = ring.prepare_recv(reader.fileno(), bytearray(1), 0, token)
            writer.send(b"x")
            assert wait_one(ring, 1.0) is pending2
        assert calls == []
    finally:
        reader.close()
        writer.close()


def test_close_skip_success_error_delivers_when_user_data_set():
    """skip_success + user_data keeps the Completion* and delivers only on error."""
    require_uring()

    handler_seen: list[dict[str, object]] = []
    token = object()

    def on_error(context: dict[str, object]) -> None:
        handler_seen.append(context)

    with uring_api.Ring() as ring:
        ring.nowait_error_handler = on_error
        pending = ring.construct_close(2_000_000_000, token)
        pending.skip_success = True
        assert pending.skip_all is False
        ring.prepare(pending)
        assert ring.pending_count() == 1
        deadline = time.monotonic() + 1.0
        done = None
        while time.monotonic() < deadline:
            batch = ring.wait(0.1)
            if pending in batch:
                done = pending
                break
        assert done is pending
        assert done.res < 0
        assert done.user_data is token
        assert ring.pending_count() == 0
        assert handler_seen == []


def test_close_skip_success_success_with_user_data_still_skipped():
    require_uring()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fd = sock.detach()
    token = object()
    with uring_api.Ring() as ring:
        pending = ring.construct_close(fd, token)
        pending.skip_success = True
        assert ring.prepare(pending) == 1
        assert ring.pending_count() == 1
        deadline = time.monotonic() + 1.0
        while ring.pending_count() and time.monotonic() < deadline:
            assert pending not in ring.wait(0.1)
        assert ring.pending_count() == 0


def test_close_skip_success_error_delivers_without_user_data():
    require_uring()

    handler_seen: list[dict[str, object]] = []

    def on_error(context: dict[str, object]) -> None:
        handler_seen.append(context)

    with uring_api.Ring() as ring:
        ring.nowait_error_handler = on_error
        pending = ring.construct_close(2_000_000_000)
        pending.skip_success = True
        ring.prepare(pending)
        deadline = time.monotonic() + 1.0
        done = None
        while time.monotonic() < deadline:
            batch = ring.wait(0.1)
            if pending in batch:
                done = pending
                break
        assert done is pending
        assert done.res < 0
        assert handler_seen == []


def test_skip_all_implies_skip_success_and_clears_with_it():
    require_uring()

    with uring_api.Ring() as ring:
        close = ring.construct_close(0)
        close.skip_all = True
        assert close.skip_all is True
        assert close.skip_success is True
        close.skip_success = False
        assert close.skip_success is False
        assert close.skip_all is False


def test_ring_close_nowait_no_completion():
    """Nowait close: no Completion object, not delivered."""

    require_uring()

    delivered: list[object] = []

    def on_complete(completion: object) -> None:
        delivered.append(completion)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fd = sock.detach()
    marker = object()
    with uring_api.Ring() as ring:
        ring.callback = on_complete
        result = ring.prepare_close_nowait(fd)
        assert result is None
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            buf = bytearray(4)
            pending = ring.prepare_recv(reader.fileno(), buf, 0, marker)
            writer.send(b"xxxx")
            assert ring.wait(1.0) is None
        finally:
            reader.close()
            writer.close()

    assert delivered == [pending]
    with pytest.raises(OSError) as excinfo:
        os.fstat(fd)
    assert excinfo.value.errno == errno.EBADF


def test_ring_close_nowait_pull_mode_does_not_surface():
    """Discard close never appears in pull-mode wait() batches."""

    require_uring()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fd = sock.detach()
    with uring_api.Ring() as ring:
        assert ring.prepare_close_nowait(fd) is None
        # force a user-visible CQE so wait drains any silent nowait CQE too
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            token = object()
            buf = bytearray(1)
            pending = ring.prepare_recv(reader.fileno(), buf, 0, token)
            writer.send(b"z")
            batch = ring.wait(1.0)
        finally:
            reader.close()
            writer.close()

    assert batch == [pending]
    with pytest.raises(OSError) as excinfo:
        os.fstat(fd)
    assert excinfo.value.errno == errno.EBADF



def test_ring_nowait_error_handler_skips_successful_close():
    """Successful nowait close never calls the handler (even if a CQE arrives).

    Without IOSQE_CQE_SKIP_SUCCESS the kernel still posts res == 0 CQEs; those
    must be silent. With skip-success there is no CQE at all on success.
    """

    require_uring()

    calls: list[dict] = []

    def on_nowait_error(context: dict) -> None:
        calls.append(dict(context))

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fd = sock.detach()
    with uring_api.Ring() as ring:
        ring.nowait_error_handler = on_nowait_error
        assert ring.prepare_close_nowait(fd) is None
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            pending = ring.prepare_recv(reader.fileno(), bytearray(1), 0, object())
            writer.send(b"y")
            batch = ring.wait(1.0)
        finally:
            reader.close()
            writer.close()

    assert batch == [pending]
    assert calls == []
    with pytest.raises(OSError) as excinfo:
        os.fstat(fd)
    assert excinfo.value.errno == errno.EBADF


def test_ring_nowait_error_handler_on_failed_close():
    """Failed nowait close invokes nowait_error_handler."""

    require_uring()

    contexts: list[dict] = []

    def on_nowait_error(context: dict) -> None:
        contexts.append(dict(context))

    with uring_api.Ring() as ring:
        ring.nowait_error_handler = on_nowait_error
        # close a never-open fd → -EBADF CQE (skip-success only suppresses success)
        ring.prepare_close_nowait(2_000_000_000)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            pending = ring.prepare_recv(reader.fileno(), bytearray(1), 0, object())
            writer.send(b"x")
            batch = ring.wait(1.0)
        finally:
            reader.close()
            writer.close()

    assert batch == [pending]
    assert len(contexts) == 1
    ctx = contexts[0]
    assert ctx["message"] == "Nowait operation failed"
    assert ctx["ring"] is not None
    assert ctx["res"] == -errno.EBADF
    assert isinstance(ctx["flags"], int)
    assert ctx["kind"] == uring_api.COMPLETION_KIND_CLOSE
    assert ctx["fd"] == 2_000_000_000


def test_ring_nowait_error_handler_raise_uses_exception_handler():
    """nowait_error_handler exceptions are absorbed by exception_handler."""

    require_uring()

    exception_contexts: list[dict] = []

    def on_nowait_error(context: dict) -> None:
        raise RuntimeError("nowait boom")

    def on_exception(context: dict) -> None:
        exception_contexts.append(dict(context))

    with uring_api.Ring() as ring:
        ring.nowait_error_handler = on_nowait_error
        ring.exception_handler = on_exception
        ring.prepare_close_nowait(2_000_000_000)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            pending = ring.prepare_recv(reader.fileno(), bytearray(1), 0, object())
            writer.send(b"x")
            batch = ring.wait(1.0)
        finally:
            reader.close()
            writer.close()

    assert batch == [pending]
    assert len(exception_contexts) == 1
    ctx = exception_contexts[0]
    assert "nowait_error_handler" in ctx["message"]
    assert isinstance(ctx["exception"], RuntimeError)
    assert str(ctx["exception"]) == "nowait boom"
    assert ctx["completion"] is None


def test_ring_nowait_error_handler_property_validation():
    require_uring()
    with uring_api.Ring() as ring:
        assert ring.nowait_error_handler is None
        ring.nowait_error_handler = None
        assert ring.nowait_error_handler is None

        def handler(context: dict) -> None:
            pass

        ring.nowait_error_handler = handler
        assert ring.nowait_error_handler is handler
        with pytest.raises(TypeError, match="nowait_error_handler"):
            ring.nowait_error_handler = 1  # type: ignore[assignment]
        with pytest.raises(TypeError, match="cannot delete nowait_error_handler"):
            del ring.nowait_error_handler


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
            ring.prepare_sendto(sender.fileno(), b"hello", receiver.getsockname(), 0, token)

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
            ring.prepare_recvmsg(receiver.fileno(), buf, 0, token)
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
            pending = ring.prepare_sendmsg(sender.fileno(), b"hello", receiver.getsockname(), 0, token)

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
        pending = ring.prepare_socket(socket.AF_INET, socket.SOCK_STREAM, user_data=token)

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
        pending = ring.prepare_socket(
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
        pending = ring.prepare_socket(
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
            ring.prepare_recv(left.fileno(), recv_buf, 0, 130)
            ring.prepare_send(right.fileno(), b"ping", 0, 131)

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

