"""construct_* + prepare: bind VIEW args without taking an SQE."""

from __future__ import annotations

import errno
import os
import select
import socket
import tempfile

import pytest
from conftest import require_uring_capability
from helpers import connect_to_listener, require_uring, wait_one, wait_one_data

import uring_api


def test_construct_send_is_not_kernel_visible_until_prepare():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            token = object()
            pending = ring.construct_send(writer.fileno(), b"hi", 0, token)
            assert pending.kind == uring_api.COMPLETION_KIND_SEND
            assert pending.user_data is token
            assert pending.prepared is False

            # no SQE yet: wait times out and the peer has nothing
            assert ring.wait(0.05) == []
            with pytest.raises(BlockingIOError):
                reader.recv(2)

            n = ring.prepare(pending)
            assert n == 1
            assert pending.prepared is True

            completion = wait_one(ring, 1.0)
            assert completion is pending
            assert completion.res == 2
            assert reader.recv(2) == b"hi"
    finally:
        reader.close()
        writer.close()


def test_prepare_batch_and_reverse_link_before_sqe():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            first_waitable = {"completion": None}
            second_waitable = {"completion": None}
            first = ring.construct_send(writer.fileno(), b"ab", 0, first_waitable)
            second = ring.construct_send(writer.fileno(), b"cd", 0, second_waitable)
            first_waitable["completion"] = first
            second_waitable["completion"] = second
            assert first.prepared is False
            assert second.prepared is False

            n = ring.prepare([first, second])
            assert n == 2
            assert first.prepared is True
            assert second.prepared is True
            assert first_waitable["completion"] is first
            assert second_waitable["completion"] is second

            seen = []
            for _ in range(10):
                seen.extend(ring.wait(0.2))
                if first in seen and second in seen:
                    break
            assert first in seen and second in seen
            assert reader.recv(4) == b"abcd"
    finally:
        reader.close()
        writer.close()


def test_construct_send_all_is_not_kernel_visible_until_prepare():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            pending = ring.construct_send_all(writer.fileno(), b"hi", 0, object())
            assert pending.kind == uring_api.COMPLETION_KIND_SEND_ALL
            assert pending.prepared is False
            assert ring.wait(0.05) == []
            assert ring.prepare(pending) == 1
            assert pending.prepared is True
            completion = wait_one(ring, 1.0)
            assert completion is pending
            assert completion.res == 2
            assert reader.recv(2) == b"hi"
    finally:
        reader.close()
        writer.close()


def test_construct_send_zc_prepares_like_send():
    require_uring()
    if not uring_api.probe().get("IORING_OP_SEND_ZC"):
        pytest.skip("IORING_OP_SEND_ZC not available")

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        # AF_UNIX often returns -EOPNOTSUPP for ZC; still exercises construct/prepare.
        with uring_api.Ring() as ring:
            pending = ring.construct_send_zc(writer.fileno(), b"zc")
            assert pending.kind == uring_api.COMPLETION_KIND_SEND_ZC
            assert pending.prepared is False
            assert ring.prepare(pending) == 1
            assert pending.prepared is True
            completion = wait_one(ring, 1.0)
            assert completion is pending
    finally:
        reader.close()
        writer.close()


def test_prepare_send_is_construct_plus_prepare():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            token = object()
            pending = ring.prepare_send(writer.fileno(), b"xyz", 0, token)
            assert pending.prepared is True
            assert pending.user_data is token
            completion = wait_one(ring, 1.0)
            assert completion is pending
            assert completion.res == 3
            assert reader.recv(3) == b"xyz"
    finally:
        reader.close()
        writer.close()


def test_prepare_rejects_already_prepared():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            send = ring.construct_send(writer.fileno(), b"x")
            ring.prepare(send)
            with pytest.raises(ValueError, match="already prepared"):
                ring.prepare(send)
            assert wait_one(ring, 1.0) is send
    finally:
        reader.close()
        writer.close()


def test_unprepared_completion_can_be_dropped():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            pending = ring.construct_send(writer.fileno(), b"nope")
            del pending
            assert ring.wait(0.05) == []
            with pytest.raises(BlockingIOError):
                reader.recv(4)
    finally:
        reader.close()
        writer.close()


def test_prepare_empty_sequence():
    require_uring()

    with uring_api.Ring() as ring:
        assert ring.prepare([]) == 0
        assert ring.prepare(()) == 0


def test_construct_recv_is_not_kernel_visible_until_prepare():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        writer.send(b"hi")
        with uring_api.Ring() as ring:
            token = object()
            buf = bytearray(2)
            pending = ring.construct_recv(reader.fileno(), buf, 0, token)
            assert pending.kind == uring_api.COMPLETION_KIND_RECV
            assert pending.user_data is token
            assert pending.prepared is False

            assert ring.wait(0.05) == []
            assert bytes(buf) == b"\x00\x00"

            n = ring.prepare(pending)
            assert n == 1
            assert pending.prepared is True

            completion = wait_one(ring, 1.0)
            assert completion is pending
            assert completion.res == 2
            assert bytes(buf) == b"hi"
    finally:
        reader.close()
        writer.close()


def test_prepare_recv_is_construct_plus_prepare():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        writer.send(b"xy")
        with uring_api.Ring() as ring:
            buf = bytearray(2)
            pending = ring.prepare_recv(reader.fileno(), buf)
            assert pending.prepared is True
            completion = wait_one(ring, 1.0)
            assert completion is pending
            assert completion.res == 2
            assert bytes(buf) == b"xy"
    finally:
        reader.close()
        writer.close()


def test_construct_read_write_file():
    require_uring()

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        path = tmp.name
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT)
        try:
            with uring_api.Ring() as ring:
                write = ring.construct_write(fd, b"hello", 0)
                assert write.kind == uring_api.COMPLETION_KIND_WRITE
                assert write.prepared is False
                assert ring.prepare(write) == 1
                assert write.prepared is True
                assert wait_one(ring, 1.0) is write
                assert write.res == 5

                buf = bytearray(5)
                read = ring.construct_read(fd, buf, 0)
                assert read.kind == uring_api.COMPLETION_KIND_READ
                assert read.prepared is False
                assert ring.prepare(read) == 1
                assert wait_one(ring, 1.0) is read
                assert read.res == 5
                assert bytes(buf) == b"hello"
        finally:
            os.close(fd)
    finally:
        os.unlink(path)


def test_construct_read_write_reject_negative_offset():
    require_uring()

    with uring_api.Ring() as ring:
        with pytest.raises(ValueError, match="offset must be non-negative"):
            ring.construct_read(0, bytearray(1), -1)
        with pytest.raises(ValueError, match="offset must be non-negative"):
            ring.construct_write(0, b"x", -1)


def test_prepare_mixed_view_kinds():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            send = ring.construct_send(writer.fileno(), b"ab")
            buf = bytearray(2)
            recv = ring.construct_recv(reader.fileno(), buf)
            assert ring.prepare([send, recv]) == 2
            seen = []
            for _ in range(10):
                seen.extend(ring.wait(0.2))
                if send in seen and recv in seen:
                    break
            assert send in seen and recv in seen
            assert bytes(buf) == b"ab"
    finally:
        reader.close()
        writer.close()


def test_construct_sendto_recvmsg():
    require_uring()

    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        receiver.bind(("127.0.0.1", 0))
        receiver.setblocking(False)
        sender.setblocking(False)
        with uring_api.Ring() as ring:
            send = ring.construct_sendto(sender.fileno(), b"hello", receiver.getsockname())
            assert send.kind == uring_api.COMPLETION_KIND_SENDTO
            assert send.prepared is False
            buf = bytearray(5)
            recv = ring.construct_recvmsg(receiver.fileno(), buf)
            assert recv.kind == uring_api.COMPLETION_KIND_RECVMSG
            assert recv.prepared is False
            assert ring.prepare([send, recv]) == 2
            seen = []
            for _ in range(10):
                seen.extend(ring.wait(0.2))
                if send in seen and recv in seen:
                    break
            assert send in seen and recv in seen
            assert send.res == 5
            assert recv.res == 5
            assert bytes(buf) == b"hello"
    finally:
        sender.close()
        receiver.close()


def test_construct_sendmsg_and_connect():
    require_uring()

    receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        receiver.bind(("127.0.0.1", 0))
        receiver.setblocking(False)
        sender.setblocking(False)
        with uring_api.Ring() as ring:
            pending = ring.construct_sendmsg(sender.fileno(), b"hello", receiver.getsockname())
            assert pending.kind == uring_api.COMPLETION_KIND_SENDMSG
            assert pending.prepared is False
            assert ring.prepare(pending) == 1
            assert wait_one(ring, 1.0) is pending
            assert pending.res == 5
            data, address = receiver.recvfrom(5)
            assert data == b"hello"
            assert address[1] == sender.getsockname()[1]
    finally:
        sender.close()
        receiver.close()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    accepted = None
    try:
        server.setblocking(False)
        server.bind(("127.0.0.1", 0))
        server.listen()
        client.setblocking(False)
        with uring_api.Ring() as ring:
            pending = ring.construct_connect(client.fileno(), server.getsockname())
            assert pending.kind == uring_api.COMPLETION_KIND_CONNECT
            assert pending.prepared is False
            assert ring.prepare(pending) == 1
            assert wait_one(ring, 1.0) is pending
            assert pending.res == 0
            accepted, _address = server.accept()
    finally:
        if accepted is not None:
            accepted.close()
        client.close()
        server.close()


def test_construct_recv_buf():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            try:
                buf_group = ring.create_buf_group(8, 4)
                pending = ring.construct_recv_buf(reader.fileno(), buf_group)
            except OSError as exc:
                if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    pytest.skip(f"provided-buffer recv is not supported: errno {exc.errno}")
                raise
            assert pending.kind == uring_api.COMPLETION_KIND_RECV_BUF
            assert pending.prepared is False
            writer.send(b"hello")
            assert ring.wait(0.05) == []
            assert ring.prepare(pending) == 1
            completion = wait_one(ring, 1.0)
            if completion.res < 0:
                errno_value = -completion.res
                if errno_value in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP, errno.ENOBUFS}:
                    pytest.skip(f"provided-buffer recv is not supported: errno {errno_value}")
            assert completion is pending
            assert completion.res == 5
            assert isinstance(completion.result, uring_api.BufView)
            assert bytes(memoryview(completion.result)) == b"hello"
    finally:
        reader.close()
        writer.close()


def test_construct_openat_and_statx_fdsize():
    require_uring()

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "construct-openat.txt")
        with uring_api.Ring() as ring:
            open_pending = ring.construct_openat(uring_api.AT_FDCWD, path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
            assert open_pending.kind == uring_api.COMPLETION_KIND_OPENAT
            assert open_pending.prepared is False
            assert ring.prepare(open_pending) == 1
            assert wait_one(ring, 1.0) is open_pending
            assert open_pending.res >= 0
            fd = open_pending.res
            try:
                assert os.write(fd, b"hello") == 5
                statx = ring.construct_statx_fdsize(fd)
                assert statx.kind == uring_api.COMPLETION_KIND_STATX_FDSIZE
                assert statx.prepared is False
                assert ring.prepare(statx) == 1
                assert wait_one(ring, 1.0) is statx
                assert statx.res == 0
                assert statx.result == 5
            finally:
                os.close(fd)


def test_construct_poll_socket_and_close():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        writer.send(b"x")
        with uring_api.Ring() as ring:
            poll = ring.construct_poll(reader.fileno(), select.POLLIN)
            assert poll.kind == uring_api.COMPLETION_KIND_POLL
            assert poll.prepared is False
            assert ring.prepare(poll) == 1
            assert wait_one(ring, 1.0) is poll
            assert poll.res > 0

            created = ring.construct_socket(socket.AF_UNIX, socket.SOCK_STREAM)
            assert created.kind == uring_api.COMPLETION_KIND_SOCKET
            assert created.prepared is False
            assert ring.prepare(created) == 1
            assert wait_one(ring, 1.0) is created
            if created.res < 0:
                errno_value = -created.res
                if errno_value in {errno.ENOSYS, errno.EOPNOTSUPP, errno.EINVAL}:
                    pytest.skip(f"IORING_OP_SOCKET is not supported: errno {errno_value}")
                pytest.fail(f"construct_socket failed: errno {errno_value}")
            fd = created.res
            close = ring.construct_close(fd)
            assert close.kind == uring_api.COMPLETION_KIND_CLOSE
            assert close.prepared is False
            assert ring.prepare(close) == 1
            assert wait_one(ring, 1.0) is close
            assert close.res == 0
    finally:
        reader.close()
        writer.close()


def test_construct_cancel_of_unprepared_target():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            buf = bytearray(1)
            recv = ring.construct_recv(reader.fileno(), buf)
            cancel = ring.construct_cancel(recv)
            assert recv.prepared is False
            assert cancel.prepared is False
            assert cancel.kind == uring_api.COMPLETION_KIND_CANCEL
            assert ring.wait(0.05) == []

            assert ring.prepare([recv, cancel]) == 2
            seen = []
            for _ in range(10):
                seen.extend(ring.wait(0.2))
                if recv in seen and cancel in seen:
                    break
            assert recv in seen and cancel in seen
            assert recv.res < 0
            assert -recv.res == errno.ECANCELED
    finally:
        reader.close()
        writer.close()


def test_construct_poll_remove_of_unprepared_target():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            poll = ring.construct_poll(reader.fileno(), select.POLLIN)
            remove = ring.construct_poll_remove(poll)
            assert poll.prepared is False
            assert remove.prepared is False
            assert remove.kind == uring_api.COMPLETION_KIND_POLL_REMOVE
            assert ring.wait(0.05) == []

            assert ring.prepare([poll, remove]) == 2
            seen = []
            for _ in range(10):
                seen.extend(ring.wait(0.2))
                if poll in seen and remove in seen:
                    break
            assert poll in seen and remove in seen
    finally:
        reader.close()
        writer.close()


def test_construct_close_nowait_is_hold_only():
    require_uring()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fd = sock.detach()
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            close = ring.construct_close_nowait(fd)
            send = ring.construct_send(writer.fileno(), b"hi")
            assert close.nowait is True
            assert close.prepared is False
            assert ring.prepare([send, close]) == 2
            assert close.prepared is True
            del close
            completion = wait_one(ring, 1.0)
            assert completion is send
            assert reader.recv(2) == b"hi"
        with pytest.raises(OSError) as excinfo:
            os.fstat(fd)
        assert excinfo.value.errno == errno.EBADF
    finally:
        reader.close()
        writer.close()


def test_nowait_flag_rejected_on_send_and_after_prepare():
    require_uring()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fd = sock.detach()
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            send = ring.construct_send(writer.fileno(), b"x")
            with pytest.raises(ValueError, match="only valid for close"):
                send.nowait = True
            ring.prepare(send)
            with pytest.raises(ValueError, match="already prepared"):
                ring.prepare(send)
            close = ring.construct_close(fd)
            close.nowait = True
            ring.prepare(close)
            with pytest.raises(ValueError, match="cannot change nowait after prepare"):
                close.nowait = False
            assert wait_one(ring, 1.0) is send
    finally:
        reader.close()
        writer.close()


def test_prepare_rejects_recv_multishot_more_shell():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            try:
                buf_group = ring.create_buf_group(8, 4)
                handle = ring.prepare_recv_multishot(reader.fileno(), buf_group)
            except OSError as exc:
                if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    pytest.skip(f"recv multishot buffers are not supported: errno {exc.errno}")
                raise
            writer.send(b"hello")
            shell = wait_one_data(ring, 1.0)
            if shell.res < 0:
                errno_value = -shell.res
                if errno_value in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP, errno.ENOBUFS}:
                    pytest.skip(f"recv multishot is not supported: errno {errno_value}")
            assert shell is not handle
            assert shell.flags & uring_api.IORING_CQE_F_MORE
            assert shell.prepared is False
            with pytest.raises(ValueError, match="only accepts constructed completions"):
                ring.prepare(shell)
    finally:
        reader.close()
        writer.close()


def test_prepare_rejects_accept_multishot_more_shell():
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
        with uring_api.Ring() as ring:
            handle = ring.prepare_accept_multishot(server.fileno(), socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC, None)
            client = connect_to_listener(server)
            shell = wait_one(ring, 1.0)
            if shell.res < 0:
                errno_value = -shell.res
                if errno_value in {errno.EINVAL, errno.EOPNOTSUPP, errno.ENOSYS}:
                    pytest.skip(f"IORING_ACCEPT_MULTISHOT is not supported: errno {errno_value}")
            assert shell is not handle
            assert shell.flags & uring_api.IORING_CQE_F_MORE
            assert shell.prepared is False
            accepted = socket.socket(fileno=shell.res)
            with pytest.raises(ValueError, match="only accepts constructed completions"):
                ring.prepare(shell)
    finally:
        if accepted is not None:
            accepted.close()
        if client is not None:
            client.close()
        server.close()


def test_prepare_rejects_poll_multishot_more_shell():
    require_uring_capability("IORING_POLL_MULTISHOT")

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            handle = ring.prepare_poll_multishot(reader.fileno(), select.POLLIN)
            writer.send(b"a")
            shell = wait_one(ring, 1.0)
            assert shell is not handle
            assert shell.flags & uring_api.IORING_CQE_F_MORE
            assert shell.prepared is False
            with pytest.raises(ValueError, match="only accepts constructed completions"):
                ring.prepare(shell)
    finally:
        reader.close()
        writer.close()
