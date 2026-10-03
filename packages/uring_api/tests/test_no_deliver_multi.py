"""no_deliver_multi consumes later CQEs without delivering them."""

import errno
import select
import socket
import time

import pytest

import uring_api

from conftest import require_uring
from helpers import wait_one


def _drain_until(ring, predicate, timeout=1.0):
    seen = []
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and not predicate():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        seen.extend(ring.wait(min(0.05, remaining)))
    return seen


def _prepare_multishot(ring, reader):
    try:
        group = ring.create_buf_group(8, 2)
        handle = ring.prepare_recv_multishot(reader.fileno(), group, 0, "ms")
    except OSError as exc:
        if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
            pytest.skip(f"recv multishot buffers are not supported: errno {exc.errno}")
        raise
    return group, handle


def test_no_deliver_multi_is_settable_after_prepare():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        with uring_api.Ring() as ring:
            buf = bytearray(4)
            handle = ring.prepare_recv(reader.fileno(), buf, 0, "recv")
            assert handle.prepared
            assert handle.no_deliver_multi is False
            with pytest.raises(ValueError):
                handle.skip_success = True
            handle.no_deliver_multi = True
            assert handle.no_deliver_multi is True
            built = ring.construct_cancel(handle, no_deliver_multi=False)
            assert handle.no_deliver_multi is True
            assert built.kind == uring_api.COMPLETION_KIND_CANCEL
            handle.no_deliver_multi = False
            assert handle.no_deliver_multi is False
            with pytest.raises(TypeError):
                ring.prepare_cancel_nowait(handle, True)
            ring.prepare_cancel_nowait(handle)
            seen = _drain_until(ring, lambda: handle.res == -errno.ECANCELED)
            assert handle in seen
    finally:
        reader.close()
        writer.close()


def test_failed_cancel_clears_only_the_bit_it_set():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        ring = uring_api.Ring()
        try:
            handle = ring.construct_recv(reader.fileno(), bytearray(4), 0, "recv")
            ring.close()
            with pytest.raises(RuntimeError, match="ring is closed"):
                ring.prepare_cancel(handle, no_deliver_multi=True)
            assert handle.no_deliver_multi is False
            with pytest.raises(RuntimeError, match="ring is closed"):
                ring.prepare_cancel_nowait(handle, no_deliver_multi=True)
            assert handle.no_deliver_multi is False
            handle.no_deliver_multi = True
            with pytest.raises(RuntimeError, match="ring is closed"):
                ring.prepare_cancel(handle, no_deliver_multi=True)
            assert handle.no_deliver_multi is True
            with pytest.raises(RuntimeError, match="ring is closed"):
                ring.prepare_cancel_nowait(handle, no_deliver_multi=True)
            assert handle.no_deliver_multi is True
        finally:
            ring.close()
    finally:
        reader.close()
        writer.close()


def test_cancel_keyword_sets_flag_but_oneshot_is_still_delivered():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        with uring_api.Ring() as ring:
            buf = bytearray(4)
            handle = ring.prepare_recv(reader.fileno(), buf, 0, "recv")
            cancel = ring.prepare_cancel(handle, no_deliver_multi=True)
            assert handle.no_deliver_multi is True
            seen = _drain_until(ring, lambda: handle.res == -errno.ECANCELED)
            assert handle.res == -errno.ECANCELED
            assert handle in seen
            assert cancel in seen
    finally:
        reader.close()
        writer.close()


def test_property_on_oneshot_still_delivers_cancel():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        with uring_api.Ring() as ring:
            buf = bytearray(4)
            handle = ring.prepare_recv(reader.fileno(), buf, 0, "recv")
            handle.no_deliver_multi = True
            assert ring.prepare_cancel_nowait(handle) is None
            seen = _drain_until(ring, lambda: handle.res == -errno.ECANCELED)
            assert handle.res == -errno.ECANCELED
            assert handle in seen
    finally:
        reader.close()
        writer.close()


def test_no_deliver_multi_does_not_drop_oneshot_data():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            buf = bytearray(4)
            handle = ring.prepare_recv(reader.fileno(), buf, 0, "recv")
            handle.no_deliver_multi = True
            writer.send(b"hi")
            data = wait_one(ring, 1.0)
            assert data is handle
            assert data.res == 2
            assert bytes(buf[:2]) == b"hi"
    finally:
        reader.close()
        writer.close()


def test_no_deliver_multi_keeps_data_already_seen_and_drops_terminal():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            _group, handle = _prepare_multishot(ring, reader)
            writer.send(b"hello")
            data = wait_one(ring, 1.0)
            assert data is not None
            if data.res < 0:
                errno_value = -data.res
                if errno_value in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP, errno.ENOBUFS}:
                    pytest.skip(f"recv multishot is not supported: errno {errno_value}")
            assert data is not handle
            assert data.res == 5
            assert data.flags & uring_api.IORING_CQE_F_MORE
            assert data.no_deliver_multi is False
            handle.no_deliver_multi = True
            assert handle.no_deliver_multi is True
            ring.prepare_cancel_nowait(handle)
            # the recv is already submitted, so this cancel is the only SQE.
            # wait() does not enter a nowait-only queue.
            assert ring.submit() >= 1
            seen = _drain_until(ring, lambda: handle.res == -errno.ECANCELED)
            assert handle.res == -errno.ECANCELED
            assert handle not in seen
            assert data not in seen
    finally:
        reader.close()
        writer.close()


def test_no_deliver_multi_drops_multishot_data():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            _group, handle = _prepare_multishot(ring, reader)
            handle.no_deliver_multi = True
            writer.send(b"hello")
            seen = _drain_until(ring, lambda: False, timeout=0.3)
            assert seen == []
            ring.prepare_cancel_nowait(handle)
            # wait() already flushed the recv. a lone cancel stays queued.
            assert ring.submit() >= 1
            seen = _drain_until(ring, lambda: handle.res == -errno.ECANCELED)
            if handle.res < 0 and -handle.res in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                pytest.skip(f"recv multishot is not supported: errno {-handle.res}")
            assert handle.res == -errno.ECANCELED
            assert handle not in seen
            assert all(item.res != 5 for item in seen)
    finally:
        reader.close()
        writer.close()


def test_no_deliver_multi_does_not_drop_accept_fd():
    """The flag does not swallow accept_multishot. The caller still owns the fd."""

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
            handle = ring.prepare_accept_multishot(
                server.fileno(), socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC, object()
            )
            handle.no_deliver_multi = True
            assert handle.no_deliver_multi is True
            client = socket.create_connection(server.getsockname(), timeout=1.0)
            seen = ring.wait(1.0)
            assert len(seen) == 1
            completion = seen[0]
            if completion.res < 0:
                errno_value = -completion.res
                if errno_value in {errno.EINVAL, errno.EOPNOTSUPP, errno.ENOSYS}:
                    pytest.skip(f"IORING_ACCEPT_MULTISHOT is not supported: errno {errno_value}")
                pytest.fail(f"accept failed: errno {errno_value}")
            assert completion is not handle
            assert completion.kind == uring_api.COMPLETION_KIND_ACCEPT
            accepted_fd = completion.result
            assert accepted_fd == completion.res
            accepted = socket.socket(fileno=accepted_fd)
            assert accepted.getpeername() == client.getsockname()
    finally:
        if accepted is not None:
            accepted.close()
        if client is not None:
            client.close()
        server.close()


def test_no_deliver_multi_does_not_drop_poll_multishot():
    """The flag does not swallow poll_multishot readiness."""

    require_uring()
    if not uring_api.probe().get("IORING_POLL_MULTISHOT", False):
        pytest.skip("IORING_POLL_MULTISHOT is not available")

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        with uring_api.Ring() as ring:
            handle = ring.prepare_poll_multishot(reader.fileno(), select.POLLIN, object())
            handle.no_deliver_multi = True
            assert handle.no_deliver_multi is True
            writer.send(b"a")
            seen = ring.wait(1.0)
            assert len(seen) == 1
            completion = seen[0]
            if completion.res < 0:
                errno_value = -completion.res
                if errno_value in {errno.EINVAL, errno.EOPNOTSUPP, errno.ENOSYS}:
                    pytest.skip(f"IORING_POLL_MULTISHOT is not supported: errno {errno_value}")
                pytest.fail(f"poll failed: errno {errno_value}")
            assert completion is not handle
            assert completion.kind == uring_api.COMPLETION_KIND_POLL_MULTISHOT
            assert completion.res & select.POLLIN
            assert completion.result == completion.res
    finally:
        reader.close()
        writer.close()


def test_no_deliver_multi_drops_multishot_eof():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            _group, handle = _prepare_multishot(ring, reader)
            assert ring.pending_count() == 1
            handle.no_deliver_multi = True
            writer.close()
            seen = _drain_until(ring, lambda: ring.pending_count() == 0)
            if handle.res < 0 and -handle.res in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP, errno.ENOBUFS}:
                pytest.skip(f"recv multishot is not supported: errno {-handle.res}")
            assert handle.res == 0
            assert not (handle.flags & uring_api.IORING_CQE_F_MORE)
            assert handle not in seen
    finally:
        reader.close()
        writer.close()


def test_no_deliver_multi_drops_enobufs():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            try:
                group = ring.create_buf_group(8, 1)
                handle = ring.prepare_recv_multishot(reader.fileno(), group, 0, "ms")
            except OSError as exc:
                if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    pytest.skip(f"recv multishot buffers are not supported: errno {exc.errno}")
                raise
            writer.send(b"x")
            first = wait_one(ring, 1.0)
            assert first is not None
            if first.res < 0:
                errno_value = -first.res
                if errno_value in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    pytest.skip(f"recv multishot is not supported: errno {errno_value}")
                if errno_value == errno.ENOBUFS:
                    pytest.skip("first recv was ENOBUFS before no_deliver_multi was set")
            assert first is not handle
            assert first.res > 0
            held = memoryview(first.result)
            try:
                handle.no_deliver_multi = True
                writer.send(b"y")
                seen = _drain_until(ring, lambda: handle.res == -errno.ENOBUFS)
            finally:
                del held
            assert handle.res == -errno.ENOBUFS
            assert handle not in seen
    finally:
        reader.close()
        writer.close()
