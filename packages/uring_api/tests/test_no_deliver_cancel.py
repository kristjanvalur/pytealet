"""Terminal -ECANCELED can be consumed without delivering the armed handle."""

import errno
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


def test_no_deliver_cancel_is_settable_after_prepare():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        with uring_api.Ring() as ring:
            buf = bytearray(4)
            handle = ring.prepare_recv(reader.fileno(), buf, 0, "recv")
            assert handle.prepared
            assert handle.no_deliver_cancel is False
            with pytest.raises(ValueError):
                handle.skip_success = True
            handle.no_deliver_cancel = True
            assert handle.no_deliver_cancel is True
            built = ring.construct_cancel(handle, no_deliver=False)
            assert handle.no_deliver_cancel is True
            assert built.kind == uring_api.COMPLETION_KIND_CANCEL
            handle.no_deliver_cancel = False
            assert handle.no_deliver_cancel is False
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
                ring.prepare_cancel(handle, no_deliver=True)
            assert handle.no_deliver_cancel is False
            with pytest.raises(RuntimeError, match="ring is closed"):
                ring.prepare_cancel_nowait(handle, no_deliver=True)
            assert handle.no_deliver_cancel is False
            handle.no_deliver_cancel = True
            with pytest.raises(RuntimeError, match="ring is closed"):
                ring.prepare_cancel(handle, no_deliver=True)
            assert handle.no_deliver_cancel is True
            with pytest.raises(RuntimeError, match="ring is closed"):
                ring.prepare_cancel_nowait(handle, no_deliver=True)
            assert handle.no_deliver_cancel is True
        finally:
            ring.close()
    finally:
        reader.close()
        writer.close()


def test_cancel_keyword_sets_flag_before_submit_and_suppresses_target():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        with uring_api.Ring() as ring:
            buf = bytearray(4)
            handle = ring.prepare_recv(reader.fileno(), buf, 0, "recv")
            cancel = ring.prepare_cancel(handle, no_deliver=True)
            assert handle.no_deliver_cancel is True
            seen = _drain_until(ring, lambda: handle.res == -errno.ECANCELED)
            assert handle.res == -errno.ECANCELED
            assert handle not in seen
            assert cancel in seen
    finally:
        reader.close()
        writer.close()


def test_property_then_cancel_nowait_suppresses_without_keyword():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        with uring_api.Ring() as ring:
            buf = bytearray(4)
            handle = ring.prepare_recv(reader.fileno(), buf, 0, "recv")
            handle.no_deliver_cancel = True
            assert ring.prepare_cancel_nowait(handle) is None
            seen = _drain_until(ring, lambda: handle.res == -errno.ECANCELED)
            assert handle.res == -errno.ECANCELED
            assert handle not in seen
    finally:
        reader.close()
        writer.close()


def test_no_deliver_cancel_keeps_multishot_data_and_drops_terminal():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            _group, handle = _prepare_multishot(ring, reader)
            handle.no_deliver_cancel = True
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
            assert data.no_deliver_cancel is False
            assert handle.no_deliver_cancel is True
            ring.prepare_cancel_nowait(handle)
            seen = _drain_until(ring, lambda: handle.res == -errno.ECANCELED)
            assert handle.res == -errno.ECANCELED
            assert handle not in seen
            assert data not in seen
    finally:
        reader.close()
        writer.close()


def test_no_deliver_cancel_still_delivers_multishot_eof():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            _group, handle = _prepare_multishot(ring, reader)
            handle.no_deliver_cancel = True
            writer.close()
            completion = wait_one(ring, 1.0)
            assert completion is handle
            if completion.res < 0:
                errno_value = -completion.res
                if errno_value in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP, errno.ENOBUFS}:
                    pytest.skip(f"recv multishot is not supported: errno {errno_value}")
            assert completion.res == 0
            assert not (completion.flags & uring_api.IORING_CQE_F_MORE)
    finally:
        reader.close()
        writer.close()


def test_no_deliver_cancel_still_delivers_enobufs():
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
            handle.no_deliver_cancel = True
            writer.send(b"x")
            first = wait_one(ring, 1.0)
            assert first is not None
            if first.res < 0:
                errno_value = -first.res
                if errno_value in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    pytest.skip(f"recv multishot is not supported: errno {errno_value}")
                if errno_value == errno.ENOBUFS:
                    assert first is handle
                    return
            assert first is not handle
            assert first.res > 0
            held = memoryview(first.result)
            try:
                writer.send(b"y")
                terminal = wait_one(ring, 1.0)
            finally:
                del held
            assert terminal is handle
            assert terminal.res == -errno.ENOBUFS
    finally:
        reader.close()
        writer.close()
