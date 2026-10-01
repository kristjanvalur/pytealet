"""BufGroup.inflight_count tracks armed provided-buffer receives, not leased views."""

import errno
import socket
import time

import pytest

import uring_api

from conftest import require_uring
from helpers import wait_one


def _drain_until(ring, predicate, timeout=1.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and not predicate():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        ring.wait(min(0.05, remaining))


def _multishot(ring, sock, group):
    try:
        return ring.prepare_recv_multishot(sock.fileno(), group, 0, "ms")
    except OSError as exc:
        if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
            pytest.skip(f"recv multishot buffers are not supported: errno {exc.errno}")
        raise


def test_inflight_tracks_overlapping_recvs_and_close_defers():
    require_uring()

    first_r, first_w = socket.socketpair()
    second_r, second_w = socket.socketpair()
    try:
        first_r.setblocking(False)
        first_w.setblocking(False)
        second_r.setblocking(False)
        with uring_api.Ring() as ring:
            try:
                group = ring.create_buf_group(8, 4)
            except OSError as exc:
                if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    pytest.skip(f"provided buffers are not supported: errno {exc.errno}")
                raise
            assert group.inflight_count == 0
            constructed = ring.construct_recv_multishot(first_r.fileno(), group, 0, "constructed")
            assert group.inflight_count == 0
            ring.prepare(constructed)
            assert group.inflight_count == 1

            second = _multishot(ring, second_r, group)
            assert group.inflight_count == 2
            group_id = group.group_id
            assert group_id != 0

            returned = []
            group.release_callback = returned.append
            group.close()
            group.close()
            assert returned == [group, group]
            assert group.inflight_count == 2
            assert group.group_id == group_id

            first_w.send(b"hello")
            data = wait_one(ring, 1.0)
            assert data is not None
            if data.res < 0:
                errno_value = -data.res
                if errno_value in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP, errno.ENOBUFS}:
                    pytest.skip(f"recv multishot is not supported: errno {errno_value}")
            assert data is not constructed
            assert data.res == 5
            assert group.inflight_count == 2

            constructed.no_deliver_cancel = True
            second.no_deliver_cancel = True
            ring.prepare_cancel_nowait(constructed)
            _drain_until(ring, lambda: constructed.res == -errno.ECANCELED)
            assert constructed.res == -errno.ECANCELED
            assert group.inflight_count == 1
            ring.prepare_cancel_nowait(second)
            _drain_until(ring, lambda: second.res == -errno.ECANCELED)
            assert second.res == -errno.ECANCELED
            assert group.inflight_count == 0
            # the hook is not a close: completions did not unregister
            assert returned == [group, group]
            assert group.group_id == group_id
            group.release_callback = None
            group.close()
            assert group.group_id == 0
    finally:
        first_r.close()
        first_w.close()
        second_r.close()
        second_w.close()


def test_recv_buf_inflight_drops_on_completion():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            try:
                group = ring.create_buf_group(8, 2)
                pending = ring.prepare_recv_buf(reader.fileno(), group, 0, "oneshot")
            except OSError as exc:
                if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    pytest.skip(f"provided-buffer recv is not supported: errno {exc.errno}")
                raise
            assert group.inflight_count == 1
            group_id = group.group_id
            group.close()
            assert group.group_id == group_id
            writer.send(b"z")
            completion = wait_one(ring, 1.0)
            assert completion is pending
            if completion.res < 0:
                errno_value = -completion.res
                if errno_value in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP, errno.ENOBUFS}:
                    pytest.skip(f"provided-buffer recv is not supported: errno {errno_value}")
            assert completion.res == 1
            assert group.inflight_count == 0
            # no hook: the terminal CQE unregisters, and the id can be reused
            assert group.group_id == 0
            group.close()
            assert group.group_id == 0
            again = ring.create_buf_group(8, 2)
            assert again.group_id == group_id
    finally:
        reader.close()
        writer.close()
