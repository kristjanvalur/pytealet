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
    wait_one_data,
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

def test_buf_group_callback_cycles_are_collectable():
    require_uring()

    class Marker:
        pass

    def make_cycle():
        ring = uring_api.Ring(entries=4)
        buf_group = ring.create_buf_group(16, 4)
        marker = Marker()
        marker_ref = weakref.ref(marker)

        def callback(_completion):
            marker
            buf_group.buffer_size
            ring.closed

        ring.callback = callback
        return marker_ref

    marker_ref = make_cycle()
    gc.collect()

    assert marker_ref() is None

def test_ring_recv_buf_completion_when_available():
    require_uring()

    token = object()
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            try:
                buf_group = ring.create_buf_group(8, 4)
                pending = ring.submit_recv_buf(reader.fileno(), buf_group, token)
            except OSError as exc:
                if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    pytest.skip(f"provided-buffer recv is not supported: errno {exc.errno}")
                raise

            writer.send(b"hello")
            completion = wait_one(ring, 1.0)

            assert completion is not None
            if completion.res < 0:
                errno_value = -completion.res
                if errno_value in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP, errno.ENOBUFS}:
                    pytest.skip(f"provided-buffer recv is not supported: errno {errno_value}")
            assert completion is pending
            assert completion.user_data is token
            assert completion.kind == uring_api.COMPLETION_KIND_RECV_BUF
            assert completion.res == 5
            assert isinstance(completion.result, uring_api.BufView)
            assert completion.result.length == 5
            assert completion.result.buf_group is buf_group
            view = memoryview(completion.result)
            try:
                assert bytes(view) == b"hello"
            finally:
                del view
            assert completion.result.recycled
    finally:
        reader.close()
        writer.close()

def test_ring_recv_buf_eof_returns_empty_bytes_when_available():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            try:
                buf_group = ring.create_buf_group(8, 4)
                pending = ring.submit_recv_buf(reader.fileno(), buf_group)
            except OSError as exc:
                if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    pytest.skip(f"provided-buffer recv is not supported: errno {exc.errno}")
                raise

            writer.close()
            completion = wait_one(ring, 1.0)

        assert completion is not None
        if completion.res < 0:
            errno_value = -completion.res
            if errno_value in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP, errno.ENOBUFS}:
                pytest.skip(f"provided-buffer recv is not supported: errno {errno_value}")
        assert completion is pending
        assert completion.kind == uring_api.COMPLETION_KIND_RECV_BUF
        assert completion.res == 0
        assert isinstance(completion.result, uring_api.BufView)
        assert completion.result.length == 0
        assert not completion.result
        view = memoryview(completion.result)
        try:
            assert bytes(view) == b""
        finally:
            del view
    finally:
        reader.close()
        writer.close()

def test_buf_group_exposes_creating_ring():
    require_uring()

    with uring_api.Ring() as ring:
        buf_group = ring.create_buf_group(16, 4)
        assert buf_group.ring is ring

def test_buf_group_tracks_leased_wrapper_count():
    require_uring()

    with uring_api.Ring() as ring:
        buf_group = ring.create_buf_group(16, 4)
        assert buf_group.buffer_count == 4
        assert buf_group.leased_count == 0

        buf_view = ring.create_buf_view(buf_group, 1, 5)
        assert buf_group.leased_count == 1

        mv = memoryview(buf_view)
        del mv
        assert buf_view.recycled
        assert buf_group.leased_count == 0

def test_buf_group_id_recycles_after_release():
    require_uring()

    with uring_api.Ring() as ring:
        first = ring.create_buf_group(16, 4)
        first_id = first.group_id
        del first
        collect_until_stable()

        second = ring.create_buf_group(16, 4)
        assert second.group_id == first_id

def test_buf_group_ids_stay_unique_while_live():
    require_uring()

    with uring_api.Ring() as ring:
        first = ring.create_buf_group(16, 4)
        second = ring.create_buf_group(16, 4)
        third = ring.create_buf_group(16, 4)
        assert len({first.group_id, second.group_id, third.group_id}) == 3

def test_buf_group_id_reuses_freed_slot_before_allocating_new():
    require_uring()

    with uring_api.Ring() as ring:
        first = ring.create_buf_group(16, 4)
        second = ring.create_buf_group(16, 4)
        second_id = second.group_id
        del second
        collect_until_stable()

        third = ring.create_buf_group(16, 4)
        assert third.group_id == second_id
        assert first.group_id != third.group_id

def test_buf_group_id_survives_many_create_release_cycles():
    require_uring()

    with uring_api.Ring() as ring:
        seen_ids: set[int] = set()
        for _ in range(512):
            buf_group = ring.create_buf_group(16, 4)
            seen_ids.add(buf_group.group_id)
            del buf_group
        collect_until_stable()

        reused = ring.create_buf_group(16, 4)
        assert reused.group_id in seen_ids

def test_buf_group_id_tail_shrink_reuses_highest_slot_without_new_id():
    require_uring()

    with uring_api.Ring() as ring:
        groups = [ring.create_buf_group(16, 4) for _ in range(4)]
        ids = [group.group_id for group in groups]
        assert ids == [1, 2, 3, 4]

        del groups[3]
        collect_until_stable()
        tail_reused = ring.create_buf_group(16, 4)
        assert tail_reused.group_id == 4

        del groups[2]
        collect_until_stable()
        middle_reused = ring.create_buf_group(16, 4)
        assert middle_reused.group_id == 3

def test_buf_group_id_resets_in_new_ring_session_after_close_without_gc():
    require_uring()

    ring = uring_api.Ring()
    held: list[uring_api.BufGroup] = []
    try:
        held.append(ring.create_buf_group(16, 4))
        held.append(ring.create_buf_group(16, 4))
        assert [group.group_id for group in held] == [1, 2]
        ring.close()
        with pytest.raises(RuntimeError, match="ring is closed"):
            ring.create_buf_group(16, 4)
    finally:
        if not ring.closed:
            ring.close()

    fresh = uring_api.Ring()
    try:
        assert fresh.create_buf_group(16, 4).group_id == 1
    finally:
        fresh.close()

def test_buf_group_rejects_use_on_different_ring():
    require_uring()

    with uring_api.Ring() as ring_a, uring_api.Ring() as ring_b:
        buf_group = ring_a.create_buf_group(16, 4)
        with pytest.raises(ValueError, match="buf_group was not created by this ring"):
            ring_b.submit_recv_multishot(0, buf_group)

def test_buf_view_buf_result_exposes_buf_group():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            try:
                buf_group = ring.create_buf_group(8, 4)
                ring.submit_recv_multishot(reader.fileno(), buf_group)
            except OSError as exc:
                if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    pytest.skip(f"recv multishot buffers are not supported: errno {exc.errno}")
                raise

            writer.send(b"x")
            completion = wait_one_data(ring, 1.0)

        assert completion is not None
        if completion.res < 0:
            errno_value = -completion.res
            if errno_value in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP, errno.ENOBUFS}:
                pytest.skip(f"recv multishot is not supported: errno {errno_value}")
        assert isinstance(completion.result, uring_api.BufView)
        assert completion.result.buf_group is buf_group
        assert completion.result.buf_group.ring is ring
        assert buf_group.leased_count == 1
        del completion
        assert buf_group.leased_count == 0
    finally:
        reader.close()
        writer.close()

def test_buf_group_rejects_direct_instantiation():
    require_uring()

    with pytest.raises(TypeError, match="cannot be instantiated directly"):
        uring_api.BufGroup()

def test_buf_view_rejects_direct_instantiation():
    require_uring()

    with pytest.raises(TypeError, match="cannot be instantiated directly"):
        uring_api.BufView()

def test_buf_view_zero_length_is_falsy():
    require_uring()

    with uring_api.Ring() as ring:
        buf_group = ring.create_buf_group(16, 4)
        buf_view = ring.create_buf_view(buf_group, 0, 0)
        assert buf_view.length == 0
        assert not buf_view
        view = memoryview(buf_view)
        try:
            assert bytes(view) == b""
        finally:
            del view

def test_buf_view_memoryview_recycles_on_last_release():
    require_uring()

    with uring_api.Ring() as ring:
        buf_group = ring.create_buf_group(16, 4)
        buf_view = ring.create_buf_view(buf_group, 2, 5)
        assert buf_view.length == 5
        assert buf_view.buffer_id == 2
        assert buf_view.buf_group is buf_group
        assert not buf_view.recycled
        assert buf_group.leased_count == 1

        mv1 = memoryview(buf_view)
        mv2 = memoryview(buf_view)
        assert len(mv1) == 5
        assert len(mv2) == 5
        assert not buf_view.recycled
        assert buf_group.leased_count == 1

        del mv1
        assert not buf_view.recycled
        del mv2
        assert buf_view.recycled
        assert buf_group.leased_count == 0

        with pytest.raises(BufferError, match="already been released"):
            memoryview(buf_view)

def test_buf_view_close_requires_no_active_exports():
    require_uring()

    with uring_api.Ring() as ring:
        buf_group = ring.create_buf_group(16, 4)
        buf_view = ring.create_buf_view(buf_group, 0, 4)
        mv = memoryview(buf_view)
        with pytest.raises(BufferError, match="buffer exports are active"):
            buf_view.close()
        del mv
        buf_view.close()
        assert buf_view.recycled

def test_buf_view_memoryview_is_readonly():
    require_uring()

    with uring_api.Ring() as ring:
        buf_group = ring.create_buf_group(8, 4)
        buf_view = ring.create_buf_view(buf_group, 0, 4)
        mv = memoryview(buf_view)
        assert mv.readonly
        with pytest.raises(TypeError):
            mv[0] = 1
        del mv

def test_ring_recv_multishot_completion_when_available():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        token = {"operation": "recv-multishot"}
        with uring_api.Ring() as ring:
            try:
                buf_group = ring.create_buf_group(8, 4)
                handle = ring.submit_recv_multishot(reader.fileno(), buf_group, token)
            except OSError as exc:
                if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    pytest.skip(f"recv multishot buffers are not supported: errno {exc.errno}")
                raise

            writer.send(b"hello")
            first = wait_one_data(ring, 1.0)
            writer.send(b"world")
            second = wait_one_data(ring, 1.0)

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
                assert completion.multishot is True
                assert completion.user_data is token
                assert completion.sequence == sequence
                assert isinstance(completion.result, uring_api.BufView)
                assert completion.result.length == len(expected)
                view = memoryview(completion.result)
                try:
                    assert bytes(view) == expected
                finally:
                    del view
                assert completion.res == len(expected)
                assert completion.flags & uring_api.IORING_CQE_F_MORE

            ring.submit_cancel(handle)
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                for completion in ring.wait(0.0):
                    if completion is handle:
                        break
                else:
                    continue
                break
    finally:
        reader.close()
        writer.close()

def test_ring_recv_multishot_eof_returns_empty_bufview_when_available():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        token = {"operation": "recv-multishot-eof"}
        with uring_api.Ring() as ring:
            try:
                buf_group = ring.create_buf_group(8, 4)
                handle = ring.submit_recv_multishot(reader.fileno(), buf_group, token)
            except OSError as exc:
                if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    pytest.skip(f"recv multishot buffers are not supported: errno {exc.errno}")
                raise

            writer.send(b"hello")
            first = wait_one_data(ring, 1.0)
            writer.close()
            final = wait_one(ring, 1.0)

        assert first is not None
        if first.res < 0:
            errno_value = -first.res
            if errno_value in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP, errno.ENOBUFS}:
                pytest.skip(f"recv multishot is not supported: errno {errno_value}")
        assert first is not handle
        assert first.multishot is True
        assert first.sequence == 0
        assert isinstance(first.result, uring_api.BufView)
        view = memoryview(first.result)
        try:
            assert bytes(view) == b"hello"
        finally:
            del view
        assert first.res == 5
        assert first.flags & uring_api.IORING_CQE_F_MORE

        assert final is handle
        assert final.kind == uring_api.COMPLETION_KIND_RECV_MULTISHOT
        assert final.multishot is True
        assert final.user_data is token
        assert final.sequence == 1
        assert final.res == 0
        assert isinstance(final.result, uring_api.BufView)
        assert final.result.length == 0
        assert not final.result
        assert not (final.flags & uring_api.IORING_CQE_F_MORE)
        assert buf_group.leased_count == 1
        del first
        assert buf_group.leased_count == 0
    finally:
        reader.close()
        writer.close()

