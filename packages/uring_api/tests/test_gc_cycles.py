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
            completion = ring.prepare_recv(reader.fileno(), bytearray(8), user_data=user_data)
            user_data.append(completion)
            writer.send(b"x")
            assert wait_one(ring, 1.0).res == 1
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


def test_take_user_data_on_idle_handle_returns_and_clears():
    require_uring()

    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4) as ring:
            token = object()
            pending = ring.prepare_recv(reader.fileno(), bytearray(4), user_data=token)
            assert pending.take_user_data() is token
            assert pending.user_data is None
            assert pending.take_user_data() is None
            writer.send(b"abcd")
            done = wait_one(ring, 1.0)
            assert done is pending
            assert done.user_data is None
    finally:
        reader.close()
        writer.close()


def test_two_workers_recv_multishot_take_keeps_token_on_every_leg():
    """MORE shells still see user_data when a !MORE callback takes the parent.

    Two ``serve_completions`` workers can package ``!MORE`` and run
    ``take_user_data()`` before an earlier MORE shell is built. The callback
    takes possession (same as tealetio delivery).
    """

    require_uring()

    reader, writer = socket.socketpair()
    writer_open = True
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        token = object()
        seen: list[tuple[bool, object]] = []
        lock = threading.Lock()
        got_any = threading.Event()
        got_terminal = threading.Event()
        handle_box: list[uring_api.Completion] = []

        def callback(completion: uring_api.Completion) -> None:
            handle = handle_box[0]
            with lock:
                seen.append((completion is handle, completion.take_user_data()))
            got_any.set()
            if completion is handle:
                got_terminal.set()

        with uring_api.Ring() as ring:
            threads: list[threading.Thread] = []
            try:
                try:
                    buf_group = ring.create_buf_group(8, 4)
                    handle = ring.prepare_recv_multishot(reader.fileno(), buf_group, 0, token)
                except OSError as exc:
                    if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                        pytest.skip(f"recv multishot buffers are not supported: errno {exc.errno}")
                    raise
                handle_box.append(handle)
                ring.callback = callback
                threads = [threading.Thread(target=ring.serve_completions) for _ in range(2)]
                for thread in threads:
                    thread.start()
                wait_until_running(ring)
                ring.submit()
                writer.send(b"hello")
                assert got_any.wait(2.0), f"no CQE; seen={seen!r}"
                # close after the first leg so the kernel posts !MORE while the
                # request is armed (send+close before the first CQE can stall).
                if not got_terminal.is_set():
                    writer.close()
                    writer_open = False
                    assert got_terminal.wait(2.0), f"no terminal CQE; seen={seen!r}"

                assert seen
                assert any(is_handle for is_handle, _ud in seen)
                for is_handle, user_data in seen:
                    assert user_data is token, (is_handle, user_data)
                assert handle.user_data is None
            finally:
                # stop workers before Ring.close() so wait_cqe cannot outlive the test
                ring.stop_serving()
                for thread in threads:
                    thread.join(3.0)
    finally:
        reader.close()
        if writer_open:
            writer.close()


def test_replace_user_data_while_multishot_shells_are_copied():
    """Replacing user_data during serve must not deadlock or drop the copy.

    Two workers package shells while another thread swaps the armed slot
    and reads it back. Each delivery keeps whichever object it copied.
    The hammer stops before the writer is closed. A free-threaded
    gc.collect() stops the world, and leaving that running across the
    end-of-stream reap stalls the workers past the wait.
    """

    require_uring()

    reader, writer = socket.socketpair()
    writer_open = True
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        seen: list[object] = []
        seen_lock = threading.Lock()
        got_terminal = threading.Event()
        handle_box: list[uring_api.Completion] = []
        stop_hammer = threading.Event()
        hammer_error: list[BaseException] = []

        def callback(completion: uring_api.Completion) -> None:
            user_data = completion.user_data
            with seen_lock:
                seen.append(user_data)
            if completion is handle_box[0]:
                got_terminal.set()

        def hammer() -> None:
            handle = handle_box[0]
            swaps = 0
            collects = 0
            try:
                while not stop_hammer.is_set():
                    handle.user_data = object()
                    if handle.user_data is None:
                        raise AssertionError("armed user_data was cleared")
                    swaps += 1
                    # four collections, then pure swaps. a free-threaded
                    # gc.collect() stops the world; doing it until the
                    # terminal CQE arrives stalls the reapers.
                    if swaps % 32 == 0 and collects < 4:
                        collects += 1
                        gc.collect()
            except BaseException as exc:
                hammer_error.append(exc)
                got_terminal.set()

        with uring_api.Ring() as ring:
            threads: list[threading.Thread] = []
            hammer_thread: threading.Thread | None = None
            try:
                try:
                    buf_group = ring.create_buf_group(8, 4)
                    handle = ring.prepare_recv_multishot(reader.fileno(), buf_group, 0, object())
                except OSError as exc:
                    if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                        pytest.skip(f"recv multishot buffers are not supported: errno {exc.errno}")
                    raise
                handle_box.append(handle)
                ring.callback = callback
                threads = [threading.Thread(target=ring.serve_completions) for _ in range(2)]
                for thread in threads:
                    thread.start()
                wait_until_running(ring)
                ring.submit()
                hammer_thread = threading.Thread(target=hammer)
                hammer_thread.start()
                for _ in range(6):
                    writer.send(b"xy")
                    time.sleep(0.01)
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline:
                    with seen_lock:
                        if seen:
                            break
                    time.sleep(0.01)
                stop_hammer.set()
                hammer_thread.join(3.0)
                assert not hammer_thread.is_alive()
                assert not hammer_error, hammer_error
                with seen_lock:
                    assert seen, "no CQE while user_data was being replaced"
                writer.close()
                writer_open = False
                assert got_terminal.wait(2.0), f"no terminal CQE; seen={len(seen)}"
            finally:
                stop_hammer.set()
                if hammer_thread is not None:
                    hammer_thread.join(3.0)
                    assert not hammer_thread.is_alive()
                ring.stop_serving()
                for thread in threads:
                    thread.join(3.0)
                    assert not thread.is_alive()
        assert not hammer_error, hammer_error
        assert seen
        assert all(item is not None for item in seen)
    finally:
        reader.close()
        if writer_open:
            writer.close()


def test_completion_user_data_is_settable_and_clearable():
    require_uring()

    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4) as ring:
            token = {"op": "recv"}
            pending = ring.prepare_recv(reader.fileno(), bytearray(4), user_data=token)
            assert pending.user_data is token
            pending.user_data = {"replaced": True}
            assert pending.user_data == {"replaced": True}
            writer.send(b"abcd")
            done = wait_one(ring, 1.0)
            assert done is pending
            assert done.user_data == {"replaced": True}
            done.user_data = None
            assert done.user_data is None
            del done.user_data
            assert done.user_data is None
    finally:
        reader.close()
        writer.close()


def test_take_user_data_breaks_waitable_cycle():
    """Taking user_data after delivery allows GC without dropping reverse first."""

    require_uring()

    class Marker:
        pass

    reader, writer = connected_tcp_pair()
    try:
        ring = uring_api.Ring(entries=4)
        try:
            marker = Marker()
            marker_ref = weakref.ref(marker)
            payload = [marker]
            completion = ring.prepare_recv(reader.fileno(), bytearray(8), user_data=payload)
            payload.append(completion)
            writer.send(b"y")
            assert wait_one(ring, 1.0).res == 1
            taken = completion.take_user_data()
            del taken
            del payload
            del marker
            gc.collect()
            assert marker_ref() is None
        finally:
            ring.close()
    finally:
        reader.close()
        writer.close()


def test_assigning_none_user_data_breaks_waitable_cycle():
    """Assigning None after delivery allows GC without dropping reverse first."""

    require_uring()

    class Marker:
        pass

    reader, writer = connected_tcp_pair()
    try:
        ring = uring_api.Ring(entries=4)
        try:
            marker = Marker()
            marker_ref = weakref.ref(marker)
            # waitable-shaped cycle: list holds marker and completion
            payload = [marker]
            completion = ring.prepare_recv(reader.fileno(), bytearray(8), user_data=payload)
            payload.append(completion)
            writer.send(b"y")
            assert wait_one(ring, 1.0).res == 1
            # nerf without del completion
            completion.user_data = None
            del payload
            del marker
            gc.collect()
            assert marker_ref() is None
        finally:
            ring.close()
    finally:
        reader.close()
        writer.close()


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
