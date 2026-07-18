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

def test_ring_wait_batches_multiple_ready_completions_when_available():
    require_uring()

    left, right = socket.socketpair()
    try:
        left.setblocking(False)
        right.setblocking(False)
        with uring_api.Ring() as ring:
            ring.submit_recv(left.fileno(), bytearray(1), 150)
            ring.submit_recv(left.fileno(), bytearray(1), 151)
            right.send(b"ab")
            batch = ring.wait(1.0)

        assert len(batch) == 2
        by_user_data = {completion.user_data: completion for completion in batch}
        assert by_user_data[150].res == 1
        assert by_user_data[151].res == 1
    finally:
        left.close()
        right.close()


def test_ring_recv_multishot_wait_from_allow_threads_path():
    """Multishot CQE drain updates refcount_mutex from Py_BEGIN_ALLOW_THREADS.

    Free-threaded builds must not use PyCriticalSection on that path (see
    uring_api_refcount_mutex_lock in uring_api_common.h).
    """

    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            try:
                buf_group = ring.create_buf_group(8, 4)
                ring.submit_recv_multishot(reader.fileno(), buf_group, 99)
            except OSError as exc:
                if exc.errno in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                    pytest.skip(f"recv multishot buffers are not supported: errno {exc.errno}")
                raise

            writer.send(b"x")
            batch = ring.wait(1.0)

        assert len(batch) == 1
        assert batch[0].multishot is True
        assert batch[0].res == 1
    finally:
        reader.close()
        writer.close()


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
    assert results == [[]]


def test_ring_wait_idle_timeout_when_available():
    require_uring()

    with uring_api.Ring() as ring:
        assert ring.wait_idle(0.05) is False
        assert ring.wait_idle(0) is False


def test_ring_break_wait_wakes_wait_idle_when_available():
    require_uring()

    with uring_api.Ring() as ring:
        results: list[bool] = []
        thread = threading.Thread(target=lambda: results.append(ring.wait_idle(10.0)))
        thread.start()
        # give the waiter a moment to park
        time.sleep(0.05)
        ring.break_wait()
        thread.join(1.0)
        if thread.is_alive():
            ring.break_wait()
            thread.join(1.0)

    assert thread.is_alive() is False
    assert results == [True]


def test_ring_break_wait_latches_wait_idle_before_park_when_available():
    """break_wait opens wait_idle immediately (not only after a NOP CQE)."""

    require_uring()

    with uring_api.Ring() as ring:
        ring.break_wait()
        # latch is open without reaping the internal NOP from the CQ
        assert ring.wait_idle(0) is True
        assert ring.wait_idle(0) is False


def test_ring_break_wait_opens_idle_while_serving_when_available():
    """While serve workers own the CQ, break_wait still opens wait_idle (no wait())."""

    require_uring()

    with uring_api.Ring() as ring:
        ring.callback = lambda _batch: None
        thread = threading.Thread(target=ring.serve_completions)
        thread.start()
        wait_until_running(ring)
        try:
            ring.break_wait()
            assert ring.wait_idle(0) is True
        finally:
            ring.stop_serving()
            thread.join(1.0)
            assert not thread.is_alive()


def test_ring_wait_with_callback_delivers_and_returns_none_when_available():
    require_uring()

    left, right = socket.socketpair()
    try:
        left.setblocking(False)
        right.setblocking(False)
        batches: list[list[uring_api.Completion]] = []

        def callback(batch):
            batches.append(list(batch))

        with uring_api.Ring() as ring:
            ring.callback = callback
            ring.submit_recv(left.fileno(), bytearray(1), 170)
            right.send(b"x")
            result = ring.wait(1.0)

        assert result is None
        assert len(batches) == 1
        assert len(batches[0]) == 1
        assert batches[0][0].user_data == 170
        assert batches[0][0].res == 1
    finally:
        left.close()
        right.close()


def test_ring_wait_with_callback_skips_empty_batch_when_available():
    require_uring()

    calls: list[object] = []

    def callback(batch):
        calls.append(list(batch))

    with uring_api.Ring() as ring:
        ring.callback = callback
        result = ring.wait(0.0)

    assert result is None
    assert calls == []


def test_ring_break_wait_with_callback_returns_none_without_callback_when_available():
    require_uring()

    calls: list[object] = []
    results: list[object] = []

    def callback(batch):
        calls.append(list(batch))

    with uring_api.Ring() as ring:
        ring.callback = callback
        thread = threading.Thread(target=lambda: results.append(ring.wait(10.0)))
        thread.start()
        ring.break_wait()
        thread.join(1.0)
        if thread.is_alive():
            ring.break_wait()
            thread.join(1.0)

    assert thread.is_alive() is False
    assert results == [None]
    assert calls == []

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
                completion = wait_one(ring, 0)
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
    assert results == [[]]

def test_ring_serve_completions_delivers_single_batched_callback_when_available():
    require_uring()

    left, right = socket.socketpair()
    try:
        left.setblocking(False)
        right.setblocking(False)
        batches: list[list[uring_api.Completion]] = []
        delivered = threading.Event()

        def callback(batch):
            batches.append(list(batch))
            if any(len(entry) == 2 for entry in batches):
                delivered.set()

        with uring_api.Ring() as ring:
            ring.callback = callback
            thread = threading.Thread(target=ring.serve_completions)
            thread.start()
            wait_until_running(ring)
            ring.submit_recv(left.fileno(), bytearray(1), 160)
            ring.submit_recv(left.fileno(), bytearray(1), 161)
            right.send(b"ab")
            assert delivered.wait(1.0)
            ring.stop_serving()
            thread.join(1.0)
            assert not thread.is_alive()

        assert any(len(batch) == 2 for batch in batches)
    finally:
        left.close()
        right.close()


def test_ring_serve_completions_invokes_callback_when_available():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        delivered = threading.Event()
        completions: list[uring_api.Completion] = []

        with uring_api.Ring() as ring:
            ring.callback = lambda batch: (completions.extend(batch), delivered.set())
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

        def callback(batch):
            completions.extend(batch)
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
        release_first_callback = threading.Semaphore(0)
        delivered_two = threading.Event()
        completions: list[uring_api.Completion] = []
        lock = threading.Lock()

        def callback(batch):
            with lock:
                prev_count = len(completions)
                completions.extend(batch)
                count = len(completions)
            if prev_count == 0 and count >= 1:
                first_callback_blocking.set()
                if count == 1:
                    release_first_callback.acquire()
            if count >= 2:
                delivered_two.set()

        with uring_api.Ring() as ring:
            ring.callback = callback
            threads = [threading.Thread(target=ring.serve_completions) for _ in range(2)]
            for thread in threads:
                thread.start()
            wait_until_running(ring)
            first_buf = bytearray(1)
            second_buf = bytearray(1)
            ring.submit_recv(left.fileno(), first_buf, 140)
            right.send(b"x")
            assert first_callback_blocking.wait(1.0)

            ring.submit_recv(left.fileno(), second_buf, 141)
            right.send(b"y")
            assert delivered_two.wait(1.0)
            release_first_callback.release()
            ring.stop_serving()
            for thread in threads:
                thread.join(3.0)
                assert not thread.is_alive()

        by_user_data = {completion.user_data: completion for completion in completions}
        assert by_user_data[140].result == 1
        assert by_user_data[141].result == 1
        assert {bytes(first_buf), bytes(second_buf)} == {b"x", b"y"}
    finally:
        release_first_callback.release()
        left.close()
        right.close()

def _run_serve_completions(ring: uring_api.Ring) -> BaseException | None:
    try:
        ring.serve_completions()
    except BaseException as exc:
        return exc
    return None

def test_ring_serve_completions_propagates_callback_error_to_worker():
    require_uring()

    reader, writer = socket.socketpair()
    errors: list[BaseException] = []

    def fail_callback(batch):
        raise RuntimeError("callback failed")

    ring = uring_api.Ring()
    thread: threading.Thread | None = None
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        ring.callback = fail_callback
        thread = threading.Thread(target=lambda: errors.append(_run_serve_completions(ring)))
        thread.start()
        wait_until_running(ring)
        ring.submit_recv(reader.fileno(), bytearray(1), 126)
        writer.send(b"x")

        thread.join(1.0)
        assert not thread.is_alive()
        assert len(errors) == 1
        assert str(errors[0]) == "callback failed"
        assert not ring.running
    finally:
        if thread is not None and thread.is_alive():
            ring.stop_serving()
            thread.join(1.0)
        ring.close()
        reader.close()
        writer.close()

def test_ring_callback_error_exits_only_failing_worker():
    require_uring()

    reader, writer = socket.socketpair()
    errors: list[BaseException] = []
    fail_once = {"done": False}

    def fail_once_callback(batch):
        if not fail_once["done"]:
            fail_once["done"] = True
            raise RuntimeError("callback failed")

    ring = uring_api.Ring()
    threads: list[threading.Thread] = []
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        ring.callback = fail_once_callback
        threads = [
            threading.Thread(target=lambda: errors.append(_run_serve_completions(ring))),
            threading.Thread(target=ring.serve_completions),
        ]
        for thread in threads:
            thread.start()
        wait_until_running(ring)
        ring.submit_recv(reader.fileno(), bytearray(1), 126)
        writer.send(b"x")

        deadline = time.monotonic() + 1.0
        while len(errors) < 1 and time.monotonic() < deadline:
            time.sleep(0.01)

        assert len(errors) == 1
        assert str(errors[0]) == "callback failed"
        assert ring.running
        assert threads[1].is_alive()

        ring.stop_serving()
        for thread in threads:
            thread.join(1.0)
            assert not thread.is_alive()
    finally:
        if threads:
            ring.stop_serving()
            for thread in threads:
                if thread.is_alive():
                    thread.join(1.0)
        ring.close()
        reader.close()
        writer.close()

def test_ring_exception_handler_absorbs_callback_error_and_keeps_serving():
    require_uring()

    reader, writer = socket.socketpair()
    handled: list[dict[str, object]] = []
    invocations = 0
    delivered = 0

    def callback(batch):
        nonlocal invocations, delivered
        invocations += 1
        if invocations == 1:
            raise RuntimeError("callback failed")
        delivered += len(batch)

    def exception_handler(context):
        handled.append(context)

    ring = uring_api.Ring()
    thread: threading.Thread | None = None
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        ring.callback = callback
        ring.exception_handler = exception_handler
        thread = threading.Thread(target=ring.serve_completions)
        thread.start()
        wait_until_running(ring)
        ring.submit_recv(reader.fileno(), bytearray(1), 126)
        writer.send(b"x")

        deadline = time.monotonic() + 1.0
        while len(handled) < 1 and time.monotonic() < deadline:
            time.sleep(0.01)

        ring.submit_recv(reader.fileno(), bytearray(1), 127)
        writer.send(b"y")

        while delivered < 1 and time.monotonic() < deadline:
            time.sleep(0.01)

        assert invocations == 2
        assert delivered == 1
        assert len(handled) == 1
        assert handled[0]["message"] == "Exception in delivery callback"
        assert str(handled[0]["exception"]) == "callback failed"
        assert handled[0]["ring"] is ring
        assert len(handled[0]["completions"]) == 1
        assert ring.running

        ring.stop_serving()
        thread.join(1.0)
        assert not thread.is_alive()
    finally:
        if thread is not None and thread.is_alive():
            ring.stop_serving()
            thread.join(1.0)
        ring.close()
        reader.close()
        writer.close()

def test_ring_exception_handler_failure_propagates_to_worker():
    require_uring()

    reader, writer = socket.socketpair()
    errors: list[BaseException] = []

    def fail_callback(batch):
        raise RuntimeError("callback failed")

    def failing_handler(context):
        raise ValueError("handler failed")

    ring = uring_api.Ring()
    thread: threading.Thread | None = None
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        ring.callback = fail_callback
        ring.exception_handler = failing_handler
        thread = threading.Thread(target=lambda: errors.append(_run_serve_completions(ring)))
        thread.start()
        wait_until_running(ring)
        ring.submit_recv(reader.fileno(), bytearray(1), 126)
        writer.send(b"x")

        thread.join(1.0)
        assert not thread.is_alive()
        assert len(errors) == 1
        assert str(errors[0]) == "handler failed"
        assert not ring.running
    finally:
        if thread is not None and thread.is_alive():
            ring.stop_serving()
            thread.join(1.0)
        ring.close()
        reader.close()
        writer.close()

def test_ring_exception_handler_property_validation_when_available():
    require_uring()

    def handler(context):
        return None

    with uring_api.Ring() as ring:
        assert ring.exception_handler is None
        ring.exception_handler = handler
        assert ring.exception_handler is handler
        ring.exception_handler = None
        assert ring.exception_handler is None

        with pytest.raises(TypeError, match="exception_handler must be callable or None"):
            ring.exception_handler = object()
        with pytest.raises(TypeError, match="cannot delete exception_handler"):
            del ring.exception_handler

def test_ring_allows_exception_handler_change_while_completion_service_runs_when_available():
    require_uring()

    seen: list[str] = []

    def replacement_handler(context):
        seen.append(str(context["exception"]))

    with uring_api.Ring() as ring:
        ring.callback = lambda batch: None
        thread = threading.Thread(target=ring.serve_completions)
        thread.start()
        try:
            wait_until_running(ring)
            ring.exception_handler = replacement_handler
            assert ring.exception_handler is replacement_handler
        finally:
            ring.stop_serving()
            thread.join(1.0)
            assert not thread.is_alive()
            assert seen == []

def test_ring_callback_property_validation_when_available():
    require_uring()

    def callback(batch):
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

        def callback(batch):
            nonlocal calls
            calls += len(batch)

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
        ring.callback = lambda batch: None
        thread = threading.Thread(target=ring.serve_completions)
        thread.start()
        try:
            with pytest.raises(RuntimeError, match="cannot change callback while completion service is active"):
                ring.callback = lambda batch: None
        finally:
            ring.stop_serving()
            thread.join(1.0)
            assert not thread.is_alive()

