"""Synthetic send-all: one waitable drain of a stream buffer."""

from __future__ import annotations

import errno
import socket
import time

import pytest
from conftest import require_uring

import uring_api


def _wait_handle(ring: uring_api.Ring, handle: uring_api.Completion, timeout: float = 2.0) -> uring_api.Completion:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        remaining = max(0.0, deadline - time.monotonic())
        batch = ring.wait(remaining)
        if handle in batch:
            return handle
    raise AssertionError("send_all completion did not arrive")


def test_experimental_send_all_submit_next_defaults_false_and_is_settable():
    require_uring()

    with uring_api.Ring() as ring:
        assert ring.experimental_send_all_submit_next is False
    with uring_api.Ring(experimental_send_all_submit_next=True) as ring:
        assert ring.experimental_send_all_submit_next is True
        ring.experimental_send_all_submit_next = False
        assert ring.experimental_send_all_submit_next is False
        ring.experimental_send_all_submit_next = True
        assert ring.experimental_send_all_submit_next is True


def test_send_all_one_cqe_full_drain():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        payload = b"hello send-all"
        with uring_api.Ring() as ring:
            pending = ring.prepare_send_all(writer.fileno(), payload, 0, object())
            assert pending.kind == uring_api.COMPLETION_KIND_SEND_ALL
            assert ring.pending_count() == 1
            done = _wait_handle(ring, pending)
            assert done is pending
            assert done.res == len(payload)
            assert done.result == len(payload)
            assert ring.pending_count() == 0
            assert reader.recv(len(payload)) == payload
    finally:
        reader.close()
        writer.close()


def test_send_all_empty_succeeds():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            pending = ring.prepare_send_all(writer.fileno(), b"", 0, object())
            assert ring.pending_count() == 1
            done = _wait_handle(ring, pending)
            assert done.res == 0
            assert ring.pending_count() == 0
    finally:
        reader.close()
        writer.close()


def test_send_all_error_bad_fd():
    require_uring()

    with uring_api.Ring() as ring:
        pending = ring.prepare_send_all(1 << 24, b"x", 0, object())
        done = _wait_handle(ring, pending)
        assert done.res < 0
        assert ring.pending_count() == 0


def test_send_all_pending_count_holds_between_legs():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        writer.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
        reader.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        payload = b"x" * (256 * 1024)
        with uring_api.Ring() as ring:
            pending = ring.prepare_send_all(writer.fileno(), payload)
            assert ring.pending_count() == 1
            saw_mid_drain = False
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                batch = ring.wait(0.05)
                if pending in batch:
                    break
                if ring.pending_count() == 1:
                    saw_mid_drain = True
                try:
                    reader.recv(8192)
                except BlockingIOError:
                    pass
            assert pending.res == len(payload)
            assert ring.pending_count() == 0
            if not saw_mid_drain:
                pytest.skip("kernel accepted the whole payload in one send CQE")
    finally:
        reader.close()
        writer.close()


def test_send_all_eager_next_leg_still_drains():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        payload = b"eager-next"
        with uring_api.Ring(experimental_send_all_submit_next=True) as ring:
            pending = ring.prepare_send_all(writer.fileno(), payload)
            done = _wait_handle(ring, pending)
            assert done.res == len(payload)
            assert reader.recv(len(payload)) == payload
    finally:
        reader.close()
        writer.close()


def test_send_all_nowait_success():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        payload = b"nowait-all"
        with uring_api.Ring() as ring:
            pending = ring.construct_send_all(writer.fileno(), payload)
            pending.nowait = True
            assert ring.prepare(pending) == 1
            assert ring.pending_count() == 1
            deadline = time.monotonic() + 1.0
            while ring.pending_count() and time.monotonic() < deadline:
                assert pending not in ring.wait(0.1)
            assert ring.pending_count() == 0
            assert reader.recv(len(payload)) == payload
    finally:
        reader.close()
        writer.close()


def test_send_all_nowait_error_handler():
    require_uring()

    seen: list[dict[str, object]] = []

    def on_error(context: dict[str, object]) -> None:
        seen.append(context)

    with uring_api.Ring() as ring:
        ring.nowait_error_handler = on_error
        pending = ring.construct_send_all(1 << 24, b"x")
        pending.nowait = True
        ring.prepare(pending)
        deadline = time.monotonic() + 1.0
        while ring.pending_count() and time.monotonic() < deadline:
            ring.wait(0.1)
        assert ring.pending_count() == 0
        assert seen
        assert seen[0]["kind"] == uring_api.COMPLETION_KIND_SEND_ALL
        assert int(seen[0]["res"]) < 0


def test_send_all_cancel_in_flight():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        writer.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
        reader.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        payload = b"x" * (256 * 1024)
        with uring_api.Ring() as ring:
            pending = ring.prepare_send_all(writer.fileno(), payload)
            ring.prepare_cancel(pending)
            done = _wait_handle(ring, pending)
            if done.res == len(payload):
                pytest.skip("kernel accepted the whole payload before cancel")
            assert done.res < 0
            assert -done.res == errno.ECANCELED
            assert ring.pending_count() == 0
    finally:
        reader.close()
        writer.close()
