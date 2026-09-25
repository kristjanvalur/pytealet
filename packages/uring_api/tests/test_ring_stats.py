"""Cumulative Ring.stats() counters. No reset; tests subtract snapshots."""

from __future__ import annotations

import socket
import threading
import time

import pytest
from conftest import require_uring
from helpers import wait_until_running

import uring_api

_STAT_KEYS = (
    "sqe",
    "cqe",
    "sq_full",
    "next_leg",
    "submit_front_events",
    "submit_front_sqes",
    "submit_waiter_events",
    "submit_waiter_sqes",
    "submit_next_events",
    "submit_next_sqes",
)


def _stats(ring: uring_api.Ring) -> dict[str, int]:
    stats = ring.stats()
    assert set(stats) == set(_STAT_KEYS)
    for key in _STAT_KEYS:
        assert isinstance(stats[key], int)
        assert stats[key] >= 0
    return stats


def _zero() -> dict[str, int]:
    return {key: 0 for key in _STAT_KEYS}


def test_stats_start_at_zero_and_empty_submit_is_not_an_event():
    require_uring()

    with uring_api.Ring() as ring:
        assert _stats(ring) == _zero()
        assert ring.submit() == 0
        assert _stats(ring) == _zero()


def test_stats_submit_is_front_and_wait_of_empty_sq_is_not_waiter():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            buf = bytearray(4)
            ring.prepare_recv(reader.fileno(), buf, 0, object())
            prepared = _stats(ring)
            assert prepared["sqe"] == 1
            assert prepared["submit_front_events"] == 0
            assert prepared["submit_waiter_events"] == 0
            assert ring.submit() == 1
            submitted = _stats(ring)
            assert submitted["submit_front_events"] == 1
            assert submitted["submit_front_sqes"] == 1
            assert submitted["submit_waiter_events"] == 0
            writer.send(b"abcd")
            got = ring.wait(1.0)
            assert got and got[0].res == 4
            done = _stats(ring)
            assert done["cqe"] == 1
            assert done["sqe"] == 1
            assert done["submit_waiter_events"] == 0
            assert done["submit_next_events"] == 0
            assert done["sq_full"] == 0
            assert done["next_leg"] == 0
    finally:
        reader.close()
        writer.close()


def test_stats_inline_wait_flush_is_waiter():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            buf = bytearray(2)
            ring.prepare_recv(reader.fileno(), buf, 0, object())
            writer.send(b"ok")
            got = ring.wait(1.0)
            assert got and bytes(buf) == b"ok"
            stats = _stats(ring)
            assert stats["sqe"] == 1
            assert stats["cqe"] == 1
            assert stats["submit_front_events"] == 0
            assert stats["submit_front_sqes"] == 0
            assert stats["submit_waiter_events"] == 1
            assert stats["submit_waiter_sqes"] == 1
            assert stats["submit_next_events"] == 0
    finally:
        reader.close()
        writer.close()


def test_stats_sq_full_counts_one_failed_peek():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring(entries=2, auto_submit=False) as ring:
            for _ in range(ring.sq_entries):
                ring.prepare_recv(reader.fileno(), bytearray(1), 0, object())
            filled = _stats(ring)
            assert filled["sqe"] == ring.sq_entries
            assert filled["sq_full"] == 0
            assert filled["submit_front_events"] == 0
            with pytest.raises(uring_api.SubmissionQueueFull):
                ring.prepare_recv(reader.fileno(), bytearray(1), 0, object())
            blocked = _stats(ring)
            assert blocked["sqe"] == ring.sq_entries
            assert blocked["sq_full"] == 1
            assert ring.submit() == ring.sq_entries
            flushed = _stats(ring)
            assert flushed["sq_full"] == 1
            assert flushed["submit_front_events"] == 1
            assert flushed["submit_front_sqes"] == ring.sq_entries
            assert flushed["submit_waiter_events"] == 0
    finally:
        reader.close()
        writer.close()


def test_stats_prepare_flush_of_a_full_sq_is_front():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring(entries=2) as ring:
            for _ in range(ring.sq_entries):
                ring.prepare_recv(reader.fileno(), bytearray(1), 0, object())
            ring.prepare_recv(reader.fileno(), bytearray(1), 0, object())
            flushed = _stats(ring)
            assert flushed["sq_full"] == 1
            assert flushed["sqe"] == ring.sq_entries + 1
            assert flushed["submit_front_events"] == 1
            assert flushed["submit_front_sqes"] == ring.sq_entries
            assert ring.submit() == 1
            submitted = _stats(ring)
            assert submitted["submit_front_events"] == 2
            assert submitted["submit_front_sqes"] == ring.sq_entries + 1
            assert submitted["submit_waiter_events"] == 0
    finally:
        reader.close()
        writer.close()


def test_stats_inline_send_all_next_leg_is_not_a_next_submit():
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
            deadline = time.monotonic() + 2.0
            while pending.result is None and time.monotonic() < deadline:
                ring.wait(0.05)
                try:
                    reader.recv(65536)
                except BlockingIOError:
                    pass
            assert pending.res == len(payload)
            stats = _stats(ring)
            # inline wait() is not "a waiter already parked", so continuations
            # ride the next harvest flush instead of submit_next.
            assert stats["sqe"] == 1 + stats["next_leg"]
            assert stats["cqe"] == stats["sqe"]
            assert stats["submit_next_events"] == 0
            assert stats["submit_next_sqes"] == 0
            published = stats["submit_front_sqes"] + stats["submit_waiter_sqes"]
            assert published == stats["sqe"]
    finally:
        reader.close()
        writer.close()


def test_stats_parked_waiter_submits_send_all_next_leg():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        writer.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
        reader.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        payload = b"x" * (256 * 1024)
        finished = threading.Event()

        def on_complete(completion: object) -> None:
            if completion is pending:
                finished.set()
                ring.stop_serving()

        with uring_api.Ring() as ring:
            ring.callback = on_complete
            thread = threading.Thread(target=ring.serve_completions)
            thread.start()
            wait_until_running(ring)
            pending = ring.prepare_send_all(writer.fileno(), payload)
            assert ring.submit() >= 1
            deadline = time.monotonic() + 2.0
            while not finished.is_set() and time.monotonic() < deadline:
                try:
                    reader.recv(65536)
                except BlockingIOError:
                    time.sleep(0.005)
            thread.join(1.0)
            assert not thread.is_alive()
            assert finished.is_set()
            assert pending.res == len(payload)
            stats = _stats(ring)
            if stats["next_leg"] == 0:
                pytest.skip("kernel accepted the payload in one send")
            assert stats["submit_next_events"] == stats["next_leg"]
            assert stats["submit_next_sqes"] == stats["next_leg"]
    finally:
        reader.close()
        writer.close()
