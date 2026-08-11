"""Lazy submit: prepare without flush; submit()/wait() push SQEs to the kernel."""

from __future__ import annotations

import socket

import pytest

import uring_api
from helpers import require_uring


def test_ring_submit_flushes_prepared_ops():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            buf = bytearray(4)
            pending = ring.submit_recv(reader.fileno(), buf, object())
            # not kernel-visible yet; explicit flush
            n = ring.submit()
            assert n >= 1
            writer.send(b"abcd")
            batch = ring.wait(1.0)
            assert pending in batch
            assert bytes(buf) == b"abcd"
    finally:
        reader.close()
        writer.close()


def test_wait_flushes_without_explicit_submit():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            buf = bytearray(3)
            pending = ring.submit_recv(reader.fileno(), buf, object())
            writer.send(b"xyz")
            # CQ empty → wait flushes prepared SQEs then reaps
            batch = ring.wait(1.0)
            assert pending in batch
            assert bytes(buf) == b"xyz"
    finally:
        reader.close()
        writer.close()


def test_wait_delivers_ready_cqe_before_flushing_pending():
    """CQ-first: harvest ready CQEs without flushing later prepares (SQ still pending)."""

    import time

    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring(entries=32) as ring:
            buf = bytearray(1)
            done = ring.submit_recv(reader.fileno(), buf, "done")
            assert ring.submit() >= 1
            writer.send(b"z")
            # let the CQE land on the CQ before we prepare more work
            time.sleep(0.05)
            pending = ring.submit_recv(reader.fileno(), bytearray(1), "pending")
            batch = ring.wait(1.0)
            assert done in batch
            assert pending not in batch
            assert bytes(buf) == b"z"
            # pending prepare should still be unflushed
            assert ring.submit() >= 1
    finally:
        reader.close()
        writer.close()


def test_batch_prepare_then_one_submit():
    require_uring()

    r1, w1 = socket.socketpair()
    r2, w2 = socket.socketpair()
    try:
        for s in (r1, w1, r2, w2):
            s.setblocking(False)
        with uring_api.Ring(entries=32) as ring:
            b1 = bytearray(1)
            b2 = bytearray(1)
            c1 = ring.submit_recv(r1.fileno(), b1, 1)
            c2 = ring.submit_recv(r2.fileno(), b2, 2)
            n = ring.submit()
            assert n >= 2
            w1.send(b"a")
            w2.send(b"b")
            seen = []
            while len(seen) < 2:
                batch = ring.wait(1.0)
                assert batch is not None
                seen.extend(batch)
            assert c1 in seen and c2 in seen
            assert bytes(b1) == b"a" and bytes(b2) == b"b"
    finally:
        r1.close()
        w1.close()
        r2.close()
        w2.close()


def test_submit_returns_zero_when_empty():
    require_uring()

    with uring_api.Ring() as ring:
        assert ring.submit() == 0


def test_serve_completions_flushes_prepared_ops():
    require_uring()

    delivered: list[object] = []

    def on_complete(batch: list[object]) -> None:
        delivered.extend(batch)
        ring.stop_serving()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            ring.callback = on_complete
            buf = bytearray(2)
            pending = ring.submit_recv(reader.fileno(), buf, object())
            writer.send(b"ok")
            # same-thread serve: wait peeks empty then flushes prepared SQEs
            ring.serve_completions()
            assert pending in delivered
            assert bytes(buf) == b"ok"
    finally:
        reader.close()
        writer.close()
