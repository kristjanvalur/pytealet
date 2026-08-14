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
            pending = ring.prepare_recv(reader.fileno(), buf, object())
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
            pending = ring.prepare_recv(reader.fileno(), buf, object())
            writer.send(b"xyz")
            # CQ empty → wait flushes prepared SQEs then reaps
            batch = ring.wait(1.0)
            assert pending in batch
            assert bytes(buf) == b"xyz"
    finally:
        reader.close()
        writer.close()


def test_wait_flushes_pending_even_when_cq_already_has_completions():
    """wait() publishes prepared SQEs at entry, not only when the CQ is empty."""

    import time

    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring(entries=32) as ring:
            buf = bytearray(1)
            done = ring.prepare_recv(reader.fileno(), buf, "done")
            assert ring.submit() >= 1
            writer.send(b"z")
            time.sleep(0.05)
            # second prepare only — wait must flush it even though `done` is on CQ
            pending = ring.prepare_recv(reader.fileno(), bytearray(1), "pending")
            writer.send(b"y")
            seen: list[object] = []
            for _ in range(20):
                batch = ring.wait(0.2)
                if batch:
                    seen.extend(batch)
                if done in seen and pending in seen:
                    break
            assert done in seen and pending in seen
            assert bytes(buf) == b"z"
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
            c1 = ring.prepare_recv(r1.fileno(), b1, 1)
            c2 = ring.prepare_recv(r2.fileno(), b2, 2)
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


def test_cancel_is_lazy_like_other_submits():
    """cancel only prepares; target+cancel publish together on the next flush."""

    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            buf = bytearray(4)
            target = ring.prepare_recv(reader.fileno(), buf, "target")
            cancel = ring.prepare_cancel(target)
            # neither should be kernel-visible yet — non-blocking wait peeks only
            # (wait still flushes at entry, so use sq_ready style: one submit)
            n = ring.submit()
            assert n >= 2
            seen: list[object] = []
            for _ in range(20):
                batch = ring.wait(0.2)
                if batch:
                    seen.extend(batch)
                if target in seen and cancel in seen:
                    break
            assert target in seen and cancel in seen
            # target cancelled before data, or cancel found it; either is fine
            assert target.res < 0 or cancel.res == 0 or cancel.res < 0
    finally:
        reader.close()
        writer.close()


def test_auto_submit_defaults_true_and_is_settable():
    require_uring()

    with uring_api.Ring() as ring:
        assert ring.auto_submit is True
    with uring_api.Ring(auto_submit=False) as ring:
        assert ring.auto_submit is False
        ring.auto_submit = True
        assert ring.auto_submit is True
        ring.auto_submit = False
        assert ring.auto_submit is False


def _fill_sq_with_recv(ring: uring_api.Ring, reader: socket.socket) -> list[object]:
    pending = []
    for _ in range(ring.sq_entries):
        pending.append(ring.prepare_recv(reader.fileno(), bytearray(1), object()))
    return pending


def test_auto_submit_off_raises_submission_queue_full():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring(entries=2, auto_submit=False) as ring:
            pending = _fill_sq_with_recv(ring, reader)
            assert len(pending) == ring.sq_entries
            with pytest.raises(uring_api.SubmissionQueueFull, match="no submission queue entries available"):
                ring.prepare_recv(reader.fileno(), bytearray(1), object())
            extra = ring.construct_recv(reader.fileno(), bytearray(1), object())
            with pytest.raises(uring_api.SubmissionQueueFull):
                ring.prepare(extra)
            assert extra.prepared is False
            assert ring.submit() == ring.sq_entries
            assert ring.prepare(extra) == 1
            assert extra.prepared is True
    finally:
        reader.close()
        writer.close()


def test_prepare_batch_stops_at_sq_full_when_auto_submit_off():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring(entries=2, auto_submit=False) as ring:
            ops = [ring.construct_recv(reader.fileno(), bytearray(1), i) for i in range(ring.sq_entries + 2)]
            with pytest.raises(uring_api.SubmissionQueueFull):
                ring.prepare(ops)
            prepared = [op for op in ops if op.prepared]
            assert len(prepared) == ring.sq_entries
            assert not ops[-1].prepared
    finally:
        reader.close()
        writer.close()


def test_auto_submit_on_flushes_when_sq_full():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring(entries=2) as ring:
            assert ring.auto_submit is True
            _fill_sq_with_recv(ring, reader)
            extra = ring.prepare_recv(reader.fileno(), bytearray(1), object())
            assert extra.prepared is True
            # the overflow prepare flushed the full SQ; only extra is still pending
            assert ring.submit() == 1
    finally:
        reader.close()
        writer.close()


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
            pending = ring.prepare_recv(reader.fileno(), buf, object())
            writer.send(b"ok")
            # same-thread serve: wait peeks empty then flushes prepared SQEs
            ring.serve_completions()
            assert pending in delivered
            assert bytes(buf) == b"ok"
    finally:
        reader.close()
        writer.close()
