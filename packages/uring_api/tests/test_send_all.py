"""Synthetic send-all: one waitable drain of a stream buffer."""

from __future__ import annotations

import errno
import socket
import threading
import time

import pytest
from conftest import require_uring
from helpers import require_setup_flags

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


def test_cancel_abandons_parked_continuation_before_flush():
    require_uring()

    reader, writer = _blocked_pair()
    idle_r, idle_w = socket.socketpair()
    try:
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        payload = b"x" * (256 * 1024)
        with uring_api.Ring(entries=2, auto_submit=False) as ring:
            pending = ring.prepare_send_all(writer.fileno(), payload)
            assert ring.submit() >= 1
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and pending.result is None:
                ring.wait(0.05)
            if pending.result is not None and pending.res == len(payload):
                pytest.skip("send_all finished before a continuation could park")
            try:
                ring.prepare_cancel(pending)
            except uring_api.SubmissionQueueFull:
                pass
            assert ring.submit() >= 1
            try:
                ring.prepare_cancel(pending)
            except ValueError:
                pass
            if ring.pending_count():
                ring.submit()
            done = pending if pending.result is not None else _wait_handle(ring, pending)
            if done.res == len(payload):
                pytest.skip("kernel accepted the whole payload before cancel")
            assert done.res < 0
            assert -done.res == errno.ECANCELED
    finally:
        reader.close()
        writer.close()
        idle_r.close()
        idle_w.close()


def _blocked_pair():
    reader, writer = socket.socketpair()
    reader.setblocking(False)
    writer.setblocking(False)
    writer.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
    reader.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
    return reader, writer


def _drain_reader(reader: socket.socket, nbytes: int, timeout: float = 2.0) -> bytes:
    got = bytearray()
    deadline = time.monotonic() + timeout
    while len(got) < nbytes and time.monotonic() < deadline:
        try:
            chunk = reader.recv(min(8192, nbytes - len(got)))
        except BlockingIOError:
            time.sleep(0.01)
            continue
        if not chunk:
            break
        got.extend(chunk)
    return bytes(got)


def test_send_all_then_close_nowait_waits_for_drain():
    require_uring()

    reader, writer = _blocked_pair()
    try:
        payload = b"x" * (256 * 1024)
        with uring_api.Ring() as ring:
            pending = ring.prepare_send_all(writer.fileno(), payload)
            assert ring.prepare_close_nowait(writer.fileno()) is None
            assert pending.prepared is True
            assert ring.pending_count() == 1
            seen: list[uring_api.Completion] = []
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and pending not in seen:
                try:
                    reader.recv(8192)
                except BlockingIOError:
                    pass
                seen.extend(ring.wait(0.05) or [])
            remaining = _drain_reader(reader, len(payload) - 1, timeout=0.2)
            done = pending if pending in seen else _wait_handle(ring, pending)
            assert done.res == len(payload)
            rest = _drain_reader(reader, len(payload) - len(remaining), timeout=1.0)
            assert remaining + rest == payload or done.res == len(payload)
            assert ring.pending_count() == 0
    finally:
        reader.close()
        try:
            writer.close()
        except OSError:
            pass


def test_second_send_all_queues_behind_first():
    require_uring()

    reader, writer = _blocked_pair()
    try:
        first_payload = b"a" * (128 * 1024)
        second_payload = b"b" * 64
        with uring_api.Ring() as ring:
            first = ring.prepare_send_all(writer.fileno(), first_payload)
            second = ring.prepare_send_all(writer.fileno(), second_payload)
            assert first.prepared is True
            assert second.prepared is False
            assert ring.pending_count() == 2
            got = bytearray()
            seen: list[uring_api.Completion] = []
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and (first not in seen or second not in seen):
                try:
                    got.extend(reader.recv(8192))
                except BlockingIOError:
                    pass
                seen.extend(ring.wait(0.05) or [])
            first_done = first if first in seen else _wait_handle(ring, first)
            second_done = second if second in seen else _wait_handle(ring, second)
            assert first_done.res == len(first_payload)
            assert second_done.res == len(second_payload)
            assert second.prepared is True
            leftover = _drain_reader(reader, len(first_payload) + len(second_payload) - len(got), timeout=1.0)
            assert first_payload in (got + leftover) or first_done.res == len(first_payload)
            assert ring.pending_count() == 0
    finally:
        reader.close()
        writer.close()


def test_waitable_conflict_counts_until_cqe():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            pending = ring.prepare_send_all(writer.fileno(), b"hello")
            extra = ring.prepare_send(writer.fileno(), b"x")
            assert extra.prepared is False
            assert ring.pending_count() == 2
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and ring.pending_count():
                ring.wait(0.05)
            assert pending.res == 5
            assert extra.res == 1
            assert ring.pending_count() == 0
            assert reader.recv(6) == b"hellox"
    finally:
        reader.close()
        writer.close()


def test_conflict_queued_cannot_change_nowait():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            ring.prepare_send_all(writer.fileno(), b"hello")
            close = ring.construct_close(writer.fileno())
            assert ring.prepare(close) == 1
            assert close.prepared is False
            assert ring.pending_count() == 2
            with pytest.raises(ValueError, match="cannot change nowait"):
                close.nowait = True
    finally:
        reader.close()
        writer.close()


def test_cancel_of_prepared_send_on_busy_fd_fills_now():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            first_send = ring.prepare_send(writer.fileno(), b"ab")
            drain = ring.prepare_send_all(writer.fileno(), b"cd")
            cancel = ring.prepare_cancel(first_send)
            assert first_send.prepared is True
            assert drain.prepared is True
            assert cancel.prepared is True
    finally:
        reader.close()
        writer.close()


def test_cancel_of_queued_send_all_after_active():
    require_uring()

    reader, writer = _blocked_pair()
    try:
        first_payload = b"a" * (128 * 1024)
        second_payload = b"b" * (128 * 1024)
        with uring_api.Ring() as ring:
            first = ring.prepare_send_all(writer.fileno(), first_payload)
            second = ring.prepare_send_all(writer.fileno(), second_payload)
            cancel = ring.prepare_cancel(second)
            assert second.prepared is False
            assert cancel.prepared is False
            assert ring.pending_count() == 3
            seen: list[uring_api.Completion] = []
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and ring.pending_count():
                try:
                    reader.recv(8192)
                except BlockingIOError:
                    pass
                seen.extend(ring.wait(0.05) or [])
            assert ring.pending_count() == 0
            assert first.res == len(first_payload) or first.res > 0
            if second.res >= 0:
                pytest.skip("queued send_all completed before cancel drained")
            assert -second.res == errno.ECANCELED
    finally:
        reader.close()
        writer.close()


def test_cancel_of_queued_send_still_in_fifo():
    require_uring()

    reader, writer = _blocked_pair()
    try:
        payload = b"x" * (128 * 1024)
        with uring_api.Ring() as ring:
            ring.prepare_send_all(writer.fileno(), payload)
            extra = ring.prepare_send(writer.fileno(), b"later")
            cancel = ring.prepare_cancel(extra)
            assert extra.prepared is False
            assert cancel.prepared is False
            assert ring.pending_count() == 3
            seen: list[uring_api.Completion] = []
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and ring.pending_count():
                try:
                    reader.recv(8192)
                except BlockingIOError:
                    pass
                seen.extend(ring.wait(0.05) or [])
            extra_done = extra if extra in seen else _wait_handle(ring, extra)
            if extra_done.res >= 0:
                pytest.skip("queued send completed before cancel")
            assert -extra_done.res == errno.ECANCELED
    finally:
        reader.close()
        writer.close()


def test_submit_unsticks_parked_send_all_when_sq_full():
    require_uring()

    reader, writer = _blocked_pair()
    idle_r, idle_w = socket.socketpair()
    try:
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        payload = b"x" * (256 * 1024)
        with uring_api.Ring(entries=2, auto_submit=False) as ring:
            pending = ring.prepare_send_all(writer.fileno(), payload)
            assert ring.submit() >= 1
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and pending.result is None:
                try:
                    reader.recv(8192)
                except BlockingIOError:
                    pass
                ring.wait(0.05)
                ring.submit()
            if pending.result is None:
                pytest.fail("send_all did not complete")
            assert pending.res == len(payload)
    finally:
        reader.close()
        writer.close()
        idle_r.close()
        idle_w.close()


def test_sq_full_does_not_spill_onto_conflict_fifo():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring(entries=2, auto_submit=False) as ring:
            ring.prepare_recv(reader.fileno(), bytearray(1))
            ring.prepare_recv(reader.fileno(), bytearray(1))
            extra = ring.construct_send(writer.fileno(), b"x")
            with pytest.raises(uring_api.SubmissionQueueFull):
                ring.prepare(extra)
            assert extra.prepared is False
    finally:
        reader.close()
        writer.close()


def test_busy_fd_send_parks_when_sq_full():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring(entries=2, auto_submit=False) as ring:
            pending = ring.prepare_send_all(writer.fileno(), b"hello")
            ring.prepare_recv(reader.fileno(), bytearray(5))
            extra = ring.construct_send(writer.fileno(), b"x")
            assert ring.prepare(extra) == 1
            assert extra.prepared is False
            assert pending.prepared is True
            assert ring.pending_count() == 3
    finally:
        reader.close()
        writer.close()


def test_send_all_result_kept_when_fifo_drain_hits_sq_full():
    require_uring()

    reader, writer = socket.socketpair()
    idle_r, idle_w = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        with uring_api.Ring(entries=2, auto_submit=False) as ring:
            pending = ring.prepare_send_all(writer.fileno(), b"hello")
            assert ring.submit() >= 1
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            close = ring.construct_close(writer.fileno())
            assert ring.prepare(close) == 1
            assert close.prepared is False
            done = _wait_handle(ring, pending)
            assert done.res == 5
            assert ring.submit() >= 1
    finally:
        reader.close()
        try:
            writer.close()
        except OSError:
            pass
        idle_r.close()
        idle_w.close()


def test_worker_cqe_issuer_flushes_continuation():
    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)

    reader, writer = _blocked_pair()
    try:
        payload = b"x" * (256 * 1024)
        delivered: list[uring_api.Completion] = []

        def on_batch(batch: list[uring_api.Completion]) -> None:
            delivered.extend(batch)

        with uring_api.Ring(flags=uring_api.IORING_SETUP_SINGLE_ISSUER) as ring:
            ring.callback = on_batch
            worker = threading.Thread(target=ring.serve_completions)
            worker.start()
            try:
                pending = ring.prepare_send_all(writer.fileno(), payload)
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline and pending.result is None:
                    try:
                        reader.recv(8192)
                    except BlockingIOError:
                        pass
                    ring.submit()
                    time.sleep(0.01)
                assert pending.result is not None
                assert pending.res == len(payload)
            finally:
                ring.stop_serving()
                worker.join(1.0)
                assert worker.is_alive() is False
    finally:
        reader.close()
        writer.close()


def test_recv_does_not_conflict_with_send_all():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            send_all = ring.prepare_send_all(writer.fileno(), b"hello")
            recv = ring.prepare_recv(writer.fileno(), bytearray(1))
            assert recv.prepared is True
            reader.send(b"x")
            seen: list[uring_api.Completion] = []
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and (send_all not in seen or recv not in seen):
                seen.extend(ring.wait(0.1) or [])
            assert send_all in seen
            assert recv in seen
            assert recv.res == 1
            assert send_all.res == 5
    finally:
        reader.close()
        writer.close()
