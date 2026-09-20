"""Ring.poll(): CQ-ready wait that does not harvest."""

import threading
import time

import uring_api

from helpers import connected_tcp_pair, require_setup_flags, wait_one
from conftest import require_uring


def test_poll_peek_empty_then_ready_leaves_cqe_for_wait():
    require_uring()
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4) as ring:
            assert ring.poll(0) is False
            recv_buf = bytearray(8)
            ring.prepare_recv(reader.fileno(), recv_buf)
            writer.send(b"x")
            assert ring.poll(1.0) is True
            assert ring.poll(0) is True
            completion = wait_one(ring, 0)
            assert completion is not None
            assert completion.res == 1
            assert bytes(recv_buf[:1]) == b"x"
            assert ring.poll(0) is False
    finally:
        reader.close()
        writer.close()


def test_wait_rejects_while_poll_is_parked():
    require_uring()
    with uring_api.Ring(entries=4) as ring:
        results: list[bool] = []

        def block_in_poll() -> None:
            results.append(ring.poll(10.0))

        thread = threading.Thread(target=block_in_poll)
        thread.start()
        deadline = time.monotonic() + 1.0
        raised: RuntimeError | None = None
        while time.monotonic() < deadline:
            try:
                ring.wait(0)
            except RuntimeError as exc:
                raised = exc
                break
            time.sleep(0.01)
        ring.break_wait()
        thread.join(1.0)
        assert thread.is_alive() is False
        assert raised is not None
        assert "another wait is already active" in str(raised)
        assert results == [True]


def test_poll_timeout_on_empty_ring():
    require_uring()
    with uring_api.Ring(entries=4) as ring:
        started = time.monotonic()
        assert ring.poll(0.05) is False
        assert time.monotonic() - started >= 0.04


def test_poll_accepts_timeout_keyword():
    require_uring()
    with uring_api.Ring(entries=4) as ring:
        assert ring.poll(timeout=0) is False


def test_break_wait_unblocks_poll():
    require_uring()
    results: list[bool] = []
    with uring_api.Ring(entries=4) as ring:

        def poll_until_wake() -> None:
            results.append(ring.poll(10.0))

        thread = threading.Thread(target=poll_until_wake)
        thread.start()
        deadline = time.monotonic() + 1.0
        while thread.is_alive() and time.monotonic() < deadline:
            ring.break_wait()
            thread.join(0.05)
        assert thread.is_alive() is False
        assert results == [True]
        batch = ring.wait(0)
        assert batch == []
        assert ring.poll(0) is False


def test_single_issuer_allows_cross_thread_poll():
    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=uring_api.IORING_SETUP_SINGLE_ISSUER) as ring:
            recv_buf = bytearray(8)
            ring.prepare_recv(reader.fileno(), recv_buf)
            assert ring.submit() >= 1
            writer.send(b"x")
            results: list[object] = []

            def poll_from_other_thread() -> None:
                results.append(ring.poll(1.0))

            thread = threading.Thread(target=poll_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert results == [True]
            completion = wait_one(ring, 0)
            assert completion is not None
            assert completion.res == 1
    finally:
        reader.close()
        writer.close()


def test_defer_taskrun_allows_owner_poll_then_wait():
    flags = uring_api.IORING_SETUP_SINGLE_ISSUER | uring_api.IORING_SETUP_DEFER_TASKRUN
    require_setup_flags(flags)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=flags) as ring:
            recv_buf = bytearray(8)
            ring.prepare_recv(reader.fileno(), recv_buf)
            writer.send(b"x")
            assert ring.poll(1.0) is True
            completion = wait_one(ring, 0)
            assert completion is not None
            assert completion.res == 1
            assert bytes(recv_buf[:1]) == b"x"
    finally:
        reader.close()
        writer.close()


def test_defer_taskrun_rejects_cross_thread_poll():
    flags = uring_api.IORING_SETUP_SINGLE_ISSUER | uring_api.IORING_SETUP_DEFER_TASKRUN
    require_setup_flags(flags)
    with uring_api.Ring(entries=4, flags=flags) as ring:
        errors: list[RuntimeError] = []

        def poll_from_other_thread() -> None:
            try:
                ring.poll(0)
            except RuntimeError as exc:
                errors.append(exc)

        thread = threading.Thread(target=poll_from_other_thread)
        thread.start()
        thread.join(1.0)
        assert thread.is_alive() is False
        assert len(errors) == 1
        assert "IORING_SETUP_DEFER_TASKRUN" in str(errors[0])
