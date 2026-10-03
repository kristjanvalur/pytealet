"""Ring.wait parks again after a silent burst, unless it was a wakeup.

A timed wait keeps the original deadline and parks again with the time still
left. A peek still returns. The retry submits first, so a send-all next leg
prepared while handling the silent CQE cannot sit unsubmitted.
"""

import socket
import threading
import time

import pytest

import uring_api

from conftest import require_uring


def _break_from_other_thread(ring: uring_api.Ring) -> None:
    """Owner-thread break_wait is a no-op; the wake has to come from elsewhere."""

    thread = threading.Thread(target=ring.break_wait)
    thread.start()
    thread.join(1.0)
    assert thread.is_alive() is False


def _wait_on_thread(ring: uring_api.Ring, timeout: float):
    """Run ring.wait() off the owner thread. Unstick with break_wait on timeout."""

    result: list[object] = []
    errors: list[BaseException] = []

    def run() -> None:
        try:
            result.append(ring.wait())
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        _break_from_other_thread(ring)
        thread.join(1.0)
        pytest.fail("ring.wait() did not return")
    if errors:
        raise errors[0]
    return result[0]


def test_infinite_wait_returns_on_sticky_break_wait():
    require_uring()

    with uring_api.Ring() as ring:
        _break_from_other_thread(ring)
        started = time.monotonic()
        assert ring.wait() == []
        assert time.monotonic() - started < 0.5


def test_infinite_wait_returns_on_wake_nop():
    require_uring()

    with uring_api.Ring() as ring:
        result: list[object] = []
        waiter = threading.Thread(target=lambda: result.append(ring.wait()))
        waiter.start()
        time.sleep(0.05)
        _break_from_other_thread(ring)
        waiter.join(1.0)
        if waiter.is_alive():
            _break_from_other_thread(ring)
            waiter.join(1.0)

    assert waiter.is_alive() is False
    assert result == [[]]


def _prepare_silent_send(ring: uring_api.Ring, fd: int):
    """A send-all whose success CQE is reaped and not delivered."""

    pending = ring.construct_send_all(fd, b"ping")
    pending.skip_success = True
    assert ring.prepare(pending) == 1
    return pending


def test_infinite_wait_retries_a_silent_cqe_until_delivery():
    require_uring()

    reader, writer = socket.socketpair()
    send_reader, send_writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        send_writer.setblocking(False)
        with uring_api.Ring() as ring:
            _prepare_silent_send(ring, send_writer.fileno())
            buf = bytearray(8)
            pending = ring.prepare_recv(reader.fileno(), buf)

            def send_later() -> None:
                time.sleep(0.15)
                writer.send(b"hello")

            threading.Thread(target=send_later, daemon=True).start()
            started = time.monotonic()
            batch = _wait_on_thread(ring, 2.0)
            elapsed = time.monotonic() - started
            assert pending in batch
            assert pending.res == 5
            # the skipped send CQE arrives first; returning on it would be immediate
            assert elapsed >= 0.05
    finally:
        reader.close()
        writer.close()
        send_reader.close()
        send_writer.close()


def test_sticky_silent_burst_does_not_retry():
    """A break_wait that peeks a silent CQE must return, not park again."""

    require_uring()

    send_reader, send_writer = socket.socketpair()
    try:
        send_writer.setblocking(False)
        with uring_api.Ring() as ring:
            _prepare_silent_send(ring, send_writer.fileno())
            ring.submit()
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                if ring.poll(0):
                    break
                time.sleep(0.001)
            else:
                pytest.fail("silent send CQE did not arrive")
            _break_from_other_thread(ring)
            started = time.monotonic()
            assert ring.wait() == []
            assert time.monotonic() - started < 0.3
    finally:
        send_reader.close()
        send_writer.close()


def test_infinite_wait_does_not_retry_when_submit_is_not_allowed():
    require_uring()

    send_reader, send_writer = socket.socketpair()
    try:
        send_writer.setblocking(False)
        with uring_api.Ring(auto_submit=False) as ring:
            _prepare_silent_send(ring, send_writer.fileno())
            ring.submit()
            started = time.monotonic()
            assert _wait_on_thread(ring, 1.0) == []
            assert time.monotonic() - started < 1.0
    finally:
        send_reader.close()
        send_writer.close()


def test_timed_wait_retries_a_silent_cqe_until_delivery():
    require_uring()

    reader, writer = socket.socketpair()
    send_reader, send_writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        send_writer.setblocking(False)
        with uring_api.Ring() as ring:
            _prepare_silent_send(ring, send_writer.fileno())
            buf = bytearray(8)
            pending = ring.prepare_recv(reader.fileno(), buf)

            def send_later() -> None:
                time.sleep(0.15)
                writer.send(b"hello")

            threading.Thread(target=send_later, daemon=True).start()
            started = time.monotonic()
            batch = ring.wait(1.0)
            elapsed = time.monotonic() - started
            assert pending in batch
            assert pending.res == 5
            # the skipped send CQE arrives first; returning on it would be immediate
            assert elapsed >= 0.05
            assert elapsed < 0.9
    finally:
        reader.close()
        writer.close()
        send_reader.close()
        send_writer.close()


def test_timed_wait_keeps_the_original_deadline():
    """A silent CQE near the deadline must not restart the full timeout."""

    require_uring()

    reader, writer = socket.socketpair()
    send_reader, send_writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        send_writer.setblocking(False)
        with uring_api.Ring() as ring:
            buf = bytearray(8)
            pending = ring.prepare_recv(reader.fileno(), buf)
            send_pending: list[uring_api.Completion] = []
            # set once wait() has returned, so the late payload is not sent
            # into a socket the test has already closed
            wait_done = threading.Event()

            def silent_then_payload() -> None:
                time.sleep(0.2)
                send_pending.append(_prepare_silent_send(ring, send_writer.fileno()))
                ring.submit()
                time.sleep(0.35)
                if wait_done.is_set():
                    return
                writer.send(b"hello")

            sender = threading.Thread(target=silent_then_payload, daemon=True)
            sender.start()
            started = time.monotonic()
            batch = ring.wait(0.4)
            elapsed = time.monotonic() - started
            wait_done.set()
            sender.join(1.0)
            assert batch == []
            # returned on the original deadline, not on the silent CQE and not
            # on a timeout restarted from that CQE (that would still be parked
            # at 0.55s and would have taken the payload)
            assert 0.35 <= elapsed < 0.55
            assert not sender.is_alive()
            assert send_pending and send_pending[0].res == 4
            writer.close()
            seen = ring.wait(0.5)
            assert pending in seen
            assert pending.res == 0
    finally:
        reader.close()
        writer.close()
        send_reader.close()
        send_writer.close()


def test_peek_still_returns_on_a_silent_cqe():
    require_uring()

    send_reader, send_writer = socket.socketpair()
    try:
        send_writer.setblocking(False)
        with uring_api.Ring() as ring:
            _prepare_silent_send(ring, send_writer.fileno())
            ring.submit()
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                if ring.poll(0):
                    break
                time.sleep(0.001)
            else:
                pytest.fail("silent send CQE did not arrive")
            started = time.monotonic()
            assert ring.wait(0) == []
            assert time.monotonic() - started < 0.2
    finally:
        send_reader.close()
        send_writer.close()


def test_infinite_wait_submits_send_all_continuation():
    require_uring()

    reader, writer = socket.socketpair()
    stop = threading.Event()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        writer.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)
        reader.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2048)
        payload = b"x" * (256 * 1024)

        def drain() -> None:
            while not stop.is_set():
                try:
                    if not reader.recv(65536):
                        return
                except BlockingIOError:
                    time.sleep(0.001)

        threading.Thread(target=drain, daemon=True).start()
        with uring_api.Ring() as ring:
            pending = ring.prepare_send_all(writer.fileno(), payload)
            batch = _wait_on_thread(ring, 2.0)
            stats = ring.stats()
            assert pending in batch
            assert pending.res == len(payload)
            if stats["next_leg"] == 0:
                pytest.skip("kernel accepted the whole payload in one send CQE")
            # one submit for the first leg, then one per continuation before the next park
            assert stats["submit_main_events"] >= stats["next_leg"] + 1
    finally:
        stop.set()
        reader.close()
        writer.close()
