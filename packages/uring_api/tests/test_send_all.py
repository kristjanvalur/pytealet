"""Synthetic send-all: one waitable drain of a stream buffer."""

from __future__ import annotations

import errno
import os
import socket
import threading
import time

import pytest
from conftest import require_uring, require_uring_capability
from helpers import require_setup_flags, so_linger

import uring_api


def _wait_handle(ring: uring_api.Ring, handle: uring_api.Completion, timeout: float = 2.0) -> uring_api.Completion:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        remaining = max(0.0, deadline - time.monotonic())
        batch = ring.wait(remaining)
        if handle in batch:
            return handle
    raise AssertionError("send_all completion did not arrive")


def _send_all_leaves_fifo_on_drain_list(ring: uring_api.Ring, writer: socket.socket, idle_r: socket.socket):
    """Terminal send-all CQE leftover-drains close onto fd_drain_head (SQ still full)."""
    pending = ring.prepare_send_all(writer.fileno(), b"hello")
    assert ring.submit() >= 1
    ring.prepare_recv(idle_r.fileno(), bytearray(1))
    ring.prepare_recv(idle_r.fileno(), bytearray(1))
    close = ring.construct_close(writer.fileno())
    assert ring.prepare(close) == 1
    assert close.prepared is False
    done = _wait_handle(ring, pending)
    assert done.res == 5
    return close


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
                # peek: a timed wait would park again on each silent leg and
                # this thread would not get back to drain the socket.
                batch = ring.wait(0)
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
        with uring_api.Ring() as ring:
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
            pending.skip_success = True
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
        pending.skip_all = True
        assert pending.skip_success is True
        ring.prepare(pending)
        deadline = time.monotonic() + 1.0
        while ring.pending_count() and time.monotonic() < deadline:
            assert pending not in ring.wait(0.1)
        assert ring.pending_count() == 0
        assert seen
        assert seen[0]["kind"] == uring_api.COMPLETION_KIND_SEND_ALL
        assert int(seen[0]["res"]) < 0


def test_send_all_skip_success_error_delivers_without_user_data():
    """skip_success (not skip_all) completes the handle on error even with no token."""
    require_uring()

    handler_seen: list[dict[str, object]] = []

    def on_error(context: dict[str, object]) -> None:
        handler_seen.append(context)

    with uring_api.Ring() as ring:
        ring.nowait_error_handler = on_error
        pending = ring.construct_send_all(1 << 24, b"x")
        pending.skip_success = True
        assert pending.skip_all is False
        ring.prepare(pending)
        done = _wait_handle(ring, pending)
        assert done is pending
        assert done.res < 0
        assert ring.pending_count() == 0
        assert handler_seen == []


def test_send_all_nowait_error_delivers_when_user_data_set():
    """skip_success + user_data: error delivers the handle; token is just identity."""
    require_uring()

    handler_seen: list[dict[str, object]] = []
    token = object()

    def on_error(context: dict[str, object]) -> None:
        handler_seen.append(context)

    with uring_api.Ring() as ring:
        ring.nowait_error_handler = on_error
        pending = ring.construct_send_all(1 << 24, b"x", 0, token)
        pending.skip_success = True
        ring.prepare(pending)
        done = _wait_handle(ring, pending)
        assert done is pending
        assert done.res < 0
        assert done.user_data is token
        assert ring.pending_count() == 0
        assert handler_seen == []


def test_send_all_nowait_success_with_user_data_still_skipped():
    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        payload = b"nowait-token"
        token = object()
        with uring_api.Ring() as ring:
            pending = ring.construct_send_all(writer.fileno(), payload, 0, token)
            pending.skip_success = True
            assert ring.prepare(pending) == 1
            deadline = time.monotonic() + 1.0
            while ring.pending_count() and time.monotonic() < deadline:
                assert pending not in ring.wait(0.1)
            assert ring.pending_count() == 0
            assert reader.recv(len(payload)) == payload
    finally:
        reader.close()
        writer.close()


def test_send_all_nowait_error_callback_mode():
    """Callback-mode delivery is the same path tealetio already uses for waitable send."""
    require_uring()

    delivered: list[uring_api.Completion] = []
    token = object()

    def on_complete(completion: uring_api.Completion) -> None:
        delivered.append(completion)

    with uring_api.Ring() as ring:
        ring.callback = on_complete
        pending = ring.construct_send_all(1 << 24, b"x", 0, token)
        pending.skip_success = True
        ring.prepare(pending)
        deadline = time.monotonic() + 1.0
        while ring.pending_count() and time.monotonic() < deadline:
            assert ring.wait(0.1) is None
        assert ring.pending_count() == 0
        assert delivered == [pending]
        assert pending.res < 0
        assert pending.user_data is token


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
    """A full queue parks the cancel behind the next leg.

    That leg is still a send. submit() publishes the send, then the cancel.
    Abandon stops the leg's CQE from arming another send.
    """
    require_uring()

    reader, writer = _blocked_pair()
    idle_r, idle_w = socket.socketpair()
    try:
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        payload = b"x" * (256 * 1024)
        with uring_api.Ring(entries=2, flags=0, auto_submit=False, make_room=False) as ring:
            pending = ring.prepare_send_all(writer.fileno(), payload)
            assert ring.submit() >= 1
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and pending.result is None and ring.stats()["next_leg_park"] == 0:
                ring.wait(0.05)
            if pending.result is not None:
                if pending.res == len(payload):
                    pytest.skip("send_all finished before a continuation could park")
                pytest.fail(f"send_all finished before a continuation parked: {pending.res}")
            if ring.stats()["next_leg_park"] == 0:
                pytest.fail("next leg was not parked")
            legs = ring.stats()["next_leg"]
            sqes = ring.stats()["sqe"]
            cancel = ring.prepare_cancel(pending)
            assert cancel.prepared is False
            assert ring.stats()["next_leg"] == legs
            assert ring.stats()["sqe"] == sqes
            assert ring.submit() >= 1
            assert ring.stats()["next_leg"] == legs + 1
            assert ring.stats()["sqe"] == sqes + 2
            assert cancel.prepared is True
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
                # peek, so a silent send leg returns and this thread can read.
                seen.extend(ring.wait(0) or [])
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


def _drive_callback(ring: uring_api.Ring, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while ring.pending_count() and time.monotonic() < deadline:
        assert ring.wait(0.2) is None
    assert ring.pending_count() == 0


def test_abortive_nowait_close_does_not_linger_while_send_all_is_busy():
    """A close parked behind send-all keeps the socket graceful until it is filled."""
    require_uring()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    fd = sock.detach()
    seen: dict[str, object] = {}

    def on_complete(completion: uring_api.Completion) -> None:
        if completion.kind == uring_api.COMPLETION_KIND_SEND_ALL:
            seen["send_res"] = completion.res
            seen["linger"] = so_linger(fd)

    try:
        with uring_api.Ring(flags=0) as ring:
            ring.callback = on_complete
            send = ring.prepare_send_all(fd, b"hi")
            assert send.prepared is True
            assert ring.prepare_close_nowait(fd, abortive=True) is None
            assert so_linger(fd) == (0, 0)
            _drive_callback(ring)
        assert isinstance(seen["send_res"], int)
        assert seen["send_res"] < 0
        assert seen["linger"] == (1, 0)
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def test_send_all_failure_makes_queued_close_abortive():
    """A close parked behind a failed drain sets SO_LINGER before it is entered."""
    require_uring()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    fd = sock.detach()
    seen: dict[str, object] = {}

    def on_complete(completion: uring_api.Completion) -> None:
        if completion.kind == uring_api.COMPLETION_KIND_SEND_ALL:
            seen["send_res"] = completion.res
            seen["linger"] = so_linger(fd)
            os.fstat(fd)
        elif completion.kind == uring_api.COMPLETION_KIND_CLOSE:
            seen["close_res"] = completion.res

    try:
        with uring_api.Ring(flags=0) as ring:
            ring.callback = on_complete
            send = ring.construct_send_all(fd, b"hi")
            close = ring.construct_close(fd)
            assert ring.prepare([send, close]) == 2
            assert close.prepared is False
            _drive_callback(ring)
        assert isinstance(seen["send_res"], int)
        assert seen["send_res"] < 0
        assert seen["linger"] == (1, 0)
        assert seen["close_res"] == 0
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def _drain_abortive_close_without_enter(ring: uring_api.Ring) -> None:
    """prepare() of another fd drains a parked close and does not enter."""
    idle_r, idle_w = socket.socketpair()
    try:
        filled = ring.prepare_recv(idle_r.fileno(), bytearray(1))
        assert filled.prepared is True
    finally:
        idle_r.close()
        idle_w.close()


def test_failed_send_all_makes_a_later_close_abortive():
    """The fd stays abortive until the application closes it through the ring."""
    require_uring()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    fd = sock.detach()
    seen: dict[str, object] = {}

    def on_complete(completion: uring_api.Completion) -> None:
        if completion.kind == uring_api.COMPLETION_KIND_SEND_ALL:
            seen["send_res"] = completion.res
            seen["linger"] = so_linger(fd)
            os.fstat(fd)

    try:
        with uring_api.Ring(flags=0) as ring:
            ring.callback = on_complete
            ring.prepare_send_all(fd, b"hi")
            _drive_callback(ring)
            assert isinstance(seen["send_res"], int)
            assert seen["send_res"] < 0
            assert seen["linger"] == (0, 0)
            close = ring.construct_close(fd)
            assert ring.prepare(close) == 1
            assert close.prepared is False
            assert so_linger(fd) == (0, 0)
            _drain_abortive_close_without_enter(ring)
            assert close.prepared is True
            assert so_linger(fd) == (1, 0)
            again = ring.construct_close(fd)
            assert ring.prepare(again) == 1
            assert again.prepared is True
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def test_failed_send_all_makes_a_later_nowait_close_abortive():
    require_uring()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    fd = sock.detach()
    try:
        with uring_api.Ring(flags=0) as ring:
            pending = ring.prepare_send_all(fd, b"hi")
            _wait_handle(ring, pending)
            assert pending.res < 0
            assert so_linger(fd) == (0, 0)
            assert ring.prepare_close_nowait(fd) is None
            assert so_linger(fd) == (0, 0)
            _drain_abortive_close_without_enter(ring)
            assert so_linger(fd) == (1, 0)
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def test_successful_send_all_does_not_stick_abortive():
    require_uring()

    reader, writer = socket.socketpair()
    reader.setblocking(False)
    writer.setblocking(False)
    fd = writer.detach()
    try:
        with uring_api.Ring(flags=0) as ring:
            pending = ring.prepare_send_all(fd, b"hello")
            assert _wait_handle(ring, pending).res == 5
            close = ring.construct_close(fd)
            assert ring.prepare(close) == 1
            assert close.prepared is True
            assert so_linger(fd) == (0, 0)
    finally:
        reader.close()
        try:
            os.close(fd)
        except OSError:
            pass


def _unconnected_tcp_fd() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    return sock.detach()


def test_abortive_pipeline_poisons_queued_ops_and_keeps_the_close():
    """A send and a shutdown taken off the failed drain fail; the close does not."""
    require_uring()

    fd = _unconnected_tcp_fd()
    seen: dict[str, object] = {}

    def on_complete(completion: uring_api.Completion) -> None:
        if completion.kind == uring_api.COMPLETION_KIND_SEND_ALL:
            seen["send_all"] = completion.res
            seen["linger"] = so_linger(fd)
        elif completion.kind == uring_api.COMPLETION_KIND_SEND:
            seen["send"] = completion.res
        elif completion.kind == uring_api.COMPLETION_KIND_SHUTDOWN:
            seen["shutdown"] = completion.res
        elif completion.kind == uring_api.COMPLETION_KIND_CLOSE:
            seen["close"] = completion.res

    try:
        with uring_api.Ring(flags=0) as ring:
            ring.callback = on_complete
            send_all = ring.construct_send_all(fd, b"hi")
            send = ring.construct_send(fd, b"more")
            shutdown = ring.construct_shutdown(fd, socket.SHUT_WR)
            close = ring.construct_close(fd)
            assert ring.prepare([send_all, send, shutdown, close]) == 4
            assert send.prepared is False
            assert shutdown.prepared is False
            assert close.prepared is False
            _drive_callback(ring)
        assert isinstance(seen["send_all"], int)
        assert seen["send_all"] < 0
        assert seen["send_all"] != -errno.EBADF
        assert seen["send"] == -errno.EBADF
        assert seen["shutdown"] == -errno.EBADF
        assert seen["close"] == 0
        assert seen["linger"] == (1, 0)
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def test_abortive_pipeline_poisons_a_send_queued_behind_the_close():
    """The close clears the mark, and a send copied later in that drain still fails."""
    require_uring()

    fd = _unconnected_tcp_fd()
    seen: dict[str, object] = {}

    def on_complete(completion: uring_api.Completion) -> None:
        if completion.kind == uring_api.COMPLETION_KIND_SEND_ALL:
            seen["linger"] = so_linger(fd)
        elif completion.kind == uring_api.COMPLETION_KIND_SEND:
            seen["send"] = completion.res
        elif completion.kind == uring_api.COMPLETION_KIND_CLOSE:
            seen["close"] = completion.res

    try:
        with uring_api.Ring(flags=0) as ring:
            ring.callback = on_complete
            send_all = ring.construct_send_all(fd, b"hi")
            close = ring.construct_close(fd)
            send = ring.construct_send(fd, b"tail")
            assert ring.prepare([send_all, close, send]) == 3
            assert send.prepared is False
            _drive_callback(ring)
        assert seen["send"] == -errno.EBADF
        assert seen["close"] == 0
        assert seen["linger"] == (1, 0)
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def test_send_all_on_idle_abortive_fd_fails_without_holding_the_slot():
    """A later send_all is not the active drain, so the close can still reset."""
    require_uring()

    fd = _unconnected_tcp_fd()
    try:
        with uring_api.Ring(flags=0) as ring:
            failed = ring.prepare_send_all(fd, b"hi")
            assert _wait_handle(ring, failed).res < 0
            assert so_linger(fd) == (0, 0)
            again = ring.prepare_send_all(fd, b"more")
            assert again.prepared is True
            close = ring.construct_close(fd)
            assert ring.prepare(close) == 1
            assert close.prepared is False
            # fill the close without entering, while the fd is still open.
            # an active send_all would leave the close parked.
            _drain_abortive_close_without_enter(ring)
            assert close.prepared is True
            assert so_linger(fd) == (1, 0)
            seen: set[uring_api.Completion] = set()
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and (again not in seen or close not in seen):
                seen.update(ring.wait(0.2) or [])
            assert again.res == -errno.EBADF
            assert close.res == 0
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def test_direct_send_on_idle_abortive_fd_keeps_the_descriptor():
    """A send that never queued is not rewritten. It fails as a real send would."""
    require_uring()

    fd = _unconnected_tcp_fd()
    try:
        with uring_api.Ring(flags=0) as ring:
            failed = ring.prepare_send_all(fd, b"hi")
            done = _wait_handle(ring, failed)
            assert done.res < 0
            assert done.res != -errno.EBADF
            send = ring.prepare_send(fd, b"x")
            assert send.prepared is True
            assert _wait_handle(ring, send).res == done.res
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def test_successful_send_all_does_not_poison_a_queued_send():
    require_uring()

    reader, writer = socket.socketpair()
    reader.setblocking(False)
    writer.setblocking(False)
    fd = writer.detach()
    try:
        with uring_api.Ring(flags=0) as ring:
            send_all = ring.construct_send_all(fd, b"hello")
            send = ring.construct_send(fd, b"!")
            assert ring.prepare([send_all, send]) == 2
            seen: set[uring_api.Completion] = set()
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and (send_all not in seen or send not in seen):
                seen.update(ring.wait(0.2) or [])
            assert send_all.res == 5
            assert send.res == 1
            assert reader.recv(6) == b"hello!"
    finally:
        reader.close()
        try:
            os.close(fd)
        except OSError:
            pass


def test_abortive_pipeline_poisons_queued_send_zc_without_a_notif():
    require_uring_capability("IORING_OP_SEND_ZC")

    fd = _unconnected_tcp_fd()
    payload = bytearray(b"hello")
    view = memoryview(payload)
    seen: dict[str, object] = {}
    released = False

    def on_complete(completion: uring_api.Completion) -> None:
        nonlocal released
        if completion.kind == uring_api.COMPLETION_KIND_SEND_ZC:
            view.release()
            released = True
            seen["zc"] = completion.res

    try:
        with uring_api.Ring(flags=0) as ring:
            ring.callback = on_complete
            send_all = ring.construct_send_all(fd, b"hi")
            send_zc = ring.construct_send_zc(fd, view)
            close = ring.construct_close(fd)
            assert ring.prepare([send_all, send_zc, close]) == 3
            assert send_zc.prepared is False
            _drive_callback(ring)
            assert ring.pending_count() == 0
        assert released
        assert seen["zc"] == -errno.EBADF
    finally:
        if not released:
            view.release()
        try:
            os.close(fd)
        except OSError:
            pass


def test_send_all_success_leaves_queued_close_graceful():
    require_uring()

    reader, writer = socket.socketpair()
    reader.setblocking(False)
    writer.setblocking(False)
    fd = writer.detach()
    payload = b"hello"
    seen: dict[str, object] = {}

    def on_complete(completion: uring_api.Completion) -> None:
        if completion.kind == uring_api.COMPLETION_KIND_SEND_ALL:
            seen["send_res"] = completion.res
            seen["linger"] = so_linger(fd)
        elif completion.kind == uring_api.COMPLETION_KIND_CLOSE:
            seen["close_res"] = completion.res

    try:
        with uring_api.Ring(flags=0) as ring:
            ring.callback = on_complete
            send = ring.construct_send_all(fd, payload)
            close = ring.construct_close(fd)
            assert ring.prepare([send, close]) == 2
            _drive_callback(ring)
        assert seen["send_res"] == len(payload)
        assert seen["linger"] == (0, 0)
        assert seen["close_res"] == 0
        assert reader.recv(16) == payload
        assert reader.recv(1) == b""
    finally:
        reader.close()
        try:
            os.close(fd)
        except OSError:
            pass


def test_send_all_success_keeps_explicit_abortive_close():
    require_uring()

    reader, writer = socket.socketpair()
    reader.setblocking(False)
    fd = writer.detach()
    seen: dict[str, object] = {}

    def on_complete(completion: uring_api.Completion) -> None:
        if completion.kind == uring_api.COMPLETION_KIND_SEND_ALL:
            seen["linger"] = so_linger(fd)
            seen["send_res"] = completion.res

    try:
        with uring_api.Ring(flags=0) as ring:
            ring.callback = on_complete
            send = ring.construct_send_all(fd, b"x")
            close = ring.construct_close(fd, abortive=True)
            assert ring.prepare([send, close]) == 2
            _drive_callback(ring)
        assert seen["send_res"] == 1
        assert seen["linger"] == (1, 0)
    finally:
        reader.close()
        try:
            os.close(fd)
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
                seen.extend(ring.wait(0) or [])
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
            with pytest.raises(ValueError, match="cannot change skip_success"):
                close.skip_success = True
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
                seen.extend(ring.wait(0) or [])
            assert ring.pending_count() == 0
            assert first.res == len(first_payload) or first.res > 0
            assert second in seen
            assert -second.res == errno.ECANCELED
            assert cancel in seen
    finally:
        reader.close()
        writer.close()


# The queued send_all has no leg. prepare_cancel sets abandon and parks
# the cancel behind it. The drain fills a NOP so wait() delivers that
# send_all; abandon finishes the NOP CQE as -ECANCELED.


def test_submit_unsticks_parked_send_all_when_sq_full():
    require_uring()

    reader, writer = _blocked_pair()
    idle_r, idle_w = socket.socketpair()
    try:
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        payload = b"x" * (256 * 1024)
        with uring_api.Ring(entries=2, flags=0, auto_submit=False, make_room=False) as ring:
            pending = ring.prepare_send_all(writer.fileno(), payload)
            assert ring.submit() >= 1
            published = ring.stats()["submit_main_sqes"]
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            # the reader has to run during wait(): a timed wait that published
            # a next leg reparks until something user-visible arrives or the
            # deadline passes, and this thread cannot recv from inside that.
            drainer = threading.Thread(target=_drain_reader, args=(reader, len(payload), 2.0), daemon=True)
            drainer.start()
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and ring.stats()["next_leg_park"] == 0:
                ring.wait(0.05)
            if ring.stats()["next_leg_park"] == 0:
                pytest.fail("next leg was not parked")
            # full SQ: wait does not publish the idle recvs to free a slot
            assert ring.stats()["submit_main_sqes"] == published
            assert ring.stats()["submit_sq_full_sqes"] == 0
            assert ring.submit() >= 1
            while time.monotonic() < deadline and pending.result is None:
                ring.wait(0.05)
            if pending.result is None:
                pytest.fail("send_all did not complete")
            assert pending.res == len(payload)
            drainer.join(timeout=1.0)
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
        with uring_api.Ring(entries=2, flags=0, auto_submit=False, make_room=False) as ring:
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
        with uring_api.Ring(entries=2, flags=0, auto_submit=False, make_room=False) as ring:
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


def test_conflict_parks_when_leftover_drain_hits_sq_full():
    require_uring()

    reader, writer = _blocked_pair()
    idle_r, idle_w = socket.socketpair()
    try:
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        payload = b"x" * (256 * 1024)
        with uring_api.Ring(entries=2, flags=0, auto_submit=False, make_room=False) as ring:
            pending = ring.prepare_send_all(writer.fileno(), payload)
            assert ring.submit() >= 1
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and pending.result is None:
                ring.wait(0.05)
            if pending.result is not None and pending.res == len(payload):
                pytest.skip("send_all finished before a continuation could park")
            close = ring.construct_close(writer.fileno())
            assert ring.prepare(close) == 1
            assert close.prepared is False
            extra = ring.construct_send(writer.fileno(), b"x")
            assert ring.prepare(extra) == 1
            assert extra.prepared is False
    finally:
        reader.close()
        writer.close()
        idle_r.close()
        idle_w.close()


def test_send_all_result_kept_when_fifo_drain_hits_sq_full():
    require_uring()

    reader, writer = socket.socketpair()
    idle_r, idle_w = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        with uring_api.Ring(entries=2, flags=0, auto_submit=False, make_room=False) as ring:
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


def test_leftover_prepare_raises_when_conflict_fifo_on_drain_list():
    """Leftover SQ-full must stop drain; issuer auto_submit=False, make_room=False raises instead of hanging."""
    require_uring()

    reader, writer = socket.socketpair()
    idle_r, idle_w = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        with uring_api.Ring(entries=2, flags=0, auto_submit=False, make_room=False) as ring:
            close = _send_all_leaves_fifo_on_drain_list(ring, writer, idle_r)
            extra = ring.construct_recv(reader.fileno(), bytearray(1))
            errors: list[BaseException] = []

            def prepare_leftover() -> None:
                try:
                    ring.prepare(extra)
                except BaseException as exc:
                    errors.append(exc)

            thread = threading.Thread(target=prepare_leftover)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert len(errors) == 1
            assert isinstance(errors[0], uring_api.SubmissionQueueFull)
            assert extra.prepared is False
            assert close.prepared is False
    finally:
        reader.close()
        try:
            writer.close()
        except OSError:
            pass
        idle_r.close()
        idle_w.close()


def test_non_issuer_leftover_prepare_parks_when_conflict_fifo_on_drain_list():
    """Same drain-list leftover: a worker parks on fill-wait instead of hanging."""
    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)

    reader, writer = socket.socketpair()
    idle_r, idle_w = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        with uring_api.Ring(entries=2, flags=uring_api.IORING_SETUP_SINGLE_ISSUER, auto_submit=False, make_room=False) as ring:
            close = _send_all_leaves_fifo_on_drain_list(ring, writer, idle_r)
            parked: list[uring_api.Completion] = []
            errors: list[BaseException] = []

            def prepare_from_other_thread() -> None:
                try:
                    parked.append(ring.prepare_recv(reader.fileno(), bytearray(1)))
                except BaseException as exc:
                    errors.append(exc)

            thread = threading.Thread(target=prepare_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert errors == []
            assert len(parked) == 1
            assert parked[0].prepared is False
            assert close.prepared is False
            assert ring.pending_count() == 4
            assert ring.submit() >= 1
            assert parked[0].prepared is True
            assert close.prepared is True
    finally:
        reader.close()
        try:
            writer.close()
        except OSError:
            pass
        idle_r.close()
        idle_w.close()


def test_send_all_result_kept_when_fifo_drain_hits_sq_full_on_worker():
    """CQ workers share send_all_on_cqe with wait(); SQ-full FIFO drain must not drop the batch."""
    require_uring()

    reader, writer = socket.socketpair()
    idle_r, idle_w = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        delivered: list[uring_api.Completion] = []
        worker_error: list[BaseException] = []

        def on_batch(completion: uring_api.Completion) -> None:
            delivered.append(completion)

        def run_worker(ring: uring_api.Ring) -> None:
            try:
                ring.serve_completions()
            except BaseException as exc:
                worker_error.append(exc)

        with uring_api.Ring(entries=2, flags=0, auto_submit=False, make_room=False) as ring:
            ring.callback = on_batch
            pending = ring.prepare_send_all(writer.fileno(), b"hello")
            assert ring.submit() >= 1
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            close = ring.construct_close(writer.fileno())
            assert ring.prepare(close) == 1
            assert close.prepared is False
            worker = threading.Thread(target=run_worker, args=(ring,))
            worker.start()
            try:
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline and pending not in delivered:
                    time.sleep(0.01)
                assert worker_error == []
                assert pending in delivered
                assert pending.res == 5
                assert ring.submit() >= 1
            finally:
                ring.stop_serving()
                worker.join(1.0)
                assert worker.is_alive() is False
                assert worker_error == []
    finally:
        reader.close()
        try:
            writer.close()
        except OSError:
            pass
        idle_r.close()
        idle_w.close()


def test_issuer_prepare_parks_after_worker_activates_fifo_send_all():
    """Worker CQE fills the FIFO send-all into a free SQ slot without enter; issuer prepare parks behind it."""
    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        delivered: list[uring_api.Completion] = []

        def on_batch(completion: uring_api.Completion) -> None:
            delivered.append(completion)

        with uring_api.Ring(flags=uring_api.IORING_SETUP_SINGLE_ISSUER) as ring:
            ring.callback = on_batch
            worker = threading.Thread(target=ring.serve_completions)
            worker.start()
            try:
                first = ring.prepare_send_all(writer.fileno(), b"hello")
                second = ring.prepare_send_all(writer.fileno(), b"world")
                assert second.prepared is False
                assert ring.submit() >= 1
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline and first not in delivered:
                    try:
                        reader.recv(16)
                    except BlockingIOError:
                        pass
                    time.sleep(0.01)
                assert first in delivered
                assert second.prepared is True
                assert second not in delivered
                third = ring.prepare_send(writer.fileno(), b"x")
                assert third.prepared is False
            finally:
                ring.stop_serving()
                worker.join(1.0)
                assert worker.is_alive() is False
    finally:
        reader.close()
        writer.close()


@pytest.mark.parametrize(
    "ring_flags",
    [
        pytest.param(0, id="plain"),
        pytest.param(None, id="sqpoll-default"),
    ],
)
def test_auto_submit_off_submit_publishes_parked_fifo(ring_flags: int | None):
    """submit() covers a send parked on a busy fd. The completion that copies it publishes."""

    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with _ring_auto_submit_off(ring_flags) as ring:
            first = ring.prepare_send_all(writer.fileno(), b"hello")
            second = ring.prepare_send_all(writer.fileno(), b"world")
            assert second.prepared is False
            assert ring.submit() >= 1
            published = ring.stats()["submit_main_sqes"]
            assert _wait_handle(ring, first).res == 5
            assert second.prepared is True
            assert ring.stats()["submit_main_sqes"] > published
            assert _wait_handle(ring, second).res == 5
            assert reader.recv(10) == b"helloworld"
    finally:
        reader.close()
        writer.close()


@pytest.mark.parametrize(
    "ring_flags",
    [
        pytest.param(0, id="plain"),
        pytest.param(None, id="sqpoll-default"),
    ],
)
def test_auto_submit_off_fd_queue_publishes_queued_prepare(ring_flags: int | None):
    """Copying a parked send publishes it, and an unpublished prepare already on the tail."""

    require_uring()

    reader, writer = socket.socketpair()
    idle_r, idle_w = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        with _ring_auto_submit_off(ring_flags) as ring:
            first = ring.prepare_send_all(writer.fileno(), b"hello")
            assert ring.submit() >= 1
            published = ring.stats()["submit_main_sqes"]
            waiting = ring.prepare_recv(idle_r.fileno(), bytearray(1))
            assert waiting.prepared is True
            second = ring.prepare_send_all(writer.fileno(), b"world")
            assert second.prepared is False
            assert ring.stats()["submit_main_sqes"] == published
            assert _wait_handle(ring, first).res == 5
            assert second.prepared is True
            assert ring.stats()["submit_main_sqes"] >= published + 2
            assert _wait_handle(ring, second).res == 5
            idle_w.send(b"z")
            assert _wait_handle(ring, waiting).res == 1
            assert reader.recv(10) == b"helloworld"
    finally:
        reader.close()
        writer.close()
        idle_r.close()
        idle_w.close()


@pytest.mark.parametrize(
    "ring_flags",
    [
        pytest.param(0, id="plain"),
        pytest.param(None, id="sqpoll-default"),
    ],
)
def test_auto_submit_off_wait_callback_error_publishes_fd_queue_copy(ring_flags: int | None):
    """A callback exception does not skip the wait() publish of an fd-queue copy."""

    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)

        def on_complete(_completion: uring_api.Completion) -> None:
            raise RuntimeError("callback failed")

        with _ring_auto_submit_off(ring_flags) as ring:
            ring.callback = on_complete
            first = ring.prepare_send_all(writer.fileno(), b"hello")
            second = ring.prepare_send_all(writer.fileno(), b"world")
            assert second.prepared is False
            assert ring.submit() >= 1
            published = ring.stats()["submit_main_sqes"]
            with pytest.raises(RuntimeError, match="callback failed"):
                ring.wait(2.0)
            assert first.res == 5
            assert second.prepared is True
            assert ring.stats()["submit_main_sqes"] > published
            ring.callback = None
            assert _wait_handle(ring, second).res == 5
            assert reader.recv(16) == b"helloworld"
    finally:
        reader.close()
        writer.close()


def _ring_auto_submit_off(ring_flags: int | None) -> uring_api.Ring:
    if ring_flags is None:
        return uring_api.Ring(auto_submit=False)
    return uring_api.Ring(flags=ring_flags, auto_submit=False)


@pytest.mark.parametrize(
    "ring_flags",
    [
        pytest.param(0, id="plain"),
        pytest.param(None, id="sqpoll-default"),
    ],
)
def test_auto_submit_off_publishes_send_all_next_leg(ring_flags: int | None):
    """A multi-leg send_all finishes with no second submit() when auto_submit is off.

    The partial completion is not delivered, so the caller cannot submit that
    leg. wait() publishes it and keeps reaping. A caller prepare stays lazy.
    """

    require_uring()

    reader, writer = _blocked_pair()
    try:
        payload = b"x" * (256 * 1024)
        drainer = threading.Thread(target=_drain_reader, args=(reader, len(payload), 2.0))
        with _ring_auto_submit_off(ring_flags) as ring:
            pending = ring.prepare_send_all(writer.fileno(), payload)
            assert ring.submit() >= 1
            submitted = ring.stats()["submit_main_sqes"]
            drainer.start()
            try:
                # one wait: partial legs are not delivered, so this call
                # publishes them and reparks until the drain completes.
                batch = ring.wait(2.0)
            finally:
                drainer.join(1.0)
            assert pending in batch
            assert pending.res == len(payload)
            assert ring.stats()["submit_main_sqes"] > submitted
    finally:
        reader.close()
        writer.close()


@pytest.mark.parametrize(
    "ring_flags",
    [
        pytest.param(0, id="plain"),
        pytest.param(None, id="sqpoll-default"),
    ],
)
def test_auto_submit_off_worker_publishes_fd_queue_copy(ring_flags: int | None):
    """A service thread publishes an fd-queue copy. The host does not submit() again."""

    require_uring()

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        done = threading.Event()
        parked: dict[str, uring_api.Completion] = {}

        def on_complete(completion: uring_api.Completion) -> None:
            second = parked.get("second")
            if second is not None and completion is second and completion.result is not None:
                done.set()

        with _ring_auto_submit_off(ring_flags) as ring:
            ring.callback = on_complete
            first = ring.prepare_send_all(writer.fileno(), b"hello")
            second = ring.prepare_send_all(writer.fileno(), b"world")
            parked["second"] = second
            assert second.prepared is False
            assert ring.submit() >= 1
            worker = threading.Thread(target=ring.serve_completions)
            worker.start()
            try:
                deadline = time.monotonic() + 2.0
                while not done.is_set() and time.monotonic() < deadline:
                    try:
                        reader.recv(16)
                    except BlockingIOError:
                        time.sleep(0.01)
                assert done.is_set()
                assert first.res == 5
                assert second.res == 5
                assert ring.stats()["submit_worker_sqes"] >= 1
            finally:
                ring.stop_serving()
                worker.join(1.0)
                assert worker.is_alive() is False
    finally:
        reader.close()
        writer.close()


@pytest.mark.parametrize(
    "ring_flags",
    [
        pytest.param(0, id="plain"),
        pytest.param(None, id="sqpoll-default"),
    ],
)
def test_auto_submit_off_worker_publishes_send_all_next_leg(ring_flags: int | None):
    """A service thread that may submit publishes a next leg when auto_submit is off.

    The host is not in wait(), and it does not call submit() again. The
    partial completion never reaches the callback.
    """

    require_uring()

    reader, writer = _blocked_pair()
    try:
        payload = b"x" * (256 * 1024)
        done = threading.Event()

        def on_complete(completion: uring_api.Completion) -> None:
            if completion.result is not None:
                done.set()

        with _ring_auto_submit_off(ring_flags) as ring:
            ring.callback = on_complete
            pending = ring.prepare_send_all(writer.fileno(), payload)
            assert ring.submit() >= 1
            worker = threading.Thread(target=ring.serve_completions)
            worker.start()
            try:
                deadline = time.monotonic() + 2.0
                while not done.is_set() and time.monotonic() < deadline:
                    try:
                        reader.recv(8192)
                    except BlockingIOError:
                        time.sleep(0.01)
                assert done.is_set()
                assert pending.res == len(payload)
                assert ring.stats()["submit_worker_sqes"] >= 1
            finally:
                ring.stop_serving()
                worker.join(1.0)
                assert worker.is_alive() is False
    finally:
        reader.close()
        writer.close()


def test_worker_cqe_issuer_flushes_continuation():
    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)

    reader, writer = _blocked_pair()
    try:
        payload = b"x" * (256 * 1024)
        delivered: list[uring_api.Completion] = []

        def on_batch(completion: uring_api.Completion) -> None:
            delivered.append(completion)

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


def test_worker_next_leg_parks_when_sq_full():
    """auto_submit off: next-leg cannot make SQ room, so it parks until submit()."""
    require_uring()

    reader, writer = _blocked_pair()
    idle_r, idle_w = socket.socketpair()
    try:
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        payload = b"x" * (256 * 1024)
        delivered: list[uring_api.Completion] = []

        def on_complete(completion: uring_api.Completion) -> None:
            delivered.append(completion)

        with uring_api.Ring(entries=2, flags=0, auto_submit=False, make_room=False) as ring:
            ring.callback = on_complete
            pending = ring.prepare_send_all(writer.fileno(), payload)
            assert ring.submit() >= 1
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            threads = [threading.Thread(target=ring.serve_completions) for _ in range(2)]
            for thread in threads:
                thread.start()
            try:
                parked_deadline = time.monotonic() + 0.5
                while time.monotonic() < parked_deadline and pending not in delivered:
                    time.sleep(0.01)
                if pending in delivered:
                    pytest.skip("send_all finished before a continuation could park")
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline and pending not in delivered:
                    try:
                        reader.recv(8192)
                    except BlockingIOError:
                        pass
                    ring.submit()
                    time.sleep(0.01)
                assert pending in delivered
                assert pending.res == len(payload)
            finally:
                try:
                    ring.submit()
                except Exception:
                    pass
                ring.stop_serving()
                for thread in threads:
                    thread.join(1.0)
                    assert thread.is_alive() is False
    finally:
        reader.close()
        writer.close()
        idle_r.close()
        idle_w.close()


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
