import errno
import os
import socket
import threading
import time

import pytest


import uring_api

from helpers import (
    wait_one,
    connected_tcp_pair,
    require_setup_flags,
)

def test_single_issuer_allows_submit_and_wait_from_one_thread():
    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=uring_api.IORING_SETUP_SINGLE_ISSUER) as ring:
            recv_buf = bytearray(8)
            ring.prepare_recv(reader.fileno(), recv_buf)
            writer.send(b"x")
            completion = wait_one(ring, 1.0)
            assert completion is not None
            assert completion.res == 1
            assert bytes(recv_buf[:1]) == b"x"
    finally:
        reader.close()
        writer.close()

def test_single_issuer_allows_cross_thread_prepare():
    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=uring_api.IORING_SETUP_SINGLE_ISSUER) as ring:
            pending: list[uring_api.Completion] = []
            errors: list[BaseException] = []

            def prepare_from_other_thread():
                try:
                    pending.append(ring.prepare_recv(reader.fileno(), bytearray(8)))
                except BaseException as exc:
                    errors.append(exc)

            thread = threading.Thread(target=prepare_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert errors == []
            assert len(pending) == 1
            assert pending[0].prepared is True
            assert ring.submit() >= 1
            writer.send(b"x")
            completion = wait_one(ring, 1.0)
            assert completion is pending[0]
            assert completion.res == 1
    finally:
        reader.close()
        writer.close()


def test_single_issuer_rejects_cross_thread_submit():
    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=uring_api.IORING_SETUP_SINGLE_ISSUER) as ring:
            ring.prepare_recv(reader.fileno(), bytearray(8))
            errors: list[RuntimeError] = []

            def submit_from_other_thread():
                try:
                    ring.submit()
                except RuntimeError as exc:
                    errors.append(exc)

            thread = threading.Thread(target=submit_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert len(errors) == 1
            assert "IORING_SETUP_SINGLE_ISSUER" in str(errors[0])
    finally:
        reader.close()
        writer.close()


def test_sqpoll_ignores_single_issuer_for_submit():
    """SQPOLL's issuer is the kernel thread. Another thread may publish."""

    require_setup_flags(uring_api.IORING_SETUP_SQPOLL | uring_api.IORING_SETUP_SINGLE_ISSUER)
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        flags = uring_api.IORING_SETUP_SQPOLL | uring_api.IORING_SETUP_SINGLE_ISSUER
        with uring_api.Ring(entries=8, flags=flags) as ring:
            pending = ring.prepare_send(writer.fileno(), b"ping")
            errors: list[BaseException] = []
            submitted: list[int] = []

            def submit_from_other_thread():
                try:
                    submitted.append(ring.submit())
                except BaseException as exc:
                    errors.append(exc)

            thread = threading.Thread(target=submit_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert errors == []
            assert submitted == [1]
            batch = ring.wait(1.0)
            assert pending in batch
            assert pending.res == 4
    finally:
        reader.close()
        writer.close()


def test_sqpoll_single_issuer_worker_makes_room():
    """A worker on an SQPOLL ring publishes a full SQ. SINGLE_ISSUER does not park it."""

    require_setup_flags(uring_api.IORING_SETUP_SQPOLL | uring_api.IORING_SETUP_SINGLE_ISSUER)
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        flags = uring_api.IORING_SETUP_SQPOLL | uring_api.IORING_SETUP_SINGLE_ISSUER
        with uring_api.Ring(entries=2, flags=flags) as ring:
            ring.prepare_send(writer.fileno(), b"a", 0, "a")
            ring.prepare_send(writer.fileno(), b"b", 0, "b")
            filled: list[uring_api.Completion] = []
            errors: list[BaseException] = []

            def prepare_from_other_thread():
                try:
                    filled.append(ring.prepare_send(writer.fileno(), b"c", 0, "c"))
                except BaseException as exc:
                    errors.append(exc)

            thread = threading.Thread(target=prepare_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert errors == []
            assert len(filled) == 1
            assert filled[0].prepared is True
            made = ring.stats()
            assert made["submit_sq_full_events"] == 1
            assert made["submit_sq_full_sqes"] == 2
            assert made["submit_main_sqes"] == 0
    finally:
        reader.close()
        writer.close()


def test_defer_taskrun_allows_wait_from_one_thread():
    flags = uring_api.IORING_SETUP_SINGLE_ISSUER | uring_api.IORING_SETUP_DEFER_TASKRUN
    require_setup_flags(flags)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=flags) as ring:
            recv_buf = bytearray(8)
            ring.prepare_recv(reader.fileno(), recv_buf)
            writer.send(b"x")
            completion = wait_one(ring, 1.0)
            assert completion is not None
            assert completion.res == 1
            assert bytes(recv_buf[:1]) == b"x"
    finally:
        reader.close()
        writer.close()

def test_defer_taskrun_rejects_cross_thread_wait():
    flags = uring_api.IORING_SETUP_SINGLE_ISSUER | uring_api.IORING_SETUP_DEFER_TASKRUN
    require_setup_flags(flags)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=flags) as ring:
            ring.prepare_recv(reader.fileno(), bytearray(8))
            assert ring.wait(0) == []
            errors: list[RuntimeError] = []

            def wait_from_other_thread():
                try:
                    ring.wait(0)
                except RuntimeError as exc:
                    errors.append(exc)

            thread = threading.Thread(target=wait_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert len(errors) == 1
            assert "IORING_SETUP_DEFER_TASKRUN" in str(errors[0])
    finally:
        reader.close()
        writer.close()

def test_defer_taskrun_rejects_cross_thread_submit():
    flags = uring_api.IORING_SETUP_SINGLE_ISSUER | uring_api.IORING_SETUP_DEFER_TASKRUN
    require_setup_flags(flags)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=flags) as ring:
            ring.prepare_recv(reader.fileno(), bytearray(8))
            errors: list[RuntimeError] = []

            def submit_from_other_thread():
                try:
                    ring.submit()
                except RuntimeError as exc:
                    errors.append(exc)

            thread = threading.Thread(target=submit_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert len(errors) == 1
            assert "IORING_SETUP_DEFER_TASKRUN" in str(errors[0])
    finally:
        reader.close()
        writer.close()

def test_single_issuer_allows_cross_thread_wait():
    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=uring_api.IORING_SETUP_SINGLE_ISSUER) as ring:
            ring.prepare_recv(reader.fileno(), bytearray(8))
            # non-issuer wait cannot flush; issuer must publish prepared SQEs
            assert ring.submit() >= 1
            writer.send(b"x")
            results: list[object] = []

            def wait_from_other_thread():
                results.append(ring.wait(1.0))

            thread = threading.Thread(target=wait_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert len(results) == 1
            batch = results[0]
            assert len(batch) == 1
            assert batch[0].res == 1
    finally:
        reader.close()
        writer.close()

def test_single_issuer_allows_break_wait_from_owner_thread():
    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)
    with uring_api.Ring(entries=4, flags=uring_api.IORING_SETUP_SINGLE_ISSUER) as ring:
        # owner may call it; from this thread it is a no-op (no NOP to submit)
        assert ring.break_wait() is None

def test_single_issuer_rejects_cross_thread_break_wait():
    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=uring_api.IORING_SETUP_SINGLE_ISSUER) as ring:
            ring.prepare_recv(reader.fileno(), bytearray(8))
            errors: list[RuntimeError] = []

            def break_from_other_thread():
                try:
                    ring.break_wait()
                except RuntimeError as exc:
                    errors.append(exc)

            thread = threading.Thread(target=break_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert len(errors) == 1
            assert "IORING_SETUP_SINGLE_ISSUER" in str(errors[0])
    finally:
        reader.close()
        writer.close()

def test_defer_taskrun_rejects_cross_thread_serve_completions():
    flags = uring_api.IORING_SETUP_SINGLE_ISSUER | uring_api.IORING_SETUP_DEFER_TASKRUN
    require_setup_flags(flags)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=flags) as ring:
            ring.callback = lambda _completion: None
            ring.prepare_recv(reader.fileno(), bytearray(8))
            errors: list[RuntimeError] = []

            def serve_from_other_thread():
                try:
                    ring.serve_completions()
                except RuntimeError as exc:
                    errors.append(exc)

            thread = threading.Thread(target=serve_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert len(errors) == 1
            assert "IORING_SETUP_DEFER_TASKRUN" in str(errors[0])
    finally:
        reader.close()
        writer.close()


def test_non_issuer_prepare_parks_when_sq_full():
    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)

    reader, writer = socket.socketpair()
    idle_r, idle_w = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        with uring_api.Ring(entries=2, flags=uring_api.IORING_SETUP_SINGLE_ISSUER) as ring:
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            parked: list[uring_api.Completion] = []
            errors: list[BaseException] = []

            def prepare_from_other_thread():
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
            assert ring.pending_count() == 3
            assert ring.submit() >= 1
            assert parked[0].prepared is True
            writer.send(b"x")
            deadline = time.monotonic() + 2.0
            seen: list[uring_api.Completion] = []
            while time.monotonic() < deadline and parked[0] not in seen:
                seen.extend(ring.wait(0.05) or [])
            assert parked[0] in seen
            assert parked[0].res == 1
    finally:
        reader.close()
        writer.close()
        idle_r.close()
        idle_w.close()


def test_second_non_issuer_prepare_parks_when_fill_wait_nonempty():
    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)

    reader, writer = socket.socketpair()
    idle_r, idle_w = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        with uring_api.Ring(entries=2, flags=uring_api.IORING_SETUP_SINGLE_ISSUER) as ring:
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            ring.prepare_recv(idle_r.fileno(), bytearray(1))
            parked: list[uring_api.Completion] = []
            errors: list[BaseException] = []

            def prepare_two_from_other_thread():
                try:
                    parked.append(ring.prepare_recv(reader.fileno(), bytearray(1)))
                    parked.append(ring.prepare_recv(reader.fileno(), bytearray(1)))
                except BaseException as exc:
                    errors.append(exc)

            thread = threading.Thread(target=prepare_two_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert errors == []
            assert len(parked) == 2
            assert parked[0].prepared is False
            assert parked[1].prepared is False
            assert ring.pending_count() == 4
            assert ring.submit() >= 1
            assert parked[0].prepared is True
            assert parked[1].prepared is True
    finally:
        reader.close()
        writer.close()
        idle_r.close()
        idle_w.close()


def test_prepare_does_not_pass_a_parked_link_pair():
    """One free slot must not let a prepare pass a two-slot fill-wait head."""

    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)

    reader, writer = socket.socketpair()
    idle_r, idle_w = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        with uring_api.Ring(
            entries=2,
            flags=uring_api.IORING_SETUP_SINGLE_ISSUER,
            auto_submit=False,
            make_room=False,
        ) as ring:
            assert ring.sq_entries >= 2
            fillers = [
                ring.prepare_recv(idle_r.fileno(), bytearray(1)) for _ in range(ring.sq_entries - 1)
            ]
            assert all(item.prepared for item in fillers)
            held = ring.stats()["sqe"]
            parked: list[uring_api.Completion] = []
            errors: list[BaseException] = []

            def prepare_link_from_other_thread() -> None:
                try:
                    parked.append(ring.prepare_recv(reader.fileno(), bytearray(1), timeout=30.0))
                except BaseException as exc:
                    errors.append(exc)

            thread = threading.Thread(target=prepare_link_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert errors == []
            assert len(parked) == 1
            link = parked[0]
            assert link.prepared is False
            assert ring.stats()["sqe"] == held
            follower = ring.prepare_recv(reader.fileno(), bytearray(1))
            assert follower.prepared is False
            assert link.prepared is False
            assert ring.stats()["sqe"] == held
            assert ring.submit() >= 1
            assert link.prepared is True
            assert follower.prepared is True
            assert ring.stats()["sqe"] == held + 3
    finally:
        reader.close()
        writer.close()
        idle_r.close()
        idle_w.close()


def test_send_all_next_leg_parks_behind_link_pair_when_make_room():
    """One free slot must park a send_all leg behind a two-slot fill-wait head.

    make_room is on. An entering drain would flush to force that head in, or
    raise when the flush cannot free a second slot, and the in-flight send
    would be left with no next SQE. The leg parks instead.
    """

    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)

    reader, writer = socket.socketpair()
    idle_r, idle_w = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        writer.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
        reader.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        payload = b"x" * (256 * 1024)
        with uring_api.Ring(
            entries=2,
            flags=uring_api.IORING_SETUP_SINGLE_ISSUER,
            auto_submit=False,
            make_room=True,
        ) as ring:
            assert ring.sq_entries >= 2
            pending = ring.construct_send_all(writer.fileno(), payload)
            assert ring.prepare(pending) == 1
            assert ring.submit() >= 1
            fillers = [ring.prepare_recv(idle_r.fileno(), bytearray(1)) for _ in range(ring.sq_entries - 1)]
            assert all(item.prepared for item in fillers)
            held = ring.stats()["sqe"]
            parked: list[uring_api.Completion] = []
            errors: list[BaseException] = []

            def prepare_link_from_other_thread() -> None:
                try:
                    parked.append(ring.prepare_recv(reader.fileno(), bytearray(1), timeout=30.0))
                except BaseException as exc:
                    errors.append(exc)

            thread = threading.Thread(target=prepare_link_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert errors == []
            assert len(parked) == 1
            link = parked[0]
            assert link.prepared is False
            assert ring.stats()["sqe"] == held
            before = ring.stats()["next_leg_park"]
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and pending.result is None and ring.stats()["next_leg_park"] == before:
                ring.wait(0.05)
            if pending.result == len(payload):
                pytest.skip("send_all finished before a continuation was needed")
            assert pending.result is None
            assert ring.stats()["next_leg_park"] == before + 1
            deadline = time.monotonic() + 2.0
            while pending.result is None and time.monotonic() < deadline:
                try:
                    reader.recv(8192)
                except BlockingIOError:
                    pass
                ring.submit()
                ring.wait(0)
            assert pending.result == len(payload)
    finally:
        reader.close()
        writer.close()
        idle_r.close()
        idle_w.close()


def _park_link_with_one_slot_free(ring: uring_api.Ring, reader: socket.socket, idle_r: socket.socket):
    """Leave one userspace slot free and park a two-slot recv on fill-wait."""

    fillers = [ring.prepare_recv(idle_r.fileno(), bytearray(1)) for _ in range(ring.sq_entries - 1)]
    assert all(item.prepared for item in fillers)
    held = ring.stats()["sqe"]
    parked: list[uring_api.Completion] = []
    errors: list[BaseException] = []

    def prepare_link_from_other_thread() -> None:
        try:
            parked.append(ring.prepare_recv(reader.fileno(), bytearray(1), timeout=30.0))
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=prepare_link_from_other_thread)
    thread.start()
    thread.join(1.0)
    assert thread.is_alive() is False
    assert errors == []
    assert len(parked) == 1
    link = parked[0]
    assert link.prepared is False
    assert ring.stats()["sqe"] == held
    return link, held


def test_prepare_parks_behind_link_pair_when_make_room():
    """make_room must not submit to force a two-slot fill-wait head.

    One free slot and a parked link timeout. An entering prepare would publish
    the filler or raise stuck-queue. The issuer prepare parks instead.
    """

    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)

    reader, writer = socket.socketpair()
    idle_r, idle_w = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        with uring_api.Ring(
            entries=2,
            flags=uring_api.IORING_SETUP_SINGLE_ISSUER,
            auto_submit=False,
            make_room=True,
        ) as ring:
            assert ring.sq_entries >= 2
            link, held = _park_link_with_one_slot_free(ring, reader, idle_r)
            submitted = _submitted_sqes(ring.stats())
            sq_full = ring.stats()["sq_full"]
            follower = ring.prepare_recv(writer.fileno(), bytearray(1))
            assert follower.prepared is False
            assert link.prepared is False
            assert ring.stats()["sqe"] == held
            assert ring.stats()["sq_full"] == sq_full
            assert _submitted_sqes(ring.stats()) == submitted
            assert ring.submit() >= 1
            assert link.prepared is True
            assert follower.prepared is True
            assert ring.stats()["sqe"] == held + 3
    finally:
        reader.close()
        writer.close()
        idle_r.close()
        idle_w.close()


def test_nowait_parks_behind_link_pair_when_make_room():
    """A nowait close must not submit to pass a two-slot fill-wait head."""

    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)

    reader, writer = socket.socketpair()
    idle_r, idle_w = socket.socketpair()
    spare = socket.socket()
    spare_fd = spare.detach()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        idle_r.setblocking(False)
        idle_w.setblocking(False)
        with uring_api.Ring(
            entries=2,
            flags=uring_api.IORING_SETUP_SINGLE_ISSUER,
            auto_submit=False,
            make_room=True,
        ) as ring:
            assert ring.sq_entries >= 2
            link, held = _park_link_with_one_slot_free(ring, reader, idle_r)
            submitted = _submitted_sqes(ring.stats())
            sq_full = ring.stats()["sq_full"]
            assert ring.prepare_close_nowait(spare_fd) is None
            assert link.prepared is False
            assert ring.stats()["sqe"] == held
            assert ring.stats()["sq_full"] == sq_full
            assert _submitted_sqes(ring.stats()) == submitted
            deadline = time.monotonic() + 2.0
            closed = False
            while time.monotonic() < deadline:
                ring.submit()
                ring.wait(0)
                try:
                    os.fstat(spare_fd)
                except OSError as exc:
                    assert exc.errno == errno.EBADF
                    closed = True
                    break
                time.sleep(0.01)
            assert link.prepared is True
            assert closed
    finally:
        reader.close()
        writer.close()
        idle_r.close()
        idle_w.close()
        try:
            os.close(spare_fd)
        except OSError:
            pass


def _submitted_sqes(stats: dict[str, int]) -> int:
    return stats["submit_main_sqes"] + stats["submit_worker_sqes"] + stats["submit_sq_full_sqes"]


def _fd_closed(fd: int) -> bool:
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        try:
            os.fstat(fd)
        except OSError as exc:
            assert exc.errno == errno.EBADF
            return True
        time.sleep(0.01)
    return False


def test_omitted_flags_publish_nowait_before_wait():
    """Ring() is an SQPOLL ring. flags=0 keeps a nowait op queued across wait()."""

    require_setup_flags(uring_api.IORING_SETUP_SQPOLL)
    first = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    second = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    first_fd = first.detach()
    second_fd = second.detach()
    try:
        with uring_api.Ring() as ring:
            assert ring.prepare_close_nowait(first_fd) is None
            assert ring.wait(0) == []
            assert ring.stats()["submit_main_sqes"] == 1
            assert _fd_closed(first_fd)

        with uring_api.Ring(flags=0) as ring:
            assert ring.prepare_close_nowait(second_fd) is None
            assert ring.wait(0) == []
            os.fstat(second_fd)
            assert ring.stats()["submit_main_sqes"] == 0
            assert ring.submit() == 1
            assert _fd_closed(second_fd)
    finally:
        for fd in (first_fd, second_fd):
            try:
                os.close(fd)
            except OSError:
                pass
        first.close()
        second.close()


def test_sqpoll_close_issues_published_nowait():
    """close() issues a tail submit() already published, and drops one it did not.

    queue_exit before the poller consumes the tail leaves the fd open.
    An unpublished close is still dropped with the userspace SQ.
    """

    require_setup_flags(uring_api.IORING_SETUP_SQPOLL)
    published = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    unpublished = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    published_fd = published.detach()
    unpublished_fd = unpublished.detach()
    try:
        with uring_api.Ring() as ring:
            assert ring.prepare_close_nowait(published_fd) is None
            assert ring.submit() == 1
        assert _fd_closed(published_fd)

        with uring_api.Ring() as ring:
            assert ring.prepare_close_nowait(unpublished_fd) is None
        os.fstat(unpublished_fd)
    finally:
        for fd in (published_fd, unpublished_fd):
            try:
                os.close(fd)
            except OSError:
                pass
        published.close()
        unpublished.close()


def test_sqpoll_wait_publishes():
    """wait() publishes a prepared SQE. Under SQPOLL that copies the tail."""

    require_setup_flags(uring_api.IORING_SETUP_SQPOLL)
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring(entries=8, flags=uring_api.IORING_SETUP_SQPOLL) as ring:
            pending = ring.prepare_send(writer.fileno(), b"ping")
            queued = ring.stats()
            assert queued["sqe"] == 1
            assert _submitted_sqes(queued) == 0
            batch = ring.wait(1.0)
            assert pending in batch
            assert pending.res == 4
            published = ring.stats()
            assert published["submit_main_sqes"] == 1
    finally:
        reader.close()
        writer.close()


def test_sqpoll_wait_publishes_nowait_before_blocking():
    """wait() publishes a nowait-only queue before it blocks.

    A second wait does not publish again once the shared tail has caught up.
    """

    require_setup_flags(uring_api.IORING_SETUP_SQPOLL)
    spare = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    spare_fd = spare.detach()
    try:
        with uring_api.Ring(entries=8, flags=uring_api.IORING_SETUP_SQPOLL) as ring:
            assert ring.prepare_close_nowait(spare_fd) is None
            queued = ring.stats()
            assert queued["sqe"] == 1
            assert _submitted_sqes(queued) == 0
            assert ring.wait(0) == []
            published = ring.stats()
            assert published["submit_main_sqes"] == 1
            again = published["submit_main_sqes"]
            assert ring.wait(0) == []
            assert ring.stats()["submit_main_sqes"] == again
            deadline = time.monotonic() + 1.0
            closed = False
            while time.monotonic() < deadline:
                try:
                    os.fstat(spare_fd)
                except OSError as exc:
                    assert exc.errno == errno.EBADF
                    closed = True
                    break
                time.sleep(0.01)
            assert closed
    finally:
        try:
            os.close(spare_fd)
        except OSError:
            pass
        spare.close()


def test_sqpoll_full_sq_publishes_to_make_room():
    """A full SQ publishes so prepare can take a slot. The next wait() publishes the new SQE."""

    require_setup_flags(uring_api.IORING_SETUP_SQPOLL)
    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring(entries=2, flags=uring_api.IORING_SETUP_SQPOLL) as ring:
            first = ring.prepare_send(writer.fileno(), b"a", 0, "a")
            second = ring.prepare_send(writer.fileno(), b"b", 0, "b")
            queued = ring.stats()
            assert queued["sqe"] == 2
            assert _submitted_sqes(queued) == 0
            third = ring.prepare_send(writer.fileno(), b"c", 0, "c")
            made = ring.stats()
            assert made["sqe"] == 3
            # the make-room publish hands over the two SQEs that filled the
            # queue. the third stays unpublished until wait().
            assert made["submit_sq_full_events"] == 1
            assert made["submit_sq_full_sqes"] == 2
            assert made["submit_main_sqes"] == 0
            assert made["submit_worker_sqes"] == 0
            seen: list[uring_api.Completion] = []
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline and len(seen) < 3:
                seen.extend(ring.wait(0.05) or [])
            assert {item.user_data for item in seen} == {"a", "b", "c"}
            assert first in seen and second in seen and third in seen
            assert third.res == 1
            held = ring.stats()
            assert held["submit_main_sqes"] == 1
    finally:
        reader.close()
        writer.close()

