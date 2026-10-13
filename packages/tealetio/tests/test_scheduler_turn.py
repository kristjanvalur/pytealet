"""Host-shaped driver: turn() and set_driver_wakeup."""

import socket
import threading
import time

import pytest

from tealetio import (
    BasicScheduler,
    SyncProactorScheduler,
    open_connection,
    set_scheduler,
    sleep,
)
from tealetio.proactor import ThreadedSelectorProactor


class _PollCountingScheduler(BasicScheduler):
    def __init__(self) -> None:
        super().__init__()
        self.polls = 0

    def _poll_io(self) -> None:
        self.polls += 1
        super()._poll_io()


def _drive_until(scheduler, done, timeout_s: float = 3.0) -> None:
    """Host loop: turn, and wait on the driver wakeup or the returned deadline."""

    woke = threading.Event()
    scheduler.set_driver_wakeup(woke.set)
    end = time.monotonic() + timeout_s
    while time.monotonic() < end:
        woke.clear()
        deadline = scheduler.turn()
        if done():
            return
        if deadline is not None and deadline <= scheduler.time():
            continue
        remaining = end - time.monotonic()
        if remaining <= 0:
            break
        if deadline is None:
            woke.wait(remaining)
        else:
            woke.wait(min(remaining, max(0.0, deadline - scheduler.time())))
    if not done():
        raise AssertionError("host timed out before the tealet finished")


class TestTurn:
    def test_idle_turn_returns_none(self):
        scheduler = BasicScheduler()
        set_scheduler(scheduler)
        try:
            assert scheduler.turn() is None
        finally:
            set_scheduler(None)

    def test_idle_turn_polls_once(self):
        scheduler = _PollCountingScheduler()
        set_scheduler(scheduler)
        try:
            assert scheduler.turn() is None
            assert scheduler.polls == 1
        finally:
            set_scheduler(None)

    def test_a_finished_task_polls_twice(self):
        scheduler = _PollCountingScheduler()
        set_scheduler(scheduler)
        seen: list[int] = []
        try:
            scheduler.spawn(lambda: seen.append(1))
            scheduler.turn()
            assert seen == [1]
            assert scheduler.polls == 2
        finally:
            set_scheduler(None)

    def test_a_callback_polls_twice(self):
        scheduler = _PollCountingScheduler()
        set_scheduler(scheduler)
        try:
            scheduler.call_soon(lambda: None)
            scheduler.turn()
            assert scheduler.polls == 2
        finally:
            set_scheduler(None)

    def test_a_due_timer_polls_twice(self):
        scheduler = _PollCountingScheduler()
        set_scheduler(scheduler)
        try:
            scheduler.call_later(0, lambda: None)
            scheduler.turn()
            assert scheduler.polls == 2
        finally:
            set_scheduler(None)

    def test_sleep_turn_returns_the_timer_deadline(self):
        scheduler = BasicScheduler()
        set_scheduler(scheduler)

        def sleeper() -> None:
            sleep(10)

        try:
            wakes: list[int] = []
            scheduler.set_driver_wakeup(lambda: wakes.append(1))
            scheduler.spawn(sleeper)
            assert len(wakes) == 1
            started = scheduler.time()
            deadline = scheduler.turn()
            # parking the driver at the start of the slice must not ask for another
            assert len(wakes) == 1
            assert deadline is not None
            assert started + 9.5 <= deadline <= started + 10.5
            again = scheduler.turn()
            assert len(wakes) == 1
            assert again is not None
            assert again > scheduler.time()
        finally:
            set_scheduler(None)

    def test_ready_work_returns_an_immediate_deadline(self):
        scheduler = BasicScheduler()
        set_scheduler(scheduler)
        steps: list[int] = []

        def spin() -> None:
            for i in range(4):
                steps.append(i)
                sleep(0)

        try:
            scheduler.spawn(spin)
            first = scheduler.turn()
            assert steps == [0]
            assert first is not None
            assert first <= scheduler.time()
            for _ in range(8):
                if len(steps) == 4:
                    break
                scheduler.turn()
            assert steps == [0, 1, 2, 3]
        finally:
            set_scheduler(None)

    def test_driver_wakeup_follows_break_wait_not_call_soon(self):
        scheduler = BasicScheduler()
        set_scheduler(scheduler)
        wakes: list[int] = []
        scheduler.set_driver_wakeup(lambda: wakes.append(1))
        try:
            scheduler.spawn(lambda: None)
            assert len(wakes) == 1
            scheduler.call_soon(lambda: None)
            assert len(wakes) == 1
            scheduler.call_soon_threadsafe(lambda: None)
            assert len(wakes) == 2
        finally:
            set_scheduler(None)

    def test_turn_from_another_thread_during_turn_is_rejected(self):
        scheduler = BasicScheduler()
        set_scheduler(scheduler)
        errors: list[BaseException] = []
        started = threading.Event()

        def wake_from_outside() -> None:
            started.wait(1)
            scheduler.call_soon_threadsafe(lambda: None)

        def body() -> None:
            threading.Thread(target=wake_from_outside, daemon=True).start()
            started.set()
            # hold this slice on the host thread so the other thread's turn()
            # overlaps it. sleep() would park and let turn() return first.
            time.sleep(0.2)

        try:
            scheduler.set_driver_wakeup(lambda: None)
            scheduler.spawn(body)

            def reenter() -> None:
                # the wakeup runs on the waking thread, which has no scheduler
                # bound. bind it so the rejection is the shared driver flag.
                set_scheduler(scheduler)
                try:
                    scheduler.turn()
                except RuntimeError as exc:
                    errors.append(exc)
                finally:
                    set_scheduler(None)

            scheduler.set_driver_wakeup(reenter)
            scheduler.turn()
            assert errors
            assert "Scheduler already running" in str(errors[0])
        finally:
            set_scheduler(None)


class TestTurnIo:
    def test_threaded_proactor_wakes_the_host_when_the_read_completes(self):
        ready = threading.Event()
        port: list[int] = []

        def serve() -> None:
            srv = socket.socket()
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind(("127.0.0.1", 0))
            srv.listen(1)
            srv.settimeout(3)
            port.append(srv.getsockname()[1])
            ready.set()
            try:
                conn, _addr = srv.accept()
            except TimeoutError:
                srv.close()
                return
            try:
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                conn.sendall(b"pong")
            finally:
                conn.close()
                srv.close()

        server = threading.Thread(target=serve, daemon=True)
        server.start()
        assert ready.wait(1)
        got: list[bytes] = []

        def client() -> None:
            reader, writer = open_connection(addr=("127.0.0.1", port[0]))
            try:
                got.append(reader.read())
            finally:
                writer.close()
                writer.wait_closed()

        scheduler = SyncProactorScheduler(proactor_factory=ThreadedSelectorProactor)
        set_scheduler(scheduler)
        try:
            scheduler.spawn(client)
            _drive_until(scheduler, lambda: got == [b"pong"])
        finally:
            set_scheduler(None)
            scheduler.close()
            server.join(timeout=1)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
