import asyncio
import threading

import pytest

from tealetio import (
    Barrier,
    BoundedSemaphore,
    Condition,
    DefaultTaskFactory,
    Lock,
    PriorityLock,
    PriorityRunnableQueue,
    PriorityTask,
    PulseEvent,
    Scheduler,
    Semaphore,
    TASK_PRIORITY_CRITICAL,
    TASK_PRIORITY_DEFAULT,
    TASK_PRIORITY_HIGH,
    TASK_PRIORITY_LOW,
    CrossThreadCondition,
    set_scheduler,
)
from tealetio.locks import timeout_at
from helpers import new_scheduler as _new_scheduler


class TestPriorityLockExamples:
    def test_priority_lock_works_with_regular_tasks(self):
        s = _new_scheduler()
        set_scheduler(s)
        lock = PriorityLock()
        seen: list[str] = []

        def second() -> None:
            seen.append("second:start")
            lock.sacquire()
            try:
                seen.append("second:acquired")
            finally:
                lock.release()

        def first() -> None:
            lock.sacquire()
            try:
                seen.append("first:acquired")
                s.spawn(second)
                s.yield_()
                seen.append("first:release")
            finally:
                lock.release()

        s.spawn(first)
        s.run()

        assert seen == [
            "first:acquired",
            "second:start",
            "first:release",
            "second:acquired",
        ]

    def test_priority_lock_inherits_high_waiter_priority(self):
        s = Scheduler(runnable_queue_factory=PriorityRunnableQueue)
        s.set_task_factory(DefaultTaskFactory(task_constructor=PriorityTask))
        set_scheduler(s)
        lock = PriorityLock()
        seen: list[str] = []

        def high() -> None:
            seen.append("high:start")
            lock.sacquire()
            try:
                seen.append("high:acquired")
            finally:
                lock.release()

        def medium() -> None:
            seen.append("medium")

        def low() -> None:
            lock.sacquire()
            try:
                seen.append("low:acquired")
                s.spawn(high, priority=TASK_PRIORITY_HIGH)
                s.spawn(medium, priority=TASK_PRIORITY_DEFAULT)
                s.yield_()
                seen.append("low:inherited")
            finally:
                lock.release()

        s.spawn(low, priority=TASK_PRIORITY_LOW)
        s.run()

        assert seen == [
            "low:acquired",
            "high:start",
            "low:inherited",
            "high:acquired",
            "medium",
        ]

    def test_priority_lock_asyncio_acquire_release(self):
        lock = PriorityLock()
        seen: list[str] = []

        async def worker(name: str) -> None:
            seen.append(f"{name}:before")
            await lock.acquire()
            try:
                seen.append(f"{name}:acquired")
                await asyncio.sleep(0)
            finally:
                lock.release()
                seen.append(f"{name}:released")

        async def orchestrate() -> None:
            await asyncio.gather(worker("a"), worker("b"))

        asyncio.run(orchestrate())

        assert seen == [
            "a:before",
            "a:acquired",
            "b:before",
            "a:released",
            "b:acquired",
            "b:released",
        ]

    def test_priority_lock_asyncio_context_manager(self):
        lock = PriorityLock()
        seen: list[str] = []

        async def worker(name: str) -> None:
            seen.append(f"{name}:before")
            async with lock:
                seen.append(f"{name}:inside")
                await asyncio.sleep(0)
            seen.append(f"{name}:after")

        async def orchestrate() -> None:
            await asyncio.gather(worker("x"), worker("y"))

        asyncio.run(orchestrate())

        assert seen == [
            "x:before",
            "x:inside",
            "y:before",
            "x:after",
            "y:inside",
            "y:after",
        ]


class TestLockExamples:
    def test_lock_serializes_access(self):
        s = _new_scheduler()
        lock = Lock()
        seen: list[str] = []

        def worker(name: str) -> None:
            seen.append(f"{name}:before")
            with lock:
                seen.append(f"{name}:acquired")
                s.yield_()
                seen.append(f"{name}:releasing")
            seen.append(f"{name}:after")

        s.spawn(lambda: worker("a"))
        s.spawn(lambda: worker("b"))
        s.run()

        assert seen == [
            "a:before",
            "a:acquired",
            "b:before",
            "a:releasing",
            "a:after",
            "b:acquired",
            "b:releasing",
            "b:after",
        ]

    def test_lock_asyncio_acquire_release(self):
        lock = Lock()
        seen: list[str] = []

        async def worker(name: str) -> None:
            seen.append(f"{name}:before")
            await lock.acquire()
            try:
                seen.append(f"{name}:acquired")
                await asyncio.sleep(0)
            finally:
                lock.release()
                seen.append(f"{name}:released")

        async def orchestrate() -> None:
            await asyncio.gather(worker("a"), worker("b"))

        asyncio.run(orchestrate())

        assert seen == [
            "a:before",
            "a:acquired",
            "b:before",
            "a:released",
            "b:acquired",
            "b:released",
        ]

    def test_lock_release_unsets_locked_state(self):
        lock = Lock()
        assert lock.sacquire() is True
        lock.release()
        assert lock.locked() is False

    def test_lock_asyncio_context_manager(self):
        lock = Lock()
        seen: list[str] = []

        async def worker(name: str) -> None:
            seen.append(f"{name}:before")
            async with lock:
                seen.append(f"{name}:inside")
                await asyncio.sleep(0)
            seen.append(f"{name}:after")

        async def orchestrate() -> None:
            await asyncio.gather(worker("x"), worker("y"))

        asyncio.run(orchestrate())

        assert seen == [
            "x:before",
            "x:inside",
            "y:before",
            "x:after",
            "y:inside",
            "y:after",
        ]

    def test_semaphore_limits_concurrency(self):
        s = _new_scheduler()
        sem = Semaphore(2)
        active = 0
        max_active = 0
        seen: list[str] = []

        def worker(name: str) -> None:
            nonlocal active, max_active
            sem.sacquire()
            try:
                active += 1
                max_active = max(max_active, active)
                seen.append(f"{name}:entered")
                s.yield_()
            finally:
                active -= 1
                sem.release()
                seen.append(f"{name}:left")

        s.spawn(lambda: worker("a"))
        s.spawn(lambda: worker("b"))
        s.spawn(lambda: worker("c"))
        s.run()

        assert max_active == 2
        assert seen == [
            "a:entered",
            "b:entered",
            "a:left",
            "b:left",
            "c:entered",
            "c:left",
        ]

    def test_bounded_semaphore_overrelease_raises(self):
        sem = BoundedSemaphore(1)

        sem.sacquire()
        sem.release()
        with pytest.raises(ValueError, match="released too many times"):
            sem.release()

    def test_semaphore_asyncio_acquire_release(self):
        sem = Semaphore(1)
        seen: list[str] = []

        async def worker(name: str) -> None:
            seen.append(f"{name}:before")
            await sem.acquire()
            try:
                seen.append(f"{name}:inside")
                await asyncio.sleep(0)
            finally:
                sem.release()
                seen.append(f"{name}:after")

        async def run() -> None:
            await asyncio.gather(worker("x"), worker("y"))

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))

        assert seen == [
            "x:before",
            "x:inside",
            "y:before",
            "x:after",
            "y:inside",
            "y:after",
        ]

    def test_semaphore_asyncio_context_manager(self):
        sem = Semaphore(1)
        seen: list[str] = []

        async def worker(name: str) -> None:
            seen.append(f"{name}:before")
            async with sem:
                seen.append(f"{name}:inside")
                await asyncio.sleep(0)
            seen.append(f"{name}:after")

        async def run() -> None:
            await asyncio.gather(worker("x"), worker("y"))

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))

        assert seen == [
            "x:before",
            "x:inside",
            "y:before",
            "x:after",
            "y:inside",
            "y:after",
        ]


class TestPulseEvent:
    def test_set_wakes_all_current_waiters(self):
        s = _new_scheduler()
        set_scheduler(s)
        pulse = PulseEvent()
        seen: list[str] = []

        def waiter(name: str) -> None:
            seen.append(f"{name}:waiting")
            pulse.swait()
            seen.append(f"{name}:resumed")

        def notifier() -> None:
            s.yield_()
            seen.append("notify")
            pulse.set()

        s.spawn(lambda: waiter("a"))
        s.spawn(lambda: waiter("b"))
        s.spawn(notifier)
        s.run()

        assert seen == [
            "a:waiting",
            "b:waiting",
            "notify",
            "a:resumed",
            "b:resumed",
        ]

    def test_set_is_not_sticky(self):
        s = _new_scheduler()
        set_scheduler(s)
        pulse = PulseEvent()
        seen: list[str] = []

        def early_set() -> None:
            pulse.set()
            seen.append("early_set")

        def late_waiter() -> None:
            s.yield_()
            seen.append("late:enter")
            pulse.swait()
            seen.append("late:resumed")

        def late_set() -> None:
            s.yield_()
            s.yield_()
            seen.append("late_set")
            pulse.set()

        s.spawn(early_set)
        s.spawn(late_waiter)
        s.spawn(late_set)
        s.run()

        assert seen == ["early_set", "late:enter", "late_set", "late:resumed"]

    def test_swait_for_predicate(self):
        s = _new_scheduler()
        set_scheduler(s)
        pulse = PulseEvent()
        state = {"ready": False}
        seen: list[str] = []

        def waiter() -> None:
            pulse.swait_for(lambda: state["ready"])
            seen.append("waiter:done")

        def setter() -> None:
            s.yield_()
            state["ready"] = True
            pulse.set()

        s.spawn(waiter)
        s.spawn(setter)
        s.run()

        assert seen == ["waiter:done"]

    def test_swait_for_returns_ready_none_via_tuple_box(self):
        s = _new_scheduler()
        set_scheduler(s)
        pulse = PulseEvent()
        state: dict[str, object | None] = {"value": False}

        def waiter() -> object:
            return pulse.swait_for(lambda: (None,) if state["value"] else None)

        def setter() -> None:
            s.yield_()
            state["value"] = True
            pulse.set()

        task = s.spawn(waiter)
        s.spawn(setter)
        assert s.run_until_complete(task) == (None,)

    def test_swait_suppresses_overdue_timeout_when_already_pulsed(self):
        """Regression: set() then competing timeout must not fail the waiter.

        ``Timeout`` throws into the parked tealet after unlinking; without a
        wake signal independent of ``_waiters``, ``swait`` would re-raise and
        ``swait_for`` / ``take_next`` would lose a ready payload.
        """

        class FakeTimeScheduler(Scheduler):
            def __init__(self) -> None:
                super().__init__()
                self.now = 0.0

            def time(self) -> float:
                return self.now

        s = FakeTimeScheduler()
        set_scheduler(s)
        pulse = PulseEvent()
        state = {"ready": False}
        seen: list[str] = []

        def waiter() -> None:
            tm = timeout_at(10.0)
            with tm:
                pulse.swait_for(lambda: state["ready"])
            seen.append(f"resumed={tm.expired()}")

        s.spawn(waiter)
        s.pump(1)
        assert pulse._waiters

        def deliver() -> None:
            state["ready"] = True
            pulse.set()

        s.call_at(9.0, deliver)
        s.now = 11.0
        s.run()

        assert seen == ["resumed=True"]


class TestConditionExamples:
    def test_condition_wait_notify(self):
        s = _new_scheduler()
        cond = Condition()
        seen: list[str] = []

        def waiter(name: str) -> None:
            with cond:
                seen.append(f"{name}:waiting")
                cond.swait()
                seen.append(f"{name}:resumed")

        def notifier() -> None:
            s.yield_()
            with cond:
                seen.append("notifier:notify")
                cond.notify()
            s.yield_()
            with cond:
                seen.append("notifier:notify_all")
                cond.notify_all()

        s.spawn(lambda: waiter("a"))
        s.spawn(lambda: waiter("b"))
        s.spawn(notifier)
        s.run()

        assert seen == [
            "a:waiting",
            "b:waiting",
            "notifier:notify",
            "a:resumed",
            "notifier:notify_all",
            "b:resumed",
        ]

    def test_condition_wait_for_predicate(self):
        s = _new_scheduler()
        cond = Condition()
        state = {"ready": False}
        seen: list[str] = []

        def waiter() -> None:
            with cond:
                cond.swait_for(lambda: state["ready"])
                seen.append("waiter:done")

        def setter() -> None:
            s.yield_()
            with cond:
                state["ready"] = True
                cond.notify_all()

        s.spawn(waiter)
        s.spawn(setter)
        s.run()

        assert seen == ["waiter:done"]

    def test_condition_wait_and_notify_require_lock(self):
        cond = Condition()

        with pytest.raises(RuntimeError, match="un-acquired lock"):
            cond.swait()
        with pytest.raises(RuntimeError, match="un-acquired lock"):
            cond.notify()

    def test_cross_thread_condition_wait_notify(self):
        s = _new_scheduler()
        cond = CrossThreadCondition(scheduler=s)
        seen: list[str] = []

        def waiter(name: str) -> None:
            with cond:
                seen.append(f"{name}:waiting")
                cond.swait()
                seen.append(f"{name}:resumed")

        def notifier() -> None:
            s.yield_()
            with cond:
                seen.append("notifier:notify")
                cond.notify()
            s.yield_()
            with cond:
                seen.append("notifier:notify_all")
                cond.notify_all()

        s.spawn(lambda: waiter("a"))
        s.spawn(lambda: waiter("b"))
        s.spawn(notifier)
        s.run()

        assert seen == [
            "a:waiting",
            "b:waiting",
            "notifier:notify",
            "a:resumed",
            "notifier:notify_all",
            "b:resumed",
        ]

    def test_cross_thread_condition_wait_for_predicate(self):
        s = _new_scheduler()
        cond = CrossThreadCondition(scheduler=s)
        state = {"ready": False}
        seen: list[str] = []

        def waiter() -> None:
            with cond:
                cond.swait_for(lambda: state["ready"])
                seen.append("waiter:done")

        def setter() -> None:
            s.yield_()
            with cond:
                state["ready"] = True
                cond.notify_all()

        s.spawn(waiter)
        s.spawn(setter)
        s.run()

        assert seen == ["waiter:done"]

    def test_condition_swait_for_returns_ready_none_via_tuple_box(self):
        s = _new_scheduler()
        cond = Condition()
        state = {"value": False}

        def waiter() -> object:
            with cond:
                return cond.swait_for(lambda: (None,) if state["value"] else None)

        def setter() -> None:
            s.yield_()
            with cond:
                state["value"] = True
                cond.notify_all()

        task = s.spawn(waiter)
        s.spawn(setter)
        s.run()
        assert task.result() == (None,)

    def test_cross_thread_condition_cross_thread_notify(self, monkeypatch):
        s = _new_scheduler()
        cond = CrossThreadCondition(scheduler=s)
        state = {"ready": False}
        seen: list[str] = []
        waiter_blocked = threading.Event()
        real_swait = cond.swait

        def swait_and_signal() -> bool:
            waiter_blocked.set()
            return real_swait()

        monkeypatch.setattr(cond, "swait", swait_and_signal)

        def waiter() -> None:
            with cond:
                seen.append("waiting")
                cond.swait_for(lambda: state["ready"])
                seen.append("resumed")

        def notifier_from_thread() -> None:
            assert waiter_blocked.wait(timeout=2.0)
            with cond:
                state["ready"] = True
                cond.notify_all()

        threading.Thread(target=notifier_from_thread, daemon=True).start()
        s.run_until_complete(s.spawn(waiter))

        assert seen == ["waiting", "resumed"]

    def test_cross_thread_condition_wait_and_notify_require_lock(self):
        s = _new_scheduler()
        cond = CrossThreadCondition(scheduler=s)

        with pytest.raises(RuntimeError, match="un-acquired lock"):
            cond.swait()
        with pytest.raises(RuntimeError, match="un-acquired lock"):
            cond.notify()

    def test_condition_asyncio_wait_notify(self):
        cond = Condition()
        seen: list[str] = []

        async def waiter(name: str) -> None:
            async with cond:
                seen.append(f"{name}:waiting")
                await cond.wait()
                seen.append(f"{name}:resumed")

        async def notifier() -> None:
            await asyncio.sleep(0)
            async with cond:
                seen.append("notifier:notify")
                cond.notify()
            await asyncio.sleep(0)
            async with cond:
                seen.append("notifier:notify_all")
                cond.notify_all()

        async def run() -> None:
            t1 = asyncio.create_task(waiter("a"))
            t2 = asyncio.create_task(waiter("b"))
            await notifier()
            await asyncio.wait_for(asyncio.gather(t1, t2), timeout=1.0)

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))

        assert set(seen) == {
            "a:waiting",
            "b:waiting",
            "notifier:notify",
            "notifier:notify_all",
            "a:resumed",
            "b:resumed",
        }
        notify_idx = seen.index("notifier:notify")
        notify_all_idx = seen.index("notifier:notify_all")
        assert notify_idx < seen.index("a:resumed")
        assert notify_idx < seen.index("b:resumed")
        assert notify_idx < notify_all_idx

    def test_condition_asyncio_wait_for_predicate(self):
        cond = Condition()
        state = {"ready": False}
        seen: list[str] = []

        async def waiter() -> None:
            async with cond:
                await cond.wait_for(lambda: state["ready"])
                seen.append("waiter:done")

        async def setter() -> None:
            await asyncio.sleep(0)
            async with cond:
                state["ready"] = True
                cond.notify_all()

        async def run() -> None:
            t_waiter = asyncio.create_task(waiter())
            t_setter = asyncio.create_task(setter())
            await asyncio.wait_for(asyncio.gather(t_waiter, t_setter), timeout=1.0)

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))
        assert seen == ["waiter:done"]

    def test_barrier_swait_releases_group(self):
        s = _new_scheduler()
        barrier = Barrier(3)
        seen: list[str] = []

        def worker(name: str) -> None:
            seen.append(f"{name}:before")
            idx = barrier.swait()
            seen.append(f"{name}:after:{idx}")

        s.spawn(lambda: worker("a"))
        s.spawn(lambda: worker("b"))
        s.spawn(lambda: worker("c"))
        s.run()

        assert seen[:3] == ["a:before", "b:before", "c:before"]
        assert set(seen[3:]) == {"a:after:2", "b:after:1", "c:after:0"}

    def test_barrier_async_wait_releases_group(self):
        barrier = Barrier(3)
        seen: list[str] = []

        async def worker(name: str) -> None:
            seen.append(f"{name}:before")
            idx = await barrier.wait()
            seen.append(f"{name}:after:{idx}")

        async def run() -> None:
            await asyncio.gather(worker("a"), worker("b"), worker("c"))

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))

        assert set(seen) == {
            "a:before",
            "b:before",
            "c:before",
            "a:after:2",
            "b:after:1",
            "c:after:0",
        }

    def test_barrier_requires_positive_parties(self):
        with pytest.raises(ValueError, match="parties must be > 0"):
            Barrier(0)
