import asyncio
import contextvars
import threading

import pytest

import _tealet
from tealet.locks import (
    Barrier,
    BoundedSemaphore,
    Condition,
    Event,
    InvalidStateError,
    LifoQueue,
    Lock,
    PriorityQueue,
    Queue,
    QueueEmpty,
    QueueFull,
    Semaphore,
)
from tealet.scheduler import (
    CancelledError,
    Channel,
    Future,
    get_default_scheduler_factory,
    get_running_scheduler,
    get_scheduler,
    new_scheduler,
    set_default_scheduler_factory,
    set_scheduler,
    TealetTask,
    RawTimeoutError,
    SimpleScheduler,
    TimeoutError,
    _scheduler,
    scheduler,
    timeout,
)
from tealet_examples import (
    demo_future_result,
    demo_future_timeout_then_success,
    demo_scheduler_append_with_yield,
    demo_sleep,
    demo_wait_for_event_start,
)


@pytest.fixture(autouse=True)
def _reset_scheduler_tls():
    _scheduler.instance = SimpleScheduler()
    set_default_scheduler_factory(None)
    try:
        yield
    finally:
        _scheduler.instance = SimpleScheduler()
        set_default_scheduler_factory(None)


class TestSchedulerAccessors:
    def test_default_scheduler_factory_is_callable(self):
        factory = get_default_scheduler_factory()
        assert callable(factory)

    def test_set_default_scheduler_factory_used_by_new_scheduler(self):
        custom = SimpleScheduler()

        def factory() -> SimpleScheduler:
            return custom

        set_default_scheduler_factory(factory)
        assert new_scheduler() is custom

    def test_set_default_scheduler_factory_used_by_get_scheduler(self):
        custom = SimpleScheduler()

        def factory() -> SimpleScheduler:
            return custom

        set_default_scheduler_factory(factory)
        set_scheduler(None)

        assert get_scheduler() is custom
        assert get_scheduler() is custom

    def test_default_scheduler_factory_must_return_simple_scheduler(self):
        def bad_factory():
            return object()

        set_default_scheduler_factory(bad_factory)
        with pytest.raises(TypeError, match="scheduler factory must return"):
            new_scheduler()

    def test_new_scheduler_returns_unbound_instance(self):
        s = new_scheduler()
        assert isinstance(s, SimpleScheduler)
        assert s is not get_scheduler()

    def test_set_and_get_scheduler(self):
        s = new_scheduler()
        set_scheduler(s)
        assert get_scheduler() is s

    def test_get_scheduler_creates_when_unbound(self):
        set_scheduler(None)
        s = get_scheduler()
        assert isinstance(s, SimpleScheduler)
        assert get_scheduler() is s

    def test_get_running_scheduler_raises_when_not_running(self):
        with pytest.raises(RuntimeError, match="no running scheduler"):
            get_running_scheduler()

    def test_get_running_scheduler_during_run(self):
        s = new_scheduler()
        set_scheduler(s)
        seen: list[SimpleScheduler] = []

        def check_running() -> None:
            seen.append(get_running_scheduler())

        s.spawn(check_running)
        s.run()

        assert seen == [s]

    def test_get_running_scheduler_during_arun(self):
        s = new_scheduler()
        set_scheduler(s)
        seen: list[SimpleScheduler] = []

        def check_running() -> None:
            seen.append(get_running_scheduler())

        async def run() -> None:
            s.spawn(check_running)
            await s.arun()

        asyncio.run(run())

        assert seen == [s]

    def test_run_requires_scheduler_to_be_current(self):
        s = new_scheduler()
        set_scheduler(new_scheduler())
        with pytest.raises(RuntimeError, match="current scheduler"):
            s.run()

    def test_pump_requires_scheduler_to_be_current(self):
        s = new_scheduler()
        set_scheduler(new_scheduler())
        with pytest.raises(RuntimeError, match="current scheduler"):
            s.pump()

    def test_arun_requires_scheduler_to_be_current(self):
        s = new_scheduler()
        set_scheduler(new_scheduler())

        async def run() -> None:
            with pytest.raises(RuntimeError, match="current scheduler"):
                await s.arun()

        asyncio.run(run())

    def test_run_until_complete_returns_result(self):
        s = new_scheduler()
        set_scheduler(s)

        def worker() -> int:
            s.yield_()
            return 42

        fut = s.spawn(worker)
        assert s.run_until_complete(fut) == 42

    def test_run_until_complete_propagates_exception(self):
        s = new_scheduler()
        set_scheduler(s)

        def worker() -> None:
            raise ValueError("boom")

        fut = s.spawn(worker)
        with pytest.raises(ValueError, match="boom"):
            s.run_until_complete(fut)

    def test_run_until_complete_rejects_foreign_task(self):
        s1 = new_scheduler()
        s2 = new_scheduler()
        set_scheduler(s1)
        fut = s2.spawn(lambda: 1)
        with pytest.raises(RuntimeError, match="different scheduler"):
            s1.run_until_complete(fut)

    def test_run_until_complete_raises_if_stopped_early(self):
        s = new_scheduler()
        set_scheduler(s)
        fut: Future[int] = Future()
        s.call_soon(s.stop)
        with pytest.raises(RuntimeError, match="stopped before Future completed"):
            s.run_until_complete(fut)

    def test_run_until_complete_accepts_callable(self):
        s = new_scheduler()
        set_scheduler(s)

        def worker() -> int:
            return 42

        assert s.run_until_complete(worker) == 42

    def test_run_until_complete_rejects_callable_args(self):
        s = new_scheduler()
        set_scheduler(s)

        def worker() -> int:
            return 42

        with pytest.raises(TypeError, match="unexpected keyword argument"):
            s.run_until_complete(worker, x=20, y=22)

        with pytest.raises(TypeError, match="takes 2 positional arguments but 4 were given"):
            s.run_until_complete(worker, 20, 22)

    def test_run_until_complete_rejects_non_future_non_callable(self):
        s = new_scheduler()
        set_scheduler(s)
        with pytest.raises(TypeError, match="Future or callable"):
            s.run_until_complete(object())


class TestSchedulerExamples:
    def test_scheduler_is_running_for_run_only(self):
        s = scheduler()
        seen: list[bool] = []

        def check() -> None:
            seen.append(s.is_running())

        assert s.is_running() is False
        s.spawn(check)
        s.run()
        assert seen == [True]
        assert s.is_running() is False

    def test_scheduler_is_running_for_arun_only(self):
        s = scheduler()
        seen: list[bool] = []

        def check() -> None:
            seen.append(s.is_running())

        async def orchestrate() -> None:
            assert s.is_running() is False
            s.spawn(check)
            await s.arun()
            assert s.is_running() is False

        asyncio.run(orchestrate())
        assert seen == [True]

    def test_scheduler_is_running_set_during_pump_only(self):
        s = scheduler()
        seen: list[bool] = []

        def check() -> None:
            seen.append(s.is_running())

        assert s.is_running() is False
        s.spawn(check)
        s.pump(1)
        assert seen == [True]
        assert s.is_running() is False

    def test_run_forever_stops_when_stop_called(self):
        s = scheduler()
        seen: list[str] = []

        def worker() -> None:
            seen.append("ran")
            s.stop()

        s.spawn(worker)
        s.run_forever()

        assert seen == ["ran"]
        assert s.is_running() is False

    def test_stop_breaks_sleep_in_run_forever_via_call_soon_threadsafe(self):
        s = scheduler()
        started = threading.Event()

        # Ensure run_forever enters a long timed wait after startup.
        s.call_later(60.0, lambda: None)
        s.call_soon(started.set)

        def run_forever_in_thread() -> None:
            set_scheduler(s)
            s.run_forever()

        t = threading.Thread(target=run_forever_in_thread)
        t.start()
        try:
            assert started.wait(timeout=1.0)
            s.call_soon_threadsafe(s.stop)
            t.join(timeout=1.0)
            assert not t.is_alive()
            assert s.is_running() is False
        finally:
            s.call_soon_threadsafe(s.stop)
            t.join(timeout=1.0)

    def test_append_with_yield_demo(self):
        seen = demo_scheduler_append_with_yield()
        assert seen == ["a0", "b0", "c0", "a1", "b1", "a2"]

    def test_wait_for_event_start_demo(self):
        seen = demo_wait_for_event_start()
        assert seen == ["waiter:waiting", "starter:set", "waiter:started"]

    def test_sleep_demo(self):
        seen = demo_sleep()
        assert seen == ["before:sleep", "after:sleep"]

    def test_timer_handle_cancel(self):
        s = scheduler()
        seen: list[str] = []

        def mark() -> None:
            seen.append("fired")

        handle = s.call_later(0.001, mark)
        handle.cancel()
        s.run()

        assert handle.cancelled()
        assert seen == []

    def test_call_later_captures_current_context(self):
        s = scheduler()
        marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker", default="default")
        seen: list[str] = []

        marker.set("scheduled")

        def mark() -> None:
            seen.append(marker.get())

        s.call_later(0, mark)
        marker.set("after-schedule")

        s.run()

        assert seen == ["scheduled"]

    def test_call_later_uses_explicit_context(self):
        s = scheduler()
        marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker", default="default")
        seen: list[str] = []

        marker.set("ambient")

        def mark() -> None:
            seen.append(marker.get())

        ctx = contextvars.copy_context()
        ctx.run(marker.set, "explicit")

        s.call_later(0, mark, context=ctx)
        marker.set("after-schedule")

        s.run()

        assert seen == ["explicit"]

    def test_spawn_captures_current_context(self):
        s = scheduler()
        marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker", default="default")
        seen: list[str] = []

        marker.set("scheduled")
        s.spawn(lambda: seen.append(marker.get()))
        marker.set("after-schedule")

        s.run()

        assert seen == ["scheduled"]

    def test_spawn_uses_explicit_context(self):
        s = scheduler()
        marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker", default="default")
        seen: list[str] = []

        marker.set("ambient")
        ctx = contextvars.copy_context()
        ctx.run(marker.set, "explicit")

        s.spawn(lambda: seen.append(marker.get()), context=ctx)
        marker.set("after-schedule")

        s.run()

        assert seen == ["explicit"]

    def test_event_wait_timeout_and_success(self):
        s = scheduler()
        evt = Event()
        seen: list[str] = []

        def timeout_waiter() -> None:
            tm = timeout(0.001)
            with pytest.raises(TimeoutError, match="Operation timed out"):
                with tm:
                    evt.wait()
            seen.append(f"timeout={not tm.expired()}")

        def success_waiter() -> None:
            tm = timeout(0.01)
            with tm:
                evt.wait()
            seen.append(f"success={not tm.expired()}")

        def setter() -> None:
            s.sleep(0.002)
            evt.set()

        s.spawn(timeout_waiter)
        s.spawn(success_waiter)
        s.spawn(setter)
        s.run()

        assert seen == ["timeout=False", "success=True"]

    def test_timeout_context_event_wait_timeout_and_success(self):
        s = scheduler()
        evt = Event()
        seen: list[str] = []

        def timeout_waiter() -> None:
            tm = timeout(0.001)
            with pytest.raises(TimeoutError, match="Operation timed out"):
                with tm:
                    evt.wait()
            seen.append(f"timeout={tm.expired()}")

        def success_waiter() -> None:
            tm = timeout(0.01)
            with tm:
                evt.wait()
            seen.append(f"success={not tm.expired()}")

        def setter() -> None:
            s.sleep(0.002)
            evt.set()

        s.spawn(timeout_waiter)
        s.spawn(success_waiter)
        s.spawn(setter)
        s.run()

        assert seen == ["timeout=True", "success=True"]

    def test_timeout_demo(self):
        seen = demo_future_timeout_then_success()
        assert seen == ["timeout_waiter:False", "success_waiter:True"]

    def test_arun_runs_inside_asyncio_task(self):
        s = scheduler()
        seen: list[str] = []

        # Keep arun() active and inject runnable work while it is waiting.
        s.call_later(0.001, lambda: s.spawn(lambda: seen.append("spawned")))

        async def orchestrate() -> None:
            runner = asyncio.create_task(s.arun())
            try:
                await asyncio.wait_for(runner, timeout=1.0)
            except asyncio.TimeoutError:
                # Fail fast with bounded cancellation cleanup instead of hanging.
                runner.cancel()
                try:
                    await asyncio.wait_for(runner, timeout=0.2)
                except asyncio.CancelledError:
                    pass
                except asyncio.TimeoutError:
                    pass
                pytest.fail(
                    "scheduler arun timed out: "
                    f"tasks={len(s._tasks)} timers={len(s._timers)} "
                    f"runner={s._runner is not None} seen={seen}"
                )

        asyncio.run(orchestrate())

        assert seen == ["spawned"]

    def test_event_async_wait_from_asyncio_task(self):
        evt = Event()

        async def orchestrate() -> bool:
            waiter = asyncio.create_task(evt.async_wait())
            await asyncio.sleep(0)
            assert not waiter.done()
            evt.set()
            return await asyncio.wait_for(waiter, timeout=1.0)

        assert asyncio.run(orchestrate()) is True

    def test_run_switches_immediately_to_target(self):
        s = scheduler()
        evt = Event()
        seen: list[str] = []
        target_ref: dict[str, TealetTask] = {}

        def target_worker() -> None:
            target_ref["t"] = _tealet.current()
            seen.append("target:started")
            evt.wait()
            seen.append("target:resumed")

        def caller() -> None:
            seen.append("caller:before-run")
            target_ref["t"].run()
            seen.append("caller:after-run")

        s.spawn(target_worker)
        s.spawn(caller)
        s.run()

        assert seen == ["target:started", "caller:before-run", "target:resumed", "caller:after-run"]

    def test_throw_switches_immediately_to_target(self):
        s = scheduler()
        evt = Event()
        seen: list[str] = []
        target_ref: dict[str, TealetTask] = {}

        def target_worker() -> None:
            target_ref["t"] = _tealet.current()
            seen.append("target:started")
            try:
                evt.wait()
            except ValueError as exc:
                seen.append(f"target:caught:{exc}")
            seen.append("target:finished")

        def caller() -> None:
            seen.append("caller:before-throw")
            target_ref["t"].throw(ValueError("boom"))
            seen.append("caller:after-throw")

        s.spawn(target_worker)
        s.spawn(caller)
        s.run()

        assert seen == [
            "target:started",
            "caller:before-throw",
            "target:caught:boom",
            "target:finished",
            "caller:after-throw",
        ]

    def test_wait_async_returns_result(self):
        s = scheduler()
        seen: list[int] = []

        async def compute() -> int:
            await asyncio.sleep(0.001)
            return 11

        def worker() -> None:
            seen.append(s.wait_async(compute()))

        s.spawn(worker)
        asyncio.run(asyncio.wait_for(s.arun(), timeout=1.0))

        assert seen == [11]

    def test_wait_async_propagates_exception(self):
        s = scheduler()
        seen: list[str] = []

        async def boom() -> int:
            await asyncio.sleep(0.001)
            raise ValueError("boom")

        def worker() -> None:
            with pytest.raises(ValueError, match="boom"):
                s.wait_async(boom())
            seen.append("handled")

        s.spawn(worker)
        asyncio.run(asyncio.wait_for(s.arun(), timeout=1.0))

        assert seen == ["handled"]

    def test_wait_async_marks_tealet_blocked(self):
        s = scheduler()
        seen: list[tuple[str, bool, bool]] = []

        async def compute() -> int:
            await asyncio.sleep(0.001)
            return 1

        def worker() -> None:
            current = _tealet.current()
            seen.append(("before", current.is_blocked(), current.is_runnable()))
            s.call_later(0.0, lambda: seen.append(("during", current.is_blocked(), current.is_runnable())))
            s.wait_async(compute())
            seen.append(("after", current.is_blocked(), current.is_runnable()))

        s.spawn(worker)
        asyncio.run(asyncio.wait_for(s.arun(), timeout=1.0))

        assert seen == [
            ("before", False, False),
            ("during", True, False),
            ("after", False, False),
        ]

    def test_lock_serializes_access(self):
        s = scheduler()
        lock = Lock()
        seen: list[str] = []

        def worker(name: str) -> None:
            seen.append(f"{name}:before")
            with lock:
                seen.append(f"{name}:acquired")
                s.yield_()
                seen.append(f"{name}:releasing")
            seen.append(f"{name}:after")

        s.spawn(worker, "a")
        s.spawn(worker, "b")
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
        assert lock.sync_acquire() is True
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
        s = scheduler()
        sem = Semaphore(2)
        active = 0
        max_active = 0
        seen: list[str] = []

        def worker(name: str) -> None:
            nonlocal active, max_active
            sem.sync_acquire()
            try:
                active += 1
                max_active = max(max_active, active)
                seen.append(f"{name}:entered")
                s.yield_()
            finally:
                active -= 1
                sem.release()
                seen.append(f"{name}:left")

        s.spawn(worker, "a")
        s.spawn(worker, "b")
        s.spawn(worker, "c")
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

        sem.sync_acquire()
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

    def test_condition_wait_notify(self):
        s = scheduler()
        cond = Condition()
        seen: list[str] = []

        def waiter(name: str) -> None:
            with cond:
                seen.append(f"{name}:waiting")
                cond.sync_wait()
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

        s.spawn(waiter, "a")
        s.spawn(waiter, "b")
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
        s = scheduler()
        cond = Condition()
        state = {"ready": False}
        seen: list[str] = []

        def waiter() -> None:
            with cond:
                cond.sync_wait_for(lambda: state["ready"])
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
            cond.sync_wait()
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

    def test_barrier_sync_wait_releases_group(self):
        s = scheduler()
        barrier = Barrier(3)
        seen: list[str] = []

        def worker(name: str) -> None:
            seen.append(f"{name}:before")
            idx = barrier.sync_wait()
            seen.append(f"{name}:after:{idx}")

        s.spawn(worker, "a")
        s.spawn(worker, "b")
        s.spawn(worker, "c")
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


class TestFutureExamples:
    def test_future_demo(self):
        seen = demo_future_result()
        assert seen == ["producer:start", "producer:done", "consumer:result=42"]

    def test_future_exception_propagates(self):
        s = scheduler()

        def boom():
            raise ValueError("boom")

        future = s.spawn(boom)
        s.run()

        assert future.done()
        with pytest.raises(ValueError, match="boom"):
            future.result()
        assert isinstance(future.exception(), ValueError)

    def test_future_exception_before_task_main_starts(self):
        s = scheduler()
        gate = Event()
        seen: list[str] = []

        def blocked() -> int:
            seen.append("blocked:start")
            gate.wait()
            seen.append("blocked:done")
            return 1

        future_blocked = s.spawn(blocked)

        def thrower() -> None:
            seen.append("thrower:start")
            future_victim = s.spawn(lambda: 7)
            victim = s._tasks[-1]
            victim.throw(ValueError("pre-start"))
            assert future_victim.done()
            with pytest.raises(ValueError, match="pre-start"):
                future_victim.result()
            gate.set()
            seen.append("thrower:done")

        s.spawn(thrower)
        s.run()

        assert future_blocked.result() == 1
        assert seen == ["blocked:start", "thrower:start", "thrower:done", "blocked:done"]

    def test_future_set_result_once(self):
        future = Future()
        future.set_result(123)

        assert future.done()
        assert future.wait() == 123
        assert future.result() == 123
        assert future.exception() is None

        with pytest.raises(InvalidStateError):
            future.set_result(456)

    def test_future_done_callback_runs_on_completion(self):
        future: Future[int] = Future()
        seen: list[str] = []

        def on_done(done: Future[int]) -> None:
            seen.append(f"done={done.result()}")

        future.add_done_callback(on_done)
        future.set_result(5)

        assert seen == ["done=5"]

    def test_future_done_callback_is_scheduled_when_already_done(self):
        async def case() -> None:
            future: Future[int] = Future()
            future.set_result(7)
            seen: list[str] = []

            def on_done(done: Future[int]) -> None:
                seen.append(f"done={done.result()}")

            future.add_done_callback(on_done)

            assert seen == []
            await asyncio.sleep(0)
            assert seen == ["done=7"]

        asyncio.run(case())

    def test_future_done_callback_uses_context(self):
        marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker", default="default")
        future: Future[int] = Future()
        seen: list[str] = []

        def on_done(_done: Future[int]) -> None:
            seen.append(marker.get())

        ctx = contextvars.copy_context()
        ctx.run(marker.set, "callback-context")

        future.add_done_callback(on_done, context=ctx)
        future.set_result(1)

        assert seen == ["callback-context"]

    def test_future_done_callback_captures_current_context_by_default(self):
        marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker", default="default")
        future: Future[int] = Future()
        seen: list[str] = []

        marker.set("registered")

        def on_done(_done: Future[int]) -> None:
            seen.append(marker.get())

        future.add_done_callback(on_done)
        marker.set("after-register")
        future.set_result(1)

        assert seen == ["registered"]

    def test_future_remove_done_callback(self):
        future: Future[int] = Future()
        seen: list[str] = []

        def cb_one(_done: Future[int]) -> None:
            seen.append("one")

        def cb_two(_done: Future[int]) -> None:
            seen.append("two")

        future.add_done_callback(cb_one)
        future.add_done_callback(cb_one)
        future.add_done_callback(cb_two)

        assert future.remove_done_callback(cb_one) == 2

        future.set_result(1)

        assert seen == ["two"]

    def test_future_result_and_exception_require_done(self):
        future = Future()

        with pytest.raises(InvalidStateError, match="Result is not ready"):
            future.result()
        with pytest.raises(InvalidStateError, match="Exception is not set"):
            future.exception()

    def test_future_cancel_marks_done_and_raises_cancelled(self):
        future = Future()

        assert future.cancel() is True
        assert future.done()
        assert future.cancelled()
        assert future.cancel() is False

        with pytest.raises(CancelledError):
            future.result()
        with pytest.raises(CancelledError):
            future.exception()

    def test_future_wait_after_cancel_raises_cancelled(self):
        future = Future()
        assert future.cancel() is True
        with pytest.raises(CancelledError):
            future.wait()

    def test_future_await_after_cancel_raises_cancelled(self):
        future = Future()
        assert future.cancel() is True

        async def orchestrate() -> None:
            with pytest.raises(CancelledError):
                await future

        asyncio.run(orchestrate())

    def test_future_result_timeout(self):
        s = scheduler()
        future: Future[int] = Future()
        seen: list[str] = []

        def complete_later() -> None:
            s.sleep(0.01)
            future.set_result(1)

        def waiter() -> None:
            tm = timeout(0.001)
            with pytest.raises(TimeoutError, match="Operation timed out"):
                with tm:
                    future.wait()
            seen.append(f"timed-out={tm.expired()}")
            future.wait()
            seen.append(f"value={future.result()}")

        s.spawn(complete_later)
        s.spawn(waiter)
        s.run()
        assert seen == ["timed-out=True", "value=1"]

    def test_timeout_context_future_result_timeout(self):
        s = scheduler()
        future: Future[int] = Future()
        seen: list[str] = []

        def complete_later() -> None:
            s.sleep(0.01)
            future.set_result(1)

        def waiter() -> None:
            tm = timeout(0.001)
            with pytest.raises(TimeoutError, match="Operation timed out"):
                with tm:
                    future.wait()
            seen.append(f"timed-out={tm.expired()}")
            future.wait()
            seen.append(f"value={future.result()}")

        s.spawn(complete_later)
        s.spawn(waiter)
        s.run()
        assert seen == ["timed-out=True", "value=1"]

    def test_future_async_result(self):
        s = scheduler()
        future: Future[int] = Future()

        async def orchestrate() -> None:
            s.call_later(0.001, future.set_result, 7)
            runner = asyncio.create_task(s.arun())
            try:
                assert await asyncio.wait_for(future.async_wait(), timeout=1.0) == 7
                assert future.result() == 7
            finally:
                await asyncio.wait_for(runner, timeout=1.0)

        asyncio.run(orchestrate())

    def test_future_async_exception(self):
        s = scheduler()
        future: Future[int] = Future()

        async def orchestrate() -> None:
            s.call_later(0.001, future.set_exception, ValueError("boom"))
            runner = asyncio.create_task(s.arun())
            try:
                with pytest.raises(ValueError, match="boom"):
                    await asyncio.wait_for(future.async_wait(), timeout=1.0)
                with pytest.raises(ValueError, match="boom"):
                    future.result()
                exc = future.exception()
                assert isinstance(exc, ValueError)
            finally:
                await asyncio.wait_for(runner, timeout=1.0)

        asyncio.run(orchestrate())

    def test_future_is_awaitable(self):
        s = scheduler()
        future: Future[int] = Future()

        async def orchestrate() -> None:
            s.call_later(0.001, future.set_result, 9)
            runner = asyncio.create_task(s.arun())
            try:
                awaited = await asyncio.wait_for(future, timeout=1.0)
                assert awaited == 9
                assert future.result() == 9
            finally:
                await asyncio.wait_for(runner, timeout=1.0)

        asyncio.run(orchestrate())


class TestQueueExamples:
    def test_queue_fifo_order(self):
        q: Queue[int] = Queue()
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)

        assert q.get_nowait() == 1
        assert q.get_nowait() == 2
        assert q.get_nowait() == 3

    def test_queue_nowait_errors(self):
        q: Queue[int] = Queue(maxsize=1)
        with pytest.raises(QueueEmpty):
            q.get_nowait()

        q.put_nowait(1)
        with pytest.raises(QueueFull):
            q.put_nowait(2)

    def test_queue_put_get_with_scheduler_blocking(self):
        s = scheduler()
        q: Queue[int] = Queue(maxsize=1)
        seen: list[str] = []

        def producer() -> None:
            q.sync_put(1)
            seen.append("put:1")
            q.sync_put(2)
            seen.append("put:2")

        def consumer() -> None:
            s.yield_()
            seen.append(f"get:{q.sync_get()}")
            s.yield_()
            seen.append(f"get:{q.sync_get()}")

        s.spawn(producer)
        s.spawn(consumer)
        s.run()

        assert seen == ["put:1", "get:1", "put:2", "get:2"]

    def test_queue_join_and_task_done(self):
        s = scheduler()
        q: Queue[int] = Queue()
        produced_evt = Event()
        seen: list[str] = []

        def producer() -> None:
            # Let other spawned tasks start so producer exit does not try
            # to hand off directly to an unstarted tealet.
            s.yield_()
            q.sync_put(1)
            q.sync_put(2)
            seen.append("produced")
            produced_evt.set()

        def consumer() -> None:
            s.yield_()
            q.sync_get()
            q.task_done()
            seen.append("done:1")
            q.sync_get()
            q.task_done()
            seen.append("done:2")

        def waiter() -> None:
            produced_evt.wait()
            q.sync_join()
            seen.append("joined")

        s.spawn(producer)
        s.spawn(consumer)
        s.spawn(waiter)
        s.run()

        assert seen == ["produced", "done:1", "done:2", "joined"]

    def test_queue_task_done_underflow_raises(self):
        q: Queue[int] = Queue()
        with pytest.raises(ValueError, match=r"task_done\(\) called too many times"):
            q.task_done()

    def test_queue_asyncio_put_get(self):
        q: Queue[int] = Queue(maxsize=1)
        seen: list[str] = []

        async def producer() -> None:
            await q.put(1)
            seen.append("put:1")
            await q.put(2)
            seen.append("put:2")

        async def consumer() -> None:
            await asyncio.sleep(0)
            seen.append(f"get:{await q.get()}")
            await asyncio.sleep(0)
            seen.append(f"get:{await q.get()}")

        async def run() -> None:
            await asyncio.gather(producer(), consumer())

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))
        assert seen == ["put:1", "get:1", "put:2", "get:2"]

    def test_queue_asyncio_join(self):
        q: Queue[int] = Queue()
        seen: list[str] = []

        async def producer() -> None:
            await q.put(1)
            await q.put(2)
            seen.append("produced")

        async def consumer() -> None:
            await asyncio.sleep(0)
            await q.get()
            q.task_done()
            seen.append("done:1")
            await asyncio.sleep(0)
            await q.get()
            q.task_done()
            seen.append("done:2")

        async def waiter() -> None:
            await q.join()
            seen.append("joined")

        async def run() -> None:
            await asyncio.gather(producer(), consumer(), waiter())

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))
        assert seen == ["produced", "done:1", "done:2", "joined"]

    def test_priority_queue_order(self):
        q: PriorityQueue[tuple[int, str]] = PriorityQueue()
        q.put_nowait((2, "b"))
        q.put_nowait((1, "a"))
        q.put_nowait((3, "c"))

        assert q.get_nowait() == (1, "a")
        assert q.get_nowait() == (2, "b")
        assert q.get_nowait() == (3, "c")

    def test_lifo_queue_order(self):
        q: LifoQueue[int] = LifoQueue()
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)

        assert q.get_nowait() == 3
        assert q.get_nowait() == 2
        assert q.get_nowait() == 1


class TestChannelExamples:
    def test_channel_balance_tracks_waiting_senders(self):
        s = scheduler()
        ch = Channel()
        seen: list[str] = []

        def sender() -> None:
            seen.append("sender:before")
            ch.send(7)
            seen.append("sender:after")

        s.spawn(sender)
        s.pump(1)

        assert ch.balance == 1

        def receiver() -> None:
            seen.append(f"receiver:{ch.receive()}")

        s.spawn(receiver)
        s.run()

        assert ch.balance == 0
        assert seen == ["sender:before", "receiver:7", "sender:after"]

    def test_channel_balance_tracks_waiting_receivers(self):
        s = scheduler()
        ch = Channel()
        seen: list[str] = []

        def receiver() -> None:
            seen.append("receiver:before")
            seen.append(f"receiver:{ch.receive()}")

        s.spawn(receiver)
        s.pump(1)

        assert ch.balance == -1

        def sender() -> None:
            ch.send(11)
            seen.append("sender:after")

        s.spawn(sender)
        s.run()

        assert ch.balance == 0
        assert seen == ["receiver:before", "receiver:11", "sender:after"]

    def test_channel_preference_sender(self):
        s = scheduler()
        ch = Channel(preference=1)
        seen: list[str] = []

        def receiver() -> None:
            seen.append("receiver:before")
            seen.append(f"receiver:{ch.receive()}")

        def sender() -> None:
            ch.send(3)
            seen.append("sender:after")

        s.spawn(receiver)
        s.spawn(sender)
        s.run()

        assert seen == ["receiver:before", "sender:after", "receiver:3"]

    def test_channel_preference_validation(self):
        with pytest.raises(ValueError, match="preference must be -1, 0, or 1"):
            Channel(preference=2)

    def test_channel_send_exception(self):
        s = scheduler()
        ch = Channel()
        seen: list[str] = []

        def receiver() -> None:
            try:
                ch.receive()
            except ValueError as exc:
                seen.append(f"caught:{exc}")

        def sender() -> None:
            ch.send_exception(ValueError("boom"))

        s.spawn(receiver)
        s.spawn(sender)
        s.run()

        assert seen == ["caught:boom"]

    def test_channel_send_exception_requires_instance(self):
        ch = Channel()
        with pytest.raises(TypeError, match="BaseException instance"):
            ch.send_exception(ValueError)  # type: ignore[arg-type]

    def test_channel_async_send_wakes_tealet_non_immediate(self):
        s = scheduler()
        ch = Channel(preference=-1)
        seen: list[str] = []

        def receiver() -> None:
            seen.append("receiver:before")
            seen.append(f"receiver:{ch.receive()}")

        s.spawn(receiver)
        s.pump(1)
        assert ch.balance == -1

        asyncio.run(asyncio.wait_for(ch.async_send(9), timeout=1.0))
        assert seen == ["receiver:before"]

        s.run()
        assert seen == ["receiver:before", "receiver:9"]

    def test_channel_async_receive_wakes_tealet_non_immediate(self):
        s = scheduler()
        ch = Channel(preference=1)
        seen: list[str] = []

        def sender() -> None:
            seen.append("sender:before")
            ch.send(4)
            seen.append("sender:after")

        s.spawn(sender)
        s.pump(1)
        assert ch.balance == 1

        value = asyncio.run(asyncio.wait_for(ch.async_receive(), timeout=1.0))
        assert value == 4
        assert seen == ["sender:before"]

        s.run()
        assert seen == ["sender:before", "sender:after"]

    def test_channel_async_sender_and_receiver_pair(self):
        ch = Channel()

        async def run() -> None:
            recv_task = asyncio.create_task(ch.async_receive())
            await asyncio.sleep(0)
            await ch.async_send(12)
            got = await asyncio.wait_for(recv_task, timeout=1.0)
            assert got == 12

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))

    def test_channel_async_receive_cancelled_with_pending_packet_delivers(self):
        ch = Channel()

        async def run() -> None:
            recv_task = asyncio.create_task(ch.async_receive())
            await asyncio.sleep(0)

            # Queue payload first, then cancel before receiver resumes.
            await ch.async_send(None)
            recv_task.cancel()

            got = await asyncio.wait_for(recv_task, timeout=1.0)
            assert got is None

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))

    def test_channel_async_receive_cancelled_without_packet_propagates(self):
        ch = Channel()

        async def run() -> None:
            recv_task = asyncio.create_task(ch.async_receive())
            await asyncio.sleep(0)
            recv_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await recv_task

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))

    def test_channel_send_raw_timeout_suppressed_when_packet_already_consumed(self):
        s = scheduler()
        ch = Channel(preference=0)
        seen: list[object] = []

        def sender() -> None:
            try:
                ch.send(5)
                seen.append("send:ok")
            except BaseException as exc:
                seen.append(type(exc).__name__)

        sender_task = s.spawn(sender)
        s.pump(1)
        assert ch.balance == 1

        # Receiver consumes the packet first; timeout throw races after.
        s.call_soon(ch.receive)
        s.call_soon(sender_task.throw, RawTimeoutError())
        s.run()

        assert seen == ["send:ok"]
        assert ch.balance == 0

    def test_channel_async_send_cancelled_with_consumed_packet_returns(self):
        ch = Channel()

        async def run() -> None:
            send_task = asyncio.create_task(ch.async_send(None))
            await asyncio.sleep(0)

            # Consume payload first, then race cancellation against sender wake.
            got = await ch.async_receive()
            assert got is None
            send_task.cancel()

            await asyncio.wait_for(send_task, timeout=1.0)

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))

    def test_channel_receive_external_exception_drops_pending_packet(self):
        s = scheduler()
        ch = Channel(preference=0)
        seen: list[str] = []

        def receiver() -> None:
            try:
                ch.receive()
            except RuntimeError as exc:
                seen.append(f"receiver:exc:{exc}")

        receiver_task = s.spawn(receiver)
        s.pump(1)
        assert ch.balance == -1

        s.call_soon(ch.send, 42)
        s.call_soon(receiver_task.throw, RuntimeError("interrupt"))
        s.run()

        assert "receiver:exc:interrupt" in seen
        assert ch.balance == 0

        # The pending packet must have been discarded with the external wake.
        got: list[int] = []

        def receiver2() -> None:
            got.append(ch.receive())

        s.spawn(receiver2)
        s.pump(1)
        assert ch.balance == -1

        s.spawn(lambda: ch.send(99))
        s.run()
        assert got == [99]

    def test_channel_receive_raw_timeout_suppressed_when_packet_already_delivered(self):
        s = scheduler()
        ch = Channel(preference=0)
        seen: list[object] = []

        def receiver() -> None:
            try:
                seen.append(ch.receive())
            except BaseException as exc:
                seen.append(type(exc).__name__)

        receiver_task = s.spawn(receiver)
        s.pump(1)
        assert ch.balance == -1

        # Sender callback runs first and delivers packet; timeout throw races after.
        # Use None payload to ensure packet existence check does not treat None
        # as "missing".
        s.call_soon(ch.send, None)
        s.call_soon(receiver_task.throw, RawTimeoutError())
        s.run()

        assert seen == [None]
        assert ch.balance == 0
