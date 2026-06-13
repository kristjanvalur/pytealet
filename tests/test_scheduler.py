import asyncio

import pytest

import _tealet
from tealet.scheduler import (
    Barrier,
    BoundedSemaphore,
    CancelledError,
    Condition,
    Event,
    Future,
    InvalidStateError,
    LifoQueue,
    Lock,
    PriorityQueue,
    Queue,
    QueueEmpty,
    QueueFull,
    ScheduledTealet,
    Semaphore,
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
    try:
        yield
    finally:
        _scheduler.instance = SimpleScheduler()


class TestSchedulerExamples:
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
        target_ref: dict[str, ScheduledTealet] = {}

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
        target_ref: dict[str, ScheduledTealet] = {}

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
        assert future.result() == 123
        assert future.exception() is None

        with pytest.raises(InvalidStateError):
            future.set_result(456)

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
                await asyncio.wait_for(future.async_wait(), timeout=1.0)
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
                assert awaited is None
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
