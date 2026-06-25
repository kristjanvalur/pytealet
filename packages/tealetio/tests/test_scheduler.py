import asyncio
import concurrent.futures
import contextvars
import gc
import selectors
import socket
import threading
import weakref

import pytest

import _tealet
import tealetio.scheduler as scheduler_module
from tealetio import (
    ALL_COMPLETED,
    Barrier,
    BoundedSemaphore,
    CancelledError,
    Channel,
    Condition,
    DefaultTaskFactory,
    Event,
    FIRST_COMPLETED,
    FIRST_EXCEPTION,
    Future,
    InvalidStateError,
    LifoQueue,
    Lock,
    PriorityTask,
    PriorityQueue,
    Queue,
    QueueEmpty,
    QueueFull,
    QueueShutDown,
    Scheduler,
    SelectorScheduler,
    Semaphore,
    StubTaskFactory,
    TASK_PRIORITY_CRITICAL,
    TASK_PRIORITY_DEFAULT,
    TASK_PRIORITY_HIGH,
    TASK_PRIORITY_IDLE,
    TASK_PRIORITY_LOW,
    TealetSelectorEventLoop,
    TealetTask,
    TimeoutError,
    AsyncScheduler,
    as_completed,
    ensure_future,
    gather,
    get_current,
    get_running_scheduler,
    get_scheduler,
    set_scheduler,
    shield,
    timeout,
    timeout_at,
    to_thread,
    wait,
    wait_for,
)
from tealetio.locks import (
    RawTimeoutError,
)
from tealetio.scheduler import (
    _scheduler,
)
from tealetio.examples import (
    demo_future_result,
    demo_future_timeout_then_success,
    demo_scheduler_append_with_yield,
    demo_sleep,
    demo_wait_for_event_start,
)


_SELECTOR_TYPES = [
    pytest.param(selector_type, id=name)
    for name in (
        "SelectSelector",
        "PollSelector",
        "EpollSelector",
        "KqueueSelector",
        "DevpollSelector",
    )
    if (selector_type := getattr(selectors, name, None)) is not None
]


@pytest.fixture(autouse=True)
def _reset_scheduler_tls():
    _scheduler.instance = Scheduler()
    try:
        yield
    finally:
        _scheduler.instance = Scheduler()


def _new_scheduler(task_factory_maker=None) -> Scheduler:
    scheduler = Scheduler()
    if task_factory_maker is not None:
        scheduler.set_task_factory(task_factory_maker())
    set_scheduler(scheduler)
    return scheduler


class _PriorityTaskFactory:
    def __init__(self, priorities: list[float] | None = None):
        self._priorities = iter(priorities) if priorities is not None else None

    def __call__(
        self,
        scheduler,
        func,
        *,
        context,
        priority=TASK_PRIORITY_DEFAULT,
        eager_start=None,
    ):
        if self._priorities is not None:
            priority = next(self._priorities)
        task = PriorityTask(scheduler, priority)
        scheduler_module._tasks._prepare_task(task, func, context)
        return task


class TestSchedulerAccessors:
    def test_get_scheduler_does_not_create_when_unbound(self):
        set_scheduler(None)
        with pytest.raises(RuntimeError, match="no current scheduler"):
            get_scheduler()

    def test_get_running_scheduler_does_not_create_when_unbound(self):
        set_scheduler(None)
        with pytest.raises(RuntimeError, match="no running scheduler"):
            get_running_scheduler()

    def test_get_scheduler_returns_bound_scheduler_when_not_running(self):
        s = Scheduler()
        set_scheduler(s)

        assert get_scheduler() is s

    def test_base_and_concrete_scheduler_api_surfaces_are_split(self):
        sync = Scheduler()
        async_ = AsyncScheduler()

        for name in (
            "spawn",
            "await_",
            "runnable_tasks",
            "reschedule",
            "yield_to",
        ):
            assert callable(getattr(sync, name))
            assert callable(getattr(async_, name))
        for name in ("run", "run_forever", "run_until_complete"):
            assert callable(getattr(sync, name))
            assert not hasattr(async_, name)
        for name in ("arun", "arun_forever", "arun_until_complete"):
            assert not hasattr(sync, name)
            assert callable(getattr(async_, name))

    def test_get_running_scheduler_raises_when_not_running(self):
        with pytest.raises(RuntimeError, match="no running scheduler"):
            get_running_scheduler()

    def test_get_running_scheduler_during_run(self):
        s = _new_scheduler()
        set_scheduler(s)
        seen: list[Scheduler] = []

        def check_running() -> None:
            seen.append(get_running_scheduler())

        s.spawn(check_running)
        s.run()

        assert seen == [s]

    def test_get_current_returns_none_outside_tealetio_task(self):
        assert get_current() is None

    def test_get_current_returns_running_tealetio_task(self):
        s = _new_scheduler()
        seen: list[TealetTask | None] = []

        def check_current() -> None:
            seen.append(get_current())

        task = s.spawn(check_current)
        s.run()

        assert seen == [task]

    def test_get_current_returns_none_in_asyncio_task(self):
        async def check_current() -> TealetTask | None:
            return get_current()

        assert asyncio.run(check_current()) is None

    def test_get_current_returns_none_inside_awaited_asyncio_coroutine(self):
        s = AsyncScheduler()
        set_scheduler(s)
        seen: list[tuple[str, TealetTask | None]] = []

        async def check_current() -> str:
            seen.append(("start", get_current()))
            await asyncio.sleep(0)
            seen.append(("after", get_current()))
            return "done"

        def worker() -> None:
            seen.append(("worker", get_current()))
            result = s.await_(check_current())
            seen.append((result, get_current()))

        task = s.spawn(worker)

        async def run_scheduler() -> None:
            await s.arun()

        asyncio.run(run_scheduler())

        assert seen == [
            ("worker", task),
            ("start", None),
            ("after", None),
            ("done", task),
        ]

    def test_runnable_tasks_returns_scheduler_tasks_in_run_order(self):
        s = _new_scheduler()
        set_scheduler(s)

        first = s.spawn(lambda: "first")
        second = s.spawn(lambda: "second")

        assert s.runnable_tasks() == (first, second)

    def test_scheduler_accepts_runnable_queue_factory(self):
        events: list[str] = []

        class RecordingQueue(scheduler_module._PrescheduledRunnableQueue):
            def __init__(self) -> None:
                events.append("init")
                super().__init__()

            def add(self, task):
                events.append("add")
                return super().add(task)

            def pop_next(self):
                events.append("pop")
                return super().pop_next()

        s = Scheduler(runnable_queue_factory=RecordingQueue)
        set_scheduler(s)
        seen: list[str] = []

        s.spawn(lambda: seen.append("task"))
        s.run()

        assert seen == ["task"]
        assert events[0] == "init"
        assert "add" in events
        assert "pop" in events

    def test_reschedule_moves_runnable_task_to_immediate_position(self):
        s = _new_scheduler()
        set_scheduler(s)
        seen: list[str] = []

        first = s.spawn(lambda: seen.append("first"))
        second = s.spawn(lambda: seen.append("second"))
        third = s.spawn(lambda: seen.append("third"))

        s.reschedule(third, position=0)
        assert s.runnable_tasks() == (third, first, second)

        s.run()

        assert seen == ["third", "first", "second"]

    def test_reschedule_negative_position_counts_from_immediate_lane_end(self):
        s = _new_scheduler()
        set_scheduler(s)
        seen: list[str] = []

        first = s.spawn(lambda: seen.append("first"))
        second = s.spawn(lambda: seen.append("second"))
        third = s.spawn(lambda: seen.append("third"))

        s.reschedule(first, position=0)
        s.reschedule(second, position=-1)
        s.reschedule(third, position=-2)
        assert s.runnable_tasks() == (first, third, second)

        s.run()

        assert seen == ["first", "third", "second"]

    def test_reschedule_none_moves_task_to_default_queue_position(self):
        s = _new_scheduler()
        set_scheduler(s)

        current = s.spawn(lambda: "current")
        target = s.spawn(lambda: "target")
        later = s.spawn(lambda: "later")

        s._runnable.yield_to(target, current, 0)
        assert s.runnable_tasks() == (target, current, later)

        s.reschedule(target)

        assert s.runnable_tasks() == (current, later, target)

    def test_priority_runnable_queue_runs_lowest_priority_value_first(self):
        s = Scheduler(runnable_queue_factory=scheduler_module._PriorityRunnableQueue)
        s.set_task_factory(
            _PriorityTaskFactory(
                [TASK_PRIORITY_DEFAULT, TASK_PRIORITY_HIGH, TASK_PRIORITY_HIGH / 2]
            )
        )
        set_scheduler(s)
        seen: list[str] = []

        first = s.spawn(lambda: seen.append("first"))
        second = s.spawn(lambda: seen.append("second"))
        third = s.spawn(lambda: seen.append("third"))

        assert s.runnable_tasks() == (second, third, first)

        s.run()

        assert seen == ["second", "third", "first"]

    def test_priority_task_values_are_float_priority_bands(self):
        priorities = [
            TASK_PRIORITY_CRITICAL,
            TASK_PRIORITY_HIGH,
            TASK_PRIORITY_DEFAULT,
            TASK_PRIORITY_LOW,
            TASK_PRIORITY_IDLE,
        ]

        assert all(isinstance(priority, float) for priority in priorities)
        assert priorities == [-20.0, -10.0, 0.0, 10.0, 20.0]

    def test_priority_task_reports_active_priority(self):
        s = _new_scheduler()

        task = PriorityTask(s, TASK_PRIORITY_LOW)

        assert task.priority == TASK_PRIORITY_LOW
        assert task.get_active_priority() == TASK_PRIORITY_LOW

    def test_priority_task_factory_accepts_spawn_priority_keyword(self):
        s = Scheduler(runnable_queue_factory=scheduler_module._PriorityRunnableQueue)
        s.set_task_factory(_PriorityTaskFactory())
        set_scheduler(s)
        seen: list[str] = []

        s.spawn(lambda: seen.append("default"))
        s.spawn(lambda: seen.append("early"), priority=TASK_PRIORITY_HIGH)
        s.spawn(lambda: seen.append("middle"), priority=TASK_PRIORITY_HIGH / 2)

        s.run()

        assert seen == ["early", "middle", "default"]

    def test_default_task_factory_rejects_unknown_spawn_keyword(self):
        s = _new_scheduler()
        set_scheduler(s)

        with pytest.raises(TypeError, match="unexpected task factory keyword argument: priority"):
            s.spawn(lambda: "ok", priority=-10)

    def test_priority_runnable_queue_uses_stable_default_priority(self):
        s = Scheduler(runnable_queue_factory=scheduler_module._PriorityRunnableQueue)
        set_scheduler(s)
        seen: list[str] = []

        first = s.spawn(lambda: seen.append("first"))
        second = s.spawn(lambda: seen.append("second"))
        third = s.spawn(lambda: seen.append("third"))

        assert s.runnable_tasks() == (first, second, third)

        s.run()

        assert seen == ["first", "second", "third"]

    def test_priority_runnable_queue_reschedule_none_requeries_priority(self):
        s = Scheduler(runnable_queue_factory=scheduler_module._PriorityRunnableQueue)
        s.set_task_factory(
            _PriorityTaskFactory([TASK_PRIORITY_DEFAULT, TASK_PRIORITY_LOW])
        )
        set_scheduler(s)

        first = s.spawn(lambda: "first")
        second = s.spawn(lambda: "second")
        first.priority = TASK_PRIORITY_CRITICAL

        s.reschedule(first)

        assert s.runnable_tasks() == (first, second)

    def test_priority_runnable_queue_reorders_when_task_is_modified(self):
        s = Scheduler(runnable_queue_factory=scheduler_module._PriorityRunnableQueue)
        s.set_task_factory(
            _PriorityTaskFactory([TASK_PRIORITY_DEFAULT, TASK_PRIORITY_LOW])
        )
        set_scheduler(s)

        first = s.spawn(lambda: "first")
        second = s.spawn(lambda: "second")
        second.priority = TASK_PRIORITY_CRITICAL

        assert s.runnable_tasks() == (second, first)

    def test_priority_runnable_queue_does_not_reorder_prescheduled_modified_task(self):
        s = Scheduler(runnable_queue_factory=scheduler_module._PriorityRunnableQueue)
        s.set_task_factory(
            _PriorityTaskFactory([TASK_PRIORITY_DEFAULT, TASK_PRIORITY_HIGH])
        )
        set_scheduler(s)

        first = s.spawn(lambda: "first")
        second = s.spawn(lambda: "second")
        s.reschedule(first, position=0)
        first.priority = TASK_PRIORITY_CRITICAL

        assert s.runnable_tasks() == (first, second)

    def test_priority_runnable_queue_immediate_lane_beats_priority(self):
        s = Scheduler(runnable_queue_factory=scheduler_module._PriorityRunnableQueue)
        s.set_task_factory(
            _PriorityTaskFactory([TASK_PRIORITY_DEFAULT, TASK_PRIORITY_HIGH])
        )
        set_scheduler(s)
        seen: list[str] = []

        low = s.spawn(lambda: seen.append("low"))
        high = s.spawn(lambda: seen.append("high"))

        s.reschedule(low, position=0)
        assert s.runnable_tasks() == (low, high)

        s.run()

        assert seen == ["low", "high"]

    def test_reschedule_rejects_non_runnable_task(self):
        s = _new_scheduler()
        set_scheduler(s)

        task = s.spawn(lambda: "done")
        assert s.run_until_complete(task) == "done"

        with pytest.raises(ValueError, match="task is not runnable"):
            s.reschedule(task)

    def test_reschedule_rejects_task_from_different_scheduler(self):
        first = _new_scheduler()
        second = Scheduler()
        set_scheduler(first)

        task = second.spawn(lambda: "done")

        with pytest.raises(RuntimeError, match="different scheduler"):
            first.reschedule(task)

    def test_yield_to_default_leaves_current_at_fifo_tail(self):
        s = _new_scheduler()
        set_scheduler(s)
        seen: list[str] = []
        target: TealetTask | None = None

        def current() -> None:
            assert target is not None
            seen.append("current:start")
            s.yield_to(target)
            seen.append("current:after")

        def selected() -> None:
            seen.append("target")

        def later() -> None:
            seen.append("later")

        s.spawn(current)
        target = s.spawn(selected)
        s.spawn(later)
        s.run()

        assert seen == ["current:start", "target", "later", "current:after"]

    def test_yield_to_insert_current_at_zero_places_current_after_target(self):
        s = _new_scheduler()
        set_scheduler(s)
        seen: list[str] = []
        target: TealetTask | None = None

        def current() -> None:
            assert target is not None
            seen.append("current:start")
            s.yield_to(target, insert_current_at=0)
            seen.append("current:after")

        def selected() -> None:
            seen.append("target:start")
            s.yield_()
            seen.append("target:after")

        def later() -> None:
            seen.append("later")

        s.spawn(current)
        target = s.spawn(selected)
        s.spawn(later)
        s.run()

        assert seen == ["current:start", "target:start", "current:after", "later", "target:after"]

    def test_yield_to_insert_current_at_is_after_removed_target(self):
        s = _new_scheduler()
        set_scheduler(s)
        seen: list[str] = []
        target: TealetTask | None = None

        def current() -> None:
            assert target is not None
            seen.append("current:start")
            s.yield_to(target, insert_current_at=1)
            seen.append("current:after")

        def selected() -> None:
            seen.append("target:start")
            s.yield_()
            seen.append("target:after")

        def later() -> None:
            seen.append("later")

        s.spawn(current)
        target = s.spawn(selected)
        s.spawn(later)
        s.run()

        assert seen == ["current:start", "target:start", "current:after", "later", "target:after"]

    def test_yield_to_minus_one_places_current_at_prescheduled_tail(self):
        s = _new_scheduler()
        set_scheduler(s)
        seen: list[str] = []
        target: TealetTask | None = None

        def current() -> None:
            assert target is not None
            seen.append("current:start")
            s.yield_to(target, insert_current_at=-1)
            seen.append("current:after")

        def selected() -> None:
            seen.append("target")

        def later() -> None:
            seen.append("later")

        s.spawn(current)
        target = s.spawn(selected)
        s.spawn(later)
        s.run()

        assert seen == ["current:start", "target", "current:after", "later"]

    def test_yield_to_negative_insert_current_at_counts_from_prescheduled_end(self):
        s = _new_scheduler()
        set_scheduler(s)

        first_current = s.spawn(lambda: "first-current")
        first_target = s.spawn(lambda: "first-target")
        second_current = s.spawn(lambda: "second-current")
        second_target = s.spawn(lambda: "second-target")
        later = s.spawn(lambda: "later")

        s._runnable.yield_to(first_target, first_current, 0)
        s._runnable.yield_to(second_target, second_current, -2)

        assert s.runnable_tasks() == (second_target, first_target, second_current, first_current, later)

    def test_yield_to_rejects_non_runnable_task(self):
        s = _new_scheduler()
        set_scheduler(s)
        seen: list[str] = []

        done = s.spawn(lambda: "done")

        def current() -> None:
            with pytest.raises(ValueError, match="task is not runnable"):
                s.yield_to(done)
            seen.append("current")

        s.spawn(current)
        s.run()

        assert seen == ["current"]

    def test_yield_to_rejects_task_from_different_scheduler(self):
        first = _new_scheduler()
        second = Scheduler()
        set_scheduler(first)
        seen: list[str] = []

        task = second.spawn(lambda: "done")

        def current() -> None:
            with pytest.raises(RuntimeError, match="different scheduler"):
                first.yield_to(task)
            seen.append("current")

        first.spawn(current)
        first.run()

        assert seen == ["current"]

    def test_task_factory_accessors_reset_to_default(self):
        s = _new_scheduler()
        original = s.get_task_factory()
        custom = StubTaskFactory()

        assert isinstance(original, DefaultTaskFactory)
        assert original.eager_start is False
        s.set_task_factory(custom)
        assert s.get_task_factory() is custom

        s.set_task_factory(None)
        assert isinstance(s.get_task_factory(), DefaultTaskFactory)

    def test_spawn_uses_custom_task_factory(self):
        s = _new_scheduler()
        default = DefaultTaskFactory()
        calls = []
        marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker")

        class RecordingTaskFactory:
            def __call__(self, scheduler, func, *, context, eager_start=None, **kwargs):
                calls.append((scheduler, context.get(marker), eager_start, kwargs))
                return default(scheduler, func, context=context, eager_start=eager_start)

        marker.set("factory-context")
        s.set_task_factory(RecordingTaskFactory())

        task = s.spawn(lambda: "ok", priority=-10)

        assert s.run_until_complete(task) == "ok"
        assert calls == [(s, "factory-context", None, {"priority": -10})]

    def test_default_task_factory_eager_defers_until_scheduler_runs(self):
        s = _new_scheduler()
        seen = []
        s.set_task_factory(DefaultTaskFactory(eager_start=True))

        seen.append("before-spawn")
        task = s.spawn(lambda: seen.append("ran") or "ok")
        seen.append("after-spawn")

        assert seen == ["before-spawn", "after-spawn"]
        assert task.done() is False
        assert s.run_until_complete(task) == "ok"
        assert seen == ["before-spawn", "after-spawn", "ran"]
        assert s.all_tasks() == set()

    def test_default_task_factory_eager_runs_before_spawn_returns_inside_scheduler(self):
        s = _new_scheduler()
        seen = []
        s.set_task_factory(DefaultTaskFactory(eager_start=True))

        def parent() -> None:
            seen.append("before-spawn")
            task = s.spawn(lambda: seen.append("ran") or "ok")
            seen.append("after-spawn")
            assert task.done()
            assert task.result() == "ok"

        parent_task = s.spawn(parent)
        s.run_until_complete(parent_task)

        assert seen == ["before-spawn", "ran", "after-spawn"]

    def test_spawn_eager_true_defers_until_scheduler_runs(self):
        s = _new_scheduler()
        seen = []

        seen.append("before-spawn")
        task = s.spawn(lambda: seen.append("ran") or "ok", eager_start=True)
        seen.append("after-spawn")

        assert seen == ["before-spawn", "after-spawn"]
        assert task.done() is False
        assert s.run_until_complete(task) == "ok"
        assert seen == ["before-spawn", "after-spawn", "ran"]

    def test_spawn_eager_true_runs_before_spawn_returns_inside_scheduler(self):
        s = _new_scheduler()
        seen = []

        def parent() -> None:
            seen.append("before-spawn")
            task = s.spawn(lambda: seen.append("ran") or "ok", eager_start=True)
            seen.append("after-spawn")
            assert task.done()
            assert task.result() == "ok"

        parent_task = s.spawn(parent)
        s.run_until_complete(parent_task)

        assert seen == ["before-spawn", "ran", "after-spawn"]

    def test_spawn_eager_overrides_factory_default(self):
        s = _new_scheduler()
        seen = []
        s.set_task_factory(DefaultTaskFactory(eager_start=True))

        deferred = s.spawn(lambda: seen.append("deferred") or "deferred", eager_start=False)

        assert seen == []
        assert deferred.done() is False
        assert s.run_until_complete(deferred) == "deferred"
        assert seen == ["deferred"]

        def parent() -> None:
            task = s.spawn(lambda: seen.append("eager") or "eager", eager_start=True)
            assert task.done()
            assert task.result() == "eager"

        parent_task = s.spawn(parent, eager_start=False)
        assert seen == ["deferred"]
        assert s.run_until_complete(parent_task) is None
        assert seen == ["deferred", "eager"]

    def test_eager_task_that_yields_is_scheduled(self):
        s = _new_scheduler()
        seen = []

        def worker() -> str:
            seen.append("start")
            s.yield_()
            seen.append("after-yield")
            return "ok"

        def parent() -> None:
            task = s.spawn(worker, eager_start=True)
            assert seen == ["start"]
            assert task.done() is False

        parent_task = s.spawn(parent)
        s.run_until_complete(parent_task)

        assert seen == ["start", "after-yield"]

    def test_stub_task_factory_lazily_creates_and_reuses_stub(self):
        s = _new_scheduler()
        factory = StubTaskFactory()
        s.set_task_factory(factory)

        first = s.spawn(lambda: "first")
        stub = factory.stub
        second = s.spawn(lambda: "second")

        assert stub is not None
        assert stub.state == _tealet.STATE_STUB
        assert factory.stub is stub
        assert s.run_until_complete(first) == "first"
        assert s.run_until_complete(second) == "second"

    def test_stub_task_factory_eager_runs_before_spawn_returns_inside_scheduler(self):
        s = _new_scheduler()
        seen = []
        factory = StubTaskFactory(eager_start=True)
        s.set_task_factory(factory)

        def parent() -> None:
            task = s.spawn(lambda: seen.append("ran") or "ok")
            assert task.done()
            assert task.result() == "ok"

        parent_task = s.spawn(parent)
        s.run_until_complete(parent_task)

        assert factory.stub is not None
        assert seen == ["ran"]

    def test_stub_task_factory_can_stub_here_before_use(self):
        s = _new_scheduler()
        factory = StubTaskFactory()
        stub = factory.stub_here()
        s.set_task_factory(factory)

        task = s.spawn(lambda: "ok")

        assert factory.stub is stub
        assert stub.state == _tealet.STATE_STUB
        assert s.run_until_complete(task) == "ok"

    def test_stub_task_factory_preserves_context_and_exceptions(self):
        s = _new_scheduler()
        factory = StubTaskFactory()
        marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker")
        context = contextvars.Context()
        context.run(marker.set, "stub-context")
        s.set_task_factory(factory)

        good = s.spawn(lambda: marker.get(), context=context)

        def fail() -> object:
            raise ValueError("factory boom")

        bad = s.spawn(fail)

        assert s.run_until_complete(good) == "stub-context"
        with pytest.raises(ValueError, match="factory boom"):
            s.run_until_complete(bad)

    def test_async_scheduler_uses_stub_task_factory(self):
        async def run() -> None:
            s = AsyncScheduler()
            set_scheduler(s)
            factory = StubTaskFactory()
            s.set_task_factory(factory)

            task = s.spawn(lambda: "ok")

            assert factory.stub is not None
            assert await s.arun_until_complete(task) == "ok"

        asyncio.run(run())

    def test_get_running_scheduler_during_arun(self):
        s = AsyncScheduler()
        set_scheduler(s)
        seen: list[AsyncScheduler] = []

        def check_running() -> None:
            seen.append(get_running_scheduler())

        async def run() -> None:
            s.spawn(check_running)
            await s.arun()

        asyncio.run(run())

        assert seen == [s]

    def test_run_in_executor_waits_for_result(self, deferred_scheduler_task_factory_maker):
        s = _new_scheduler(deferred_scheduler_task_factory_maker)

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            def entry() -> int:
                future = get_running_scheduler().run_in_executor(pool, lambda: 42)
                return future.wait()

            task = s.spawn(entry)
            s.run_until_complete(task)

        assert task.result() == 42

    def test_selector_scheduler_run_in_executor_keeps_driver_alive(self):
        s = SelectorScheduler()
        set_scheduler(s)

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                def entry() -> int:
                    future = get_running_scheduler().run_in_executor(pool, lambda: 42)
                    return future.wait()

                task = s.spawn(entry)
                assert s.run_until_complete(task) == 42
        finally:
            s.close()

    def test_run_in_executor_propagates_exception(self, deferred_scheduler_task_factory_maker):
        s = _new_scheduler(deferred_scheduler_task_factory_maker)

        def fail() -> None:
            raise ValueError("boom")

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            def entry() -> None:
                future = get_running_scheduler().run_in_executor(pool, fail)
                with pytest.raises(ValueError, match="boom"):
                    future.wait()

            task = s.spawn(entry)
            s.run_until_complete(task)

        assert task.done() is True
        assert task.result() is None

    def test_run_in_executor_ignores_late_result_after_cancel(self, deferred_scheduler_task_factory_maker):
        s = _new_scheduler(deferred_scheduler_task_factory_maker)
        release = threading.Event()
        worker_started = threading.Event()

        def worker() -> str:
            worker_started.set()
            release.wait(timeout=1.0)
            return "late"

        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)

        def entry() -> None:
            future = s.run_in_executor(pool, worker)
            assert worker_started.wait(timeout=1.0) is True
            assert future.cancel() is True
            release.set()
            pool.shutdown(wait=True)
            get_running_scheduler().yield_()

            assert future.cancelled() is True
            with pytest.raises(CancelledError):
                future.result()

        task = s.spawn(entry)
        s.run_until_complete(task)

        assert task.result() is None

    def test_shutdown_default_executor_without_executor_completes_immediately(self):
        s = _new_scheduler()

        shutdown = s.shutdown_default_executor()

        assert isinstance(shutdown, Future)
        assert shutdown.done() is True
        assert shutdown.result() is None

    def test_shutdown_default_executor_waits_for_default_executor(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        release = threading.Event()
        worker_started = threading.Event()

        def worker() -> str:
            worker_started.set()
            release.wait(timeout=1.0)
            return "done"

        work_future = s.run_in_executor(None, worker)
        assert worker_started.wait(timeout=1.0) is True

        shutdown = s.shutdown_default_executor()
        assert shutdown.done() is False

        release.set()
        assert s.run_until_complete(shutdown) is None
        assert work_future.done() is True
        assert work_future.result() == "done"

    def test_shutdown_default_executor_timeout_warns_and_completes(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        release = threading.Event()
        worker_started = threading.Event()

        def worker() -> str:
            worker_started.set()
            release.wait(timeout=1.0)
            return "done"

        work_future = s.run_in_executor(None, worker)
        assert worker_started.wait(timeout=1.0) is True

        with pytest.warns(RuntimeWarning, match="did not finish joining"):
            shutdown = s.shutdown_default_executor(timeout=0.001)
            assert shutdown.done() is False
            assert s.run_until_complete(shutdown) is None

        assert work_future.done() is False

        release.set()
        assert s.run_until_complete(work_future) == "done"

    def test_to_thread_waits_and_preserves_context(self, deferred_scheduler_task_factory_maker):
        marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker", default="default")
        s = _new_scheduler(deferred_scheduler_task_factory_maker)

        def entry() -> str:
            marker.set("tealet-context")
            return to_thread(marker.get)

        task = s.spawn(entry)
        s.run()

        assert task.result() == "tealet-context"

    def test_run_in_executor_works_with_async_scheduler_driver(self):
        s = AsyncScheduler()
        set_scheduler(s)

        async def run_case() -> None:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                def entry() -> int:
                    future = get_running_scheduler().run_in_executor(pool, lambda: 7)
                    return future.wait()

                task = s.spawn(entry)
                assert await s.arun_until_complete(task) == 7

        asyncio.run(run_case())

    def test_event_wait_handles_set_during_schedule_before_link(self):
        s = _new_scheduler()
        event = Event()
        seen: list[str] = []

        def entry() -> None:
            s.call_soon(event.set)
            assert event.swait() is True
            seen.append("resumed")

        task = s.spawn(entry)
        s.run_until_complete(task)

        assert seen == ["resumed"]

    def test_selector_scheduler_wait_readable(self):
        s = SelectorScheduler()
        set_scheduler(s)

        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            states: list[tuple[str, bool, bool]] = []

            def read_one() -> bytes:
                states.append(("before", task.is_waiting(), task.is_runnable()))
                s.wait_readable(reader)
                states.append(("after", task.is_waiting(), task.is_runnable()))
                return reader.recv(1)

            def write_one() -> None:
                s.sleep(0.001)
                assert task.link is not None
                assert task.link is not s
                states.append(("during", task.is_waiting(), task.is_runnable()))
                writer.send(b"x")

            task = s.spawn(read_one)
            s.spawn(write_one)

            assert s.run_until_complete(task) == b"x"
            assert states == [
                ("before", False, False),
                ("during", True, False),
                ("after", False, False),
            ]
        finally:
            reader.close()
            writer.close()
            s.close()

    def test_selector_scheduler_wait_writable(self):
        s = SelectorScheduler()
        set_scheduler(s)

        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            payload = b"x" * 4096
            while True:
                try:
                    writer.send(payload)
                except BlockingIOError:
                    break

            def write_when_ready() -> int:
                s.wait_writable(writer)
                return writer.send(b"z")

            def drain() -> None:
                s.sleep(0.001)
                while True:
                    try:
                        if not reader.recv(65536):
                            return
                    except BlockingIOError:
                        return

            task = s.spawn(write_when_ready)
            s.spawn(drain)

            assert s.run_until_complete(task) == 1
        finally:
            reader.close()
            writer.close()
            s.close()

    def test_selector_scheduler_wait_readable_timeout_removes_callback(self):
        s = SelectorScheduler()
        set_scheduler(s)

        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            def wait_with_timeout() -> bool:
                with pytest.raises(TimeoutError):
                    with timeout(0.001):
                        s.wait_readable(reader)
                return s.remove_reader(reader.fileno())

            task = s.spawn(wait_with_timeout)

            assert s.run_until_complete(task) is False
        finally:
            reader.close()
            writer.close()
            s.close()

    def test_selector_scheduler_sock_recv_and_sendall(self):
        s = SelectorScheduler()
        set_scheduler(s)

        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            def receive() -> bytes:
                return s.sock_recv(reader, 5)

            def send() -> None:
                s.sleep(0.001)
                s.sock_sendall(writer, b"hello")

            task = s.spawn(receive)
            s.spawn(send)

            assert s.run_until_complete(task) == b"hello"
        finally:
            reader.close()
            writer.close()
            s.close()

    def test_selector_scheduler_sock_recv_into(self):
        s = SelectorScheduler()
        set_scheduler(s)

        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            buf = bytearray(5)

            def receive() -> int:
                return s.sock_recv_into(reader, buf)

            def send() -> None:
                s.sleep(0.001)
                s.sock_sendall(writer, b"world")

            task = s.spawn(receive)
            s.spawn(send)

            assert s.run_until_complete(task) == 5
            assert bytes(buf) == b"world"
        finally:
            reader.close()
            writer.close()
            s.close()

    def test_selector_scheduler_sock_accept_connect(self):
        s = SelectorScheduler()
        set_scheduler(s)

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server.setblocking(False)
            client.setblocking(False)
            server.bind(("127.0.0.1", 0))
            server.listen()

            def accept_and_read() -> bytes:
                conn, _address = s.sock_accept(server)
                try:
                    return s.sock_recv(conn, 4)
                finally:
                    conn.close()

            def connect_and_send() -> None:
                s.sock_connect(client, server.getsockname())
                s.sock_sendall(client, b"ping")

            task = s.spawn(accept_and_read)
            s.spawn(connect_and_send)

            assert s.run_until_complete(task) == b"ping"
        finally:
            client.close()
            server.close()
            s.close()

    def test_selector_scheduler_sock_datagram_helpers(self):
        s = SelectorScheduler()
        set_scheduler(s)

        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            receiver.setblocking(False)
            sender.setblocking(False)
            receiver.bind(("127.0.0.1", 0))
            buf = bytearray(5)

            def receive() -> tuple[int, object]:
                return s.sock_recvfrom_into(receiver, buf)

            def send() -> int:
                s.sleep(0.001)
                return s.sock_sendto(sender, b"hello", receiver.getsockname())

            receive_task = s.spawn(receive)
            send_task = s.spawn(send)

            result = s.run_until_complete(receive_task)
            assert isinstance(result, tuple)
            count, address = result
            assert count == 5
            assert bytes(buf) == b"hello"
            assert address[1] == sender.getsockname()[1]
            assert send_task.result() == 5

            def receive_bytes() -> tuple[bytes, object]:
                return s.sock_recvfrom(receiver, 5)

            def send_bytes() -> int:
                s.sleep(0.001)
                return s.sock_sendto(sender, b"again", receiver.getsockname())

            receive_bytes_task = s.spawn(receive_bytes)
            send_bytes_task = s.spawn(send_bytes)

            result = s.run_until_complete(receive_bytes_task)
            assert isinstance(result, tuple)
            data, address = result
            assert data == b"again"
            assert address[1] == sender.getsockname()[1]
            assert send_bytes_task.result() == 5
        finally:
            sender.close()
            receiver.close()
            s.close()

    def test_scheduler_socket_helpers_require_selector_scheduler(self):
        s = _new_scheduler()
        reader, _writer = socket.socketpair()
        try:
            reader.setblocking(False)
            with pytest.raises(NotImplementedError, match="IO-capable scheduler"):
                s.sock_recv(reader, 1)
        finally:
            reader.close()
            _writer.close()

    def test_scheduler_io_callbacks_require_io_capable_scheduler(self):
        s = _new_scheduler()
        reader, _writer = socket.socketpair()
        try:
            with pytest.raises(NotImplementedError, match="reader callbacks"):
                s.add_reader(reader.fileno(), lambda: None)
            with pytest.raises(NotImplementedError, match="reader callbacks"):
                s.remove_reader(reader.fileno())
            with pytest.raises(NotImplementedError, match="writer callbacks"):
                s.add_writer(reader.fileno(), lambda: None)
            with pytest.raises(NotImplementedError, match="writer callbacks"):
                s.remove_writer(reader.fileno())
        finally:
            reader.close()
            _writer.close()

    def test_selector_scheduler_reader_writer_callbacks(self):
        s = SelectorScheduler()
        set_scheduler(s)

        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            readable = Future[bytes]()
            writable = Future[str]()

            def on_readable() -> None:
                assert s.remove_reader(reader.fileno()) is True
                readable.set_result(reader.recv(5))

            def on_writable() -> None:
                assert s.remove_writer(writer.fileno()) is True
                writable.set_result("writable")

            def send_later() -> None:
                s.sleep(0.001)
                writer.send(b"hello")

            s.add_reader(reader.fileno(), on_readable)
            s.spawn(send_later)
            assert s.run_until_complete(readable) == b"hello"

            s.add_writer(writer.fileno(), on_writable)
            assert s.run_until_complete(writable) == "writable"
            assert s.remove_reader(reader.fileno()) is False
            assert s.remove_writer(writer.fileno()) is False
        finally:
            reader.close()
            writer.close()
            s.close()

    @pytest.mark.parametrize("selector_type", _SELECTOR_TYPES)
    def test_selector_scheduler_waits_for_socket_io_with_selector_type(self, selector_type):
        selector = selector_type()
        s = SelectorScheduler(selector=selector)
        set_scheduler(s)

        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            def receive() -> bytes:
                s.wait_readable(reader)
                return reader.recv(5)

            def send_later() -> int:
                s.sleep(0.001)
                return writer.send(b"hello")

            receive_task = s.spawn(receive)
            send_task = s.spawn(send_later)
            assert s.run_until_complete(receive_task) == b"hello"
            assert send_task.result() == 5

            def write_when_ready() -> int:
                s.wait_writable(writer)
                return writer.send(b"x")

            write_task = s.spawn(write_when_ready)
            assert s.run_until_complete(write_task) == 1
            assert reader.recv(1) == b"x"
        finally:
            reader.close()
            writer.close()
            s.close()

    @pytest.mark.parametrize("selector_type", _SELECTOR_TYPES)
    def test_selector_scheduler_callbacks_handle_socket_io_with_selector_type(self, selector_type):
        selector = selector_type()
        s = SelectorScheduler(selector=selector)
        set_scheduler(s)

        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            readable = Future[bytes]()
            writable = Future[int]()

            def on_readable() -> None:
                assert s.remove_reader(reader.fileno()) is True
                readable.set_result(reader.recv(5))

            def on_writable() -> None:
                assert s.remove_writer(writer.fileno()) is True
                writable.set_result(writer.send(b"x"))

            def send_later() -> int:
                s.sleep(0.001)
                return writer.send(b"hello")

            s.add_reader(reader.fileno(), on_readable)
            send_task = s.spawn(send_later)
            assert s.run_until_complete(readable) == b"hello"
            assert send_task.result() == 5

            s.add_writer(writer.fileno(), on_writable)
            assert s.run_until_complete(writable) == 1
            assert reader.recv(1) == b"x"
        finally:
            reader.close()
            writer.close()
            s.close()

    def test_selector_scheduler_uses_provided_selector(self):
        selector = selectors.SelectSelector()
        s = SelectorScheduler(selector=selector)
        set_scheduler(s)

        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            s.add_reader(reader.fileno(), lambda: None)
            assert selector.get_key(reader.fileno()).events == selectors.EVENT_READ

            s.add_writer(reader.fileno(), lambda: None)
            assert selector.get_key(reader.fileno()).events == (selectors.EVENT_READ | selectors.EVENT_WRITE)

            assert s.remove_reader(reader.fileno()) is True
            assert selector.get_key(reader.fileno()).events == selectors.EVENT_WRITE

            assert s.remove_writer(reader.fileno()) is True
            with pytest.raises(KeyError):
                selector.get_key(reader.fileno())
        finally:
            reader.close()
            writer.close()
            s.close()

    def test_selector_scheduler_remove_callbacks_wakes_selector(self, monkeypatch):
        s = SelectorScheduler()
        set_scheduler(s)

        reader, writer = socket.socketpair()
        try:
            wakeups: list[str] = []

            def wake_selector() -> None:
                wakeups.append("wake")

            monkeypatch.setattr(s, "_wake_selector", wake_selector)

            s.add_reader(reader.fileno(), lambda: None)
            wakeups.clear()
            assert s.remove_reader(reader.fileno()) is True
            assert wakeups == ["wake"]

            s.add_writer(writer.fileno(), lambda: None)
            wakeups.clear()
            assert s.remove_writer(writer.fileno()) is True
            assert wakeups == ["wake"]
        finally:
            reader.close()
            writer.close()
            s.close()

    def test_selector_scheduler_reader_writer_callbacks_share_fd_entry(self):
        s = SelectorScheduler()
        set_scheduler(s)

        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            seen: list[str] = []

            def on_writable() -> None:
                seen.append("writable")
                assert s.remove_writer(reader.fileno()) is True
                s.stop()

            s.add_reader(reader.fileno(), lambda: seen.append("readable"))
            s.add_writer(reader.fileno(), on_writable)

            assert s.remove_reader(reader.fileno()) is True
            assert s.remove_reader(reader.fileno()) is False

            writer.send(b"x")
            s.run_forever()

            assert seen == ["writable"]
            assert s.remove_writer(reader.fileno()) is False
        finally:
            reader.close()
            writer.close()
            s.close()

    def test_tealet_selector_event_loop_runs_asyncio_timer(self):
        s = SelectorScheduler()
        set_scheduler(s)

        def run_asyncio() -> str:
            loop = TealetSelectorEventLoop(s)
            try:
                asyncio.set_event_loop(loop)

                async def main() -> str:
                    await asyncio.sleep(0.001)
                    return "done"

                return loop.run_until_complete(main())
            finally:
                asyncio.set_event_loop(None)
                loop.close()

        try:
            assert s.run_until_complete(run_asyncio) == "done"
        finally:
            s.close()

    def test_tealet_selector_event_loop_runs_asyncio_socket_recv(self):
        s = SelectorScheduler()
        set_scheduler(s)

        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            def run_asyncio() -> bytes:
                loop = TealetSelectorEventLoop(s)
                try:
                    asyncio.set_event_loop(loop)

                    async def main() -> bytes:
                        loop.call_later(0.001, writer.send, b"hello")
                        return await loop.sock_recv(reader, 5)

                    return loop.run_until_complete(main())
                finally:
                    asyncio.set_event_loop(None)
                    loop.close()

            assert s.run_until_complete(run_asyncio) == b"hello"
        finally:
            reader.close()
            writer.close()
            s.close()

    def test_tealet_selector_event_loop_call_soon_threadsafe(self):
        s = SelectorScheduler()
        set_scheduler(s)

        def run_asyncio() -> str:
            loop = TealetSelectorEventLoop(s)
            worker: threading.Thread | None = None
            try:
                asyncio.set_event_loop(loop)

                async def main() -> str:
                    nonlocal worker
                    future = loop.create_future()

                    def complete() -> None:
                        loop.call_soon_threadsafe(future.set_result, "thread")

                    worker = threading.Thread(target=complete)
                    worker.start()
                    return await future

                return loop.run_until_complete(main())
            finally:
                if worker is not None:
                    worker.join(timeout=1.0)
                asyncio.set_event_loop(None)
                loop.close()

        try:
            assert s.run_until_complete(run_asyncio) == "thread"
        finally:
            s.close()

    def test_async_scheduler_sock_recv_and_sendall(self):
        s = AsyncScheduler()
        set_scheduler(s)

        async def run_case() -> None:
            reader, writer = socket.socketpair()
            try:
                reader.setblocking(False)
                writer.setblocking(False)

                def receive() -> bytes:
                    return s.sock_recv(reader, 5)

                def send() -> None:
                    s.sleep(0.001)
                    s.sock_sendall(writer, b"hello")

                task = s.spawn(receive)
                s.spawn(send)

                assert await s.arun_until_complete(task) == b"hello"
            finally:
                reader.close()
                writer.close()

        asyncio.run(run_case())

    def test_async_scheduler_sock_recv_into(self):
        s = AsyncScheduler()
        set_scheduler(s)

        async def run_case() -> None:
            reader, writer = socket.socketpair()
            try:
                reader.setblocking(False)
                writer.setblocking(False)
                buf = bytearray(5)

                def receive() -> int:
                    return s.sock_recv_into(reader, buf)

                def send() -> None:
                    s.sleep(0.001)
                    s.sock_sendall(writer, b"world")

                task = s.spawn(receive)
                s.spawn(send)

                assert await s.arun_until_complete(task) == 5
                assert bytes(buf) == b"world"
            finally:
                reader.close()
                writer.close()

        asyncio.run(run_case())

    def test_async_scheduler_sock_accept_connect(self):
        s = AsyncScheduler()
        set_scheduler(s)

        async def run_case() -> None:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                server.setblocking(False)
                client.setblocking(False)
                server.bind(("127.0.0.1", 0))
                server.listen()

                def accept_and_read() -> bytes:
                    conn, _address = s.sock_accept(server)
                    try:
                        return s.sock_recv(conn, 4)
                    finally:
                        conn.close()

                def connect_and_send() -> None:
                    s.sock_connect(client, server.getsockname())
                    s.sock_sendall(client, b"ping")

                task = s.spawn(accept_and_read)
                s.spawn(connect_and_send)

                assert await s.arun_until_complete(task) == b"ping"
            finally:
                client.close()
                server.close()

        asyncio.run(run_case())

    def test_async_scheduler_sock_datagram_helpers(self):
        s = AsyncScheduler()
        set_scheduler(s)

        async def run_case() -> None:
            receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                receiver.setblocking(False)
                sender.setblocking(False)
                receiver.bind(("127.0.0.1", 0))
                buf = bytearray(5)

                def receive() -> tuple[int, object]:
                    return s.sock_recvfrom_into(receiver, buf)

                def send() -> int:
                    s.sleep(0.001)
                    return s.sock_sendto(sender, b"hello", receiver.getsockname())

                receive_task = s.spawn(receive)
                send_task = s.spawn(send)

                result = await s.arun_until_complete(receive_task)
                assert isinstance(result, tuple)
                count, address = result
                assert count == 5
                assert bytes(buf) == b"hello"
                assert address[1] == sender.getsockname()[1]
                assert send_task.result() == 5

                def receive_bytes() -> tuple[bytes, object]:
                    return s.sock_recvfrom(receiver, 5)

                def send_bytes() -> int:
                    s.sleep(0.001)
                    return s.sock_sendto(sender, b"again", receiver.getsockname())

                receive_bytes_task = s.spawn(receive_bytes)
                send_bytes_task = s.spawn(send_bytes)

                result = await s.arun_until_complete(receive_bytes_task)
                assert isinstance(result, tuple)
                data, address = result
                assert data == b"again"
                assert address[1] == sender.getsockname()[1]
                assert send_bytes_task.result() == 5
            finally:
                sender.close()
                receiver.close()

        asyncio.run(run_case())

    def test_async_scheduler_reader_writer_callbacks(self):
        s = AsyncScheduler()
        set_scheduler(s)

        async def run_case() -> None:
            reader, writer = socket.socketpair()
            try:
                reader.setblocking(False)
                writer.setblocking(False)
                readable = asyncio.Event()
                writable = asyncio.Event()
                seen: list[object] = []

                def on_readable() -> None:
                    seen.append(reader.recv(5))
                    assert s.remove_reader(reader.fileno()) is True
                    readable.set()

                def on_writable() -> None:
                    seen.append("writable")
                    assert s.remove_writer(writer.fileno()) is True
                    writable.set()

                s.add_reader(reader.fileno(), on_readable)
                writer.send(b"hello")
                await asyncio.wait_for(readable.wait(), timeout=1.0)

                s.add_writer(writer.fileno(), on_writable)
                await asyncio.wait_for(writable.wait(), timeout=1.0)

                assert seen == [b"hello", "writable"]
                assert s.remove_reader(reader.fileno()) is False
                assert s.remove_writer(writer.fileno()) is False
            finally:
                reader.close()
                writer.close()

        asyncio.run(run_case())

    def test_run_requires_scheduler_to_be_current(self):
        s = _new_scheduler()
        set_scheduler(_new_scheduler())
        with pytest.raises(RuntimeError, match="current scheduler"):
            s.run()

    def test_pump_requires_scheduler_to_be_current(self):
        s = _new_scheduler()
        set_scheduler(_new_scheduler())
        with pytest.raises(RuntimeError, match="current scheduler"):
            s.pump()

    def test_arun_requires_scheduler_to_be_current(self):
        s = AsyncScheduler()
        set_scheduler(_new_scheduler())

        async def run() -> None:
            with pytest.raises(RuntimeError, match="current scheduler"):
                await s.arun()

        asyncio.run(run())

    def test_arun_until_complete_returns_result(self):
        s = AsyncScheduler()
        set_scheduler(s)

        def worker() -> int:
            s.yield_()
            return 42

        async def run() -> None:
            assert await s.arun_until_complete(worker) == 42

        asyncio.run(run())

    def test_arun_until_complete_accepts_future(self):
        s = AsyncScheduler()
        set_scheduler(s)

        future: Future[int] = Future()

        async def complete_later() -> None:
            await asyncio.sleep(0)
            s.call_soon(future.set_result, 7)

        async def run() -> None:
            trigger = asyncio.create_task(complete_later())
            try:
                assert await s.arun_until_complete(future) == 7
            finally:
                await trigger

        asyncio.run(run())

    def test_arun_forever_stops(self):
        s = AsyncScheduler()
        set_scheduler(s)

        def stop_soon() -> None:
            s.call_soon(s.stop)

        s.spawn(stop_soon)

        async def run() -> None:
            await s.arun_forever()

        asyncio.run(run())

    def test_run_until_complete_returns_result(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)

        def worker() -> int:
            s.yield_()
            return 42

        fut = s.spawn(worker)
        assert s.run_until_complete(fut) == 42

    def test_all_tasks_returns_unfinished_tealet_tasks(self, deferred_scheduler_task_factory_maker):
        s = _new_scheduler(deferred_scheduler_task_factory_maker)
        set_scheduler(s)
        event = Event()

        def worker() -> str:
            event.swait()
            return "done"

        task = s.spawn(worker)
        assert s.all_tasks() == {task}

        s.call_soon(event.set)
        assert s.run_until_complete(task) == "done"
        assert s.all_tasks() == set()

    def test_all_tasks_does_not_keep_completed_tasks_alive(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)

        task = s.spawn(lambda: "done")
        task_ref = weakref.ref(task)
        assert s.run_until_complete(task) == "done"
        assert s.all_tasks() == set()

        del task
        gc.collect()
        assert task_ref() is None

    def test_gather_accepts_tasks_and_callables_in_order(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)
        seen: list[str] = []

        def first() -> str:
            s.yield_()
            seen.append("first")
            return "a"

        def second() -> str:
            seen.append("second")
            return "b"

        first_task = s.spawn(first)
        group = gather(first_task, second)

        assert s.run_until_complete(group) == ["a", "b"]
        assert seen == ["second", "first"]

    def test_gather_empty_completes_immediately(self):
        s = _new_scheduler()
        set_scheduler(s)

        group = gather()

        assert group.done() is True
        assert group.result() == []

    def test_gather_propagates_first_exception(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)

        def fail() -> None:
            raise ValueError("boom")

        def succeed() -> str:
            s.yield_()
            return "ok"

        group = gather(fail, succeed)

        with pytest.raises(ValueError, match="boom"):
            s.run_until_complete(group)
        s.run()

    def test_gather_return_exceptions_collects_results(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)

        def fail() -> None:
            raise ValueError("boom")

        def succeed() -> str:
            return "ok"

        result = s.run_until_complete(gather(fail, succeed, return_exceptions=True))

        assert isinstance(result[0], ValueError)
        assert str(result[0]) == "boom"
        assert result[1] == "ok"

    def test_gather_cancel_cancels_unfinished_children(self, deferred_scheduler_task_factory_maker):
        s = _new_scheduler(deferred_scheduler_task_factory_maker)
        set_scheduler(s)
        event = Event()

        def worker() -> None:
            event.swait()

        task = s.spawn(worker)
        group = gather(task)
        assert group.cancel() is True

        with pytest.raises(CancelledError):
            s.run_until_complete(group)
        assert task.cancelled() is True

    def test_gather_rejects_foreign_task(self, scheduler_task_factory_maker):
        s1 = _new_scheduler(scheduler_task_factory_maker)
        s2 = _new_scheduler(scheduler_task_factory_maker)
        task = s2.spawn(lambda: "foreign")
        set_scheduler(s1)

        try:
            with pytest.raises(RuntimeError, match="different scheduler"):
                gather(task)
        finally:
            set_scheduler(s2)
            if not task.done():
                s2.run_until_complete(task)

    def test_ensure_future_returns_existing_future(self):
        fut: Future[str] = Future()

        assert ensure_future(fut) is fut

    def test_ensure_future_spawns_callable(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)

        fut = ensure_future(lambda: "done")

        assert s.run_until_complete(fut) == "done"

    def test_scheduler_ensure_future_spawns_callable(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)

        fut = s.ensure_future(lambda: "done")

        assert s.run_until_complete(fut) == "done"

    def test_ensure_future_returns_existing_shield(self):
        fut: Future[str] = Future()
        shielded = shield(fut)

        assert ensure_future(shielded) is shielded

    def test_ensure_future_rejects_foreign_task(self, scheduler_task_factory_maker):
        s1 = _new_scheduler(scheduler_task_factory_maker)
        s2 = _new_scheduler(scheduler_task_factory_maker)
        task = s2.spawn(lambda: "foreign")
        set_scheduler(s1)

        try:
            with pytest.raises(RuntimeError, match="different scheduler"):
                ensure_future(task)
        finally:
            set_scheduler(s2)
            if not task.done():
                s2.run_until_complete(task)

    def test_wait_first_completed_returns_done_and_pending(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)
        event = Event()

        def slow() -> str:
            event.swait()
            return "slow"

        def fast() -> str:
            return "fast"

        slow_task = s.spawn(slow)
        fast_task = s.spawn(fast)

        done, pending = s.run_until_complete(wait([slow_task, fast_task], return_when=FIRST_COMPLETED))

        assert done == {fast_task}
        assert pending == {slow_task}
        assert fast_task.result() == "fast"
        s.call_soon(event.set)
        assert s.run_until_complete(slow_task) == "slow"

    def test_wait_first_exception_returns_on_exception(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)
        event = Event()

        def fail() -> None:
            raise ValueError("boom")

        def slow() -> str:
            event.swait()
            return "slow"

        fail_task = s.spawn(fail)
        slow_task = s.spawn(slow)

        done, pending = s.run_until_complete(wait([fail_task, slow_task], return_when=FIRST_EXCEPTION))

        assert done == {fail_task}
        assert pending == {slow_task}
        with pytest.raises(ValueError, match="boom"):
            fail_task.result()
        s.call_soon(event.set)
        assert s.run_until_complete(slow_task) == "slow"

    def test_wait_timeout_returns_pending_without_cancelling(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)
        event = Event()

        def slow() -> str:
            event.swait()
            return "slow"

        slow_task = s.spawn(slow)

        done, pending = s.run_until_complete(wait([slow_task], timeout=0.001, return_when=ALL_COMPLETED))

        assert done == set()
        assert pending == {slow_task}
        assert slow_task.done() is False
        s.call_soon(event.set)
        assert s.run_until_complete(slow_task) == "slow"

    def test_wait_rejects_empty_and_invalid_return_when(self):
        s = _new_scheduler()
        set_scheduler(s)

        with pytest.raises(ValueError, match="empty"):
            wait([])
        with pytest.raises(ValueError, match="Invalid return_when"):
            wait([Future()], return_when="SOON")  # type: ignore[arg-type]

    def test_wait_for_returns_result(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)

        def worker() -> str:
            s.yield_()
            return "done"

        assert s.run_until_complete(wait_for(worker, timeout=1.0)) == "done"

    def test_wait_for_timeout_cancels_child(self, deferred_scheduler_task_factory_maker):
        s = _new_scheduler(deferred_scheduler_task_factory_maker)
        set_scheduler(s)
        event = Event()

        def worker() -> None:
            event.swait()

        task = s.spawn(worker)

        with pytest.raises(TimeoutError):
            s.run_until_complete(wait_for(task, timeout=0.001))
        assert task.cancelled() is True

    def test_wait_for_shield_timeout_does_not_cancel_child(self, deferred_scheduler_task_factory_maker):
        s = _new_scheduler(deferred_scheduler_task_factory_maker)
        set_scheduler(s)
        event = Event()

        def worker() -> str:
            event.swait()
            return "done"

        task = s.spawn(worker)

        with pytest.raises(TimeoutError):
            s.run_until_complete(wait_for(shield(task), timeout=0.001))
        assert task.done() is False
        s.call_soon(event.set)
        assert s.run_until_complete(task) == "done"

    def test_as_completed_yields_results_in_completion_order(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)
        seen: list[str] = []

        def slow() -> str:
            s.sleep(0.002)
            return "slow"

        def fast() -> str:
            return "fast"

        slow_task = s.spawn(slow)
        fast_task = s.spawn(fast)

        def consumer() -> None:
            for completion in as_completed([slow_task, fast_task]):
                seen.append(completion.result())

        s.spawn(consumer)
        s.run()

        assert seen == ["fast", "slow"]

    def test_as_completed_deduplicates_existing_futures(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)
        seen: list[str] = []

        task = s.spawn(lambda: "done")

        def consumer() -> None:
            for completion in as_completed([task, task]):
                seen.append(completion.result())

        s.spawn(consumer)
        s.run()

        assert seen == ["done"]

    def test_as_completed_empty_input_exits(self):
        s = _new_scheduler()
        set_scheduler(s)

        assert list(as_completed([])) == []

    def test_as_completed_timeout_marks_unfinished_slots_without_cancelling(self, deferred_scheduler_task_factory_maker):
        s = _new_scheduler(deferred_scheduler_task_factory_maker)
        set_scheduler(s)
        event = Event()
        seen: list[str] = []

        def slow() -> str:
            event.swait()
            return "slow"

        task = s.spawn(slow)

        def consumer() -> None:
            with pytest.raises(TimeoutError):
                next(as_completed([task], timeout=0.001))
            seen.append("timeout")

        s.spawn(consumer)
        s.run()

        assert seen == ["timeout"]
        assert task.done() is False
        s.call_soon(event.set)
        assert s.run_until_complete(task) == "slow"

    def test_run_until_complete_propagates_exception(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)

        def worker() -> None:
            raise ValueError("boom")

        fut = s.spawn(worker)
        with pytest.raises(ValueError, match="boom"):
            s.run_until_complete(fut)

    @pytest.mark.parametrize("exc", [SystemExit("bye"), KeyboardInterrupt("stop")])
    def test_run_until_complete_stores_fatal_baseexceptions(self, exc):
        s = _new_scheduler()
        set_scheduler(s)

        def worker() -> None:
            raise exc

        fut = s.spawn(worker)
        with pytest.raises(type(exc)) as raised:
            s.run_until_complete(fut)
        assert str(raised.value) == str(exc)
        assert fut.done()
        assert fut.exception() is exc

    def test_run_until_complete_rejects_foreign_task(self, scheduler_task_factory_maker):
        s1 = _new_scheduler(scheduler_task_factory_maker)
        s2 = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s1)
        fut = s2.spawn(lambda: 1)
        try:
            with pytest.raises(RuntimeError, match="different scheduler"):
                s1.run_until_complete(fut)
        finally:
            set_scheduler(s2)
            if not fut.done():
                s2.run_until_complete(fut)

    def test_run_until_complete_raises_if_stopped_early(self):
        s = _new_scheduler()
        set_scheduler(s)
        fut: Future[int] = Future()
        s.call_soon(s.stop)
        with pytest.raises(RuntimeError, match="stopped before Future completed"):
            s.run_until_complete(fut)

    def test_run_until_complete_accepts_callable(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)

        def worker() -> int:
            return 42

        assert s.run_until_complete(worker) == 42

    def test_run_until_complete_rejects_callable_args(self):
        s = _new_scheduler()
        set_scheduler(s)

        def worker() -> int:
            return 42

        with pytest.raises(TypeError, match="unexpected keyword argument"):
            s.run_until_complete(worker, x=20, y=22)  # type: ignore[call-arg]

        with pytest.raises(TypeError, match="takes 2 positional arguments but 4 were given"):
            s.run_until_complete(worker, 20, 22)  # type: ignore[call-arg]

    def test_run_until_complete_rejects_non_future_non_callable(self):
        s = _new_scheduler()
        set_scheduler(s)
        with pytest.raises(TypeError, match="Future or callable"):
            s.run_until_complete(object())  # type: ignore[arg-type]


class TestSchedulerExamples:
    def test_scheduler_is_running_for_run_only(self):
        s = _new_scheduler()
        seen: list[bool] = []

        def check() -> None:
            seen.append(s.is_running())

        assert s.is_running() is False
        s.spawn(check)
        s.run()
        assert seen == [True]
        assert s.is_running() is False

    def test_scheduler_is_running_for_arun_only(self):
        s = AsyncScheduler()
        set_scheduler(s)
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
        s = _new_scheduler()
        seen: list[bool] = []

        def check() -> None:
            seen.append(s.is_running())

        assert s.is_running() is False
        s.spawn(check)
        s.pump(1)
        assert seen == [True]
        assert s.is_running() is False

    def test_run_forever_stops_when_stop_called(self):
        s = _new_scheduler()
        seen: list[str] = []

        def worker() -> None:
            seen.append("ran")
            s.stop()

        s.spawn(worker)
        s.run_forever()

        assert seen == ["ran"]
        assert s.is_running() is False

    def test_stop_breaks_sleep_in_run_forever_via_call_soon_threadsafe(self):
        s = _new_scheduler()
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

    def test_arun_forever_stops_from_thread_via_call_soon_threadsafe(self):
        s = AsyncScheduler()
        set_scheduler(s)
        started = threading.Event()

        s.call_soon(started.set)

        def stop_from_thread() -> None:
            assert started.wait(timeout=1.0)

            async def request_stop() -> None:
                s.call_soon_threadsafe(s.stop)

            asyncio.run(request_stop())

        async def orchestrate() -> None:
            t = threading.Thread(target=stop_from_thread)
            t.start()
            try:
                await asyncio.wait_for(s.arun_forever(), timeout=1.0)
                t.join(timeout=1.0)
                assert not t.is_alive()
                assert s.is_running() is False
            finally:
                s.call_soon_threadsafe(s.stop)
                t.join(timeout=1.0)

        asyncio.run(orchestrate())

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
        s = _new_scheduler()
        seen: list[str] = []

        def mark() -> None:
            seen.append("fired")

        handle = s.call_later(0.001, mark)
        handle.cancel()
        s.run()

        assert handle.cancelled()
        assert seen == []

    def test_call_later_captures_current_context(self):
        s = _new_scheduler()
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
        s = _new_scheduler()
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
        s = _new_scheduler()
        marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker", default="default")
        seen: list[str] = []

        marker.set("scheduled")
        s.spawn(lambda: seen.append(marker.get()))
        marker.set("after-schedule")

        s.run()

        assert seen == ["scheduled"]

    def test_spawn_uses_explicit_context(self):
        s = _new_scheduler()
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
        s = _new_scheduler()
        timeout_evt = Event()
        success_evt = Event()
        seen: list[str] = []

        def timeout_waiter() -> None:
            tm = timeout(0.001)
            with pytest.raises(TimeoutError, match="Operation timed out"):
                with tm:
                    timeout_evt.swait()
            seen.append(f"timeout={not tm.expired()}")

        def success_waiter() -> None:
            tm = timeout(10.0)
            with tm:
                success_evt.swait()
            seen.append(f"success={not tm.expired()}")

        s.spawn(timeout_waiter)
        s.run()
        s.spawn(success_waiter)
        s.call_later(0.002, success_evt.set)
        s.run()

        assert seen == ["timeout=False", "success=True"]

    def test_timeout_context_event_wait_timeout_and_success(self):
        s = _new_scheduler()
        timeout_evt = Event()
        success_evt = Event()
        seen: list[str] = []

        def timeout_waiter() -> None:
            tm = timeout(0.001)
            with pytest.raises(TimeoutError, match="Operation timed out"):
                with tm:
                    timeout_evt.swait()
            seen.append(f"timeout={tm.expired()}")

        def success_waiter() -> None:
            tm = timeout(10.0)
            with tm:
                success_evt.swait()
            seen.append(f"success={not tm.expired()}")

        s.spawn(timeout_waiter)
        s.run()
        s.spawn(success_waiter)
        s.call_later(0.002, success_evt.set)
        s.run()

        assert seen == ["timeout=True", "success=True"]

    def test_event_wait_suppresses_overdue_timeout_when_event_already_set(self):
        class FakeTimeScheduler(Scheduler):
            def __init__(self) -> None:
                super().__init__()
                self.now = 0.0

            def time(self) -> float:
                return self.now

        s = FakeTimeScheduler()
        set_scheduler(s)
        evt = Event()
        seen: list[str] = []

        def waiter() -> None:
            tm = timeout_at(10.0)
            with tm:
                evt.swait()
            seen.append(f"resumed={tm.expired()}")

        s.spawn(waiter)
        s.pump(1)
        assert evt._waiters

        s.call_at(9.0, evt.set)
        s.now = 11.0
        s.run()

        assert seen == ["resumed=True"]

    def test_timeout_demo(self):
        seen = demo_future_timeout_then_success()
        assert seen == ["timeout_waiter:False", "success_waiter:True"]

    def test_arun_runs_inside_asyncio_task(self):
        s = AsyncScheduler()
        set_scheduler(s)
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
                    f"tasks={len(s.runnable_tasks())} timers={len(s._timers)} "
                    f"runner={s._runner is not None} seen={seen}"
                )

        asyncio.run(orchestrate())

        assert seen == ["spawned"]

    def test_event_wait_from_asyncio_task(self):
        evt = Event()

        async def orchestrate() -> bool:
            waiter = asyncio.create_task(evt.wait())
            await asyncio.sleep(0)
            assert not waiter.done()
            evt.set()
            return await asyncio.wait_for(waiter, timeout=1.0)

        assert asyncio.run(orchestrate()) is True

    def test_run_switches_immediately_to_target(self):
        s = _new_scheduler()
        evt = Event()
        seen: list[str] = []
        target_ref: dict[str, TealetTask] = {}

        def target_worker() -> None:
            target_ref["t"] = _tealet.current()
            seen.append("target:started")
            evt.swait()
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
        s = _new_scheduler()
        evt = Event()
        seen: list[str] = []
        target_ref: dict[str, TealetTask] = {}

        def target_worker() -> None:
            target_ref["t"] = _tealet.current()
            seen.append("target:started")
            try:
                evt.swait()
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

    def test_cancel_throws_cancelled_error_into_runnable_task(self):
        s = _new_scheduler()
        seen: list[str] = []
        target_ref: dict[str, TealetTask] = {}

        def target_worker() -> None:
            try:
                seen.append("target:started")
                s.yield_()
                seen.append("target:after-yield")
            except CancelledError:
                seen.append("target:cancelled")
                raise

        def caller() -> None:
            seen.append("caller:before-cancel")
            assert target_ref["t"].cancel() is True
            seen.append("caller:after-cancel")

        target_ref["t"] = s.spawn(target_worker)
        s.spawn(caller)
        s.run()

        assert seen == ["target:started", "caller:before-cancel", "target:cancelled", "caller:after-cancel"]
        assert target_ref["t"].done() is True
        assert target_ref["t"].cancelled() is True
        assert target_ref["t"].cancel() is False
        with pytest.raises(CancelledError):
            target_ref["t"].result()

    def test_cancel_throws_cancelled_error_into_waiting_task(self):
        s = _new_scheduler()
        evt = Event()
        seen: list[str] = []
        target_ref: dict[str, TealetTask] = {}

        def target_worker() -> None:
            try:
                seen.append("target:started")
                evt.swait()
                seen.append("target:after-wait")
            except CancelledError:
                seen.append("target:cancelled")
                raise

        def caller() -> None:
            seen.append("caller:before-cancel")
            assert target_ref["t"].cancel() is True
            seen.append("caller:after-cancel")

        target_ref["t"] = s.spawn(target_worker)
        s.spawn(caller)
        s.run()

        assert seen == ["target:started", "caller:before-cancel", "target:cancelled", "caller:after-cancel"]
        assert target_ref["t"].done() is True
        assert target_ref["t"].cancelled() is True
        assert target_ref["t"].is_waiting() is False
        with pytest.raises(CancelledError):
            target_ref["t"].result()

    def test_uncaught_cancelled_error_cancels_task(self):
        s = _new_scheduler()

        def target_worker() -> None:
            raise CancelledError

        task = s.spawn(target_worker)
        s.run()

        assert task.done() is True
        assert task.cancelled() is True
        assert isinstance(task._exception, CancelledError)
        with pytest.raises(CancelledError):
            task.result()

    def test_uncaught_tealet_exit_completes_task_without_exception(self):
        s = _new_scheduler()

        def target_worker() -> None:
            raise _tealet.TealetExit

        task = s.spawn(target_worker)
        s.run()

        assert task.done() is True
        assert task.cancelled() is False
        assert task.exception() is None
        assert task.result() is None

    @pytest.mark.parametrize("exc_type", [SystemExit, KeyboardInterrupt])
    def test_uncaught_base_exception_sets_task_exception(self, exc_type):
        s = _new_scheduler()

        def target_worker() -> None:
            raise exc_type

        task = s.spawn(target_worker)
        with pytest.raises(exc_type):
            s.run()

        assert task.done() is True
        assert task.cancelled() is False
        assert isinstance(task.exception(), exc_type)
        with pytest.raises(exc_type):
            task.result()

    def test_await_returns_result(self):
        s = AsyncScheduler()
        set_scheduler(s)
        seen: list[int] = []

        async def compute() -> int:
            await asyncio.sleep(0.001)
            return 11

        def worker() -> None:
            seen.append(s.await_(compute()))

        s.spawn(worker)
        asyncio.run(asyncio.wait_for(s.arun(), timeout=1.0))

        assert seen == [11]

    def test_await_propagates_exception(self):
        s = AsyncScheduler()
        set_scheduler(s)
        seen: list[str] = []

        async def boom() -> int:
            await asyncio.sleep(0.001)
            raise ValueError("boom")

        def worker() -> None:
            with pytest.raises(ValueError, match="boom"):
                s.await_(boom())
            seen.append("handled")

        s.spawn(worker)
        asyncio.run(asyncio.wait_for(s.arun(), timeout=1.0))

        assert seen == ["handled"]

    def test_await_cancelled_future_cancels_tealet_task(self):
        s = AsyncScheduler()
        set_scheduler(s)

        async def orchestrate() -> None:
            async_future = asyncio.get_running_loop().create_future()
            async_future.cancel()

            def worker() -> None:
                s.await_(async_future)

            task = s.spawn(worker)
            await asyncio.wait_for(s.arun(), timeout=1.0)

            assert task.done() is True
            assert task.cancelled() is True
            with pytest.raises(CancelledError):
                task.result()

        asyncio.run(orchestrate())

    def test_await_marks_tealet_blocked(self):
        s = AsyncScheduler()
        set_scheduler(s)
        seen: list[tuple[str, bool, bool]] = []

        async def compute() -> int:
            await asyncio.sleep(0.001)
            return 1

        def worker() -> None:
            current = _tealet.current()
            seen.append(("before", current.is_blocked(), current.is_runnable()))
            s.call_later(0.0, lambda: seen.append(("during", current.is_blocked(), current.is_runnable())))
            s.await_(compute())
            seen.append(("after", current.is_blocked(), current.is_runnable()))

        s.spawn(worker)
        asyncio.run(asyncio.wait_for(s.arun(), timeout=1.0))

        assert seen == [
            ("before", False, False),
            ("during", True, False),
            ("after", False, False),
        ]

    def test_await_asynkit_returns_synchronous_coroutine_without_task(self, monkeypatch):
        if scheduler_module._CoroStart is None:
            pytest.skip("asynkit is not installed")

        s = AsyncScheduler()
        set_scheduler(s)
        seen: list[object] = []

        async def compute() -> int:
            seen.append(("body", len(s._pending_async_waits)))
            return 12

        def worker() -> None:
            seen.append(("result", s.await_(compute())))
            seen.append(("pending", len(s._pending_async_waits)))

        async def orchestrate() -> None:
            loop = asyncio.get_running_loop()
            create_task_calls: list[object] = []
            original_create_task = loop.create_task

            def create_task(coro, *args, **kwargs):
                create_task_calls.append(coro)
                return original_create_task(coro, *args, **kwargs)

            monkeypatch.setattr(loop, "create_task", create_task)
            s.spawn(worker)
            await asyncio.wait_for(s.arun(), timeout=1.0)
            delegated = [
                coro
                for coro in create_task_calls
                if getattr(getattr(coro, "cr_code", None), "co_name", None) == "as_coroutine"
            ]
            assert delegated == []

        asyncio.run(orchestrate())

        assert seen == [("body", 0), ("result", 12), ("pending", 0)]

    def test_await_asynkit_uses_copied_context_for_synchronous_coroutine(self):
        if scheduler_module._CoroStart is None:
            pytest.skip("asynkit is not installed")

        s = AsyncScheduler()
        set_scheduler(s)
        marker = contextvars.ContextVar("marker", default="unset")
        seen: list[object] = []

        async def compute() -> str:
            seen.append(("body-before", marker.get()))
            marker.set("body-changed")
            seen.append(("body-after", marker.get()))
            return marker.get()

        def worker() -> None:
            marker.set("caller")
            seen.append(("result", s.await_(compute())))
            seen.append(("caller-after", marker.get()))

        s.spawn(worker)
        asyncio.run(asyncio.wait_for(s.arun(), timeout=1.0))

        assert seen == [
            ("body-before", "caller"),
            ("body-after", "body-changed"),
            ("result", "body-changed"),
            ("caller-after", "caller"),
        ]

    def test_await_asynkit_delegates_blocked_coroutine_to_loop(self, monkeypatch):
        if scheduler_module._CoroStart is None:
            pytest.skip("asynkit is not installed")

        s = AsyncScheduler()
        set_scheduler(s)
        seen: list[object] = []

        async def compute() -> int:
            seen.append("before-await")
            await asyncio.sleep(0)
            seen.append("after-await")
            return 13

        def worker() -> None:
            seen.append(("result", s.await_(compute())))

        async def orchestrate() -> None:
            loop = asyncio.get_running_loop()
            create_task_calls: list[object] = []
            original_create_task = loop.create_task

            def create_task(coro, *args, **kwargs):
                create_task_calls.append(coro)
                return original_create_task(coro, *args, **kwargs)

            monkeypatch.setattr(loop, "create_task", create_task)
            s.spawn(worker)
            await asyncio.wait_for(s.arun(), timeout=1.0)
            delegated = [
                coro
                for coro in create_task_calls
                if getattr(getattr(coro, "cr_code", None), "co_name", None) == "as_coroutine"
            ]
            assert len(delegated) == 1

        asyncio.run(orchestrate())

        assert seen == ["before-await", "after-await", ("result", 13)]

    def test_await_asynkit_uses_same_context_for_blocked_continuation(self):
        if scheduler_module._CoroStart is None:
            pytest.skip("asynkit is not installed")

        s = AsyncScheduler()
        set_scheduler(s)
        marker = contextvars.ContextVar("marker", default="unset")
        seen: list[object] = []

        async def compute() -> str:
            seen.append(("body-before", marker.get()))
            marker.set("body-before-await")
            await asyncio.sleep(0)
            seen.append(("body-after-await", marker.get()))
            marker.set("body-after-await")
            return marker.get()

        def worker() -> None:
            marker.set("caller")
            seen.append(("result", s.await_(compute())))
            seen.append(("caller-after", marker.get()))

        s.spawn(worker)
        asyncio.run(asyncio.wait_for(s.arun(), timeout=1.0))

        assert seen == [
            ("body-before", "caller"),
            ("body-after-await", "body-before-await"),
            ("result", "body-after-await"),
            ("caller-after", "caller"),
        ]

    def test_await_without_asynkit_falls_back_to_loop_task(self, monkeypatch):
        s = AsyncScheduler()
        set_scheduler(s)
        seen: list[object] = []
        monkeypatch.setattr(scheduler_module, "_coro_start", None)

        async def compute() -> int:
            seen.append("body")
            return 14

        def worker() -> None:
            seen.append(("result", s.await_(compute())))

        async def orchestrate() -> None:
            loop = asyncio.get_running_loop()
            create_task_calls: list[object] = []
            original_create_task = loop.create_task

            def create_task(coro, *args, **kwargs):
                create_task_calls.append(coro)
                return original_create_task(coro, *args, **kwargs)

            monkeypatch.setattr(loop, "create_task", create_task)
            s.spawn(worker)
            await asyncio.wait_for(s.arun(), timeout=1.0)
            delegated = [
                coro
                for coro in create_task_calls
                if getattr(getattr(coro, "cr_code", None), "co_name", None) == "compute"
            ]
            assert len(delegated) == 1

        asyncio.run(orchestrate())

        assert seen == ["body", ("result", 14)]

    def test_await_without_asynkit_uses_loop_task_context(self, monkeypatch):
        s = AsyncScheduler()
        set_scheduler(s)
        marker = contextvars.ContextVar("marker", default="unset")
        seen: list[object] = []
        monkeypatch.setattr(scheduler_module, "_coro_start", None)

        async def compute() -> str:
            seen.append(("body-before", marker.get()))
            marker.set("body-changed")
            await asyncio.sleep(0)
            seen.append(("body-after", marker.get()))
            return marker.get()

        def worker() -> None:
            marker.set("caller")
            seen.append(("result", s.await_(compute())))
            seen.append(("caller-after", marker.get()))

        s.spawn(worker)
        asyncio.run(asyncio.wait_for(s.arun(), timeout=1.0))

        assert seen == [
            ("body-before", "caller"),
            ("body-after", "body-changed"),
            ("result", "body-changed"),
            ("caller-after", "caller"),
        ]

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


class TestFutureExamples:
    def test_future_demo(self):
        seen = demo_future_result()
        assert seen == ["producer:start", "producer:done", "consumer:result=42"]

    def test_future_exception_propagates(self):
        s = _new_scheduler()

        def boom():
            raise ValueError("boom")

        future = s.spawn(boom)
        s.run()

        assert future.done()
        with pytest.raises(ValueError, match="boom"):
            future.result()
        assert isinstance(future.exception(), ValueError)

    def test_future_exception_before_task_main_starts(self):
        s = _new_scheduler()
        gate = Event()
        seen: list[str] = []

        def blocked() -> int:
            seen.append("blocked:start")
            gate.swait()
            seen.append("blocked:done")
            return 1

        future_blocked = s.spawn(blocked)

        def thrower() -> None:
            seen.append("thrower:start")
            future_victim = s.spawn(lambda: 7)
            victim = s.runnable_tasks()[-1]
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

    def test_future_done_callback_already_done_runs_without_asyncio_loop(self):
        future: Future[int] = Future()
        future.set_result(7)
        seen: list[str] = []

        def on_done(done: Future[int]) -> None:
            seen.append(f"done={done.result()}")

        future.add_done_callback(on_done)

        assert seen == ["done=7"]

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

    def test_future_set_cancelled_error_marks_cancelled(self):
        future = Future()

        future.set_exception(CancelledError())

        assert future.done()
        assert future.cancelled()
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

    def test_future_await_cancelled_schedules_future_cancel(self):
        s = AsyncScheduler()
        set_scheduler(s)
        future: Future[int] = Future()
        seen: list[object] = []

        async def waiter() -> None:
            try:
                await future
            except asyncio.CancelledError:
                seen.append(("waiter:cancelled", future.cancelled()))
                raise

        async def orchestrate() -> None:
            runner = asyncio.create_task(s.arun_forever())
            task = asyncio.create_task(waiter())
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            seen.append(("after-await", future.cancelled()))
            await asyncio.sleep(0)
            seen.append(("after-call-soon", future.cancelled()))
            s.stop()
            await asyncio.wait_for(runner, timeout=1.0)

        asyncio.run(orchestrate())

        assert seen == [("waiter:cancelled", False), ("after-await", True), ("after-call-soon", True)]

    def test_future_wait_cancelled_error_schedules_future_cancel(self):
        s = _new_scheduler()
        future: Future[int] = Future()
        seen: list[object] = []
        waiter_ref: dict[str, TealetTask] = {}

        def waiter() -> None:
            try:
                future.wait()
            except CancelledError:
                seen.append(("waiter:cancelled", future.cancelled()))
                raise

        def canceller() -> None:
            seen.append("canceller:start")
            assert waiter_ref["task"].cancel() is True
            seen.append(("canceller:after-cancel", future.cancelled()))

        waiter_ref["task"] = s.spawn(waiter)
        s.spawn(canceller)
        s.run()
        seen.append(("after-run", future.cancelled()))

        assert waiter_ref["task"].cancelled() is True
        assert future.cancelled() is True
        assert seen == [
            "canceller:start",
            ("waiter:cancelled", False),
            ("canceller:after-cancel", False),
            ("after-run", True),
        ]

    def test_future_wait_cancelled_future_cancels_waiting_task(self):
        s = _new_scheduler()
        future: Future[int] = Future()
        future.cancel()

        def waiter() -> None:
            future.wait()

        waiter_task = s.spawn(waiter)
        s.run()

        assert waiter_task.done() is True
        assert waiter_task.cancelled() is True
        with pytest.raises(CancelledError):
            waiter_task.result()

    def test_shield_wait_cancelled_error_does_not_cancel_future(self):
        s = _new_scheduler()
        future: Future[int] = Future()
        seen: list[object] = []
        waiter_ref: dict[str, TealetTask] = {}

        def waiter() -> None:
            try:
                shield(future).wait()
            except CancelledError:
                seen.append(("waiter:cancelled", future.cancelled()))
                raise

        def canceller() -> None:
            seen.append("canceller:start")
            assert waiter_ref["task"].cancel() is True
            seen.append(("canceller:after-cancel", future.cancelled()))
            future.set_result(9)

        waiter_ref["task"] = s.spawn(waiter)
        s.spawn(canceller)
        s.run()

        assert waiter_ref["task"].cancelled() is True
        assert future.cancelled() is False
        assert future.result() == 9
        assert seen == ["canceller:start", ("waiter:cancelled", False), ("canceller:after-cancel", False)]

    def test_shield_wait_returns_future_result(self):
        future: Future[int] = Future()
        future.set_result(12)

        assert shield(future).wait() == 12

    def test_shield_cancel_does_not_cancel_future(self):
        future: Future[int] = Future()
        shielded = shield(future)

        assert shielded.cancel() is True
        assert shielded.cancelled() is True
        assert future.cancelled() is False

        future.set_result(12)
        assert future.result() == 12
        with pytest.raises(CancelledError):
            shielded.result()

    def test_await_cancelled_error_schedules_async_future_cancel(self):
        s = AsyncScheduler()
        set_scheduler(s)
        seen: list[object] = []

        async def orchestrate() -> None:
            async_future = asyncio.get_running_loop().create_future()
            waiter_ref: dict[str, TealetTask] = {}

            def waiter() -> None:
                try:
                    s.await_(async_future)
                except CancelledError:
                    seen.append(("waiter:cancelled", async_future.cancelled()))
                    raise

            def canceller() -> None:
                seen.append("canceller:start")
                assert waiter_ref["task"].cancel() is True
                seen.append(("canceller:after-cancel", async_future.cancelled()))

            waiter_ref["task"] = s.spawn(waiter)
            s.spawn(canceller)
            await s.arun()
            seen.append(("after-arun", async_future.cancelled()))
            await asyncio.sleep(0)
            seen.append(("after-call-soon", async_future.cancelled()))

            assert waiter_ref["task"].cancelled() is True

        asyncio.run(orchestrate())

        assert seen == [
            "canceller:start",
            ("waiter:cancelled", False),
            ("canceller:after-cancel", False),
            ("after-arun", False),
            ("after-call-soon", True),
        ]

    def test_await_cancelled_error_does_not_cancel_asyncio_shielded_future(self):
        s = AsyncScheduler()
        set_scheduler(s)
        seen: list[object] = []

        async def orchestrate() -> None:
            async_future = asyncio.get_running_loop().create_future()
            shielded = asyncio.shield(async_future)
            waiter_ref: dict[str, TealetTask] = {}

            def waiter() -> None:
                try:
                    s.await_(shielded)
                except CancelledError:
                    seen.append(("waiter:cancelled", shielded.cancelled(), async_future.cancelled()))
                    raise

            def canceller() -> None:
                seen.append("canceller:start")
                assert waiter_ref["task"].cancel() is True
                seen.append(("canceller:after-cancel", shielded.cancelled(), async_future.cancelled()))

            waiter_ref["task"] = s.spawn(waiter)
            s.spawn(canceller)
            await s.arun()
            seen.append(("after-arun", shielded.cancelled(), async_future.cancelled()))
            await asyncio.sleep(0)
            seen.append(("after-call-soon", shielded.cancelled(), async_future.cancelled()))
            async_future.set_result(9)

            assert waiter_ref["task"].cancelled() is True

        asyncio.run(orchestrate())

        assert seen == [
            "canceller:start",
            ("waiter:cancelled", False, False),
            ("canceller:after-cancel", False, False),
            ("after-arun", False, False),
            ("after-call-soon", True, False),
        ]

    def test_future_wait_timeout_does_not_cancel_future(self):
        s = _new_scheduler()
        future: Future[int] = Future()

        def waiter() -> None:
            with pytest.raises(RawTimeoutError):
                future.wait()

        waiter_task = s.spawn(waiter)
        s.pump(1)
        waiter_task.throw(RawTimeoutError())
        s.run()

        assert waiter_task.done() is True
        assert future.done() is False
        assert future.cancelled() is False

    def test_future_result_timeout(self):
        s = _new_scheduler()
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
        s = _new_scheduler()
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
        s = AsyncScheduler()
        set_scheduler(s)
        future: Future[int] = Future()

        async def orchestrate() -> None:
            s.call_later(0.001, future.set_result, 7)
            runner = asyncio.create_task(s.arun())
            try:
                assert await asyncio.wait_for(future, timeout=1.0) == 7
                assert future.result() == 7
            finally:
                await asyncio.wait_for(runner, timeout=1.0)

        asyncio.run(orchestrate())

    def test_future_async_exception(self):
        s = AsyncScheduler()
        set_scheduler(s)
        future: Future[int] = Future()

        async def orchestrate() -> None:
            s.call_later(0.001, future.set_exception, ValueError("boom"))
            runner = asyncio.create_task(s.arun())
            try:
                with pytest.raises(ValueError, match="boom"):
                    await asyncio.wait_for(future, timeout=1.0)
                with pytest.raises(ValueError, match="boom"):
                    future.result()
                exc = future.exception()
                assert isinstance(exc, ValueError)
            finally:
                await asyncio.wait_for(runner, timeout=1.0)

        asyncio.run(orchestrate())

    def test_tealet_task_async_wait_cancelled_error_raises_cancelled_error(self):
        s = AsyncScheduler()
        set_scheduler(s)

        def target_worker() -> None:
            raise CancelledError

        async def orchestrate() -> None:
            task = s.spawn(target_worker)
            runner = asyncio.create_task(s.arun())
            try:
                with pytest.raises(CancelledError):
                    await asyncio.wait_for(task, timeout=1.0)
                assert task.cancelled() is True
                assert isinstance(task._exception, CancelledError)
            finally:
                await asyncio.wait_for(runner, timeout=1.0)

        asyncio.run(orchestrate())

    def test_future_is_awaitable(self):
        s = AsyncScheduler()
        set_scheduler(s)
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
        s = _new_scheduler()
        q: Queue[int] = Queue(maxsize=1)
        seen: list[str] = []

        def producer() -> None:
            q.sput(1)
            seen.append("put:1")
            q.sput(2)
            seen.append("put:2")

        def consumer() -> None:
            s.yield_()
            seen.append(f"get:{q.sget()}")
            s.yield_()
            seen.append(f"get:{q.sget()}")

        s.spawn(producer)
        s.spawn(consumer)
        s.run()

        assert seen == ["put:1", "get:1", "put:2", "get:2"]

    def test_queue_join_and_task_done(self):
        s = _new_scheduler()
        q: Queue[int] = Queue()
        produced_evt = Event()
        seen: list[str] = []

        def producer() -> None:
            # Let other spawned tasks start so producer exit does not try
            # to hand off directly to an unstarted tealet.
            s.yield_()
            q.sput(1)
            q.sput(2)
            seen.append("produced")
            produced_evt.set()

        def consumer() -> None:
            s.yield_()
            q.sget()
            q.task_done()
            seen.append("done:1")
            q.sget()
            q.task_done()
            seen.append("done:2")

        def waiter() -> None:
            produced_evt.swait()
            q.sjoin()
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

    def test_queue_shutdown_graceful_drains_existing_items(self):
        q: Queue[int] = Queue()
        q.put_nowait(1)
        q.put_nowait(2)

        q.shutdown()

        with pytest.raises(QueueShutDown):
            q.put_nowait(3)
        assert q.get_nowait() == 1
        q.task_done()
        assert q.get_nowait() == 2
        q.task_done()
        q.sjoin()
        with pytest.raises(QueueShutDown):
            q.get_nowait()

    def test_queue_shutdown_immediate_drains_and_unblocks_join(self):
        q: Queue[int] = Queue()
        q.put_nowait(1)
        q.put_nowait(2)

        q.shutdown(immediate=True)

        assert q.empty()
        q.sjoin()
        with pytest.raises(QueueShutDown):
            q.get_nowait()
        with pytest.raises(QueueShutDown):
            q.put_nowait(3)
        with pytest.raises(ValueError, match=r"task_done\(\) called too many times"):
            q.task_done()

    def test_queue_shutdown_immediate_wakes_blocked_sync_joiner(self):
        s = _new_scheduler()
        q: Queue[int] = Queue()
        q.put_nowait(1)
        q.put_nowait(2)
        seen: list[str] = []

        def waiter() -> None:
            q.sjoin()
            seen.append("joined")

        def closer() -> None:
            s.yield_()
            q.shutdown(immediate=True)
            seen.append("shutdown")

        s.spawn(waiter)
        s.spawn(closer)
        s.run()

        assert seen == ["shutdown", "joined"]

    def test_queue_shutdown_wakes_blocked_sync_getter(self):
        s = _new_scheduler()
        q: Queue[int] = Queue()
        seen: list[str] = []

        def consumer() -> None:
            try:
                q.sget()
            except QueueShutDown:
                seen.append("getter:shutdown")

        def closer() -> None:
            s.yield_()
            q.shutdown()
            seen.append("shutdown")

        s.spawn(consumer)
        s.spawn(closer)
        s.run()

        assert seen == ["shutdown", "getter:shutdown"]

    def test_queue_shutdown_wakes_blocked_sync_putter(self):
        s = _new_scheduler()
        q: Queue[int] = Queue(maxsize=1)
        q.put_nowait(1)
        seen: list[str] = []

        def producer() -> None:
            try:
                q.sput(2)
            except QueueShutDown:
                seen.append("putter:shutdown")

        def closer() -> None:
            s.yield_()
            q.shutdown()
            seen.append("shutdown")

        s.spawn(producer)
        s.spawn(closer)
        s.run()

        assert seen == ["shutdown", "putter:shutdown"]

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

    def test_queue_shutdown_wakes_blocked_asyncio_getter(self):
        q: Queue[int] = Queue()
        seen: list[str] = []

        async def consumer() -> None:
            try:
                await q.get()
            except QueueShutDown:
                seen.append("getter:shutdown")

        async def run() -> None:
            task = asyncio.create_task(consumer())
            await asyncio.sleep(0)
            q.shutdown()
            seen.append("shutdown")
            await task

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))
        assert seen == ["shutdown", "getter:shutdown"]

    def test_queue_shutdown_wakes_blocked_asyncio_putter(self):
        q: Queue[int] = Queue(maxsize=1)
        q.put_nowait(1)
        seen: list[str] = []

        async def producer() -> None:
            try:
                await q.put(2)
            except QueueShutDown:
                seen.append("putter:shutdown")

        async def run() -> None:
            task = asyncio.create_task(producer())
            await asyncio.sleep(0)
            q.shutdown()
            seen.append("shutdown")
            await task

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))
        assert seen == ["shutdown", "putter:shutdown"]

    def test_queue_shutdown_immediate_wakes_blocked_asyncio_joiner(self):
        q: Queue[int] = Queue()
        q.put_nowait(1)
        q.put_nowait(2)
        seen: list[str] = []

        async def waiter() -> None:
            await q.join()
            seen.append("joined")

        async def run() -> None:
            task = asyncio.create_task(waiter())
            await asyncio.sleep(0)
            q.shutdown(immediate=True)
            seen.append("shutdown")
            await task

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))
        assert seen == ["shutdown", "joined"]

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
        s = _new_scheduler()
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
        s = _new_scheduler()
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
        s = _new_scheduler()
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
        s = _new_scheduler()
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
        s = _new_scheduler()
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
        s = _new_scheduler()
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
        s = _new_scheduler()
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
        s = _new_scheduler()
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
            value = ch.receive()
            assert isinstance(value, int)
            got.append(value)

        s.spawn(receiver2)
        s.pump(1)
        assert ch.balance == -1

        s.spawn(lambda: ch.send(99))
        s.run()
        assert got == [99]

    def test_channel_receive_raw_timeout_suppressed_when_packet_already_delivered(self):
        s = _new_scheduler()
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
