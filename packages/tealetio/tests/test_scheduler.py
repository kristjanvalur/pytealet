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
import tealetio.tasks as task_module
from helpers import new_scheduler as _new_scheduler
from tealetio import (
    ALL_COMPLETED,
    CancelledError,
    DefaultTaskFactory,
    Event,
    FIRST_COMPLETED,
    FIRST_EXCEPTION,
    FifoRunnableQueue,
    Future,
    PrescheduledRunnableQueue,
    PriorityRunnableQueue,
    PriorityTask,
    RunnableQueue,
    Scheduler,
    AsyncSelectorScheduler,
    SyncProactorScheduler,
    SelectorScheduler,
    SyncSelectorScheduler,
    StubTaskFactory,
    TASK_PRIORITY_CRITICAL,
    TASK_PRIORITY_DEFAULT,
    TASK_PRIORITY_HIGH,
    TASK_PRIORITY_IDLE,
    TASK_PRIORITY_LOW,
    TealetProactorEventLoop,
    TealetSelectorEventLoop,
    Task,
    TimeoutError,
    AsyncScheduler,
    BasicScheduler,
    as_completed,
    await_,
    create_task,
    ensure_future,
    gather,
    get_current,
    get_running_scheduler,
    get_scheduler,
    run_asyncio_in_tealet,
    run_in_asyncio,
    set_scheduler,
    shield,
    sleep,
    spawn,
    timeout,
    timeout_at,
    to_thread,
    wait,
    wait_for,
)
from tealetio.examples import (
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


class _SocketAwaitExchange:
    PAYLOAD = b"alpha-beta-gamma"

    def __init__(self) -> None:
        self.left, self.right = socket.socketpair()
        self.left.setblocking(False)
        self.right.setblocking(False)
        self.results: dict[str, object] = {}

    async def send_payload(self) -> int:
        loop = asyncio.get_running_loop()
        sent = 0
        for chunk in (self.PAYLOAD[:5], self.PAYLOAD[5:10], self.PAYLOAD[10:]):
            await asyncio.sleep(0)
            await loop.sock_sendall(self.left, chunk)
            sent += len(chunk)
        return sent

    async def receive_payload(self) -> bytes:
        loop = asyncio.get_running_loop()
        received = bytearray()
        while len(received) < len(self.PAYLOAD):
            await asyncio.sleep(0)
            chunk = await loop.sock_recv(self.right, 3)
            if not chunk:
                break
            received.extend(chunk)
        return bytes(received)

    def spawn_tasks(self) -> tuple[Task, Task]:
        scheduler = get_running_scheduler()

        def sender() -> None:
            self.results["sent"] = await_(self.send_payload())

        def receiver() -> None:
            self.results["received"] = await_(self.receive_payload())

        return scheduler.spawn(sender), scheduler.spawn(receiver)

    def close(self) -> None:
        self.left.close()
        self.right.close()


class _PriorityTaskFactory:
    task_constructor = PriorityTask

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

    def test_scheduler_api_surfaces_expose_sync_and_async_drivers_intentionally(self):
        sync = Scheduler()
        async_ = AsyncScheduler()

        for name in (
            "spawn",
            "await_",
            "runnable_tasks",
            "reschedule",
            "yield_to",
            "bind_loop",
        ):
            assert callable(getattr(sync, name))
            assert callable(getattr(async_, name))
        for name in ("run", "run_forever", "run_until_complete"):
            assert callable(getattr(sync, name))
            assert callable(getattr(async_, name))
        for name in ("arun", "arun_forever", "arun_until_complete"):
            assert callable(getattr(sync, name))
            assert callable(getattr(async_, name))

        with pytest.raises(NotImplementedError, match="use arun"):
            async_.run()
        with pytest.raises(NotImplementedError, match="use arun_forever"):
            async_.run_forever()
        with pytest.raises(NotImplementedError, match="use arun_until_complete"):
            async_.run_until_complete(lambda: None)
        loop = asyncio.new_event_loop()
        try:
            with pytest.raises(NotImplementedError, match="bind_loop"):
                sync.bind_loop(loop)
        finally:
            loop.close()

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
        seen: list[Task | None] = []

        def check_current() -> None:
            seen.append(get_current())

        task = s.spawn(check_current)
        s.run()

        assert seen == [task]

    def test_get_current_returns_none_in_asyncio_task(self):
        async def check_current() -> Task | None:
            return get_current()

        assert asyncio.run(check_current()) is None

    def test_get_current_returns_none_inside_awaited_asyncio_coroutine(self):
        s = AsyncScheduler()
        set_scheduler(s)
        seen: list[tuple[str, Task | None]] = []

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

    def test_top_level_await_uses_running_scheduler(self):
        s = AsyncScheduler()
        set_scheduler(s)
        seen: list[object] = []

        async def compute() -> int:
            await asyncio.sleep(0)
            return 7

        def worker() -> None:
            seen.append(await_(compute()))

        s.spawn(worker)
        asyncio.run(asyncio.wait_for(s.arun(), timeout=1.0))

        assert seen == [7]

    def test_runnable_tasks_returns_scheduler_tasks_in_run_order(self):
        s = _new_scheduler()
        set_scheduler(s)

        first = s.spawn(lambda: "first")
        second = s.spawn(lambda: "second")

        assert s.runnable_tasks() == (first, second)

    def test_scheduler_accepts_runnable_queue_factory(self):
        events: list[str] = []

        class RecordingQueue(PrescheduledRunnableQueue):
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

    def test_public_runnable_queue_symbols_are_importable(self):
        assert issubclass(FifoRunnableQueue, scheduler_module._tasks.TaskLink)
        assert issubclass(PrescheduledRunnableQueue, FifoRunnableQueue)
        assert issubclass(PriorityRunnableQueue, PrescheduledRunnableQueue)
        assert RunnableQueue

    def test_top_level_spawn_and_create_task_use_current_scheduler(self):
        s = _new_scheduler()
        set_scheduler(s)
        seen: list[str] = []

        first = spawn(lambda: seen.append("spawn"))
        second = create_task(lambda: seen.append("create_task"))

        assert first.get_scheduler() is s
        assert second.get_scheduler() is s

        s.run()

        assert seen == ["spawn", "create_task"]

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
        s = Scheduler(runnable_queue_factory=PriorityRunnableQueue)
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

    def test_priority_task_reports_effective_priority(self):
        s = _new_scheduler()

        task = PriorityTask(s, TASK_PRIORITY_LOW)

        assert task.priority == TASK_PRIORITY_LOW
        assert task.get_effective_priority() == TASK_PRIORITY_LOW

    def test_priority_task_factory_accepts_spawn_priority_keyword(self):
        s = Scheduler(runnable_queue_factory=PriorityRunnableQueue)
        s.set_task_factory(_PriorityTaskFactory())
        set_scheduler(s)
        seen: list[str] = []

        s.spawn(lambda: seen.append("default"))
        s.spawn(lambda: seen.append("early"), priority=TASK_PRIORITY_HIGH)
        s.spawn(lambda: seen.append("middle"), priority=TASK_PRIORITY_HIGH / 2)

        s.run()

        assert seen == ["early", "middle", "default"]

    def test_default_task_constructor_rejects_unknown_spawn_keyword(self):
        s = _new_scheduler()
        set_scheduler(s)

        with pytest.raises(TypeError, match="priority"):
            s.spawn(lambda: "ok", priority=-10)

    def test_default_task_factory_passes_spawn_kwargs_to_task_constructor(self):
        s = Scheduler(runnable_queue_factory=PriorityRunnableQueue)
        s.set_task_factory(DefaultTaskFactory(task_constructor=PriorityTask))
        set_scheduler(s)
        seen: list[str] = []

        default = s.spawn(lambda: seen.append("default"))
        high = s.spawn(lambda: seen.append("high"), priority=TASK_PRIORITY_HIGH)

        assert isinstance(default, PriorityTask)
        assert isinstance(high, PriorityTask)
        assert default.priority == TASK_PRIORITY_DEFAULT
        assert high.priority == TASK_PRIORITY_HIGH

        s.run()

        assert seen == ["high", "default"]

    def test_priority_runnable_queue_uses_stable_default_priority(self):
        s = Scheduler(runnable_queue_factory=PriorityRunnableQueue)
        set_scheduler(s)
        seen: list[str] = []

        first = s.spawn(lambda: seen.append("first"))
        second = s.spawn(lambda: seen.append("second"))
        third = s.spawn(lambda: seen.append("third"))

        assert s.runnable_tasks() == (first, second, third)

        s.run()

        assert seen == ["first", "second", "third"]

    def test_priority_runnable_queue_reschedule_none_requeries_priority(self):
        s = Scheduler(runnable_queue_factory=PriorityRunnableQueue)
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
        s = Scheduler(runnable_queue_factory=PriorityRunnableQueue)
        s.set_task_factory(
            _PriorityTaskFactory([TASK_PRIORITY_DEFAULT, TASK_PRIORITY_LOW])
        )
        set_scheduler(s)

        first = s.spawn(lambda: "first")
        second = s.spawn(lambda: "second")
        second.priority = TASK_PRIORITY_CRITICAL

        assert s.runnable_tasks() == (second, first)

    def test_priority_runnable_queue_does_not_reorder_prescheduled_modified_task(self):
        s = Scheduler(runnable_queue_factory=PriorityRunnableQueue)
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
        s = Scheduler(runnable_queue_factory=PriorityRunnableQueue)
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

    def test_priority_runnable_queue_runs_low_priority_task_before_runner(self):
        s = Scheduler(runnable_queue_factory=PriorityRunnableQueue)
        s.set_task_factory(DefaultTaskFactory(task_constructor=PriorityTask))
        set_scheduler(s)
        seen: list[str] = []

        s.spawn(lambda: seen.append("low"), priority=TASK_PRIORITY_LOW)

        s.run()

        assert seen == ["low"]

    def test_priority_runnable_queue_runs_stub_tasks_before_runner(self):
        s = Scheduler(runnable_queue_factory=PriorityRunnableQueue)
        s.set_task_factory(StubTaskFactory(task_constructor=PriorityTask))
        set_scheduler(s)
        seen: list[str] = []

        s.spawn(lambda: seen.append("low"), priority=TASK_PRIORITY_LOW)
        s.spawn(lambda: seen.append("high"), priority=TASK_PRIORITY_HIGH)

        s.run()

        assert seen == ["high", "low"]

    def test_run_sets_main_tealet_factory_from_task_factory(self):
        s = _new_scheduler(lambda: DefaultTaskFactory(task_constructor=PriorityTask))
        original_factory = _tealet.get_tealet_factory()
        seen = []

        def worker() -> None:
            main = _tealet.main()
            seen.append(
                (
                    isinstance(main, PriorityTask),
                    main.get_scheduler() is s,
                    main.priority,
                    _tealet.get_tealet_factory() is not original_factory,
                )
            )

        s.spawn(worker)
        s.run()

        assert seen == [(True, True, task_module.TEALET_PRI_INF, True)]
        assert _tealet.get_tealet_factory() is original_factory

    def test_run_until_complete_sets_main_tealet_factory_from_task_factory(self):
        s = _new_scheduler(lambda: DefaultTaskFactory(task_constructor=PriorityTask))
        original_factory = _tealet.get_tealet_factory()

        def worker():
            main = _tealet.main()
            return (
                isinstance(main, PriorityTask),
                main.get_scheduler() is s,
                main.priority,
                _tealet.get_tealet_factory() is not original_factory,
            )

        assert s.run_until_complete(worker) == (True, True, task_module.TEALET_PRI_INF, True)
        assert _tealet.get_tealet_factory() is original_factory

    def test_main_context_sets_main_tealet_factory_from_task_factory(self):
        s = _new_scheduler(lambda: DefaultTaskFactory(task_constructor=PriorityTask))
        original_factory = _tealet.get_tealet_factory()

        with s.main_context():
            main = _tealet.main()
            assert isinstance(main, PriorityTask)
            assert main.get_scheduler() is s
            assert main.priority == TASK_PRIORITY_DEFAULT
            assert _tealet.get_tealet_factory() is not original_factory

        assert _tealet.get_tealet_factory() is original_factory

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
        target: Task | None = None

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
        target: Task | None = None

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
        target: Task | None = None

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
        target: Task | None = None

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
        assert original.task_constructor is Task
        assert original.eager_start is False
        assert custom.task_constructor is Task
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
            task_constructor = Task

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

    def test_stub_task_factory_passes_spawn_kwargs_to_task_constructor(self):
        s = _new_scheduler()
        factory = StubTaskFactory(task_constructor=PriorityTask)
        s.set_task_factory(factory)

        task = s.spawn(lambda: "ok", priority=TASK_PRIORITY_HIGH)

        assert factory.stub is not None
        assert isinstance(task, PriorityTask)
        assert task.priority == TASK_PRIORITY_HIGH
        assert s.run_until_complete(task) == "ok"

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

    def test_arun_explicit_budget_yields_to_asyncio(self):
        s = AsyncScheduler()
        set_scheduler(s)
        events: list[str] = []

        def make_worker(index: int):
            def worker() -> None:
                events.append(f"task:{index}")

            return worker

        async def asyncio_peer() -> None:
            events.append("asyncio")

        for index in range(5):
            s.spawn(make_worker(index))

        async def run() -> None:
            peer = asyncio.create_task(asyncio_peer())
            await s.arun(yield_every=2)
            await peer

        asyncio.run(run())

        assert events == ["task:0", "task:1", "asyncio", "task:2", "task:3", "task:4"]

    def test_arun_rejects_non_positive_yield_every(self):
        s = AsyncScheduler()
        set_scheduler(s)

        async def run() -> None:
            with pytest.raises(ValueError, match="yield_every"):
                await s.arun(yield_every=0)

        asyncio.run(run())

    def test_arun_waits_for_pending_driver_work(self):
        s = AsyncScheduler()
        set_scheduler(s)
        seen: list[str] = []

        def worker() -> None:
            future = s.run_in_executor(None, lambda: "done")
            seen.append(future.wait())

        async def run() -> None:
            s.spawn(worker)
            await asyncio.wait_for(s.arun(), timeout=1.0)

        try:
            asyncio.run(run())
        finally:
            s.close()

        assert seen == ["done"]

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
        s = SyncSelectorScheduler()
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
        s = SyncSelectorScheduler()
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
        s = SyncSelectorScheduler()
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
        s = SyncSelectorScheduler()
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
        s = SyncSelectorScheduler()
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
        s = SyncSelectorScheduler()
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
        s = SyncSelectorScheduler()
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
        s = SyncSelectorScheduler()
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

    def test_basic_scheduler_socket_helpers_require_io_capable_scheduler(self):
        s = BasicScheduler()
        set_scheduler(s)
        reader, _writer = socket.socketpair()
        try:
            reader.setblocking(False)
            with pytest.raises(NotImplementedError, match="IO-capable scheduler"):
                s.sock_recv(reader, 1)
        finally:
            reader.close()
            _writer.close()

    def test_scheduler_socket_helpers_use_default_proactor(self):
        s = _new_scheduler()
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

    def test_basic_scheduler_io_callbacks_require_io_capable_scheduler(self):
        s = BasicScheduler()
        set_scheduler(s)
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

    def test_selector_scheduler_is_abstract(self):
        with pytest.raises(TypeError, match="abstract"):
            SelectorScheduler()

    def test_selector_scheduler_reader_writer_callbacks(self):
        s = SyncSelectorScheduler()
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

    def test_async_selector_scheduler_waits_for_socket_io(self):
        async def run() -> bytes:
            s = AsyncSelectorScheduler()
            set_scheduler(s)
            reader, writer = socket.socketpair()
            try:
                reader.setblocking(False)
                writer.setblocking(False)

                def receive() -> bytes:
                    s.wait_readable(reader)
                    return reader.recv(5)

                task = s.spawn(receive)
                await asyncio.sleep(0)
                writer.send(b"hello")

                return await s.arun_until_complete(task)
            finally:
                reader.close()
                writer.close()
                s.close()

        assert asyncio.run(run()) == b"hello"

    def test_async_selector_scheduler_requires_loop_fd_watchers(self, monkeypatch):
        async def run() -> None:
            s = AsyncSelectorScheduler()
            set_scheduler(s)
            reader, writer = socket.socketpair()
            try:
                loop = asyncio.get_running_loop()

                def add_reader_unavailable(*args: object) -> None:
                    raise NotImplementedError

                monkeypatch.setattr(loop, "add_reader", add_reader_unavailable)
                reader.setblocking(False)

                def receive() -> None:
                    s.wait_readable(reader)

                task = s.spawn(receive)
                with pytest.raises(RuntimeError, match="add_reader/add_writer"):
                    await s.arun_until_complete(task)
            finally:
                reader.close()
                writer.close()
                s.close()

        asyncio.run(run())

    @pytest.mark.parametrize("selector_type", _SELECTOR_TYPES)
    def test_selector_scheduler_waits_for_socket_io_with_selector_type(self, selector_type):
        selector = selector_type()
        s = SyncSelectorScheduler(selector=selector)
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
        s = SyncSelectorScheduler(selector=selector)
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
        s = SyncSelectorScheduler(selector=selector)
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
        s = SyncSelectorScheduler()
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
        s = SyncSelectorScheduler()
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

    def test_run_forever_sets_main_tealet_factory_from_task_factory(self):
        s = _new_scheduler(lambda: DefaultTaskFactory(task_constructor=PriorityTask))
        original_factory = _tealet.get_tealet_factory()
        seen = []

        def worker() -> None:
            main = _tealet.main()
            seen.append(
                (
                    isinstance(main, PriorityTask),
                    main.get_scheduler() is s,
                    main.priority,
                    _tealet.get_tealet_factory() is not original_factory,
                )
            )
            s.stop()

        s.call_soon(worker)
        s.run_forever()

        assert seen == [(True, True, task_module.TEALET_PRI_INF, True)]
        assert _tealet.get_tealet_factory() is original_factory

    def test_tealet_selector_event_loop_runs_asyncio_timer(self):
        s = SyncSelectorScheduler()
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
        s = SyncSelectorScheduler()
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
        s = SyncSelectorScheduler()
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

    def test_tealet_proactor_event_loop_runs_asyncio_socket_recv(self):
        s = SyncProactorScheduler()
        set_scheduler(s)

        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            def run_asyncio() -> bytes:
                loop = TealetProactorEventLoop()
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

    def test_tealet_proactor_event_loop_call_soon_threadsafe(self):
        s = SyncProactorScheduler()
        set_scheduler(s)

        def run_asyncio() -> str:
            loop = TealetProactorEventLoop()
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

    def test_arun_until_complete_default_budget_is_initial_runnable_count(self):
        s = AsyncScheduler()
        set_scheduler(s)
        future: Future[str] = Future()
        events: list[str] = []

        def first() -> None:
            events.append("first")

        def second() -> None:
            events.append("second")

        def complete() -> None:
            events.append("complete")
            future.set_result("done")

        async def asyncio_peer() -> None:
            events.append("asyncio")

        s.spawn(first)
        s.spawn(second)
        s.spawn(complete)

        async def run() -> None:
            peer = asyncio.create_task(asyncio_peer())
            assert await s.arun_until_complete(future) == "done"
            assert events == ["first", "second", "complete"]
            await peer

        asyncio.run(run())

    def test_arun_until_complete_explicit_budget_yields_to_asyncio(self):
        s = AsyncScheduler()
        set_scheduler(s)
        future: Future[str] = Future()
        events: list[str] = []

        def make_worker(index: int):
            def worker() -> None:
                events.append(f"task:{index}")
                if index == 4:
                    future.set_result("done")

            return worker

        async def asyncio_peer() -> None:
            events.append("asyncio")

        for index in range(5):
            s.spawn(make_worker(index))

        async def run() -> None:
            peer = asyncio.create_task(asyncio_peer())
            assert await s.arun_until_complete(future, yield_every=2) == "done"
            await peer

        asyncio.run(run())

        assert events == ["task:0", "task:1", "asyncio", "task:2", "task:3", "task:4"]

    def test_arun_until_complete_rejects_non_positive_yield_every(self):
        s = AsyncScheduler()
        set_scheduler(s)

        async def run() -> None:
            with pytest.raises(ValueError, match="yield_every"):
                await s.arun_until_complete(lambda: None, yield_every=0)

        asyncio.run(run())

    def test_arun_until_complete_sets_main_tealet_factory_from_task_factory(self):
        s = AsyncScheduler()
        s.set_task_factory(DefaultTaskFactory(task_constructor=PriorityTask))
        set_scheduler(s)
        original_factory = _tealet.get_tealet_factory()

        def worker():
            main = _tealet.main()
            return (
                isinstance(main, PriorityTask),
                main.get_scheduler() is s,
                main.priority,
                _tealet.get_tealet_factory() is not original_factory,
            )

        async def run() -> None:
            assert await s.arun_until_complete(worker) == (True, True, task_module.TEALET_PRI_INF, True)

        asyncio.run(run())
        assert _tealet.get_tealet_factory() is original_factory

    def test_arun_forever_stops(self):
        s = AsyncScheduler()
        set_scheduler(s)

        def stop_soon() -> None:
            s.call_soon(s.stop)

        s.spawn(stop_soon)

        async def run() -> None:
            await s.arun_forever()

        asyncio.run(run())

    def test_arun_forever_explicit_budget_yields_to_asyncio(self):
        s = AsyncScheduler()
        set_scheduler(s)
        events: list[str] = []

        def make_worker(index: int):
            def worker() -> None:
                events.append(f"task:{index}")
                if index == 4:
                    s.stop()

            return worker

        async def asyncio_peer() -> None:
            events.append("asyncio")

        for index in range(5):
            s.spawn(make_worker(index))

        async def run() -> None:
            peer = asyncio.create_task(asyncio_peer())
            await s.arun_forever(yield_every=2)
            await peer

        asyncio.run(run())

        assert events == ["task:0", "task:1", "asyncio", "task:2", "task:3", "task:4"]

    def test_arun_forever_rejects_non_positive_yield_every(self):
        s = AsyncScheduler()
        set_scheduler(s)

        async def run() -> None:
            with pytest.raises(ValueError, match="yield_every"):
                await s.arun_forever(yield_every=0)

        asyncio.run(run())

    def test_arun_forever_sets_main_tealet_factory_from_task_factory(self):
        s = AsyncScheduler()
        s.set_task_factory(DefaultTaskFactory(task_constructor=PriorityTask))
        set_scheduler(s)
        original_factory = _tealet.get_tealet_factory()
        seen = []

        def worker() -> None:
            main = _tealet.main()
            seen.append(
                (
                    isinstance(main, PriorityTask),
                    main.get_scheduler() is s,
                    main.priority,
                    _tealet.get_tealet_factory() is not original_factory,
                )
            )
            s.stop()

        s.call_soon(worker)

        async def run() -> None:
            await s.arun_forever()

        asyncio.run(run())
        assert seen == [(True, True, task_module.TEALET_PRI_INF, True)]
        assert _tealet.get_tealet_factory() is original_factory

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
        with s.main_context():
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

    def test_pump_count_limits_one_shot_task_completion_chain(self):
        s = _new_scheduler()
        seen: list[int] = []

        for index in range(5):
            s.spawn(lambda index=index: seen.append(index))

        s.pump(2)

        assert seen == [0, 1]

        s.run()

        assert seen == [0, 1, 2, 3, 4]

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

    def test_call_soon_threadsafe_immediate_runs_on_owner_thread(self):
        s = _new_scheduler()
        set_scheduler(s)
        seen: list[str] = []

        def worker() -> None:
            seen.append("before")
            s.call_soon_threadsafe(seen.append, "callback", immediate=True)
            seen.append("after")

        s.spawn(worker)
        s.run()

        assert seen == ["before", "callback", "after"]

    def test_call_soon_threadsafe_immediate_queues_from_other_thread(self):
        s = _new_scheduler()
        started = threading.Event()
        seen: list[int] = []

        s.call_later(60.0, lambda: None)
        s.call_soon(started.set)

        def run_forever_in_thread() -> None:
            set_scheduler(s)
            s.run_forever()

        t = threading.Thread(target=run_forever_in_thread)
        t.start()
        try:
            assert started.wait(timeout=1.0)
            caller_thread = threading.get_ident()

            def callback() -> None:
                seen.append(threading.get_ident())
                s.stop()

            s.call_soon_threadsafe(callback, immediate=True)
            t.join(timeout=1.0)
            assert not t.is_alive()
            assert seen and seen[0] != caller_thread
        finally:
            s.call_soon_threadsafe(s.stop)
            t.join(timeout=1.0)

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

    def test_sleep_zero_is_plain_yield(self):
        seen: list[str] = []

        class NoTimerScheduler(Scheduler):
            def call_later(self, delay, callback, *args, context=None):
                raise AssertionError("sleep(0) should not schedule a timer")

        s = NoTimerScheduler()
        set_scheduler(s)

        def sleeper() -> None:
            seen.append("before")
            sleep(0)
            seen.append("after")

        s.spawn(sleeper)
        s.spawn(lambda: seen.append("peer"))
        s.run()

        assert seen == ["before", "peer", "after"]

    def test_next_timer_deadline_is_absolute(self):
        s = _new_scheduler()
        now = 100.0
        s._time = lambda: now

        handle = s.call_later(5.0, lambda: None)

        assert handle.when == 105.0
        assert s._next_timer_deadline() == 105.0
        now = 104.0
        assert s._delay_until(handle.when) == 1.0
        now = 106.0
        assert s._delay_until(handle.when) == 0.0

    def test_sleep_schedules_absolute_deadline(self):
        seen: list[str] = []
        deadlines: list[float] = []

        class RecordingScheduler(Scheduler):
            def _sleep_until(self, when):
                deadlines.append(when)

        s = RecordingScheduler()
        s._time = lambda: 20.0
        set_scheduler(s)

        def sleeper() -> None:
            s.sleep(3.0)
            seen.append("after")

        s.spawn(sleeper)
        s.run()

        assert deadlines == [23.0]
        assert seen == ["after"]

    def test_async_scheduler_binds_to_running_loop_time(self):
        seen: list[float] = []

        async def orchestrate() -> None:
            loop = asyncio.get_running_loop()
            s = AsyncScheduler()
            s._time = lambda: -1.0
            set_scheduler(s)

            def worker() -> None:
                seen.append(s.time() - loop.time())

            s.spawn(worker)
            await asyncio.wait_for(s.arun(), timeout=1.0)

        asyncio.run(orchestrate())

        assert seen == pytest.approx([0.0], abs=0.001)

    def test_async_scheduler_bind_loop_rejects_different_loop(self):
        async def orchestrate() -> None:
            loop = asyncio.get_running_loop()
            other_loop = asyncio.new_event_loop()
            try:
                s = AsyncScheduler()
                s.bind_loop(loop)
                s.bind_loop(loop)
                with pytest.raises(RuntimeError, match="different event loop"):
                    s.bind_loop(other_loop)
            finally:
                other_loop.close()

        asyncio.run(orchestrate())

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

    def test_run_ignores_cancelled_pending_timer(self):
        s = _new_scheduler()
        handle = s.call_later(60.0, lambda: None)
        handle.cancel()

        s.run()

        assert s._next_timer_deadline() is None

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
        target_ref: dict[str, Task] = {}

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
        target_ref: dict[str, Task] = {}

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
        target_ref: dict[str, Task] = {}

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
        target_ref: dict[str, Task] = {}

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

    def test_await_socket_pair_exchange_hosted_in_asyncio(self):
        def entry() -> dict[str, object]:
            exchange = _SocketAwaitExchange()
            try:
                sender, receiver = exchange.spawn_tasks()
                gather(sender, receiver).wait()
                return dict(exchange.results)
            finally:
                exchange.close()

        assert run_in_asyncio(entry) == {
            "sent": len(_SocketAwaitExchange.PAYLOAD),
            "received": _SocketAwaitExchange.PAYLOAD,
        }

    def test_await_socket_pair_exchange_hosted_in_tealetio(self):
        async def entry() -> dict[str, object]:
            exchange = _SocketAwaitExchange()
            try:
                tasks = exchange.spawn_tasks()
                for _ in range(100):
                    if all(task.done() for task in tasks):
                        break
                    await asyncio.sleep(0)

                assert all(task.done() for task in tasks)
                for task in tasks:
                    task.result()
                return dict(exchange.results)
            finally:
                exchange.close()

        assert run_asyncio_in_tealet(entry()) == {
            "sent": len(_SocketAwaitExchange.PAYLOAD),
            "received": _SocketAwaitExchange.PAYLOAD,
        }

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

    def test_await_treats_shield_as_future_like_without_task(self, monkeypatch):
        s = AsyncScheduler()
        set_scheduler(s)
        seen: list[object] = []

        async def orchestrate() -> None:
            loop = asyncio.get_running_loop()
            async_future = loop.create_future()
            shielded = asyncio.shield(async_future)
            create_task_calls: list[object] = []
            original_create_task = loop.create_task

            def create_task(coro, *args, **kwargs):
                task = original_create_task(coro, *args, **kwargs)
                create_task_calls.append(coro)
                return task

            def worker() -> None:
                seen.append(s.await_(shielded))

            loop.call_soon(async_future.set_result, 17)
            s.spawn(worker)
            run_task = loop.create_task(s.arun())
            monkeypatch.setattr(loop, "create_task", create_task)
            await asyncio.wait_for(run_task, timeout=1.0)

            assert create_task_calls == []

        asyncio.run(orchestrate())

        assert seen == [17]

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

    def test_await_returns_synchronous_coroutine_without_task(self, monkeypatch):
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
                task = original_create_task(coro, *args, **kwargs)
                create_task_calls.append(coro)
                return task

            monkeypatch.setattr(loop, "create_task", create_task)
            s.spawn(worker)
            await asyncio.wait_for(s.arun(), timeout=1.0)
            delegated = [
                coro
                for coro in create_task_calls
                if getattr(coro, "cr_code", None) is compute.__code__
            ]
            assert delegated == []

        asyncio.run(orchestrate())

        assert seen == [("body", 0), ("result", 12), ("pending", 0)]

    def test_await_uses_current_context_and_clears_task_for_synchronous_coroutine(self):
        s = AsyncScheduler()
        set_scheduler(s)
        marker = contextvars.ContextVar("marker", default="unset")
        seen: list[object] = []

        async def compute() -> str:
            seen.append(("body-current", get_current()))
            seen.append(("body-before", marker.get()))
            marker.set("body-changed")
            seen.append(("body-after", marker.get()))
            return marker.get()

        def worker() -> None:
            marker.set("caller")
            seen.append(("worker-current-before", get_current() is not None))
            seen.append(("result", s.await_(compute())))
            seen.append(("caller-after", marker.get()))
            seen.append(("worker-current-after", get_current() is not None))

        s.spawn(worker)
        asyncio.run(asyncio.wait_for(s.arun(), timeout=1.0))

        assert seen == [
            ("worker-current-before", True),
            ("body-current", None),
            ("body-before", "caller"),
            ("body-after", "body-changed"),
            ("result", "body-changed"),
            ("caller-after", "body-changed"),
            ("worker-current-after", True),
        ]

    def test_await_pumps_none_yield_without_loop_task(self, monkeypatch):
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
                task = original_create_task(coro, *args, **kwargs)
                create_task_calls.append(coro)
                return task

            monkeypatch.setattr(loop, "create_task", create_task)
            s.spawn(worker)
            await asyncio.wait_for(s.arun(), timeout=1.0)
            delegated = [coro for coro in create_task_calls if getattr(coro, "cr_code", None) is compute.__code__]
            assert delegated == []

        asyncio.run(orchestrate())

        assert seen == ["before-await", "after-await", ("result", 13)]

    def test_await_asynkit_coro_drive_is_used_without_loop_task(self, monkeypatch):
        s = AsyncScheduler()
        set_scheduler(s)
        seen: list[object] = []
        drive_records: list[object] = []
        real_coro_drive = scheduler_module._coro_drive

        def coro_drive(coro, callback):
            drive_records.append(getattr(coro, "cr_code", None))
            return real_coro_drive(coro, callback)

        monkeypatch.setattr(scheduler_module, "_coro_drive", coro_drive)

        async def compute() -> int:
            seen.append("before-await")
            await asyncio.sleep(0)
            seen.append("after-await")
            return 15

        def worker() -> None:
            seen.append(("result", s.await_(compute())))

        async def orchestrate() -> None:
            loop = asyncio.get_running_loop()
            create_task_calls: list[object] = []
            original_create_task = loop.create_task

            def create_task(coro, *args, **kwargs):
                task = original_create_task(coro, *args, **kwargs)
                create_task_calls.append(coro)
                return task

            monkeypatch.setattr(loop, "create_task", create_task)
            s.spawn(worker)
            await asyncio.wait_for(s.arun(), timeout=1.0)
            delegated = [coro for coro in create_task_calls if getattr(coro, "cr_code", None) is compute.__code__]
            assert delegated == []

        asyncio.run(orchestrate())

        assert drive_records == [compute.__code__]
        assert seen == ["before-await", "after-await", ("result", 15)]

    def test_await_pumps_yielded_asyncio_future_without_loop_task(self, monkeypatch):
        s = AsyncScheduler()
        set_scheduler(s)
        seen: list[object] = []

        async def orchestrate() -> None:
            loop = asyncio.get_running_loop()
            async_future = loop.create_future()

            async def compute() -> int:
                seen.append("before-await")
                result = await async_future
                seen.append(("after-await", result))
                return result + 1

            def worker() -> None:
                seen.append(("result", s.await_(compute())))

            create_task_calls: list[object] = []
            original_create_task = loop.create_task

            def create_task(coro, *args, **kwargs):
                task = original_create_task(coro, *args, **kwargs)
                create_task_calls.append(coro)
                return task

            monkeypatch.setattr(loop, "create_task", create_task)
            loop.call_soon(async_future.set_result, 20)
            s.spawn(worker)
            await asyncio.wait_for(s.arun(), timeout=1.0)
            delegated = [coro for coro in create_task_calls if getattr(coro, "cr_code", None) is compute.__code__]
            assert delegated == []

        asyncio.run(orchestrate())

        assert seen == ["before-await", ("after-await", 20), ("result", 21)]

    def test_await_pumps_yielded_asyncio_future_exception(self):
        s = AsyncScheduler()
        set_scheduler(s)
        seen: list[object] = []

        async def orchestrate() -> None:
            loop = asyncio.get_running_loop()
            async_future = loop.create_future()

            async def compute() -> str:
                try:
                    await async_future
                except ValueError as exc:
                    seen.append(("caught", str(exc)))
                    return "handled"
                return "missed"

            def worker() -> None:
                seen.append(("result", s.await_(compute())))

            loop.call_soon(async_future.set_exception, ValueError("from future"))
            s.spawn(worker)
            await asyncio.wait_for(s.arun(), timeout=1.0)

        asyncio.run(orchestrate())

        assert seen == [("caught", "from future"), ("result", "handled")]

    def test_await_uses_same_context_for_blocked_continuation(self):
        s = AsyncScheduler()
        set_scheduler(s)
        marker = contextvars.ContextVar("marker", default="unset")
        seen: list[object] = []

        async def compute() -> str:
            seen.append(("body-current-before", get_current()))
            seen.append(("body-before", marker.get()))
            marker.set("body-before-await")
            await asyncio.sleep(0)
            seen.append(("body-current-after", get_current()))
            seen.append(("body-after-await", marker.get()))
            marker.set("body-after-await")
            return marker.get()

        def worker() -> None:
            marker.set("caller")
            seen.append(("worker-current-before", get_current() is not None))
            seen.append(("result", s.await_(compute())))
            seen.append(("caller-after", marker.get()))
            seen.append(("worker-current-after", get_current() is not None))

        s.spawn(worker)
        asyncio.run(asyncio.wait_for(s.arun(), timeout=1.0))

        assert seen == [
            ("worker-current-before", True),
            ("body-current-before", None),
            ("body-before", "caller"),
            ("body-current-after", None),
            ("body-after-await", "body-before-await"),
            ("result", "body-after-await"),
            ("caller-after", "body-after-await"),
            ("worker-current-after", True),
        ]

