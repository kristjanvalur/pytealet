import asyncio
import signal
import sys
import threading

import pytest

import _tealet
import tealetio.tasks as task_module
from tealetio import (
    AsyncRunner,
    AsyncScheduler,
    DefaultTaskFactory,
    Event,
    Future,
    PriorityTask,
    Runner,
    Scheduler,
    SyncSelectorScheduler,
    TealetSelectorEventLoop,
    asyncio_get_current,
    get_current,
    get_running_scheduler,
    run,
    run_async,
    run_asyncio_in_tealet,
    run_in_asyncio,
    set_scheduler,
)
from tealetio.scheduler import _current_scheduler


requires_runner_sigint = pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="Runner SIGINT parity follows asyncio.Runner, available in Python 3.11+",
)


class FakeSignals:
    def __init__(self, monkeypatch, initial_handler=signal.default_int_handler) -> None:
        self.handler = initial_handler
        self.installed: list[object] = []
        monkeypatch.setattr(signal, "getsignal", self.getsignal)
        monkeypatch.setattr(signal, "signal", self.signal)

    def getsignal(self, signum: int):
        assert signum == signal.SIGINT
        return self.handler

    def signal(self, signum: int, handler):
        assert signum == signal.SIGINT
        previous = self.handler
        self.handler = handler
        self.installed.append(handler)
        return previous


class TestAsyncioCurrentTaskAccessor:
    def test_raises_without_running_loop(self):
        with pytest.raises(RuntimeError, match="no running event loop"):
            asyncio_get_current()

    def test_returns_current_asyncio_task(self):
        async def run() -> None:
            assert asyncio_get_current() is asyncio.current_task()

        asyncio.run(run())

    def test_returns_none_inside_tealetio_task(self):
        async def run() -> None:
            scheduler = AsyncScheduler()
            set_scheduler(scheduler)
            seen = []

            def worker() -> None:
                assert asyncio.current_task() is not None
                seen.append(asyncio_get_current())

            scheduler.spawn(worker)
            await scheduler.arun()
            assert seen == [None]

        asyncio.run(run())

    def test_awaited_coroutine_switches_from_tealet_stack_to_asyncio_task(self):
        async def run() -> None:
            scheduler = AsyncScheduler()
            set_scheduler(scheduler)
            seen = []

            async def check_current() -> str:
                seen.append(("start", asyncio_get_current()))
                await asyncio.sleep(0)
                seen.append(("after", asyncio_get_current()))
                return "done"

            def worker() -> None:
                result = scheduler.await_(check_current())
                seen.append((result, asyncio_get_current()))

            scheduler.spawn(worker)
            await scheduler.arun()

            assert seen[0][0] == "start"
            assert isinstance(seen[0][1], asyncio.Task)
            assert seen[1][0] == "after"
            assert isinstance(seen[1][1], asyncio.Task)
            assert seen[2] == ("done", None)

        asyncio.run(run())

    def test_nested_asyncio_runner_returns_current_asyncio_task(self):
        async def entry() -> tuple[object, object]:
            await asyncio.sleep(0)
            return get_current(), asyncio_get_current()

        tealet_current, asyncio_current = run_asyncio_in_tealet(entry())

        assert tealet_current is None
        assert isinstance(asyncio_current, asyncio.Task)


class TestAsyncRunner:
    def test_get_scheduler_lazy_init_and_aclose_lifecycle(self):
        async def run() -> None:
            runner = AsyncRunner()
            scheduler = runner.get_scheduler()
            assert isinstance(scheduler, AsyncScheduler)
            assert runner.task is None
            await runner.aclose()
            assert runner.task is None

        asyncio.run(run())

    def test_run_sync_callable_binds_runner_scheduler(self):
        async def run() -> None:
            runner = AsyncRunner()
            seen = []

            def entry() -> str:
                seen.append(get_running_scheduler())
                return "ok"

            result = await runner.run(entry)
            assert result == "ok"
            assert runner.get_scheduler() is not None
            assert seen == [runner.get_scheduler()]
            await runner.aclose()

        asyncio.run(run())

    def test_run_sync_callable_sets_main_tealet_factory_from_task_factory(self):
        async def run() -> None:
            runner = AsyncRunner()
            scheduler = runner.get_scheduler()
            scheduler.set_task_factory(DefaultTaskFactory(task_constructor=PriorityTask))
            original_factory = _tealet.get_tealet_factory()

            def entry():
                main = _tealet.main()
                return (
                    isinstance(main, PriorityTask),
                    main.get_scheduler() is scheduler,
                    main.priority,
                    _tealet.get_tealet_factory() is not original_factory,
                )

            try:
                assert await runner.run(entry) == (True, True, task_module.TEALET_PRI_INF, True)
                assert _tealet.get_tealet_factory() is original_factory
            finally:
                await runner.aclose()

        asyncio.run(run())

    def test_run_rejects_awaitable_entry(self):
        async def run() -> None:
            runner = AsyncRunner()

            coro = asyncio.sleep(0)
            try:
                with pytest.raises(TypeError, match="entry must be a callable or Future"):
                    await runner.run(coro)
            finally:
                coro.close()
                await runner.aclose()

        asyncio.run(run())

    def test_scheduler_factory_is_used(self):
        async def run() -> None:
            custom = AsyncScheduler()
            runner = AsyncRunner(scheduler_factory=lambda: custom)
            started = runner.get_scheduler()
            assert started is custom
            await runner.aclose()

        asyncio.run(run())

    def test_async_runner_debug_does_not_mutate_loop_debug(self):
        async def run_case() -> None:
            loop = asyncio.get_running_loop()
            previous = loop.get_debug()
            runner = AsyncRunner(debug=not previous)
            try:
                runner.get_scheduler()
                assert loop.get_debug() is previous
            finally:
                await runner.aclose()
            assert loop.get_debug() is previous

        asyncio.run(run_case())

    def test_async_runner_debug_sets_scheduler_debug_flag(self):
        async def run_case() -> None:
            custom = AsyncScheduler()
            runner = AsyncRunner(scheduler_factory=lambda: custom, debug=True)
            try:
                runner.get_scheduler()
                assert custom.get_debug() is True
            finally:
                await runner.aclose()

        asyncio.run(run_case())

    def test_invalid_factory_return_type(self):
        async def run() -> None:
            class InvalidScheduler:
                def shutdown_default_executor(self, timeout=None) -> Future[object]:
                    future: Future[object] = Future()
                    future.set_result(None)
                    return future

                def close(self) -> None:
                    pass

            runner = AsyncRunner(scheduler_factory=InvalidScheduler)
            try:
                scheduler = runner.get_scheduler()
                with pytest.raises(AttributeError):
                    await scheduler.arun_until_complete(lambda: None)
            finally:
                runner._finalize_close(runner._scheduler)
                set_scheduler(None)

        asyncio.run(run())

    def test_aclose_prevents_reuse(self):
        async def run() -> None:
            runner = AsyncRunner()
            await runner.aclose()
            with pytest.raises(RuntimeError, match="runner is closed"):
                await runner.run(lambda: None)
            with pytest.raises(RuntimeError, match="runner is closed"):
                runner.get_scheduler()

        asyncio.run(run())

    def test_async_context_manager_initializes_and_closes(self):
        async def run() -> None:
            async with AsyncRunner() as runner:
                scheduler = runner.get_scheduler()
                assert isinstance(scheduler, AsyncScheduler)
                assert await runner.run(lambda: "ok") == "ok"

            with pytest.raises(RuntimeError, match="runner is closed"):
                runner.get_scheduler()

        asyncio.run(run())

    def test_aclose_cancels_unfinished_tasks(self):
        async def run() -> None:
            runner = AsyncRunner()
            scheduler = runner.get_scheduler()
            event = Event()
            seen: list[str] = []

            def worker() -> None:
                try:
                    seen.append("start")
                    event.swait()
                finally:
                    seen.append("finally")

            task = scheduler.spawn(worker)
            await scheduler.arun()

            await runner.aclose()

            assert seen == ["start", "finally"]
            assert task.done() is True
            assert task.cancelled() is True
            with pytest.raises(asyncio.CancelledError):
                task.result()
            assert scheduler.all_tasks() == set()

        asyncio.run(run())

    def test_aclose_waits_for_default_executor_shutdown(self, scheduler_task_factory_maker):
        async def run() -> None:
            runner = AsyncRunner()
            scheduler = runner.get_scheduler()
            scheduler.set_task_factory(scheduler_task_factory_maker())
            event = Event()
            release_worker = threading.Event()
            worker_started = threading.Event()
            worker_finished = threading.Event()

            def executor_worker() -> None:
                worker_started.set()
                release_worker.wait(timeout=1.0)
                worker_finished.set()

            def worker() -> None:
                scheduler.run_in_executor(None, executor_worker)
                assert worker_started.wait(timeout=1.0) is True
                try:
                    event.swait()
                finally:
                    release_worker.set()

            scheduler.spawn(worker, eager_start=False)
            await scheduler.arun()

            await runner.aclose()

            assert worker_finished.is_set() is True

        asyncio.run(run())

    def test_aclose_ignores_tasks_exited_by_prior_shutdown_cancel(self):
        async def run() -> None:
            class OrderedShutdownScheduler(AsyncScheduler):
                def __init__(self) -> None:
                    super().__init__()
                    self.shutdown_tasks = []

                def all_tasks(self):
                    if self.shutdown_tasks:
                        return self.shutdown_tasks
                    return super().all_tasks()

            runner = AsyncRunner(scheduler_factory=OrderedShutdownScheduler)
            scheduler = runner.get_scheduler()
            event = Event()
            seen: list[str] = []

            def victim() -> None:
                try:
                    seen.append("victim:start")
                    event.swait()
                finally:
                    seen.append("victim:finally")

            victim_task = scheduler.spawn(victim)

            def closer() -> None:
                try:
                    seen.append("closer:start")
                    event.swait()
                except asyncio.CancelledError:
                    seen.append("closer:cancelled")
                    victim_task.cancel()
                    raise

            closer_task = scheduler.spawn(closer)
            await scheduler.arun_until_complete(lambda: None)
            scheduler.shutdown_tasks = [closer_task, victim_task]

            await runner.aclose()

            assert seen == ["victim:start", "closer:start", "closer:cancelled", "victim:finally"]
            assert closer_task.done() is True
            assert victim_task.done() is True
            with pytest.raises(asyncio.CancelledError):
                closer_task.result()
            with pytest.raises(asyncio.CancelledError):
                victim_task.result()

        asyncio.run(run())

    def test_run_context_override_uses_explicit_context(self):
        async def run() -> None:
            import contextvars

            marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker", default="default")
            marker.set("ambient")

            runner_ctx = contextvars.copy_context()
            runner_ctx.run(marker.set, "runner-context")

            override_ctx = contextvars.copy_context()
            override_ctx.run(marker.set, "override-context")

            runner = AsyncRunner(context=runner_ctx)
            try:
                assert await runner.run(lambda: marker.get()) == "runner-context"
                assert await runner.run(lambda: marker.get(), context=override_ctx) == "override-context"
            finally:
                await runner.aclose()

        asyncio.run(run())

    def test_run_future_entry_waits(self):
        async def run() -> None:
            runner = AsyncRunner()
            try:
                future: Future[int] = Future()
                scheduler = runner.get_scheduler()
                scheduler.call_soon(future.set_result, 123)
                assert await runner.run(future) == 123
                assert future.done() is True
                assert future.result() == 123
            finally:
                await runner.aclose()

        asyncio.run(run())

    def test_run_async_helper(self):
        def entry() -> int:
            return 42

        async def run() -> None:
            result = await run_async(entry)
            assert result == 42

        asyncio.run(run())

    @requires_runner_sigint
    def test_sigint_cancels_main_task_and_raises_keyboard_interrupt(self, monkeypatch):
        signals = FakeSignals(monkeypatch)
        seen: list[str] = []

        def entry() -> None:
            seen.append("start")
            signals.handler(signal.SIGINT, None)
            try:
                get_running_scheduler().yield_()
            except asyncio.CancelledError:
                seen.append("cancelled")
                raise

        async def run_case() -> None:
            runner = AsyncRunner()
            try:
                with pytest.raises(KeyboardInterrupt):
                    await runner.run(entry)
            finally:
                await runner.aclose()

        asyncio.run(run_case())

        assert seen == ["start", "cancelled"]
        assert signals.handler is signal.default_int_handler

    @requires_runner_sigint
    def test_sigint_second_interrupt_raises_immediately(self, monkeypatch):
        signals = FakeSignals(monkeypatch)
        seen: list[str] = []

        def entry() -> None:
            seen.append("start")
            handler = signals.handler
            handler(signal.SIGINT, None)
            handler(signal.SIGINT, None)

        async def run_case() -> None:
            runner = AsyncRunner()
            try:
                with pytest.raises(KeyboardInterrupt):
                    await runner.run(entry)
            finally:
                await runner.aclose()

        asyncio.run(run_case())

        assert seen == ["start"]
        assert signals.handler is signal.default_int_handler

    @requires_runner_sigint
    def test_sigint_overrides_and_restores_outer_asyncio_runner_handler(self, monkeypatch):
        signals = FakeSignals(monkeypatch)

        async def run_case() -> None:
            outer_handler = signals.handler
            runner = AsyncRunner()
            try:
                assert await runner.run(lambda: signals.handler) is not outer_handler
                assert signals.handler is outer_handler
            finally:
                await runner.aclose()

        asyncio.run(run_case())

        assert signals.handler is signal.default_int_handler

    @requires_runner_sigint
    def test_handle_sigint_false_does_not_install_handler(self, monkeypatch):
        signals = FakeSignals(monkeypatch)

        async def run_case() -> None:
            outer_handler = signals.handler
            runner = AsyncRunner(handle_sigint=False)
            try:
                assert await runner.run(lambda: signals.handler) is outer_handler
                assert signals.handler is outer_handler
            finally:
                await runner.aclose()

        asyncio.run(run_case())

        assert signals.handler is signal.default_int_handler


class TestRunnerScopeComposition:
    def test_nested_sync_runners_restore_scheduler_stack(self):
        previous = Scheduler()
        set_scheduler(previous)

        outer = Runner()
        try:
            outer_scheduler = outer.get_scheduler()
            assert _current_scheduler() is outer_scheduler

            inner = Runner()
            try:
                inner_scheduler = inner.get_scheduler()
                assert _current_scheduler() is inner_scheduler
                assert inner.run(lambda: "inner") == "inner"
            finally:
                inner.close()

            assert _current_scheduler() is outer_scheduler
        finally:
            outer.close()
            assert _current_scheduler() is previous
            set_scheduler(None)

    def test_nested_async_runners_restore_scheduler_stack(self):
        async def run_case() -> None:
            previous = Scheduler()
            set_scheduler(previous)

            outer = AsyncRunner()
            try:
                outer_scheduler = outer.get_scheduler()
                assert _current_scheduler() is outer_scheduler

                inner = AsyncRunner()
                try:
                    inner_scheduler = inner.get_scheduler()
                    assert _current_scheduler() is inner_scheduler
                    assert await inner.run(lambda: "inner") == "inner"
                finally:
                    await inner.aclose()

                assert _current_scheduler() is outer_scheduler
            finally:
                await outer.aclose()
                assert _current_scheduler() is previous
                set_scheduler(None)

        asyncio.run(run_case())

    def test_sync_runner_rejects_init_inside_running_async_scheduler(self):
        async def run_case() -> None:
            runner = AsyncRunner()

            def entry() -> None:
                nested = Runner()
                with pytest.raises(RuntimeError, match="another scheduler is running"):
                    nested.get_scheduler()

            try:
                await runner.run(entry)
            finally:
                await runner.aclose()

        asyncio.run(run_case())

    def test_async_runner_rejects_init_inside_running_sync_scheduler(self):
        runner = Runner()

        def entry() -> None:
            nested = AsyncRunner()
            with pytest.raises(RuntimeError, match="another scheduler is running"):
                nested.get_scheduler()

        try:
            runner.run(entry)
        finally:
            runner.close()


class TestRunner:
    def test_get_scheduler_lazy_init(self):
        runner = Runner()
        scheduler = runner.get_scheduler()
        assert isinstance(scheduler, Scheduler)
        assert runner.get_scheduler() is scheduler

    def test_runner_debug_sets_scheduler_debug_flag(self):
        runner = Runner(debug=True)
        try:
            assert runner.get_scheduler().get_debug() is True
        finally:
            runner.close()

    def test_close_closes_factory_scheduler_resources(self):
        closed = False

        class ClosingScheduler(Scheduler):
            def close(self) -> None:
                nonlocal closed
                closed = True

        runner = Runner(scheduler_factory=ClosingScheduler)
        runner.get_scheduler()

        runner.close()

        assert closed is True

    def test_run_sync_callable(self):
        runner = Runner()
        try:
            result = runner.run(lambda: "ok")
            assert result == "ok"
        finally:
            runner.close()

    def test_run_sync_callable_binds_runner_scheduler(self):
        seen = []

        def entry() -> str:
            seen.append(get_running_scheduler())
            return "ok"

        runner = Runner()
        try:
            result = runner.run(entry)
            assert result == "ok"
            assert runner.get_scheduler() is not None
            assert seen == [runner.get_scheduler()]
        finally:
            runner.close()

    def test_run_sync_callable_sets_main_tealet_factory_from_task_factory(self):
        runner = Runner()
        scheduler = runner.get_scheduler()
        scheduler.set_task_factory(DefaultTaskFactory(task_constructor=PriorityTask))
        original_factory = _tealet.get_tealet_factory()

        def entry():
            main = _tealet.main()
            return (
                isinstance(main, PriorityTask),
                main.get_scheduler() is scheduler,
                main.priority,
                _tealet.get_tealet_factory() is not original_factory,
            )

        try:
            assert runner.run(entry) == (True, True, task_module.TEALET_PRI_INF, True)
            assert _tealet.get_tealet_factory() is original_factory
        finally:
            runner.close()

    def test_run_awaitable_entry_raises(self):
        async def entry() -> str:
            await asyncio.sleep(0)
            return "done"

        coro = entry()
        runner = Runner()
        try:
            with pytest.raises(TypeError, match="entry must be a callable or Future"):
                runner.run(coro)
        finally:
            coro.close()
            runner.close()

    def test_context_manager(self):
        with Runner() as runner:
            assert runner.run(lambda: 7) == 7

    def test_run_multiple_times_reuses_scheduler(self):
        runner = Runner()
        try:
            first = runner.run(lambda: get_running_scheduler())
            second = runner.run(lambda: get_running_scheduler())
            assert first is second
            assert first is runner.get_scheduler()
        finally:
            runner.close()

    def test_run_uses_runner_creation_context(self):
        import contextvars
        marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker", default="default")
        marker.set("ambient")
        ctx = contextvars.copy_context()
        ctx.run(marker.set, "runner-context")

        runner = Runner(context=ctx)
        try:
            assert runner.run(lambda: marker.get()) == "runner-context"
        finally:
            runner.close()

    def test_run_context_override_uses_explicit_context(self):
        import contextvars

        marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker", default="default")
        marker.set("ambient")

        runner_ctx = contextvars.copy_context()
        runner_ctx.run(marker.set, "runner-context")

        override_ctx = contextvars.copy_context()
        override_ctx.run(marker.set, "override-context")

        runner = Runner(context=runner_ctx)
        try:
            assert runner.run(lambda: marker.get()) == "runner-context"
            assert runner.run(lambda: marker.get(), context=override_ctx) == "override-context"
        finally:
            runner.close()

    def test_run_future_entry_waits(self):
        runner = Runner()
        try:
            future: Future[int] = Future()
            scheduler = runner.get_scheduler()
            scheduler.call_soon(future.set_result, 123)

            # Runner future-entry path wraps future.wait(), which returns the result.
            assert runner.run(future) == 123
            assert future.done() is True
            assert future.result() == 123
        finally:
            runner.close()

    def test_close_prevents_reuse(self):
        runner = Runner()
        runner.close()
        with pytest.raises(RuntimeError, match="runner is closed"):
            runner.run(lambda: None)
        with pytest.raises(RuntimeError, match="runner is closed"):
            runner.get_scheduler()

    def test_close_cancels_unfinished_tasks(self):
        runner = Runner()
        scheduler = runner.get_scheduler()
        event = Event()
        seen: list[str] = []

        def worker() -> None:
            try:
                seen.append("start")
                event.swait()
            finally:
                seen.append("finally")

        task = scheduler.spawn(worker)
        scheduler.pump(1)

        runner.close()

        assert seen == ["start", "finally"]
        assert task.done() is True
        assert task.cancelled() is True
        with pytest.raises(asyncio.CancelledError):
            task.result()
        assert scheduler.all_tasks() == set()

    def test_close_waits_for_default_executor_shutdown(self, scheduler_task_factory_maker):
        runner = Runner()
        scheduler = runner.get_scheduler()
        scheduler.set_task_factory(scheduler_task_factory_maker())
        event = Event()
        release_worker = threading.Event()
        worker_started = threading.Event()
        worker_finished = threading.Event()

        def executor_worker() -> None:
            worker_started.set()
            release_worker.wait(timeout=1.0)
            worker_finished.set()

        def worker() -> None:
            scheduler.run_in_executor(None, executor_worker)
            assert worker_started.wait(timeout=1.0) is True
            try:
                event.swait()
            finally:
                release_worker.set()

        scheduler.spawn(worker, eager_start=False)
        scheduler.pump(1)

        runner.close()

        assert worker_finished.is_set() is True

    def test_close_ignores_tasks_exited_by_prior_shutdown_cancel(self):
        class OrderedShutdownScheduler(Scheduler):
            def __init__(self) -> None:
                super().__init__()
                self.shutdown_tasks = []

            def all_tasks(self):
                if self.shutdown_tasks:
                    return self.shutdown_tasks
                return super().all_tasks()

        runner = Runner(scheduler_factory=OrderedShutdownScheduler)
        scheduler = runner.get_scheduler()
        event = Event()
        seen: list[str] = []

        def victim() -> None:
            try:
                seen.append("victim:start")
                event.swait()
            finally:
                seen.append("victim:finally")

        victim_task = scheduler.spawn(victim)

        def closer() -> None:
            try:
                seen.append("closer:start")
                event.swait()
            except asyncio.CancelledError:
                seen.append("closer:cancelled")
                victim_task.cancel()
                raise

        closer_task = scheduler.spawn(closer)
        scheduler.pump(2)
        scheduler.shutdown_tasks = [closer_task, victim_task]

        runner.close()

        assert seen == ["victim:start", "closer:start", "closer:cancelled", "victim:finally"]
        assert closer_task.done() is True
        assert victim_task.done() is True
        with pytest.raises(asyncio.CancelledError):
            closer_task.result()
        with pytest.raises(asyncio.CancelledError):
            victim_task.result()

    def test_lazy_init_installs_and_restores_current_scheduler(self):
        previous = Scheduler()
        set_scheduler(previous)

        runner = Runner()
        installed = runner.get_scheduler()
        assert _current_scheduler() is installed

        runner.close()
        assert _current_scheduler() is previous

        set_scheduler(None)

    def test_lazy_init_rejects_when_running_scheduler_exists(self):
        running = Scheduler()
        running._running = True
        set_scheduler(running)

        runner = Runner()
        try:
            with pytest.raises(RuntimeError, match="another scheduler is running"):
                runner.get_scheduler()
        finally:
            running._running = False
            set_scheduler(None)

    @requires_runner_sigint
    def test_sigint_cancels_main_task_and_raises_keyboard_interrupt(self, monkeypatch):
        signals = FakeSignals(monkeypatch)
        seen: list[str] = []

        def entry() -> None:
            seen.append("start")
            signals.handler(signal.SIGINT, None)
            try:
                get_running_scheduler().yield_()
            except asyncio.CancelledError:
                seen.append("cancelled")
                raise

        runner = Runner()
        try:
            with pytest.raises(KeyboardInterrupt):
                runner.run(entry)
        finally:
            runner.close()

        assert seen == ["start", "cancelled"]
        assert signals.handler is signal.default_int_handler

    @requires_runner_sigint
    def test_sigint_second_interrupt_raises_immediately(self, monkeypatch):
        signals = FakeSignals(monkeypatch)
        seen: list[str] = []

        def entry() -> None:
            seen.append("start")
            handler = signals.handler
            handler(signal.SIGINT, None)
            handler(signal.SIGINT, None)

        runner = Runner()
        try:
            with pytest.raises(KeyboardInterrupt):
                runner.run(entry)
        finally:
            runner.close()

        assert seen == ["start"]
        assert signals.handler is signal.default_int_handler

    @requires_runner_sigint
    def test_handle_sigint_false_does_not_install_handler(self, monkeypatch):
        signals = FakeSignals(monkeypatch)

        runner = Runner(handle_sigint=False)
        try:
            assert runner.run(lambda: signals.handler) is signal.default_int_handler
        finally:
            runner.close()

        assert signals.installed == []
        assert signals.handler is signal.default_int_handler


class TestRunHelper:
    def test_run_helper_runs_callable(self):
        assert run(lambda: 42) == 42

    def test_run_helper_uses_context_override(self):
        import contextvars

        marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker", default="default")
        marker.set("ambient")
        ctx = contextvars.copy_context()
        ctx.run(marker.set, "helper-context")

        assert run(lambda: marker.get(), context=ctx) == "helper-context"

    def test_run_helper_restores_previous_scheduler(self):
        previous = Scheduler()
        set_scheduler(previous)
        try:
            assert run(lambda: "ok") == "ok"
            assert _current_scheduler() is previous
        finally:
            set_scheduler(None)

    def test_run_helper_sets_scheduler_debug_flag(self):
        custom = Scheduler()
        assert run(lambda: custom.get_debug(), scheduler_factory=lambda: custom, debug=True) is True

    def test_run_in_asyncio_helper_runs_callable(self):
        assert run_in_asyncio(lambda: 42) == 42

    def test_run_in_asyncio_helper_uses_context_override(self):
        import contextvars

        marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker", default="default")
        marker.set("ambient")
        ctx = contextvars.copy_context()
        ctx.run(marker.set, "helper-context")

        assert run_in_asyncio(lambda: marker.get(), context=ctx) == "helper-context"

    def test_run_in_asyncio_helper_restores_previous_scheduler(self):
        previous = Scheduler()
        set_scheduler(previous)
        try:
            assert run_in_asyncio(lambda: "ok") == "ok"
            assert _current_scheduler() is previous
        finally:
            set_scheduler(None)

    def test_run_in_asyncio_helper_sets_scheduler_debug_flag(self):
        custom = AsyncScheduler()
        assert run_in_asyncio(lambda: custom.get_debug(), scheduler_factory=lambda: custom, debug=True) is True

    def test_run_in_asyncio_helper_sets_loop_debug_flag(self):
        assert run_in_asyncio(lambda: asyncio.get_running_loop().get_debug(), debug=True) is True

    def test_run_in_asyncio_helper_uses_loop_factory(self):
        loops: list[asyncio.AbstractEventLoop] = []

        def loop_factory() -> asyncio.AbstractEventLoop:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loops.append(loop)
            return loop

        seen_loop = run_in_asyncio(asyncio.get_running_loop, loop_factory=loop_factory)

        assert seen_loop is loops[0]
        assert loops[0].is_closed() is True

    def test_run_asyncio_in_tealet_helper_runs_async_callable(self):
        async def entry() -> str:
            await asyncio.sleep(0.001)
            return "done"

        assert run_asyncio_in_tealet(entry()) == "done"

    def test_run_asyncio_in_tealet_yields_to_sibling_tealets(self):
        events: list[str] = []

        async def entry() -> tuple[str, ...]:
            scheduler = get_running_scheduler()
            scheduler.spawn(lambda: events.append("peer"))
            for _ in range(10):
                await asyncio.sleep(0)
                if events:
                    break
            events.append("entry")
            return tuple(events)

        assert run_asyncio_in_tealet(entry()) == ("peer", "entry")

    def test_run_asyncio_in_tealet_helper_uses_tealet_selector_loop(self):
        async def entry() -> asyncio.AbstractEventLoop:
            return asyncio.get_running_loop()

        loop = run_asyncio_in_tealet(entry())

        assert isinstance(loop, TealetSelectorEventLoop)
        assert loop.is_closed() is True

    def test_run_asyncio_in_tealet_helper_sets_loop_debug_flag(self):
        async def entry() -> bool:
            return asyncio.get_running_loop().get_debug()

        assert run_asyncio_in_tealet(entry(), debug=True) is True

    def test_run_asyncio_in_tealet_helper_uses_context_override(self):
        import contextvars

        marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker", default="default")
        marker.set("ambient")
        ctx = contextvars.copy_context()
        ctx.run(marker.set, "helper-context")

        async def entry() -> str:
            return marker.get()

        assert run_asyncio_in_tealet(entry(), context=ctx) == "helper-context"

    def test_run_asyncio_in_tealet_closes_factory_scheduler(self):
        closed = False

        class ClosingSelectorScheduler(SyncSelectorScheduler):
            def close(self) -> None:
                nonlocal closed
                closed = True
                super().close()

        async def entry() -> str:
            return "done"

        assert run_asyncio_in_tealet(entry(), scheduler_factory=ClosingSelectorScheduler) == "done"
        assert closed is True

    @requires_runner_sigint
    def test_run_asyncio_in_tealet_leaves_sigint_for_asyncio_runner(self, monkeypatch):
        signals = FakeSignals(monkeypatch)
        seen: list[str] = []

        async def entry() -> None:
            seen.append("start")
            signals.handler(signal.SIGINT, None)
            try:
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                seen.append("cancelled")
                raise

        with pytest.raises(KeyboardInterrupt):
            run_asyncio_in_tealet(entry())

        assert seen == ["start", "cancelled"]
        assert signals.handler is signal.default_int_handler


class TestRunnerDefaultFactoryOverride:
    def test_runner_subclass_default_factory_is_used(self):
        custom = Scheduler()

        class CustomRunner(Runner):
            default_factory = staticmethod(lambda: custom)

        runner = CustomRunner()
        try:
            assert runner.get_scheduler() is custom
        finally:
            runner.close()

    def test_async_runner_subclass_default_factory_is_used(self):
        custom = AsyncScheduler()

        class CustomAsyncRunner(AsyncRunner):
            default_factory = staticmethod(lambda: custom)

        async def run_case() -> None:
            runner = CustomAsyncRunner()
            try:
                assert runner.get_scheduler() is custom
            finally:
                await runner.aclose()

        asyncio.run(run_case())
