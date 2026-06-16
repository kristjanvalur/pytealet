import asyncio

import pytest

from tealet.runtime import AsyncRunner, Runner, run, run_async
from tealet.scheduler import SimpleScheduler, get_scheduler, set_scheduler


class TestAsyncRunner:
    def test_start_and_close_lifecycle(self):
        async def run() -> None:
            runner = AsyncRunner()
            scheduler = await runner.start()
            assert isinstance(scheduler, SimpleScheduler)
            assert runner.task is not None
            assert not runner.task.done()
            await runner.close()
            assert runner.task is None

        asyncio.run(run())

    def test_run_sync_callable_binds_runner_scheduler(self):
        async def run() -> None:
            runner = AsyncRunner()
            seen = []

            def entry() -> str:
                seen.append(get_scheduler())
                return "ok"

            result = await runner.run(entry)
            assert result == "ok"
            assert runner.get_scheduler() is not None
            assert seen == [runner.get_scheduler()]
            await runner.close()

        asyncio.run(run())

    def test_run_async_callable(self):
        async def run() -> None:
            runner = AsyncRunner()

            async def entry() -> str:
                await asyncio.sleep(0)
                return "done"

            result = await runner.run(entry)
            assert result == "done"
            await runner.close()

        asyncio.run(run())

    def test_scheduler_factory_is_used(self):
        async def run() -> None:
            custom = SimpleScheduler()
            runner = AsyncRunner(scheduler_factory=lambda: custom)
            started = await runner.start()
            assert started is custom
            await runner.close()

        asyncio.run(run())

    def test_async_runner_debug_sets_and_restores_loop_debug(self):
        async def run_case() -> None:
            loop = asyncio.get_running_loop()
            previous = loop.get_debug()
            runner = AsyncRunner(debug=not previous)
            try:
                await runner.start()
                assert loop.get_debug() is (not previous)
            finally:
                await runner.close()
            assert loop.get_debug() is previous

        asyncio.run(run_case())

    def test_async_runner_debug_sets_scheduler_debug_flag(self):
        async def run_case() -> None:
            custom = SimpleScheduler()
            runner = AsyncRunner(scheduler_factory=lambda: custom, debug=True)
            try:
                await runner.start()
                assert custom.get_debug() is True
            finally:
                await runner.close()

        asyncio.run(run_case())

    def test_invalid_factory_return_type(self):
        async def run() -> None:
            runner = AsyncRunner(scheduler_factory=lambda: object())
            with pytest.raises(TypeError, match="scheduler factory must return"):
                await runner.start()

        asyncio.run(run())

    def test_awaitable_entry_rejects_args(self):
        async def run() -> None:
            runner = AsyncRunner()
            with pytest.raises(TypeError, match="args/kwargs are not allowed"):
                await runner.run(asyncio.sleep(0), 1)
            await runner.close()

        asyncio.run(run())

    def test_run_async_helper(self):
        async def entry() -> int:
            return 42

        async def run() -> None:
            result = await run_async(entry)
            assert result == 42

        asyncio.run(run())


class TestRunner:
    def test_get_scheduler_lazy_init(self):
        runner = Runner()
        scheduler = runner.get_scheduler()
        assert isinstance(scheduler, SimpleScheduler)
        assert runner.get_scheduler() is scheduler

    def test_runner_debug_sets_scheduler_debug_flag(self):
        runner = Runner(debug=True)
        try:
            assert runner.get_scheduler().get_debug() is True
        finally:
            runner.close()

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
            seen.append(get_scheduler())
            return "ok"

        runner = Runner()
        try:
            result = runner.run(entry)
            assert result == "ok"
            assert runner.get_scheduler() is not None
            assert seen == [runner.get_scheduler()]
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
            first = runner.run(lambda: get_scheduler())
            second = runner.run(lambda: get_scheduler())
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
        from tealet.scheduler import Future

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

    def test_lazy_init_installs_and_restores_current_scheduler(self):
        previous = SimpleScheduler()
        set_scheduler(previous)

        runner = Runner()
        installed = runner.get_scheduler()
        assert get_scheduler() is installed

        runner.close()
        assert get_scheduler() is previous

        set_scheduler(None)

    def test_lazy_init_rejects_when_running_scheduler_exists(self):
        running = SimpleScheduler()
        running._running = True
        set_scheduler(running)

        runner = Runner()
        try:
            with pytest.raises(RuntimeError, match="another scheduler is running"):
                runner.get_scheduler()
        finally:
            running._running = False
            set_scheduler(None)


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
        previous = SimpleScheduler()
        set_scheduler(previous)
        try:
            assert run(lambda: "ok") == "ok"
            assert get_scheduler() is previous
        finally:
            set_scheduler(None)

    def test_run_helper_sets_scheduler_debug_flag(self):
        custom = SimpleScheduler()
        assert run(lambda: custom.get_debug(), scheduler_factory=lambda: custom, debug=True) is True
