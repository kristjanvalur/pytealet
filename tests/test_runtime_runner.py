import asyncio

import pytest

from tealet.runtime import AsyncRunner, Runner, run_async
from tealet.scheduler import SimpleScheduler, get_scheduler


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

    def test_run_async_callable_raises(self):
        async def entry() -> str:
            await asyncio.sleep(0)
            return "done"

        runner = Runner()
        try:
            with pytest.raises(TypeError, match="sync runner entry must be synchronous"):
                runner.run(entry)
        finally:
            runner.close()

    def test_context_manager(self):
        with Runner() as runner:
            assert runner.run(lambda: 7) == 7
