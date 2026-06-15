import asyncio

import pytest

from tealet.runtime import Runner, run_async
from tealet.scheduler import SimpleScheduler, get_scheduler


class TestRuntimeRunner:
    def test_start_and_close_lifecycle(self):
        async def run() -> None:
            runner = Runner()
            scheduler = await runner.start()
            assert isinstance(scheduler, SimpleScheduler)
            assert runner.task is not None
            assert not runner.task.done()
            await runner.close()
            assert runner.task is None

        asyncio.run(run())

    def test_run_sync_callable_binds_runner_scheduler(self):
        async def run() -> None:
            runner = Runner()
            seen = []

            def entry() -> str:
                seen.append(get_scheduler())
                return "ok"

            result = await runner.run(entry)
            assert result == "ok"
            assert runner.scheduler is not None
            assert seen == [runner.scheduler]
            await runner.close()

        asyncio.run(run())

    def test_run_async_callable(self):
        async def run() -> None:
            runner = Runner()

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
            runner = Runner(scheduler_factory=lambda: custom)
            started = await runner.start()
            assert started is custom
            await runner.close()

        asyncio.run(run())

    def test_invalid_factory_return_type(self):
        async def run() -> None:
            runner = Runner(scheduler_factory=lambda: object())
            with pytest.raises(TypeError, match="scheduler factory must return"):
                await runner.start()

        asyncio.run(run())

    def test_awaitable_entry_rejects_args(self):
        async def run() -> None:
            runner = Runner()
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
