import asyncio
import contextvars

import pytest

from tealetio import (
    AsyncScheduler,
    CancelledError,
    Event,
    Future,
    InvalidStateError,
    Task,
    TimeoutError,
    set_scheduler,
    shield,
    timeout,
)
from tealetio.examples import demo_future_result
from tealetio.locks import RawTimeoutError

from helpers import new_scheduler as _new_scheduler


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
        waiter_ref: dict[str, Task] = {}

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
        waiter_ref: dict[str, Task] = {}

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
            waiter_ref: dict[str, Task] = {}

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
            waiter_ref: dict[str, Task] = {}

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
        with s.main_context():
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
