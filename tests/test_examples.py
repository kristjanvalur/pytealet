import asyncio

import pytest

import _tealet
import tealet_examples as examples


@pytest.fixture(autouse=True)
def _reset_examples_scheduler_tls():
    examples._scheduler.instance = examples.SimpleScheduler()
    try:
        yield
    finally:
        examples._scheduler.instance = examples.SimpleScheduler()


class TestRawSimpleGenerator:
    def test_raw_simple_generator_yields_then_stops(self):
        t = _tealet.tealet().prepare(examples.raw_simple_generator)

        assert t.switch([10, 20, 30]) == 10
        assert t.switch(None) == 20
        assert t.switch("ignored-input") == 30

        with pytest.raises(StopIteration):
            t.switch(None)

        assert t.state == _tealet.STATE_EXIT


class TestGeneratorTealet:
    def test_generator_tealet_iter_protocol(self):
        gen = examples.simple_generator([1, 2, 3])
        assert list(gen) == [1, 2, 3]

    def test_generator_tealet_next_and_exhaustion(self):
        gen = examples.simple_generator(["a", "b"])

        assert next(gen) == "a"
        assert next(gen) == "b"

        with pytest.raises(StopIteration):
            next(gen)


class TestSchedulerExamples:
    def test_append_with_yield_demo(self):
        seen = examples.demo_scheduler_append_with_yield()
        assert seen == ["a0", "b0", "c0", "a1", "b1", "a2"]

    def test_wait_for_event_start_demo(self):
        seen = examples.demo_wait_for_event_start()
        assert seen == ["waiter:waiting", "starter:set", "waiter:started"]

    def test_sleep_demo(self):
        seen = examples.demo_sleep()
        assert seen == ["before:sleep", "after:sleep"]

    def test_timer_handle_cancel(self):
        s = examples.scheduler()
        seen: list[str] = []

        def mark() -> None:
            seen.append("fired")

        handle = s.call_later(0.001, mark)
        handle.cancel()
        s.run()

        assert handle.cancelled()
        assert seen == []

    def test_event_wait_timeout_and_success(self):
        s = examples.scheduler()
        evt = examples.Event()
        seen: list[str] = []

        def timeout_waiter() -> None:
            seen.append(f"timeout={evt.wait(timeout=0.001)}")

        def success_waiter() -> None:
            seen.append(f"success={evt.wait(timeout=0.01)}")

        def setter() -> None:
            s.sleep(0.002)
            evt.set()

        s.spawn(timeout_waiter)
        s.spawn(success_waiter)
        s.spawn(setter)
        s.run()

        assert seen == ["timeout=False", "success=True"]

    def test_timeout_context_event_wait_timeout_and_success(self):
        s = examples.scheduler()
        evt = examples.Event()
        seen: list[str] = []

        def timeout_waiter() -> None:
            tm = examples.timeout(0.001)
            with pytest.raises(examples.TimeoutError, match="Operation timed out"):
                with tm:
                    evt.wait()
            seen.append(f"timeout={tm.expired()}")

        def success_waiter() -> None:
            tm = examples.timeout(0.01)
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
        seen = examples.demo_future_timeout_then_success()
        assert seen == ["timeout_waiter:False", "success_waiter:True"]

    def test_arun_runs_inside_asyncio_task(self):
        s = examples.scheduler()
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

    def test_unlink_removes_waiting_tealet_from_event(self):
        s = examples.scheduler()
        evt = examples.Event()
        seen: list[str] = []
        waiter_ref: dict[str, examples.ScheduledTealet] = {}

        def waiter() -> None:
            waiter_ref["t"] = _tealet.current()
            seen.append("waiter:waiting")
            evt.wait()
            seen.append("waiter:resumed")

        def canceller() -> None:
            waiter_ref["t"].unlink()
            seen.append("canceller:unlinked")

        s.spawn(waiter)
        s.spawn(canceller)
        s.run()

        assert seen == ["waiter:waiting", "canceller:unlinked"]

    def test_run_switches_immediately_to_target(self):
        s = examples.scheduler()
        evt = examples.Event()
        seen: list[str] = []
        target_ref: dict[str, examples.ScheduledTealet] = {}

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
        s = examples.scheduler()
        evt = examples.Event()
        seen: list[str] = []
        target_ref: dict[str, examples.ScheduledTealet] = {}

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


class TestFutureExamples:
    def test_future_demo(self):
        seen = examples.demo_future_result()
        assert seen == ["producer:start", "producer:done", "consumer:result=42"]

    def test_future_exception_propagates(self):
        s = examples.scheduler()

        def boom():
            raise ValueError("boom")

        future = s.spawn(boom)
        s.run()

        assert future.done()
        with pytest.raises(ValueError, match="boom"):
            future.result()
        assert isinstance(future.exception(), ValueError)

    def test_future_set_result_once(self):
        future = examples.Future()
        future.set_result(123)

        assert future.done()
        assert future.result() == 123
        assert future.exception() is None

        with pytest.raises(examples.InvalidStateError):
            future.set_result(456)

    def test_future_result_timeout(self):
        s = examples.scheduler()
        future: examples.Future[int] = examples.Future()
        seen: list[str] = []

        def complete_later() -> None:
            s.sleep(0.01)
            future.set_result(1)

        def waiter() -> None:
            with pytest.raises(TimeoutError, match="Future timed out"):
                future.result(timeout=0.001)
            seen.append("timed-out")
            seen.append(f"value={future.result()}")

        s.spawn(complete_later)
        s.spawn(waiter)
        s.run()
        assert seen == ["timed-out", "value=1"]

    def test_timeout_context_future_result_timeout(self):
        s = examples.scheduler()
        future: examples.Future[int] = examples.Future()
        seen: list[str] = []

        def complete_later() -> None:
            s.sleep(0.01)
            future.set_result(1)

        def waiter() -> None:
            tm = examples.timeout(0.001)
            with pytest.raises(examples.TimeoutError, match="Operation timed out"):
                with tm:
                    future.result()
            seen.append(f"timed-out={tm.expired()}")
            seen.append(f"value={future.result()}")

        s.spawn(complete_later)
        s.spawn(waiter)
        s.run()
        assert seen == ["timed-out=True", "value=1"]
