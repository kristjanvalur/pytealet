import pytest

import _tealet
import tealet_examples as examples


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

    def test_timeout_demo(self):
        seen = examples.demo_future_timeout_then_success()
        assert seen == ["timeout_waiter:False", "success_waiter:True"]


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
