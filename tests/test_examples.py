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
