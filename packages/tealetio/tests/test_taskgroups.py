from __future__ import annotations

import pytest

from helpers import new_scheduler as _new_scheduler
from tealetio import (
    CancelledError,
    Event,
    ExceptionGroup,
    TaskGroup,
    set_scheduler,
    timeout,
    TimeoutError,
)


class TestTaskGroup:
    def test_joins_before_with_exits(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)
        seen: list[str] = []

        def child(name: str) -> None:
            s.yield_()
            seen.append(name)

        def parent() -> None:
            with TaskGroup() as group:
                group.spawn(lambda: child("a"))
                group.spawn(lambda: child("b"))
                seen.append("body")
            seen.append("after")

        s.run_until_complete(parent)
        assert seen[0] == "body"
        assert set(seen[1:3]) == {"a", "b"}
        assert seen[-1] == "after"

    def test_empty_group_is_a_no_op(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)

        def parent() -> str:
            with TaskGroup():
                pass
            return "ok"

        assert s.run_until_complete(parent) == "ok"

    def test_child_error_cancels_siblings_and_raises_group(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)
        sibling_cancelled = Event()
        started = Event()

        def fail() -> None:
            started.swait()
            raise ValueError("boom")

        def sibling() -> None:
            try:
                Event().swait()
            except CancelledError:
                sibling_cancelled.set()
                raise

        def parent() -> None:
            with TaskGroup() as group:
                group.spawn(fail)
                group.spawn(sibling)
                started.set()
                Event().swait()

        with pytest.raises(ExceptionGroup) as caught:
            s.run_until_complete(parent)
        assert any(isinstance(exc, ValueError) and str(exc) == "boom" for exc in caught.value.exceptions)
        assert sibling_cancelled.is_set()

    def test_create_task_is_spawn(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)

        def parent() -> int:
            with TaskGroup() as group:
                task = group.create_task(lambda: 7)
            return task.result()

        assert s.run_until_complete(parent) == 7

    def test_cancel_does_not_fail_the_group(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)
        cancelled = Event()

        def worker() -> None:
            try:
                Event().swait()
            except CancelledError:
                cancelled.set()
                raise

        def parent() -> str:
            with TaskGroup() as group:
                group.spawn(worker)
                s.yield_()
                group.cancel()
            return "ok"

        assert s.run_until_complete(parent) == "ok"
        assert cancelled.is_set()

    def test_body_exception_cancels_children(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)
        cancelled = Event()

        def worker() -> None:
            try:
                Event().swait()
            except CancelledError:
                cancelled.set()
                raise

        def parent() -> None:
            with TaskGroup() as group:
                group.spawn(worker)
                s.yield_()
                raise RuntimeError("body")

        with pytest.raises(ExceptionGroup) as caught:
            s.run_until_complete(parent)
        assert any(isinstance(exc, RuntimeError) and str(exc) == "body" for exc in caught.value.exceptions)
        assert cancelled.is_set()

    def test_parent_cancel_cancels_children(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)
        child_cancelled = Event()
        parent_parked = Event()

        def worker() -> None:
            try:
                Event().swait()
            except CancelledError:
                child_cancelled.set()
                raise

        def parent() -> None:
            with TaskGroup() as group:
                group.spawn(worker)
                s.yield_()
                parent_parked.set()
                Event().swait()

        task = s.spawn(parent)

        def cancel_when_ready() -> None:
            parent_parked.swait()
            task.cancel()

        s.spawn(cancel_when_ready)
        with pytest.raises(CancelledError):
            s.run_until_complete(task)
        assert child_cancelled.is_set()

    def test_parent_cancel_during_join_still_cancels(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)
        child_cancelled = Event()
        child_parked = Event()

        def worker() -> None:
            child_parked.set()
            try:
                Event().swait()
            except CancelledError:
                child_cancelled.set()
                raise

        def parent() -> None:
            with TaskGroup() as group:
                group.spawn(worker)
                child_parked.swait()

        task = s.spawn(parent)

        def cancel_when_joining() -> None:
            child_parked.swait()
            s.yield_()
            task.cancel()

        s.spawn(cancel_when_joining)
        with pytest.raises(CancelledError):
            s.run_until_complete(task)
        assert child_cancelled.is_set()

    def test_timeout_around_group_still_becomes_timeout_error(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)
        child_task: list = []

        def parent() -> None:
            with timeout(1e9) as timer:
                with TaskGroup() as group:
                    child_task.append(group.spawn(lambda: Event().swait()))
                    s.yield_()
                    timer.reschedule(s.time())
                    Event().swait()

        with pytest.raises(TimeoutError):
            s.run_until_complete(parent)
        assert child_task[0].cancelled() is True

    def test_timeout_during_join_aborts_children_then_times_out(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)
        child_cancelled = Event()
        child_task: list = []

        def worker() -> None:
            try:
                Event().swait()
            except CancelledError:
                child_cancelled.set()
                raise

        def parent() -> None:
            with timeout(1e9) as timer:
                with TaskGroup() as group:
                    child_task.append(group.spawn(worker))
                    s.yield_()
                    timer.reschedule(s.time())

        with pytest.raises(TimeoutError):
            s.run_until_complete(parent)
        assert child_task[0].cancelled() is True
        assert child_cancelled.is_set()

    def test_child_errors_win_over_body_timeout(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)

        def fail() -> None:
            raise ValueError("boom")

        def parent() -> None:
            with timeout(1e9) as timer:
                with TaskGroup() as group:
                    group.spawn(fail)
                    s.yield_()
                    timer.reschedule(s.time())
                    Event().swait()

        with pytest.raises(ExceptionGroup) as caught:
            s.run_until_complete(parent)
        assert any(isinstance(exc, ValueError) and str(exc) == "boom" for exc in caught.value.exceptions)

    def test_nested_group_failure_is_one_error_in_the_outer(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)

        def parent() -> None:
            with TaskGroup() as outer:

                def fail() -> None:
                    raise ValueError("inner")

                def inner_body() -> None:
                    with TaskGroup() as inner:
                        inner.spawn(fail)
                        Event().swait()

                outer.spawn(inner_body)
                Event().swait()

        with pytest.raises(ExceptionGroup) as caught:
            s.run_until_complete(parent)
        inner_groups = [exc for exc in caught.value.exceptions if isinstance(exc, ExceptionGroup)]
        assert inner_groups
        assert any(isinstance(exc, ValueError) and str(exc) == "inner" for exc in inner_groups[0].exceptions)

    def test_spawn_before_enter_raises(self):
        group = TaskGroup()
        with pytest.raises(RuntimeError, match="has not been entered"):
            group.spawn(lambda: None)

    def test_spawn_after_finish_raises(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)
        holder: dict[str, TaskGroup] = {}

        def parent() -> None:
            with TaskGroup() as group:
                holder["g"] = group
            group.spawn(lambda: None)

        with pytest.raises(RuntimeError, match="is finished"):
            s.run_until_complete(parent)

    def test_spawn_while_shutting_down_raises(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)

        def parent() -> None:
            with TaskGroup() as group:
                group.cancel()
                group.spawn(lambda: None)

        with pytest.raises(ExceptionGroup) as caught:
            s.run_until_complete(parent)
        assert any(isinstance(exc, RuntimeError) and "shutting down" in str(exc) for exc in caught.value.exceptions)

    def test_except_exception_does_not_swallow_group_cancel(self, scheduler_task_factory_maker):
        s = _new_scheduler(scheduler_task_factory_maker)
        set_scheduler(s)
        swallowed = False

        def worker() -> None:
            nonlocal swallowed
            try:
                Event().swait()
            except Exception:
                swallowed = True

        def parent() -> str:
            with TaskGroup() as group:
                group.spawn(worker)
                s.yield_()
                group.cancel()
            return "ok"

        assert s.run_until_complete(parent) == "ok"
        assert swallowed is False
