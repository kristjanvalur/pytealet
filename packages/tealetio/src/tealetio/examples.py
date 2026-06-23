"""Examples for tealetio scheduler usage."""

from __future__ import annotations

from .locks import Event, TimeoutError, timeout
from .scheduler import Scheduler, set_scheduler


def _new_scheduler() -> Scheduler:
    scheduler = Scheduler()
    set_scheduler(scheduler)
    return scheduler


def demo_scheduler_append_with_yield() -> list[str]:
    """Run a few tealets that append while yielding to each other."""

    s = _new_scheduler()
    seen: list[str] = []

    def worker(name: str, count: int) -> None:
        for i in range(count):
            seen.append(f"{name}{i}")
            s.yield_()

    s.spawn(lambda: worker("a", 3))
    s.spawn(lambda: worker("b", 2))
    s.spawn(lambda: worker("c", 1))
    s.run()
    return seen


def demo_wait_for_event_start() -> list[str]:
    """Run one tealet that waits on an event until another starts it."""

    s = _new_scheduler()
    evt = Event()
    seen: list[str] = []

    def waiter() -> None:
        seen.append("waiter:waiting")
        evt.swait()
        seen.append("waiter:started")

    def starter() -> None:
        seen.append("starter:set")
        evt.set()

    s.spawn(waiter)
    s.spawn(starter)
    s.run()
    return seen


def demo_wait_for_event_between_runs() -> list[str]:
    """Run twice with external event wakeup between runs."""

    s = _new_scheduler()
    evt = Event()
    seen: list[str] = []

    def waiter() -> None:
        seen.append("waiter:waiting")
        evt.swait()
        seen.append("waiter:resumed")

    s.spawn(waiter)
    s.run()
    seen.append("after:first-run")

    evt.set()

    s.run()
    seen.append("after:second-run")
    return seen


def demo_future_result() -> list[str]:
    """Run a task via Future and consume it from another tealet."""

    s = _new_scheduler()
    seen: list[str] = []

    def producer() -> int:
        seen.append("producer:start")
        s.yield_()
        seen.append("producer:done")
        return 42

    future = s.spawn(producer)

    def consumer() -> None:
        future.wait()
        seen.append(f"consumer:result={future.result()}")

    s.spawn(consumer)
    s.run()
    return seen


def demo_sleep() -> list[str]:
    """Run a tealet that sleeps and resumes via scheduled timer callback."""

    s = _new_scheduler()
    seen: list[str] = []

    def worker() -> None:
        seen.append("before:sleep")
        s.sleep(0.001)
        seen.append("after:sleep")

    s.spawn(worker)
    s.run()
    return seen


def demo_future_timeout_then_success() -> list[str]:
    """Show timeout then successful completion using timeout contexts."""

    s = _new_scheduler()
    timeout_evt = Event()
    success_evt = Event()
    seen: list[str] = []

    def timeout_waiter() -> None:
        tm = timeout(0.001)
        try:
            with tm:
                timeout_evt.swait()
        except TimeoutError:
            pass
        seen.append(f"timeout_waiter:{not tm.expired()}")

    def success_waiter() -> None:
        tm = timeout(10.0)
        with tm:
            success_evt.swait()
        seen.append(f"success_waiter:{not tm.expired()}")

    s.spawn(timeout_waiter)
    s.run()
    s.spawn(success_waiter)
    s.call_later(0.002, success_evt.set)
    s.run()
    return seen
