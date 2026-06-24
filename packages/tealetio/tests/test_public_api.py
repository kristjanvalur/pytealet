import tealetio
import tealetio.asyncio
import tealetio.locks
import tealetio.runner
import tealetio.scheduler
import tealetio.selector
import tealetio.tasks


def test_top_level_exports_public_scheduler_api():
    expected = {
        "ALL_COMPLETED",
        "AsyncRunner",
        "AsyncScheduler",
        "AsyncSchedulerDrivingAPI",
        "BaseRunner",
        "BaseScheduler",
        "Barrier",
        "BoundedSemaphore",
        "CancelledError",
        "Channel",
        "Condition",
        "CoreSchedulerDrivingAPI",
        "DEFAULT_EXECUTOR_SHUTDOWN_TIMEOUT",
        "DeadlockError",
        "DefaultTaskFactory",
        "Event",
        "FIRST_COMPLETED",
        "FIRST_EXCEPTION",
        "Future",
        "get_current",
        "InvalidStateError",
        "LifoQueue",
        "Lock",
        "PriorityQueue",
        "Queue",
        "QueueEmpty",
        "QueueFull",
        "QueueShutDown",
        "Runner",
        "Scheduler",
        "SelectorMixin",
        "SelectorScheduler",
        "Semaphore",
        "Shield",
        "StubTaskFactory",
        "SyncSchedulerDrivingAPI",
        "TaskFactory",
        "TealetSelectorEventLoop",
        "TealetTask",
        "Timeout",
        "TimeoutError",
        "TimerHandle",
        "as_completed",
        "ensure_future",
        "gather",
        "get_running_scheduler",
        "get_scheduler",
        "run",
        "run_async",
        "run_asyncio_in_tealet",
        "run_in_asyncio",
        "set_scheduler",
        "shield",
        "timeout",
        "timeout_at",
        "to_thread",
        "wait",
        "wait_for",
    }

    assert expected <= set(tealetio.__all__)
    assert len(tealetio.__all__) == len(set(tealetio.__all__))


def test_top_level_exports_are_submodule_aliases():
    assert tealetio.Event is tealetio.locks.Event
    assert tealetio.Lock is tealetio.locks.Lock
    assert tealetio.Queue is tealetio.locks.Queue
    assert tealetio.Future is tealetio.tasks.Future
    assert tealetio.get_current is tealetio.tasks.get_current
    assert tealetio.TealetTask is tealetio.tasks.TealetTask
    assert tealetio.Scheduler is tealetio.scheduler.Scheduler
    assert tealetio.BaseScheduler is tealetio.scheduler.BaseScheduler
    assert tealetio.Runner is tealetio.runner.Runner
    assert tealetio.SelectorScheduler is tealetio.selector.SelectorScheduler
    assert tealetio.AsyncRunner is tealetio.asyncio.AsyncRunner
    assert tealetio.AsyncScheduler is tealetio.asyncio.AsyncScheduler
    assert tealetio.run is tealetio.runner.run
    assert tealetio.run_async is tealetio.asyncio.run_async
    assert tealetio.gather is tealetio.scheduler.gather
    assert tealetio.wait_for is tealetio.scheduler.wait_for


def test_star_import_uses_top_level_all():
    namespace = {}

    exec("from tealetio import *", namespace)

    for name in tealetio.__all__:
        assert namespace[name] is getattr(tealetio, name)
