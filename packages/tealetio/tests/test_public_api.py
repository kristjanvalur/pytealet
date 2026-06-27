import tealetio
import tealetio.asyncio
import tealetio.locks
import tealetio.runner
import tealetio.scheduler
import tealetio.selector
import tealetio.proactor
import tealetio.tasks


def test_top_level_exports_public_scheduler_api():
    expected = {
        "ALL_COMPLETED",
        "AsyncRunner",
        "AsyncProactorScheduler",
        "AsyncSelectorScheduler",
        "AsyncScheduler",
        "AsyncSchedulerDrivingAPI",
        "AsyncDrivingMixin",
        "BaseDrivingMixin",
        "BaseRunner",
        "BaseScheduler",
        "BasicScheduler",
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
        "Operation",
        "Proactor",
        "ProactorFactory",
        "ProactorScheduler",
        "get_current",
        "InvalidStateError",
        "LifoQueue",
        "Linkable",
        "Lock",
        "PriorityLock",
        "PriorityTask",
        "PriorityQueue",
        "Queue",
        "QueueEmpty",
        "QueueFull",
        "QueueShutDown",
        "Runner",
        "Scheduler",
        "SelectorMixin",
        "SelectorProactor",
        "SelectorScheduler",
        "Semaphore",
        "Shield",
        "StubTaskFactory",
        "SyncProactorScheduler",
        "SyncSelectorScheduler",
        "SyncDrivingMixin",
        "SyncSchedulerDrivingAPI",
        "TASK_PRIORITY_CRITICAL",
        "TASK_PRIORITY_DEFAULT",
        "TASK_PRIORITY_HIGH",
        "TASK_PRIORITY_IDLE",
        "TASK_PRIORITY_LOW",
        "TaskFactory",
        "TealetHostedScheduler",
        "TealetSelectorEventLoop",
        "ThreadedSelectorProactor",
        "Task",
        "Timeout",
        "TimeoutError",
        "TimerHandle",
        "asyncio_get_current",
        "as_completed",
        "await_",
        "create_task",
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
        "sleep",
        "spawn",
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
    assert tealetio.PriorityLock is tealetio.locks.PriorityLock
    assert tealetio.Queue is tealetio.locks.Queue
    assert tealetio.Future is tealetio.tasks.Future
    assert tealetio.get_current is tealetio.tasks.get_current
    assert tealetio.Linkable is tealetio.tasks.Linkable
    assert tealetio.Linkable is tealetio.tasks.TaskLink
    assert tealetio.PriorityTask is tealetio.tasks.PriorityTask
    assert tealetio.Task is tealetio.tasks.Task
    assert tealetio.TASK_PRIORITY_CRITICAL is tealetio.tasks.TASK_PRIORITY_CRITICAL
    assert tealetio.TASK_PRIORITY_HIGH is tealetio.tasks.TASK_PRIORITY_HIGH
    assert tealetio.TASK_PRIORITY_DEFAULT is tealetio.tasks.TASK_PRIORITY_DEFAULT
    assert tealetio.TASK_PRIORITY_LOW is tealetio.tasks.TASK_PRIORITY_LOW
    assert tealetio.TASK_PRIORITY_IDLE is tealetio.tasks.TASK_PRIORITY_IDLE
    assert tealetio.Scheduler is tealetio.scheduler.Scheduler
    assert tealetio.BaseScheduler is tealetio.scheduler.BaseScheduler
    assert tealetio.BaseDrivingMixin is tealetio.scheduler.BaseDrivingMixin
    assert tealetio.SyncDrivingMixin is tealetio.scheduler.SyncDrivingMixin
    assert tealetio.AsyncDrivingMixin is tealetio.scheduler.AsyncDrivingMixin
    assert tealetio.BasicScheduler is tealetio.scheduler.BasicScheduler
    assert tealetio.Runner is tealetio.runner.Runner
    assert tealetio.SelectorScheduler is tealetio.selector.SelectorScheduler
    assert tealetio.SyncSelectorScheduler is tealetio.selector.SyncSelectorScheduler
    assert tealetio.AsyncSelectorScheduler is tealetio.selector.AsyncSelectorScheduler
    assert issubclass(tealetio.SyncSelectorScheduler, tealetio.SelectorScheduler)
    assert issubclass(tealetio.AsyncSelectorScheduler, tealetio.SelectorScheduler)
    assert tealetio.Operation is tealetio.proactor.Operation
    assert tealetio.ProactorScheduler is tealetio.proactor.ProactorScheduler
    assert tealetio.SyncProactorScheduler is tealetio.proactor.SyncProactorScheduler
    assert tealetio.AsyncProactorScheduler is tealetio.proactor.AsyncProactorScheduler
    assert tealetio.Scheduler is tealetio.SyncProactorScheduler
    assert issubclass(tealetio.SyncProactorScheduler, tealetio.ProactorScheduler)
    assert issubclass(tealetio.AsyncProactorScheduler, tealetio.ProactorScheduler)
    assert tealetio.SelectorProactor is tealetio.proactor.SelectorProactor
    assert tealetio.ThreadedSelectorProactor is tealetio.proactor.ThreadedSelectorProactor
    assert tealetio.AsyncRunner is tealetio.asyncio.AsyncRunner
    assert tealetio.AsyncScheduler is tealetio.asyncio.AsyncScheduler
    assert tealetio.TealetHostedScheduler is tealetio.asyncio.TealetHostedScheduler
    assert issubclass(tealetio.TealetHostedScheduler, tealetio.SyncSelectorScheduler)
    assert tealetio.AsyncSchedulerDrivingAPI is tealetio.scheduler.AsyncSchedulerDrivingAPI
    assert tealetio.asyncio.AsyncSchedulerDrivingAPI is tealetio.scheduler.AsyncSchedulerDrivingAPI
    assert tealetio.asyncio_get_current is tealetio.asyncio.asyncio_get_current
    assert tealetio.run is tealetio.runner.run
    assert tealetio.run_async is tealetio.asyncio.run_async
    assert tealetio.gather is tealetio.scheduler.gather
    assert tealetio.wait_for is tealetio.scheduler.wait_for
    assert tealetio.sleep is tealetio.scheduler.sleep
    assert tealetio.await_ is tealetio.scheduler.await_
    assert tealetio.spawn is tealetio.scheduler.spawn
    assert tealetio.create_task is tealetio.scheduler.create_task
    assert tealetio.create_task.__doc__


def test_star_import_uses_top_level_all():
    namespace = {}

    exec("from tealetio import *", namespace)

    for name in tealetio.__all__:
        assert namespace[name] is getattr(tealetio, name)
