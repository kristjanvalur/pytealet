from contextlib import nullcontext

from tealetio import Scheduler, set_scheduler


def new_scheduler(task_factory_maker=None) -> Scheduler:
    scheduler = Scheduler()
    if task_factory_maker is not None:
        scheduler.set_task_factory(task_factory_maker())
    set_scheduler(scheduler)
    return scheduler


def run_batch(scheduler, n: int) -> int:
    """Run at most ``n`` ready transfers.

    Tests of the batch cap use this. Hosts drive with ``turn()``, which runs
    one whole ready batch and then polls. ``turn()`` needs ``running()``;
    this enters it when the caller has not.
    """

    session = nullcontext() if scheduler.is_running() else scheduler.running()
    with session, scheduler._driving():
        return scheduler._run_ready_batch(n)
