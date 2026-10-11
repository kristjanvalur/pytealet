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
    one whole ready batch and then polls.
    """

    with scheduler._driving():
        return scheduler._run_ready_batch(n)
