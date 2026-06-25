from tealetio import Scheduler, set_scheduler


def new_scheduler(task_factory_maker=None) -> Scheduler:
    scheduler = Scheduler()
    if task_factory_maker is not None:
        scheduler.set_task_factory(task_factory_maker())
    set_scheduler(scheduler)
    return scheduler
