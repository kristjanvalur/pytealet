def test_eager_run_until_complete_with_queued_task_does_not_crash():
    from tealet.scheduler import Scheduler, set_scheduler
    from tealet.tasks import DefaultTaskFactory

    scheduler = Scheduler()
    set_scheduler(scheduler)
    scheduler.set_task_factory(DefaultTaskFactory(eager=True))

    task = scheduler.spawn(lambda: "queued", eager=False)

    result = scheduler.run_until_complete(lambda: "target")

    assert result == "target"
    assert task.result() == "queued"