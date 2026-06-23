def test_eager_spawn_with_queued_task_does_not_crash():
    from tealet.scheduler import Scheduler, set_scheduler
    from tealet.tasks import DefaultTaskFactory

    scheduler = Scheduler()
    set_scheduler(scheduler)
    try:
        scheduler.set_task_factory(DefaultTaskFactory(eager_start=True))

        task = scheduler.spawn(lambda: "queued", eager_start=False)

        def parent() -> str:
            target = scheduler.spawn(lambda: "target", eager_start=True)
            assert target.done()
            return target.result()

        result = scheduler.run_until_complete(parent)

        assert result == "target"
        assert task.result() == "queued"
    finally:
        set_scheduler(None)
        scheduler.close()