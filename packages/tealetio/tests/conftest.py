import os

import pytest


os.environ.setdefault("PYTEALET_CHECK_STACK", "1")


def _make_scheduler_task_factory(name):
    from tealetio.tasks import DefaultTaskFactory, StubTaskFactory

    if name == "default":
        return DefaultTaskFactory()
    if name == "eager":
        return DefaultTaskFactory(eager_start=True)
    if name == "stub":
        return StubTaskFactory()
    raise AssertionError(f"unknown task factory case: {name}")


@pytest.fixture(
    params=[
        pytest.param("default", id="default-factory"),
        pytest.param("eager", id="eager-factory"),
        pytest.param("stub", id="stub-factory"),
    ]
)
def scheduler_task_factory_maker(request):
    def make_factory():
        return _make_scheduler_task_factory(request.param)

    return make_factory


@pytest.fixture(
    params=[
        pytest.param("default", id="default-factory"),
        pytest.param("stub", id="stub-factory"),
    ]
)
def deferred_scheduler_task_factory_maker(request):
    def make_factory():
        return _make_scheduler_task_factory(request.param)

    return make_factory
