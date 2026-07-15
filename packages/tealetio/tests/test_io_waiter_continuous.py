from __future__ import annotations

import tealetio.io_waiter as io_waiter_module
from tealetio.io_manager import ProactorIOManager
from tealetio.io_waiter import IOWaiter
from tealetio.operations import ContinuousOperation
from io_fakes import StubScheduler


class _MockProactor:
    pass


class _RecordingContinuousOperation(ContinuousOperation[object]):
    def __init__(self) -> None:
        super().__init__(kind="accept_many", fileobj=object())
        self.add_done_callback_calls = 0

    def add_done_callback(self, callback: object) -> None:
        self.add_done_callback_calls += 1
        super().add_done_callback(callback)


def test_iowaiter_continuous_uses_done_callback(monkeypatch) -> None:
    io = ProactorIOManager(StubScheduler(), _MockProactor())  # type: ignore[arg-type]
    operation = _RecordingContinuousOperation()
    scheduled: list[object] = []

    class _FakeReady:
        def set(self) -> None:
            scheduled.append("wake")

        def swait(self) -> bool:
            operation._finish(result=None)
            return True

    monkeypatch.setattr(io_waiter_module, "CrossThreadEvent", lambda _scheduler: _FakeReady())

    waiter = IOWaiter(io, operation)
    waiter._wait_self()

    assert operation.add_done_callback_calls == 1
    assert scheduled == ["wake"]