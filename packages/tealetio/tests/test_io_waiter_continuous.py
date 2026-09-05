from __future__ import annotations

from io_fakes import StubProactor, StubScheduler

import tealetio.io_waiter as io_waiter_module
from tealetio.io_manager import ProactorIOManager
from tealetio.io_waiter import IOWaiter


class _MockProactor(StubProactor):
    pass


def test_iowaiter_wait_parks_on_accept(monkeypatch) -> None:
    io = ProactorIOManager(StubScheduler(), _MockProactor())  # type: ignore[arg-type]
    scheduled: list[object] = []
    waiter = IOWaiter(io)
    waiter.bind(object())

    class _FakeReady:
        def set(self) -> None:
            scheduled.append("wake")

        def swait(self) -> bool:
            waiter.complete(None, None)
            return True

    monkeypatch.setattr(io_waiter_module, "CrossThreadEvent", lambda _scheduler: _FakeReady())

    waiter._wait_self()

    assert scheduled == ["wake"]
    assert waiter.done()
