from __future__ import annotations

import asyncio

from tealetio import set_scheduler
from tealetio.asyncio import TealetProactorEventLoop
from tealetio.proactor import SyncProactorScheduler


def test_tealet_proactor_event_loop_runs_without_self_reading_hook(monkeypatch):
    scheduler = SyncProactorScheduler()
    set_scheduler(scheduler)

    def run_asyncio() -> str:
        monkeypatch.setattr(TealetProactorEventLoop, "_loop_self_reading", None)
        loop = TealetProactorEventLoop()
        try:
            asyncio.set_event_loop(loop)

            async def main() -> str:
                return "done"

            return loop.run_until_complete(main())
        finally:
            asyncio.set_event_loop(None)
            loop.close()

    try:
        assert scheduler.run_until_complete(run_asyncio) == "done"
    finally:
        scheduler.close()
