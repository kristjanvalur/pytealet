"""Bench-side watchdog for stream diagnostics."""

from __future__ import annotations

import sys
import threading
import time
from typing import TYPE_CHECKING

from tealetio.stream_diag import enabled, event, snapshot, total_events

if TYPE_CHECKING:
    from tealetio.proactor import ProactorScheduler
    from tealetio.streams import StreamServer


def start_watchdog(
    scheduler: ProactorScheduler,
    server: StreamServer,
    *,
    interval: float = 2.0,
    stall_after: float = 3.0,
) -> threading.Thread:
    """Periodically dump diag counters; warn when events go quiet under load."""

    def loop() -> None:
        last_total = total_events()
        while True:
            time.sleep(interval)
            if not enabled():
                continue
            idle, counters, blocking = snapshot()
            active = len(server._handler_tasks)  # noqa: SLF001
            proactor = scheduler._proactor  # noqa: SLF001
            pending_io = proactor.has_pending_operations()
            runnable = scheduler._has_runnable_work()  # noqa: SLF001
            threadsafe = not scheduler._threadsafe_callbacks.empty()  # noqa: SLF001
            ring_running = getattr(getattr(proactor, "_ring", None), "running", None)
            service_alive = [thread.is_alive() for thread in getattr(proactor, "_service_threads", ())]
            current_total = total_events()
            stalled = current_total == last_total
            last_total = current_total
            print(
                f"[bench-diag] active_handlers={active} pending_io={pending_io} "
                f"runnable={runnable} threadsafe={threadsafe} ring.running={ring_running} "
                f"service_alive={service_alive} idle={idle:.3f}s counters={counters}",
                file=sys.stderr,
                flush=True,
            )
            for ident, site, waited, detail in blocking:
                print(
                    f"[bench-diag]   blocked tid={ident} site={site} for={waited:.3f}s {detail}",
                    file=sys.stderr,
                    flush=True,
                )
            if stalled and active > 0 and idle >= stall_after:
                event(
                    "STALL",
                    active=active,
                    pending_io=pending_io,
                    runnable=runnable,
                    threadsafe=threadsafe,
                    ring_running=ring_running,
                    service_alive=service_alive,
                )

    thread = threading.Thread(target=loop, name="bench-diag", daemon=True)
    thread.start()
    return thread
