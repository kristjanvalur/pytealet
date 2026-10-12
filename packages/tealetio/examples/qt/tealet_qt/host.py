"""Drive a tealetio scheduler from ``QApplication.exec``."""

from __future__ import annotations

import sys
from collections.abc import Callable

import uring_api

from tealetio import Scheduler, ThreadedSelectorProactor, UringProactor, set_scheduler

from ._qt import QApplication, QObject, Qt, QTimer, Signal

# QTimer.start takes a signed 32-bit millisecond count. A longer scheduler
# deadline is armed in pieces; the next firing turns and arms the rest.
_QTIMER_MAX_MS = 2**31 - 1


def threaded_scheduler() -> Scheduler:
    """Scheduler whose completions wake a host parked outside ``wait()``.

    The default ``UringProactor`` reaps on service threads. Without uring,
    ``ThreadedSelectorProactor`` polls from a worker. Either posts
    ``set_driver_wakeup`` when IO finishes.
    """

    if uring_api.is_available():
        return Scheduler()
    return Scheduler(proactor_factory=ThreadedSelectorProactor)


def _require_threaded_proactor(proactor: object) -> None:
    if isinstance(proactor, ThreadedSelectorProactor):
        return
    if isinstance(proactor, UringProactor) and not proactor.inline_completions:
        return
    raise RuntimeError(
        "QtHost needs a threaded proactor: the default UringProactor, "
        "or ThreadedSelectorProactor. SelectorProactor and a UringProactor "
        "with completion_threads=0 only finish IO inside wait() on this thread, "
        "so a blocked recv never wakes Qt."
    )


class QtHost(QObject):
    """Turn a scheduler from the Qt thread.

    ``notify`` is the driver wakeup. It is queued so a completion thread can
    ask for another slice without re-entering ``turn``. The proactor must
    complete on another thread. An inline one finishes IO only inside
    ``wait()`` on this thread, and this thread is inside Qt. The scheduler's
    ``run()`` parks in the proactor; this host parks in ``QApplication.exec``.
    """

    kicked = Signal()

    def __init__(self, scheduler: Scheduler, on_turn: Callable[[int], object] | None = None) -> None:
        super().__init__()
        self.scheduler = scheduler
        self._on_turn = on_turn
        self._turns = 0
        self._again = False
        self._stopped = False
        self._timer = QTimer(self)
        self._timer.setSingleShot(True)
        self._timer.timeout.connect(self._drive)
        self.kicked.connect(self._drive, Qt.ConnectionType.QueuedConnection)

    def notify(self) -> None:
        """Scheduler wakeup. Safe from any thread. Do not call ``turn`` here."""

        self.kicked.emit()

    def set_on_turn(self, callback: Callable[[int], object] | None) -> None:
        """Report each completed slice. The application tealet attaches this."""

        self._on_turn = callback

    def start(self) -> None:
        """Bind the wakeup and run the first slice before ``exec``."""

        _require_threaded_proactor(self.scheduler.proactor)
        set_scheduler(self.scheduler)
        self.scheduler.set_driver_wakeup(self.notify)
        self._drive()

    def stop(self) -> None:
        """Drop the timer and the wakeup so shutdown does not turn again."""

        self._stopped = True
        self._timer.stop()
        self.scheduler.set_driver_wakeup(None)

    def _drive(self) -> None:
        if self._stopped:
            return
        # a nested Qt loop is already on this stack; replay the kick afterwards
        if self.scheduler.is_running():
            self._again = True
            return
        self._again = False
        deadline = self.scheduler.turn()
        self._turns += 1
        if self._on_turn is not None:
            self._on_turn(self._turns)
        if self._stopped:
            return
        if self._again:
            self.kicked.emit()
        self._arm(deadline)

    def _arm(self, deadline: float | None) -> None:
        if deadline is None:
            self._timer.stop()
            return
        delay_ms = (deadline - self.scheduler.time()) * 1000
        if delay_ms < 1:
            delay_ms = 0
        else:
            # ceil so a short timer is not early, then clamp to QTimer's range
            delay_ms = min(int(delay_ms + 0.999), _QTIMER_MAX_MS)
        self._timer.start(delay_ms)


def run(main: Callable[[QtHost], None], argv: list[str] | None = None) -> int:
    """Host ``main`` as one tealet under ``QApplication.exec``.

    ``main`` builds the windows and may spawn further tealets. This function
    is the boilerplate around that: the application object, the scheduler,
    and the loop. ``exec``'s code is the return value. An exception from
    ``main`` propagates here.
    """

    app = QApplication.instance() or QApplication(sys.argv if argv is None else argv)
    scheduler = threaded_scheduler()
    host = QtHost(scheduler)
    set_scheduler(scheduler)
    task = scheduler.spawn(lambda: main(host))
    app.aboutToQuit.connect(host.stop)
    try:
        host.start()
        if task.done():
            # main returned during the first slice, so there is no window to run.
            code = 0
        else:
            code = app.exec()
            # closing the window wakes the app tealet from between turns.
            # aboutToQuit has stopped the host, so drain that wake here.
            if not task.done():
                scheduler.turn()
        if task.done():
            task.result()
        return int(code)
    finally:
        host.stop()
        scheduler.close()
        set_scheduler(None)
