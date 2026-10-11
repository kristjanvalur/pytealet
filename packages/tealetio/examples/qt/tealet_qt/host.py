"""Drive a tealetio scheduler from ``QApplication.exec``."""

from __future__ import annotations

from collections.abc import Callable

from tealetio import Scheduler, set_scheduler

from ._qt import QObject, Qt, QTimer, Signal


class QtHost(QObject):
    """Turn a scheduler from the Qt thread.

    ``notify`` is the driver wakeup. It is queued so a completion thread, or a
    callback that runs inside ``turn``, never re-enters ``turn``. ``run()``
    parks in the proactor; this host parks in ``QApplication.exec`` instead.
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

    def start(self) -> None:
        """Bind the wakeup and run the first slice before ``exec``."""

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
            delay_ms = int(delay_ms + 0.999)  # ceil, so a short timer is not early
        self._timer.start(delay_ms)
