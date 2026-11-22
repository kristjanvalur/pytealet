"""Drive a tealetio scheduler from ``QApplication.exec``."""

from __future__ import annotations

import sys
from collections.abc import Callable

from tealetio import Future, Runner, Scheduler, Task, ThreadedSelectorProactor, UringProactor, set_scheduler

from ._qt import QApplication, QObject, Qt, QTimer, Signal

# QTimer.start takes a signed 32-bit millisecond count. A longer scheduler
# deadline is armed in pieces; the next firing turns and arms the rest.
_QTIMER_MAX_MS = 2**31 - 1


def threaded_scheduler() -> Scheduler:
    """Scheduler whose completions wake a host parked outside ``wait()``.

    ``Scheduler()`` is that proactor: ``UringProactor`` when uring is
    available, otherwise ``ThreadedSelectorProactor``. Either posts
    ``set_driver_wakeup`` when IO finishes.
    """

    return Scheduler()


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

        # stop() latches _stopped. a later run() on the same host starts clean.
        self._stopped = False
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
        # a turn() is already on this stack; replay the kick afterwards.
        # running() stays held for the whole session, including this gap.
        if self.scheduler.in_turn():
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


class QtScheduler(Scheduler):
    """Scheduler whose driver is ``QApplication.exec``.

    Same shape as ``qasync.QEventLoop``. ``run_forever`` parks in ``exec``.
    ``run_until_complete`` does that too, and ``app.exit`` runs when the
    target is done. A normal ``Runner`` is the session::

        with Runner(scheduler_factory=QtScheduler) as runner:
            runner.run(main)

    ``Runner.close`` calls ``run_until_complete`` again for leftovers, so
    that call is this method as well. An already-done target does not enter
    ``exec``: Qt drops an ``exit`` that arrived before ``exec`` starts.
    ``host`` is the ``QtHost`` for this scheduler. ``exit_code`` is the
    first ``exec`` result, or ``0`` when nothing entered ``exec``.
    """

    def __init__(self, proactor_factory=None, *, argv: list[str] | None = None, runnable_queue_factory=None) -> None:
        super().__init__(proactor_factory, runnable_queue_factory=runnable_queue_factory)
        app = QApplication.instance() or QApplication(sys.argv if argv is None else argv)
        self._app = app
        self.host = QtHost(self)
        self.exit_code = 0
        self._saw_exec = False
        self._quit_connected = False
        app.aboutToQuit.connect(self.host.stop)
        self._quit_connected = True

    def close(self) -> None:
        """Drop the quit hook, then release the proactor."""

        self._disconnect_quit()
        super().close()

    def stop(self) -> None:
        """Stop the driver. Inside ``exec`` this is ``app.exit``."""

        super().stop()
        self._app.exit()

    def run_forever(self, *, yield_every: int | None = None) -> None:
        """Park in ``QApplication.exec`` until ``stop`` or the last window."""

        self._validate_yield_every(yield_every)
        self._verify_current_scheduler()
        self._run_exec(None)

    def run_until_complete(self, future, *, yield_every: int | None = None):
        """Run until ``future`` is done. ``exec`` returns when it completes."""

        self._validate_yield_every(yield_every)
        self._verify_current_scheduler()
        with self.main_context():
            target = self._coerce_target(future)
        target.add_done_callback(self._exit_on_done)
        try:
            self._run_exec(target)
        finally:
            target.remove_done_callback(self._exit_on_done)
        if not target.done():
            raise RuntimeError("Scheduler stopped before Future completed.")
        return target.result()

    def _coerce_target(self, future):
        # same entry rule as the blocking driver: spawn a callable before running().
        if isinstance(future, Future):
            if isinstance(future, Task) and future.get_scheduler() is not self:
                raise RuntimeError("Future is bound to a different scheduler")
            return future
        if callable(future):
            return self.spawn(future)
        raise TypeError("future must be a Future or callable")

    def _exit_on_done(self, _future) -> None:
        # qasync's done callback calls stop(), which is app.exit().
        self._app.exit()

    def _run_exec(self, target) -> None:
        try:
            with self.running():
                self.host.start()
                if target is not None and target.done():
                    return
                code = int(self._app.exec())
                if not self._saw_exec:
                    self.exit_code = code
                    self._saw_exec = True
                # closing the window quits exec before the tealet parked on
                # finished has been turned. aboutToQuit has stopped the host.
                if target is not None and not target.done():
                    self.turn()
        finally:
            self.host.stop()

    def _disconnect_quit(self) -> None:
        if not self._quit_connected:
            return
        self._quit_connected = False
        try:
            self._app.aboutToQuit.disconnect(self.host.stop)
        except (RuntimeError, TypeError):
            pass


def run(main: Callable[[QtHost], None], argv: list[str] | None = None) -> int:
    """Host ``main`` as one tealet under ``QApplication.exec``.

    ``main`` receives the ``QtHost``. This is a normal ``Runner`` around a
    ``QtScheduler``. The return value is the first ``exec`` code. An
    exception from ``main`` propagates.
    """

    with Runner(scheduler_factory=lambda: QtScheduler(argv=argv)) as runner:
        host = runner.get_scheduler().host
        runner.run(lambda: main(host))
        return runner.get_scheduler().exit_code
