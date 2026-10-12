"""QtHost, tealetSlot, and the wait helpers on an offscreen QApplication."""

import sys

import pytest
from tealetio import Scheduler, set_scheduler, sleep

from tealet_qt import QtHost, message_box, run, tealetSlot, wait_call, wait_dialog, wait_signal
from tealet_qt._qt import QApplication, QDialog, QMessageBox, QObject, Qt, QTimer, Signal


@pytest.fixture(scope="module")
def app():
    return QApplication.instance() or QApplication([])


def _quit_on_error(app, fn):
    """Run ``fn`` on a tealet. A failure still ends ``exec``."""

    def wrapped() -> None:
        try:
            fn()
        except BaseException:
            app.quit()
            raise

    return wrapped


def _run(app, start) -> QtHost:
    """Spawn from ``start`` during the first events, then ``exec`` until quit."""

    scheduler = Scheduler()
    host = QtHost(scheduler)
    set_scheduler(scheduler)

    def begin() -> None:
        start(scheduler)

    QTimer.singleShot(0, begin)
    try:
        host.start()
        app.exec()
    finally:
        host.stop()
        scheduler.close()
        set_scheduler(None)
    return host


def test_tealet_slot_returns_before_the_body_runs(app):
    seen: list[str] = []

    @tealetSlot()
    def body() -> None:
        seen.append("ran")
        app.quit()

    def start(scheduler) -> None:
        def starter() -> None:
            body()
            seen.append("returned")

        scheduler.spawn(starter)

    _run(app, start)
    assert seen == ["returned", "ran"]


def test_tealet_slot_drops_trailing_signal_arguments(app):
    seen: list[str] = []

    @tealetSlot()
    def body(text: str) -> None:
        seen.append(text)
        app.quit()

    def start(scheduler) -> None:
        scheduler.spawn(lambda: body("go", True))

    _run(app, start)
    assert seen == ["go"]


def test_tealet_slot_reports_the_body_exception(app):
    reported: list[BaseException] = []
    previous = sys.excepthook

    def hook(etype, exc, tb) -> None:
        reported.append(exc)

    sys.excepthook = hook
    try:

        @tealetSlot()
        def body() -> None:
            raise RuntimeError("slot failed")

        def start(scheduler) -> None:
            def starter() -> None:
                body()
                sleep(0.05)
                app.quit()

            scheduler.spawn(_quit_on_error(app, starter))

        _run(app, start)
    finally:
        sys.excepthook = previous
    assert len(reported) == 1
    assert isinstance(reported[0], RuntimeError)
    assert "slot failed" in str(reported[0])


def test_wait_signal_parks_until_the_signal_fires(app):
    class Probe(QObject):
        ping = Signal(str)

    probe = Probe()
    seen: list[tuple] = []

    def start(scheduler) -> None:
        def waiter() -> None:
            seen.append(wait_signal(probe.ping))
            app.quit()

        def emit() -> None:
            sleep(0.05)
            probe.ping.emit("pong")

        scheduler.spawn(_quit_on_error(app, waiter))
        scheduler.spawn(_quit_on_error(app, emit))

    _run(app, start)
    assert seen == [("pong",)]


def test_wait_signal_wakes_from_a_qt_timer(app):
    """A signal between turns is the real button-click path."""

    class Probe(QObject):
        ping = Signal(str)

    probe = Probe()
    seen: list[tuple] = []

    def start(scheduler) -> None:
        def waiter() -> None:
            seen.append(wait_signal(probe.ping))
            app.quit()

        scheduler.spawn(_quit_on_error(app, waiter))
        QTimer.singleShot(50, lambda: probe.ping.emit("qt"))

    _run(app, start)
    assert seen == [("qt",)]


def test_message_box_returns_the_clicked_button(app):
    seen: list[int] = []

    def start(scheduler) -> None:
        def job() -> None:
            seen.append(
                message_box(
                    None,
                    "Keep the result?",
                    buttons=QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                )
            )
            app.quit()

        def click_yes() -> None:
            deadline = scheduler.time() + 3
            while True:
                for widget in QApplication.topLevelWidgets():
                    if isinstance(widget, QMessageBox) and widget.isVisible():
                        button = widget.button(QMessageBox.StandardButton.Yes)
                        assert button is not None
                        button.click()
                        return
                if scheduler.time() >= deadline:
                    raise RuntimeError("message box did not appear")
                sleep(0.02)

        scheduler.spawn(_quit_on_error(app, job))
        scheduler.spawn(_quit_on_error(app, click_yes))

    _run(app, start)
    assert seen == [int(QMessageBox.StandardButton.Yes)]


def test_wait_dialog_uses_finished_not_exec(app):
    seen: list[int] = []

    def start(scheduler) -> None:
        def job() -> None:
            dialog = QDialog()
            seen.append(wait_dialog(dialog))
            app.quit()

        def accept() -> None:
            deadline = scheduler.time() + 3
            while True:
                for widget in QApplication.topLevelWidgets():
                    if isinstance(widget, QDialog) and not isinstance(widget, QMessageBox) and widget.isVisible():
                        assert widget.windowModality() == Qt.WindowModality.ApplicationModal
                        widget.accept()
                        return
                if scheduler.time() >= deadline:
                    raise RuntimeError("dialog did not appear")
                sleep(0.02)

        scheduler.spawn(_quit_on_error(app, job))
        scheduler.spawn(_quit_on_error(app, accept))

    _run(app, start)
    assert seen == [int(QDialog.DialogCode.Accepted)]


def test_tealet_slot_on_a_method_drops_the_clicked_flag(app):
    class Widget(QObject):
        clicked = Signal(bool)

        def __init__(self) -> None:
            super().__init__()
            self.seen: list[str] = []

        @tealetSlot()
        def on_click(self) -> None:
            self.seen.append("ran")
            app.quit()

    widget = Widget()
    widget.clicked.connect(widget.on_click)

    def start(scheduler) -> None:
        scheduler.spawn(_quit_on_error(app, lambda: widget.clicked.emit(True)))

    _run(app, start)
    assert widget.seen == ["ran"]


def test_wait_call_runs_exec_after_the_tealet_parks(app):
    seen: list[int] = []

    def start(scheduler) -> None:
        dialog = QDialog()

        def job() -> None:
            seen.append(wait_call(dialog.exec))
            app.quit()

        def accept() -> None:
            deadline = scheduler.time() + 3
            while not dialog.isVisible():
                if scheduler.time() >= deadline:
                    raise RuntimeError("exec did not show the dialog")
                sleep(0.02)
            dialog.accept()

        scheduler.spawn(_quit_on_error(app, job))
        scheduler.spawn(_quit_on_error(app, accept))

    _run(app, start)
    assert seen == [int(QDialog.DialogCode.Accepted)]


def test_far_deadline_is_clamped_for_qtimer(app):
    scheduler = Scheduler()
    host = QtHost(scheduler)
    set_scheduler(scheduler)
    try:
        host._arm(scheduler.time() + 10**9)
        assert host._timer.interval() == 2**31 - 1
        host._arm(scheduler.time() + 0.05)
        assert host._timer.interval() == 50
    finally:
        host.stop()
        scheduler.close()
        set_scheduler(None)


def test_run_executes_main_as_a_tealet(app):
    seen: list[str] = []

    def body(host) -> None:
        seen.append("ran")
        app.quit()

    assert run(body) == 0
    assert seen == ["ran"]


def test_run_propagates_the_main_tealet_exception(app):
    def body(host) -> None:
        app.quit()
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        run(body)
