"""Modeless Qt dialog driven by tealet_qt.

Qt owns the thread. ``QtHost`` calls ``turn``. ``Run job`` is a ``tealetSlot``:
it returns immediately and the job runs as a tealet. That tealet parks in
``message_box``, which waits on ``finished`` rather than ``exec``, so the
clock and the fetch keep running while a question is up.
"""

from __future__ import annotations

import os
import sys
import time

if "--self-test" in sys.argv and not os.environ.get("QT_QPA_PLATFORM"):
    # read by QApplication(); set before the binding loads a platform plugin
    os.environ["QT_QPA_PLATFORM"] = "offscreen"

try:
    from PySide6.QtCore import QTimer
    from PySide6.QtWidgets import (
        QApplication,
        QDialog,
        QHBoxLayout,
        QLabel,
        QLineEdit,
        QMessageBox,
        QPushButton,
        QVBoxLayout,
    )
except ImportError as exc:
    raise SystemExit("PySide6 is required for examples/qt: uv pip install PySide6") from exc

from tealet_qt import QtHost, message_box, tealetSlot
from tealetio import Scheduler, open_connection, set_scheduler, sleep, timeout

_FETCH_HOST = "jsonplaceholder.typicode.com"
_FETCH_PATH = "/todos/1"
_CLOCK_DELAY = 1.0
_FETCH_DELAY = 5.0
_FLOW_PAUSE = 1.0


def ask(parent, text: str, buttons: QMessageBox.StandardButton) -> int:
    """Ask one question and return the button the user clicked."""

    return message_box(
        parent,
        text,
        informative="This tealet is parked. The clock is another tealet.",
        buttons=buttons,
    )


def tell(parent, text: str) -> None:
    ask(parent, text, QMessageBox.StandardButton.Ok)


def fetch_todo() -> str:
    """GET one JSON todo and return the status line plus the body."""

    reader, writer = open_connection(addr=(_FETCH_HOST, 443), ssl=True)
    try:
        writer.write(
            (
                f"GET {_FETCH_PATH} HTTP/1.1\r\n"
                f"Host: {_FETCH_HOST}\r\n"
                "User-Agent: tealetio-qt\r\n"
                "Connection: close\r\n"
                "\r\n"
            ).encode()
        )
        writer.drain()
        raw = reader.read()
    finally:
        writer.close()
        writer.wait_closed()
    head, _, body = raw.partition(b"\r\n\r\n")
    status = head.split(b"\r\n", 1)[0].decode("ascii", "replace")
    text = body.decode("utf-8", "replace").strip()
    return f"{status}\n{text}"


def clock_loop(publish) -> None:
    while True:
        publish(time.strftime("%H:%M:%S"))
        sleep(_CLOCK_DELAY)


def fetch_loop(publish) -> None:
    while True:
        try:
            with timeout(_FETCH_DELAY):
                text = fetch_todo()
        except Exception as exc:
            text = f"fetch failed: {exc}"
        publish(text)
        sleep(_FETCH_DELAY)


def run_job(ui: ClockDialog, pause: float) -> None:
    """One straight-line job, parked at each question and each sleep."""

    ui.run_button.setEnabled(False)
    ui.job_running = True
    try:
        ui.set_job("asking whether to start")
        answer = ask(
            ui,
            "Start the job?",
            QMessageBox.StandardButton.Ok | QMessageBox.StandardButton.Cancel,
        )
        if answer != int(QMessageBox.StandardButton.Ok):
            ui.set_job("cancelled")
            tell(ui, "Job cancelled")
            return

        ui.set_job("working")
        sleep(pause)

        ui.set_job("asking whether to keep the result")
        answer = ask(
            ui,
            "The job finished. Keep the result?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if answer != int(QMessageBox.StandardButton.Yes):
            ui.set_job("discarded")
            tell(ui, "Result discarded")
            return

        ui.set_job("saving")
        sleep(pause)
        ui.set_job("done")
        tell(ui, "Job done")
    finally:
        ui.job_running = False
        ui.run_button.setEnabled(True)


class ClockDialog(QDialog):
    """Clock, periodic fetch, and one tealet that walks a job."""

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("tealetio")
        self.setMinimumWidth(460)
        self._clicks = 0
        self.job_running = False

        self.clock = QLabel("clock: …", self)
        self.fetch = QLabel("fetch: …", self)
        self.fetch.setWordWrap(True)
        self.turns = QLabel("turns: 0", self)
        self.job = QLabel("job: idle", self)
        self.job.setWordWrap(True)
        self.typed = QLabel("you typed: ", self)
        self.typed.setWordWrap(True)
        self.clicks = QLabel("clicks: 0", self)
        self.entry = QLineEdit(self)
        self.entry.setPlaceholderText("type here between questions")
        self.entry.textChanged.connect(self._on_typed)

        click = QPushButton("Click me", self)
        click.clicked.connect(self._on_click)
        self.run_button = QPushButton("Run job", self)
        self.run_button.clicked.connect(self._on_run)
        buttons = QHBoxLayout()
        buttons.addWidget(click)
        buttons.addWidget(self.run_button)

        layout = QVBoxLayout(self)
        layout.addWidget(self.clock)
        layout.addWidget(self.fetch)
        layout.addWidget(self.turns)
        layout.addWidget(self.job)
        layout.addWidget(self.entry)
        layout.addWidget(self.typed)
        layout.addWidget(self.clicks)
        layout.addLayout(buttons)
        self.entry.setFocus()

    def set_clock(self, text: str) -> None:
        self.clock.setText(f"clock: {text}")
        self.setWindowTitle(f"tealetio {text}")
        box = QApplication.activeModalWidget()
        if isinstance(box, QMessageBox):
            box.setWindowTitle(f"still running {text}")

    def set_fetch(self, text: str) -> None:
        self.fetch.setText(f"fetch: {text}")

    def set_turns(self, count: int) -> None:
        self.turns.setText(f"turns: {count}")

    def set_job(self, text: str) -> None:
        self.job.setText(f"job: {text}")

    def _on_typed(self, text: str) -> None:
        self.typed.setText(f"you typed: {text}")

    def _on_click(self) -> None:
        self._clicks += 1
        self.clicks.setText(f"clicks: {self._clicks}")

    @tealetSlot()
    def _on_run(self) -> None:
        # the wrapper already returned to Qt. this body is the tealet.
        if self.job_running:
            return
        run_job(self, _FLOW_PAUSE)


def _visible_message_box() -> QMessageBox | None:
    for widget in QApplication.topLevelWidgets():
        if isinstance(widget, QMessageBox) and widget.isVisible():
            return widget
    return None


def click_through(choices: tuple[QMessageBox.StandardButton, ...]) -> None:
    """Press each button once a question is on screen. Used by --self-test."""

    for choice in choices:
        deadline = time.monotonic() + 3
        box = _visible_message_box()
        while box is None:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"no dialog appeared for {choice}")
            sleep(0.02)
            box = _visible_message_box()
        button = box.button(choice)
        if button is None:
            raise RuntimeError(f"{choice} is not on the dialog")
        button.click()
        while _visible_message_box() is box:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"dialog stayed up after {choice}")
            sleep(0.02)


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv if argv is None else argv)
    self_test = "--self-test" in args
    app = QApplication(sys.argv)
    dialog = ClockDialog()
    scheduler = Scheduler()
    host = QtHost(scheduler, on_turn=dialog.set_turns)
    dialog.show()
    app.aboutToQuit.connect(host.stop)

    def run_and_quit() -> None:
        try:
            run_job(dialog, 0.05)
            if dialog.job.text() != "job: done":
                raise RuntimeError(dialog.job.text())
        finally:
            app.quit()

    def scripted_clicks() -> None:
        try:
            click_through(
                (
                    QMessageBox.StandardButton.Ok,
                    QMessageBox.StandardButton.Yes,
                    QMessageBox.StandardButton.Ok,
                )
            )
        except BaseException:
            app.quit()
            raise

    # spawn the flow once exec is running. host.start() turns once before that,
    # and opening a dialog in that slice happens before Qt is dispatching.
    held: list = []

    def begin_self_test() -> None:
        held.append(scheduler.spawn(run_and_quit))
        held.append(scheduler.spawn(scripted_clicks))

    set_scheduler(scheduler)
    clock_task = scheduler.spawn(lambda: clock_loop(dialog.set_clock))
    fetch_task = None if self_test else scheduler.spawn(lambda: fetch_loop(dialog.set_fetch))
    if self_test:
        QTimer.singleShot(0, begin_self_test)
    try:
        host.start()
        if self_test and dialog.clock.text() == "clock: …":
            raise RuntimeError("first turn did not paint the clock")
        code = app.exec()
        if clock_task.done():
            clock_task.result()
        if fetch_task is not None and fetch_task.done():
            fetch_task.result()
        if self_test:
            flow_task, click_task = held
            click_task.result()
            if not flow_task.done():
                raise RuntimeError("job did not finish")
            flow_task.result()
        return int(code)
    finally:
        host.stop()
        scheduler.close()
        set_scheduler(None)


if __name__ == "__main__":
    raise SystemExit(main())
