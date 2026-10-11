"""Park a tealet until Qt reports something.

These run on a tealet, not on the Qt stack. ``tealetSlot`` is how a signal
enters such a tealet. ``wait_signal`` is how that tealet waits for the next one.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar, cast

from tealetio import Event, get_scheduler

from ._qt import QDialog, QMessageBox, Qt, QTimer, QWidget

_T = TypeVar("_T")


def _notify(done: Event) -> None:
    """Wake ``done`` on a scheduler turn.

    A Qt slot usually runs between turns. ``Event.set`` only works while the
    scheduler is running, so the wake is queued and the host turns again.
    From a tealet the same call runs inline.
    """

    get_scheduler().call_on_scheduler(done.set)


def _park_on(signal, before_wait: Callable[[], None] | None = None):
    """Connect, optionally run ``before_wait``, then park until the first emit."""

    done = Event()
    payload: tuple[object, ...] | None = None

    def slot(*args):
        nonlocal payload
        if payload is not None:
            return
        payload = args
        _notify(done)

    signal.connect(slot)
    try:
        if before_wait is not None:
            before_wait()
        done.swait()
    finally:
        try:
            signal.disconnect(slot)
        except (RuntimeError, TypeError):
            pass
    assert payload is not None
    return payload


def wait_signal(signal):
    """Park until ``signal`` fires and return the argument tuple.

    Connect before the producer can emit. A signal that already fired is
    missed, and this would then park for good. The first emission wins.
    ``Event.set`` from the slot wakes this tealet on a later ``turn``.
    """

    return _park_on(signal)


def wait_dialog(dialog: QDialog) -> int:
    """Show ``dialog`` and park until ``finished(int)``.

    ``show`` returns immediately. A non-modal dialog is made application
    modal first. ``QMessageBox`` is already application modal, and an
    explicit window-modal dialog is left alone. ``open`` is not used: it
    forces window modality, and putting application modality back hides the
    box. Modality is a window flag, not ``exec``, so the host keeps turning
    and other tealets keep running while the box is up.
    """

    def show() -> None:
        if dialog.windowModality() == Qt.WindowModality.NonModal:
            dialog.setWindowModality(Qt.WindowModality.ApplicationModal)
        dialog.setResult(0)
        dialog.show()

    (code,) = _park_on(dialog.finished, show)
    return int(code)


def wait_call(fn: Callable[..., _T], /, *args, **kwargs) -> _T:
    """Run ``fn`` on the Qt stack after this tealet parks, and return its result.

    This is the ``exec`` escape hatch. The call is posted with a zero-delay
    timer, so it starts once ``turn`` has returned and is not nested inside
    the tealet. Prefer ``wait_dialog`` when the widget has ``show`` and
    ``finished``. An exception from ``fn`` is raised in the waiting tealet.
    """

    done = Event()
    outcome: tuple[bool, object] | None = None

    def helper() -> None:
        nonlocal outcome
        try:
            outcome = (True, fn(*args, **kwargs))
        except BaseException as exc:
            outcome = (False, exc)
        _notify(done)

    QTimer.singleShot(0, helper)
    done.swait()
    assert outcome is not None
    ok, value = outcome
    if not ok:
        assert isinstance(value, BaseException)
        raise value
    return cast(_T, value)


def message_box(
    parent: QWidget | None,
    text: str,
    *,
    title: str = "",
    informative: str = "",
    buttons: QMessageBox.StandardButton = QMessageBox.StandardButton.Ok,
) -> int:
    """Show a message box and park until the user clicks a button.

    Returns the ``StandardButton`` value. Built on ``wait_dialog``, so the
    box does not take the thread into ``exec``.
    """

    box = QMessageBox(parent)
    if title:
        box.setWindowTitle(title)
    box.setText(text)
    if informative:
        box.setInformativeText(informative)
    box.setStandardButtons(buttons)
    return wait_dialog(box)
