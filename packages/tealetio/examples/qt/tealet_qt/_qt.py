"""Import whichever Qt binding the process is using.

``QT_API`` picks one of ``pyside6`` or ``pyqt6``. Otherwise an already imported
binding wins, then PySide6, then PyQt6. Qt is not imported by tealetio.
"""

from __future__ import annotations

import importlib
import os
import sys


def _candidates() -> list[str]:
    api = os.environ.get("QT_API", "").strip().lower()
    if api in {"pyside6", "pyside"}:
        return ["PySide6"]
    if api in {"pyqt6", "pyqt"}:
        return ["PyQt6"]
    loaded = [name for name in ("PySide6", "PyQt6") if name in sys.modules]
    if loaded:
        return loaded
    return ["PySide6", "PyQt6"]


def _load():
    errors: list[str] = []
    for name in _candidates():
        try:
            core = importlib.import_module(f"{name}.QtCore")
            widgets = importlib.import_module(f"{name}.QtWidgets")
        except ImportError as exc:
            errors.append(f"{name}: {exc}")
            continue
        slot = getattr(core, "Slot", None)
        if slot is None:
            slot = core.pyqtSlot
        signal = getattr(core, "Signal", None)
        if signal is None:
            signal = core.pyqtSignal
        return core, widgets, slot, signal
    raise ImportError("tealet_qt needs PySide6 or PyQt6. Install one of them, or set QT_API. " + "; ".join(errors))


_core, _widgets, Slot, Signal = _load()

QObject = _core.QObject
Qt = _core.Qt
QTimer = _core.QTimer
QApplication = _widgets.QApplication
QDialog = _widgets.QDialog
QMessageBox = _widgets.QMessageBox
QWidget = _widgets.QWidget

BINDING = _core.__name__.split(".", 1)[0]
