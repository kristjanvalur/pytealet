"""Qt application host for tealetio.

``QtHost`` turns the scheduler from ``QApplication.exec``. ``tealetSlot`` is
the edge from a Qt signal into a tealet. ``wait_signal``, ``wait_dialog``,
``wait_call``, and ``message_box`` are how that tealet parks on Qt.
"""

from .host import QtHost
from .slot import tealetSlot
from .wait import message_box, wait_call, wait_dialog, wait_signal

__all__ = [
    "QtHost",
    "message_box",
    "tealetSlot",
    "wait_call",
    "wait_dialog",
    "wait_signal",
]
