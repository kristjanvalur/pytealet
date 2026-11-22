"""Qt application host for tealetio.

``QtScheduler`` is a ``Scheduler`` whose ``run_until_complete`` and
``run_forever`` park in ``QApplication.exec``. A normal ``Runner`` drives
it. ``run`` is that session for one callable, and it passes the callable
the ``QtHost``. ``tealetSlot`` is the edge from a Qt signal into a tealet.
``wait_signal``, ``wait_dialog``, ``wait_call``, and ``message_box`` are
how that tealet parks on Qt.
"""

from .host import QtHost, QtScheduler, run
from .slot import tealetSlot
from .wait import message_box, wait_call, wait_dialog, wait_signal

__all__ = [
    "QtHost",
    "QtScheduler",
    "message_box",
    "run",
    "tealetSlot",
    "wait_call",
    "wait_dialog",
    "wait_signal",
]
