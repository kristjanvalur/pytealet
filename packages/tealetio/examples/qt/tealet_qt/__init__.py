"""Qt application host for tealetio.

``run`` is the host boilerplate: a ``QApplication``, a scheduler, and
``exec``. The callable it is given runs as one tealet. ``QtHost`` is the
driver inside that host. ``tealetSlot`` is the edge from a Qt signal into
a tealet. ``wait_signal``, ``wait_dialog``, ``wait_call``, and
``message_box`` are how that tealet parks on Qt.
"""

from .host import QtHost, run
from .slot import tealetSlot
from .wait import message_box, wait_call, wait_dialog, wait_signal

__all__ = [
    "QtHost",
    "message_box",
    "run",
    "tealetSlot",
    "wait_call",
    "wait_dialog",
    "wait_signal",
]
