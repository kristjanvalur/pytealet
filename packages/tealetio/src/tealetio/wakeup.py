"""Optional yield after break-wait wakeups (experiments)."""

from __future__ import annotations

import os
import time


def yield_after_break_wait_wakeup() -> None:
    """Yield the calling thread when ``TEALETIO_BREAK_WAIT_SLEEP`` is set."""

    if os.environ.get("TEALETIO_BREAK_WAIT_SLEEP", "").lower() in ("1", "true", "yes"):
        time.sleep(0)