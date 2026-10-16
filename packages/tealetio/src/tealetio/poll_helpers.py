from __future__ import annotations

import errno
import select
import selectors

# selector wakeups. the selector has no exceptional event, so POLLPRI
# shares EVENT_READ with POLLIN. POLLERR and POLLHUP wake read and write.
POLL_READ_MASK = select.POLLIN | select.POLLPRI | getattr(select, "POLLRDHUP", 0)
POLL_EX_MASK = select.POLLERR | select.POLLHUP

# kernel select() sets. peer close is readable, not exceptional.
# POLLPRI is the exceptional list (out-of-band).
_SELECT_READ_MASK = select.POLLIN | select.POLLERR | select.POLLHUP | getattr(select, "POLLRDHUP", 0)
_SELECT_EXC_MASK = select.POLLPRI


def poll_mask_to_selector_events(mask: int) -> int:
    events = 0
    if mask & POLL_READ_MASK:
        events |= selectors.EVENT_READ
    if mask & select.POLLOUT:
        events |= selectors.EVENT_WRITE
    if mask & POLL_EX_MASK:
        events |= selectors.EVENT_READ | selectors.EVENT_WRITE
    if events == 0:
        raise ValueError("poll mask must request at least one supported event")
    return events


def real_select_module():
    """Return the stdlib ``select`` module.

    ``tealetio.ext.poll`` captured it as ``real_select`` when that module
    was imported. ``patch`` installs ``tealetio.ext.poll`` as
    ``sys.modules["select"]``. Probes call this module so a patched
    ``select.select`` cannot recurse.
    """

    from .ext.poll import real_select

    return real_select


def probe_poll_fd_now(fd: int, mask: int) -> int:
    """Return the bits of ``mask`` that a non-blocking ``select`` sees now.

    Error and hangup are taken from the read list. Linux reports peer
    close as readable. ``POLLPRI`` is taken from the exceptional list,
    so ordinary readability is not reported as out-of-band.
    """

    read_fds: list[int] = []
    write_fds: list[int] = []
    exc_fds: list[int] = []
    if mask & _SELECT_READ_MASK:
        read_fds.append(fd)
    if mask & select.POLLOUT:
        write_fds.append(fd)
    if mask & _SELECT_EXC_MASK:
        exc_fds.append(fd)
    if not (read_fds or write_fds or exc_fds):
        raise ValueError("poll mask must request at least one supported event")
    ready_r, ready_w, ready_x = real_select_module().select(read_fds, write_fds, exc_fds, 0)
    result = 0
    if ready_r:
        result |= mask & _SELECT_READ_MASK
    if ready_w:
        result |= mask & select.POLLOUT
    if ready_x:
        result |= mask & _SELECT_EXC_MASK
    if result:
        return result
    raise BlockingIOError(errno.EWOULDBLOCK, "fd is not ready")
