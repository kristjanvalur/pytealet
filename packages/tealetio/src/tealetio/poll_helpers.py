from __future__ import annotations

import errno
import select
import selectors

POLL_READ_MASK = select.POLLIN | select.POLLPRI | getattr(select, "POLLRDHUP", 0)
POLL_EX_MASK = select.POLLERR | select.POLLHUP


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


def probe_poll_fd_now(fd: int, mask: int) -> int:
    read_fds: list[int] = []
    write_fds: list[int] = []
    exc_fds: list[int] = []
    if mask & (POLL_READ_MASK | POLL_EX_MASK):
        read_fds.append(fd)
    if mask & select.POLLOUT:
        write_fds.append(fd)
    if mask & POLL_EX_MASK:
        exc_fds.append(fd)
    if not (read_fds or write_fds or exc_fds):
        raise ValueError("poll mask must request at least one supported event")
    ready_r, ready_w, ready_x = select.select(read_fds, write_fds, exc_fds, 0)
    result = 0
    if ready_r:
        result |= mask & (POLL_READ_MASK | POLL_EX_MASK)
    if ready_w:
        result |= mask & select.POLLOUT
    if ready_x:
        result |= mask & POLL_EX_MASK
    if result:
        return result
    raise BlockingIOError(errno.EWOULDBLOCK, "fd is not ready")
