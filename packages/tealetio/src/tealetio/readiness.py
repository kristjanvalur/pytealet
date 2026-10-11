"""Tealet-blocking ``poll``, ``select``, and ``epoll`` over a readiness table.

One table per IO manager. ``io.poll_many`` is armed for each watched fd and
the latest mask is stored. A user call looks that mask up. It does not submit
a oneshot ``io.poll`` per fd.

``poll_many`` reports readiness, including an fd that is already ready at
arm time. It does not report "no longer ready", so cached bits are checked
with ``probe_poll_fd_now`` before a level-triggered result is returned.

``SelectorProactor`` allows one op per fd per direction. A standing poll
holds that slot, and the next ``sock_recv`` would raise. The selector arm is
dropped before a user call returns (the stored mask is kept). The next call
re-arms only when it has to wait. Uring leaves the multishot armed.

``EPOLLET`` keeps a ``seen`` mask. A reported edge is not reported again
while a probe still finds those bits. A probe that finds the fd not ready
clears ``seen``, so the next rise is a new edge. Selector arms are not left
up across calls, so an edge that arrives while disarmed and is already
readable at the next call is indistinguishable from the edge already
reported. Data that arrives while ``poll`` is blocked is delivered.
"""

from __future__ import annotations

import errno
import os
import select as select_mod
import weakref
from typing import Any

from .delivery import MultishotDelivery, is_io_cancellation
from .io_manager import ProactorIOManager
from .locks import PulseEvent, TimeoutError, timeout
from .poll_helpers import probe_poll_fd_now
from .scheduler import sleep

_READ_MASK = select_mod.POLLIN | select_mod.POLLPRI
_WRITE_MASK = select_mod.POLLOUT
_EXC_MASK = select_mod.POLLERR | select_mod.POLLHUP
_EPOLLET = getattr(select_mod, "EPOLLET", 1 << 31)
_EPOLLONESHOT = getattr(select_mod, "EPOLLONESHOT", 1 << 30)
_EPOLLEXCLUSIVE = getattr(select_mod, "EPOLLEXCLUSIVE", 1 << 28)
_EPOLL_FLAGS = _EPOLLET | _EPOLLONESHOT | _EPOLLEXCLUSIVE
_DEFAULT_POLL_MASK = select_mod.POLLIN | select_mod.POLLPRI | select_mod.POLLOUT
_DEFAULT_EPOLL_MASK = getattr(select_mod, "EPOLLIN", select_mod.POLLIN)

__all__ = [
    "Epoll",
    "Poll",
    "readiness_table",
    "select",
]


class _Reg:
    __slots__ = ("disarmed", "edge", "ephemeral", "interest", "latched", "oneshot", "owner", "seen")

    def __init__(
        self,
        owner: int,
        interest: int,
        *,
        edge: bool = False,
        oneshot: bool = False,
        ephemeral: bool = False,
    ) -> None:
        self.owner = owner
        self.interest = interest
        self.edge = edge
        self.oneshot = oneshot
        self.ephemeral = ephemeral
        self.seen = 0
        self.disarmed = False
        self.latched = False


class _Watch:
    __slots__ = ("armed_mask", "error", "generation", "handle", "regs", "shell", "stored")

    def __init__(self) -> None:
        self.regs: dict[int, _Reg] = {}
        self.stored = 0
        self.generation = 0
        self.handle: Any = None
        self.armed_mask = 0
        self.error: BaseException | None = None
        self.shell = False


class ReadinessTable:
    """Stored poll masks for one ``ProactorIOManager``."""

    def __init__(self, io: ProactorIOManager) -> None:
        self.io = io
        self.watches: dict[int, _Watch] = {}
        self.pulse = PulseEvent()
        self._wait_depth = 0
        self._selector: bool | None = None

    @property
    def selector_backend(self) -> bool:
        if self._selector is None:
            from .proactor import SelectorProactor

            self._selector = isinstance(self.io.proactor, SelectorProactor)
        return self._selector

    def add_reg(self, fd: int, reg: _Reg) -> None:
        watch = self.watches.get(fd)
        if watch is None:
            watch = _Watch()
            self.watches[fd] = watch
        watch.regs[reg.owner] = reg
        watch.shell = False
        self.note_interest(fd)

    def note_interest(self, fd: int) -> None:
        """Arm ``poll_many`` for the union mask. Selector drops it when idle.

        Arming on register lets an already-ready fd store its mask inline.
        The selector slot is released before the registering call returns so
        a later ``recv`` can use it, and so a level-triggered fd cannot spin
        the scheduler.
        """

        self.ensure_armed(fd)
        if self.selector_backend and self._wait_depth == 0:
            self.disarm(fd)

    def ensure_armed(self, fd: int) -> None:
        watch = self.watches.get(fd)
        if watch is None:
            return
        mask = 0
        for reg in watch.regs.values():
            if not reg.disarmed and reg.interest:
                mask |= reg.interest
        if mask == 0:
            self.disarm(fd)
            return
        if watch.handle is not None and watch.armed_mask == mask:
            return
        self.disarm(fd)
        generation = watch.generation

        def on_delivery(
            delivery: MultishotDelivery,
            fd: int = fd,
            generation: int = generation,
        ) -> None:
            current = self.watches.get(fd)
            if current is None or current.generation != generation:
                return
            self._on_delivery(current, delivery)

        watch.handle = self.io.poll_many(fd, mask, on_delivery)
        watch.armed_mask = mask

    def disarm(self, fd: int) -> None:
        watch = self.watches.get(fd)
        if watch is None or watch.handle is None:
            return
        watch.generation += 1
        handle = watch.handle
        watch.handle = None
        watch.armed_mask = 0
        handle.close()

    def remove_owner(self, fd: int, owner: int, *, keep_shell: bool) -> None:
        watch = self.watches.get(fd)
        if watch is None:
            return
        watch.regs.pop(owner, None)
        if watch.regs:
            self.note_interest(fd)
            return
        self.disarm(fd)
        if keep_shell:
            watch.shell = True
            return
        self.watches.pop(fd, None)

    def release_owner(self, owner: int) -> None:
        for fd, watch in list(self.watches.items()):
            if owner not in watch.regs:
                continue
            self.remove_owner(fd, owner, keep_shell=False)

    def sweep_shells(self, keep: set[int]) -> None:
        """Drop select-only watches that this call is not using."""

        for fd, watch in list(self.watches.items()):
            if watch.regs or fd in keep:
                continue
            self.disarm(fd)
            self.watches.pop(fd, None)

    def finish_ephemeral(self, owner: int) -> None:
        for fd, watch in list(self.watches.items()):
            reg = watch.regs.get(owner)
            if reg is None or not reg.ephemeral:
                continue
            self.remove_owner(fd, owner, keep_shell=True)
            if not self.selector_backend and fd in self.watches and self.watches[fd].regs:
                self.ensure_armed(fd)

    def wait(
        self,
        specs: list[tuple[int, _Reg]],
        timeout_seconds: float | None,
        *,
        maxevents: int | None = None,
    ) -> list[tuple[int, int]]:
        """Return ``(fd, mask)`` pairs. ``timeout_seconds`` None waits forever."""

        if not specs:
            _wait_empty(timeout_seconds)
            return []
        self._wait_depth += 1
        try:
            return self._wait(specs, timeout_seconds, maxevents)
        finally:
            self._wait_depth -= 1
            if self.selector_backend:
                for fd, _reg in specs:
                    self.disarm(fd)

    def _wait(
        self,
        specs: list[tuple[int, _Reg]],
        timeout_seconds: float | None,
        maxevents: int | None,
    ) -> list[tuple[int, int]]:
        self._revalidate(specs)
        ready, errors = self._collect(specs)
        if ready:
            return self._commit(self._cap(ready, maxevents))
        if timeout_seconds == 0:
            self._probe_empty(specs)
            ready, errors = self._collect(specs)
            if ready:
                return self._commit(self._cap(ready, maxevents))
            if errors:
                raise errors[0]
            return []
        if errors:
            raise errors[0]
        if self._needs_arm(specs):
            for fd, reg in specs:
                if reg.disarmed or reg.latched:
                    continue
                self.ensure_armed(fd)
            ready, errors = self._collect(specs)
            if ready:
                return self._commit(self._cap(ready, maxevents))
            if errors:
                raise errors[0]
            found = self._park(specs, timeout_seconds)
        elif timeout_seconds is None:
            found = self._park(specs, None)
        elif timeout_seconds > 0:
            sleep(timeout_seconds)
            found = self._pending(specs)
        else:
            found = None
        if not found:
            return []
        ready, errors = found
        if ready:
            return self._commit(self._cap(ready, maxevents))
        if errors:
            raise errors[0]
        return []

    def _needs_arm(self, specs: list[tuple[int, _Reg]]) -> bool:
        return any(not reg.disarmed and not reg.latched and reg.interest for _fd, reg in specs)

    def _park(
        self,
        specs: list[tuple[int, _Reg]],
        timeout_seconds: float | None,
    ) -> tuple[list[tuple[int, int, _Reg]], list[BaseException]] | None:
        if timeout_seconds is None:
            return self.pulse.swait_for(lambda: self._pending(specs))
        if timeout_seconds <= 0:
            return self._pending(specs)
        try:
            with timeout(timeout_seconds):
                return self.pulse.swait_for(lambda: self._pending(specs))
        except TimeoutError:
            return self._pending(specs)

    def _revalidate(self, specs: list[tuple[int, _Reg]]) -> None:
        seen_fds: set[int] = set()
        for fd, reg in specs:
            reg.latched = False
            watch = self.watches.get(fd)
            if watch is None:
                continue
            if fd not in seen_fds:
                self._revalidate_level(fd, watch)
                seen_fds.add(fd)
            if not reg.edge or not (reg.seen & reg.interest):
                continue
            mask = reg.seen & reg.interest
            got = self._probe(fd, watch, mask)
            if got is None:
                continue
            if got & mask:
                reg.latched = True
            else:
                reg.seen &= ~mask
                watch.stored &= ~mask

    def _revalidate_level(self, fd: int, watch: _Watch) -> None:
        level_mask = 0
        for reg in watch.regs.values():
            if reg.edge or reg.disarmed:
                continue
            level_mask |= watch.stored & reg.interest
        if not level_mask:
            return
        got = self._probe(fd, watch, level_mask)
        if got is None:
            return
        watch.stored = (watch.stored & ~level_mask) | (got & level_mask)
        for reg in watch.regs.values():
            reg.seen &= watch.stored

    def _probe_empty(self, specs: list[tuple[int, _Reg]]) -> None:
        for fd, reg in specs:
            if reg.disarmed or reg.latched or not reg.interest:
                continue
            if reg.edge and reg.seen:
                continue
            watch = self.watches.get(fd)
            if watch is None or (watch.stored & reg.interest):
                continue
            got = self._probe(fd, watch, reg.interest)
            if got is None:
                continue
            watch.stored = (watch.stored & ~reg.interest) | (got & reg.interest)
            for item in watch.regs.values():
                item.seen &= watch.stored

    def _probe(self, fd: int, watch: _Watch, mask: int) -> int | None:
        if not mask:
            return 0
        try:
            return probe_poll_fd_now(fd, mask)
        except BlockingIOError:
            return 0
        except OSError as exc:
            watch.error = exc
            return None

    def _collect(
        self,
        specs: list[tuple[int, _Reg]],
    ) -> tuple[list[tuple[int, int, _Reg]], list[BaseException]]:
        ready: list[tuple[int, int, _Reg]] = []
        errors: list[BaseException] = []
        for fd, reg in specs:
            if reg.disarmed:
                continue
            watch = self.watches.get(fd)
            if watch is None:
                continue
            if watch.error is not None:
                errors.append(watch.error)
            bits = watch.stored & reg.interest
            if reg.edge:
                bits &= ~reg.seen
            if bits:
                ready.append((fd, bits, reg))
        return ready, errors

    def _pending(
        self,
        specs: list[tuple[int, _Reg]],
    ) -> tuple[list[tuple[int, int, _Reg]], list[BaseException]] | None:
        ready, errors = self._collect(specs)
        if not ready and not errors:
            return None
        return ready, errors

    def _cap(
        self,
        ready: list[tuple[int, int, _Reg]],
        maxevents: int | None,
    ) -> list[tuple[int, int, _Reg]]:
        if maxevents is None or maxevents < 0:
            return ready
        return ready[:maxevents]

    def _commit(self, ready: list[tuple[int, int, _Reg]]) -> list[tuple[int, int]]:
        out: list[tuple[int, int]] = []
        for fd, bits, reg in ready:
            watch = self.watches.get(fd)
            if reg.edge:
                reg.seen |= bits
                reg.latched = True
                if watch is not None:
                    watch.stored &= ~bits
            if reg.oneshot:
                reg.disarmed = True
            out.append((fd, bits))
        if not self.selector_backend:
            for fd, _bits, reg in ready:
                if reg.oneshot:
                    self.ensure_armed(fd)
        return out

    def _on_delivery(self, watch: _Watch, delivery: MultishotDelivery) -> None:
        if delivery.exception is not None:
            if is_io_cancellation(delivery.exception):
                return
            watch.error = delivery.exception
            self.pulse.set()
            return
        if not delivery.more:
            return
        value = delivery.value
        if not isinstance(value, int):
            return
        new_bits = value & ~watch.stored
        watch.stored |= value
        visible = False
        for reg in watch.regs.values():
            if reg.disarmed:
                continue
            if reg.edge:
                fresh = new_bits & reg.interest & ~reg.seen
                if fresh:
                    reg.seen &= ~fresh
                    reg.latched = False
            reg.seen &= watch.stored
            bits = watch.stored & reg.interest
            if reg.edge:
                bits &= ~reg.seen
            if bits:
                visible = True
        if visible:
            self.pulse.set()


_tables: weakref.WeakKeyDictionary[ProactorIOManager, ReadinessTable] = weakref.WeakKeyDictionary()


def readiness_table(io: ProactorIOManager) -> ReadinessTable:
    """Return the readiness table for ``io``, creating it on first use."""

    table = _tables.get(io)
    if table is None:
        table = ReadinessTable(io)
        _tables[io] = table
    return table


def _running_table() -> ReadinessTable:
    from .proactor import ProactorScheduler
    from .scheduler import get_running_scheduler

    scheduler = get_running_scheduler()
    if isinstance(scheduler, ProactorScheduler):
        return readiness_table(scheduler.io)
    raise RuntimeError("poll operations require a proactor scheduler")


def _as_fd(obj: object) -> int:
    if isinstance(obj, int):
        return obj
    fileno = getattr(obj, "fileno", None)
    if fileno is None:
        raise TypeError("argument must be an int, or have a fileno() method")
    fd = fileno()
    if not isinstance(fd, int):
        raise TypeError("fileno() returned a non-integer")
    return fd


def _wait_empty(timeout_seconds: float | None) -> None:
    if timeout_seconds == 0:
        return
    if timeout_seconds is None:
        PulseEvent().swait()
        return
    if timeout_seconds > 0:
        sleep(timeout_seconds)


def _millis_to_seconds(timeout: float | None) -> float | None:
    if timeout is None or timeout < 0:
        return None
    return float(timeout) / 1000.0


class Poll:
    """``select.poll`` object. ``poll`` timeout is milliseconds."""

    def __init__(self) -> None:
        self._regs: dict[int, _Reg] = {}
        self._table: ReadinessTable | None = None

    def _bind(self) -> ReadinessTable:
        if self._table is None:
            self._table = _running_table()
        return self._table

    def register(self, fd: object, eventmask: int | None = None) -> None:
        """Register ``fd``. A second register overwrites the mask."""

        if eventmask is None:
            eventmask = _DEFAULT_POLL_MASK
        fd_int = _as_fd(fd)
        table = self._bind()
        reg = _Reg(id(self), eventmask)
        self._regs[fd_int] = reg
        table.add_reg(fd_int, reg)

    def modify(self, fd: object, eventmask: int) -> None:
        fd_int = _as_fd(fd)
        reg = self._regs.get(fd_int)
        if reg is None:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT))
        reg.interest = eventmask
        reg.disarmed = False
        reg.seen = 0
        reg.latched = False
        self._bind().note_interest(fd_int)

    def unregister(self, fd: object) -> None:
        fd_int = _as_fd(fd)
        if fd_int not in self._regs:
            raise KeyError(fd_int)
        del self._regs[fd_int]
        table = self._table
        if table is not None:
            table.remove_owner(fd_int, id(self), keep_shell=False)

    def poll(self, timeout: float | None = None) -> list[tuple[int, int]]:
        table = self._bind()
        specs = list(self._regs.items())
        return table.wait(specs, _millis_to_seconds(timeout))

    def close(self) -> None:
        regs = self._regs
        self._regs = {}
        table = self._table
        if table is None:
            return
        for fd in regs:
            table.remove_owner(fd, id(self), keep_shell=False)

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            return


class Epoll:
    """Level-triggered ``select.epoll``, plus ``EPOLLET`` and ``EPOLLONESHOT``.

    ``fileno`` and ``fromfd`` raise ``NotImplementedError``: there is no
    kernel epoll fd. ``EPOLLEXCLUSIVE`` is refused for the same reason.
    ``poll`` timeout is seconds. A negative or missing timeout waits forever.
    """

    def __init__(self, sizehint: int = -1, flags: int = 0) -> None:
        del sizehint, flags
        self._regs: dict[int, _Reg] = {}
        self._table: ReadinessTable | None = None
        self._closed = False

    def _bind(self) -> ReadinessTable:
        if self._closed:
            raise ValueError("I/O operation on closed epoll object")
        if self._table is None:
            self._table = _running_table()
        return self._table

    def register(self, fd: object, eventmask: int | None = None) -> None:
        if eventmask is None:
            eventmask = _DEFAULT_EPOLL_MASK
        fd_int = _as_fd(fd)
        if fd_int in self._regs:
            raise OSError(errno.EEXIST, os.strerror(errno.EEXIST))
        interest, edge, oneshot = _epoll_parts(eventmask)
        table = self._bind()
        reg = _Reg(id(self), interest, edge=edge, oneshot=oneshot)
        self._regs[fd_int] = reg
        table.add_reg(fd_int, reg)

    def modify(self, fd: object, eventmask: int) -> None:
        fd_int = _as_fd(fd)
        reg = self._regs.get(fd_int)
        if reg is None:
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT))
        interest, edge, oneshot = _epoll_parts(eventmask)
        reg.interest = interest
        reg.edge = edge
        reg.oneshot = oneshot
        reg.disarmed = False
        reg.seen = 0
        reg.latched = False
        self._bind().note_interest(fd_int)

    def unregister(self, fd: object) -> None:
        fd_int = _as_fd(fd)
        if fd_int not in self._regs:
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT))
        del self._regs[fd_int]
        table = self._table
        if table is not None:
            table.remove_owner(fd_int, id(self), keep_shell=False)

    def poll(self, timeout: float | None = None, maxevents: int = -1) -> list[tuple[int, int]]:
        table = self._bind()
        seconds: float | None
        if timeout is None or timeout < 0:
            seconds = None
        else:
            seconds = float(timeout)
        return table.wait(list(self._regs.items()), seconds, maxevents=maxevents)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        regs = self._regs
        self._regs = {}
        table = self._table
        if table is None:
            return
        for fd in regs:
            table.remove_owner(fd, id(self), keep_shell=False)

    def fileno(self) -> int:
        raise NotImplementedError("tealet epoll has no kernel file descriptor")

    @classmethod
    def fromfd(cls, fd: int) -> Epoll:
        raise NotImplementedError("tealet epoll has no kernel file descriptor")

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            return


def _epoll_parts(mask: int) -> tuple[int, bool, bool]:
    if mask & _EPOLLEXCLUSIVE:
        raise NotImplementedError("EPOLLEXCLUSIVE is not supported")
    edge = bool(mask & _EPOLLET)
    oneshot = bool(mask & _EPOLLONESHOT)
    interest = mask & ~_EPOLL_FLAGS
    if interest == 0:
        raise ValueError("epoll event mask must include a readiness bit")
    return interest, edge, oneshot


def poll() -> Poll:
    """Factory matching the ``select.poll`` builtin."""

    return Poll()


def select(
    rlist: Any,
    wlist: Any,
    xlist: Any,
    timeout: float | None = None,
) -> tuple[list[Any], list[Any], list[Any]]:
    """Tealet-blocking ``select.select``. Timeout is seconds.

    Fds introduced only by this call keep their stored mask (with the arm
    dropped) so the next call can revalidate it. A fd that a later call does
    not mention is dropped.
    """

    if timeout is not None and timeout < 0:
        raise ValueError("timeout must be non-negative")
    readers = list(rlist or ())
    writers = list(wlist or ())
    excepts = list(xlist or ())
    seconds = None if timeout is None else float(timeout)
    if not readers and not writers and not excepts:
        _wait_empty(seconds)
        return [], [], []

    table = _running_table()
    groups: dict[int, _Reg] = {}
    fd_of: dict[int, int] = {}
    owner = id(groups)

    def add(obj: object, bits: int) -> None:
        fd = _as_fd(obj)
        fd_of[id(obj)] = fd
        reg = groups.get(fd)
        if reg is None:
            reg = _Reg(owner, 0, ephemeral=True)
            groups[fd] = reg
        reg.interest |= bits

    for obj in readers:
        add(obj, _READ_MASK)
    for obj in writers:
        add(obj, _WRITE_MASK)
    for obj in excepts:
        add(obj, _EXC_MASK)

    table.sweep_shells(set(groups))
    for fd, reg in groups.items():
        table.add_reg(fd, reg)
    try:
        ready = table.wait(list(groups.items()), seconds)
    finally:
        table.finish_ephemeral(owner)
    ready_map = {fd: mask for fd, mask in ready}

    def pick(objs: list[Any], bits: int) -> list[Any]:
        out: list[Any] = []
        seen: set[int] = set()
        for obj in objs:
            key = id(obj)
            if key in seen:
                continue
            fd = fd_of.get(key)
            if fd is None:
                continue
            if ready_map.get(fd, 0) & bits:
                seen.add(key)
                out.append(obj)
        return out

    return pick(readers, _READ_MASK), pick(writers, _WRITE_MASK), pick(excepts, _EXC_MASK)
