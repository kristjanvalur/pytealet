from __future__ import annotations

import asyncio as _asyncio
import contextvars
import selectors
import socket
from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any, Callable, TypeVar, cast

from . import compat
from .locks import Event, TimeoutError
from .scheduler import (
    BaseScheduler,
    CoreSchedulerDrivingAPI,
    _current_scheduler,
    gather,
)
from .tasks import CancelledError, Future, TealetTask
from .runner import BaseRunner
from .runner import Runner as TealetRunner
from .selector import SelectorScheduler

T = TypeVar("T")


class AsyncSchedulerDrivingAPI(CoreSchedulerDrivingAPI, ABC):
    """Asyncio-hosted scheduler driver API."""

    @abstractmethod
    def stop(self) -> None:
        """Stop a currently running async driver."""

    @abstractmethod
    async def arun(self) -> None:
        """Run async scheduler loop until idle."""

    @abstractmethod
    async def arun_forever(self) -> None:
        """Run async scheduler loop until stop() is called."""

    @abstractmethod
    async def arun_until_complete(self, future: Future[T] | Callable[[], T]) -> T:
        """Run async scheduler loop until a target future/callable completes."""


class _SchedulerSelectorAdapter(selectors.BaseSelector):
    def __init__(self, scheduler: SelectorScheduler) -> None:
        self._scheduler = scheduler
        self._keys: dict[int, selectors.SelectorKey] = {}
        self._ready_masks: dict[int, int] = {}
        self._ready = Event()
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for fd in list(self._keys):
            self.unregister(fd)

    def get_map(self) -> Mapping[Any, selectors.SelectorKey]:
        return self._keys

    def get_key(self, fileobj: object) -> selectors.SelectorKey:
        fd = self._fileobj_to_fd(fileobj)
        try:
            return self._keys[fd]
        except KeyError:
            raise KeyError(f"{fileobj!r} is not registered") from None

    def register(self, fileobj: object, events: int, data: object = None) -> selectors.SelectorKey:
        fd = self._fileobj_to_fd(fileobj)
        if fd in self._keys:
            raise KeyError(f"{fileobj!r} is already registered")
        self._validate_events(events)
        key = selectors.SelectorKey(cast(Any, fileobj), fd, events, data)
        self._keys[fd] = key
        try:
            self._sync_scheduler_registration(fd, 0, events)
        except Exception:
            del self._keys[fd]
            raise
        return key

    def unregister(self, fileobj: object) -> selectors.SelectorKey:
        key = self.get_key(fileobj)
        del self._keys[key.fd]
        self._ready_masks.pop(key.fd, None)
        self._sync_scheduler_registration(key.fd, key.events, 0)
        return key

    def modify(self, fileobj: object, events: int, data: object = None) -> selectors.SelectorKey:
        old_key = self.get_key(fileobj)
        self._validate_events(events)
        new_key = selectors.SelectorKey(old_key.fileobj, old_key.fd, events, data)
        self._sync_scheduler_registration(old_key.fd, old_key.events, events)
        self._keys[old_key.fd] = new_key
        return new_key

    def select(self, timeout: float | None = None) -> list[tuple[selectors.SelectorKey, int]]:
        if timeout is not None and timeout <= 0:
            return self._drain_ready()

        if not self._ready_masks:
            self._ready.clear()
            if timeout is None:
                self._ready.swait()
            else:
                handle = self._scheduler.call_later(timeout, self._ready.set)
                try:
                    self._ready.swait()
                finally:
                    handle.cancel()

        return self._drain_ready()

    def _fileobj_to_fd(self, fileobj: object) -> int:
        return self._scheduler._fileobj_to_fd(fileobj)

    def _validate_events(self, events: int) -> None:
        valid = selectors.EVENT_READ | selectors.EVENT_WRITE
        if not events or events & ~valid:
            raise ValueError("Invalid events")

    def _sync_scheduler_registration(self, fd: int, old_events: int, new_events: int) -> None:
        if old_events & selectors.EVENT_READ and not new_events & selectors.EVENT_READ:
            self._scheduler.remove_reader(fd)
        if old_events & selectors.EVENT_WRITE and not new_events & selectors.EVENT_WRITE:
            self._scheduler.remove_writer(fd)
        if new_events & selectors.EVENT_READ and not old_events & selectors.EVENT_READ:
            self._scheduler.add_reader(fd, self._mark_ready, fd, selectors.EVENT_READ)
        if new_events & selectors.EVENT_WRITE and not old_events & selectors.EVENT_WRITE:
            self._scheduler.add_writer(fd, self._mark_ready, fd, selectors.EVENT_WRITE)

    def _mark_ready(self, fd: int, event: int) -> None:
        key = self._keys.get(fd)
        if key is None or not key.events & event:
            return
        self._ready_masks[fd] = self._ready_masks.get(fd, 0) | event
        self._ready.set()

    def _drain_ready(self) -> list[tuple[selectors.SelectorKey, int]]:
        events = []
        ready_masks = self._ready_masks
        self._ready_masks = {}
        self._ready.clear()
        for fd, mask in ready_masks.items():
            key = self._keys.get(fd)
            if key is None:
                continue
            mask &= key.events
            if mask:
                events.append((key, mask))
        return events


class TealetSelectorEventLoop(_asyncio.SelectorEventLoop):
    """Asyncio selector loop hosted by a SelectorScheduler."""

    def __init__(self, scheduler: SelectorScheduler | None = None) -> None:
        if scheduler is None:
            current = _current_scheduler()
            if not isinstance(current, SelectorScheduler):
                raise RuntimeError("TealetSelectorEventLoop requires a current SelectorScheduler")
            scheduler = current
        self._tealet_scheduler = scheduler
        super().__init__(selector=_SchedulerSelectorAdapter(scheduler))


class AsyncScheduler(BaseScheduler, AsyncSchedulerDrivingAPI):
    """Cooperative scheduler for asyncio-hosted driving."""

    def __init__(self) -> None:
        super().__init__()
        self._wakeup = _asyncio.Event()
        self._wakeup_loop: _asyncio.AbstractEventLoop | None = None

    # -- Driver wakeup -------------------------------------------------

    def _break_wait_threadsafe(self) -> None:
        loop = self._wakeup_loop
        if loop is None:
            return
        try:
            if _asyncio.get_running_loop() is loop:
                self._wakeup.set()
                return
        except RuntimeError:
            pass
        loop.call_soon_threadsafe(self._wakeup.set)

    def _break_wait(self) -> None:
        """Wake the asyncio-hosted scheduler from its owning thread."""
        self._wakeup.set()

    # -- Asyncio fd callbacks ----------------------------------------

    def add_reader(self, fd: int, callback: Callable[..., object], *args: object) -> None:
        loop = _asyncio.get_running_loop()
        loop.add_reader(fd, callback, *args)

    def remove_reader(self, fd: int) -> bool:
        loop = _asyncio.get_running_loop()
        return loop.remove_reader(fd)

    def add_writer(self, fd: int, callback: Callable[..., object], *args: object) -> None:
        loop = _asyncio.get_running_loop()
        loop.add_writer(fd, callback, *args)

    def remove_writer(self, fd: int) -> bool:
        loop = _asyncio.get_running_loop()
        return loop.remove_writer(fd)

    # -- Asyncio socket helpers --------------------------------------

    def sock_recv(self, sock: socket.socket, n: int) -> bytes:
        loop = _asyncio.get_running_loop()
        return self.await_(loop.sock_recv(sock, n))

    def sock_recv_into(self, sock: socket.socket, buf: Any) -> int:
        loop = _asyncio.get_running_loop()
        return self.await_(loop.sock_recv_into(sock, buf))

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> tuple[bytes, Any]:
        loop = _asyncio.get_running_loop()
        return self.await_(compat.sock_recvfrom(loop, sock, bufsize))

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> tuple[int, Any]:
        loop = _asyncio.get_running_loop()
        return self.await_(compat.sock_recvfrom_into(loop, sock, buf, nbytes))

    def sock_sendall(self, sock: socket.socket, data: Any) -> None:
        loop = _asyncio.get_running_loop()
        return self.await_(loop.sock_sendall(sock, data))

    def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> int:
        loop = _asyncio.get_running_loop()
        return self.await_(compat.sock_sendto(loop, sock, data, address))

    def sock_accept(self, sock: socket.socket) -> tuple[socket.socket, Any]:
        loop = _asyncio.get_running_loop()
        return self.await_(loop.sock_accept(sock))

    def sock_connect(self, sock: socket.socket, address: Any) -> None:
        loop = _asyncio.get_running_loop()
        return self.await_(loop.sock_connect(sock, address))

    # -- Async waiting -------------------------------------------------

    async def _wait_async(self) -> None:
        wakeup = self._wakeup
        if wakeup.is_set():
            wakeup.clear()
            return
        timeout = self._time_to_next_timer()
        if timeout is None:
            # No scheduler timer is pending. We may still be alive because one or
            # more tealets are blocked in await_() on external asyncio
            # awaitables, so block until an explicit wakeup arrives.
            await wakeup.wait()
            wakeup.clear()
            return
        try:
            await compat.wait_for_timeout(wakeup.wait(), timeout)
        except TimeoutError:
            pass
        finally:
            wakeup.clear()

    # -- Async run entry points ---------------------------------------

    async def arun(self) -> None:
        self._verify_current_scheduler()
        self._wakeup_loop = _asyncio.get_running_loop()
        self._running = True
        try:
            while self._tasks or self._timers or self._pending_async_waits:
                if self._tasks or self._timers:
                    self._pump()
                await self._wait_async()
        finally:
            self._running = False
            self._wakeup_loop = None

    async def arun_forever(self) -> None:
        self._verify_current_scheduler()
        self._wakeup_loop = _asyncio.get_running_loop()
        self._stopping = False
        self._running = True
        try:
            while not self._stopping:
                self._run_ready_timers()
                if self._tasks:
                    self._pump()
                    continue
                await self._wait_async()
        finally:
            self._running = False
            self._stopping = False
            self._wakeup_loop = None

    async def arun_until_complete(
        self,
        future: Future[T] | Callable[[], T],
    ) -> T:
        self._verify_current_scheduler()
        if isinstance(future, Future):
            target: Future[T] = future
            if isinstance(target, TealetTask) and target.get_scheduler() is not self:
                raise RuntimeError("Future is bound to a different scheduler")
        elif callable(future):
            target = cast(Future[T], self.spawn(future))
        else:
            raise TypeError("future must be a Future or callable")

        self._wakeup_loop = _asyncio.get_running_loop()
        self._stopping = False
        self._running = True
        try:
            while not target.done() and not self._stopping:
                self._run_ready_timers()
                if self._tasks:
                    self._pump()
                if not target.done() and not self._stopping:
                    await self._wait_async()
        finally:
            self._running = False
            self._stopping = False
            self._wakeup_loop = None

        if not target.done():
            raise RuntimeError("Scheduler stopped before Future completed.")
        return cast(T, target.result())


class AsyncRunner(BaseRunner[AsyncSchedulerDrivingAPI]):
    """Run scheduler-backed entries from within an existing asyncio task."""

    default_factory = AsyncScheduler

    @property
    def task(self) -> _asyncio.Task[None] | None:
        return None

    async def __aenter__(self) -> "AsyncRunner":
        self._lazy_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._closed:
            return
        scheduler = self._scheduler
        try:
            if scheduler is not None:
                tasks = self._shutdown_scheduler_tasks(scheduler)
                async_scheduler = cast(AsyncSchedulerDrivingAPI, scheduler)
                shutdown_group = gather(*tasks, return_exceptions=True)
                await async_scheduler.arun_until_complete(shutdown_group)
                executor_shutdown = scheduler.shutdown_default_executor()
                await async_scheduler.arun_until_complete(executor_shutdown)
        finally:
            self._finalize_close(scheduler)

    async def run(self, entry, /, *, context: contextvars.Context | None = None):
        self._lazy_init()
        scheduler = self._require_scheduler()
        run_context = self._resolve_context(context)
        target = self._target_from_entry(entry, run_context)
        sigint_handler = self._install_sigint_handler(target, cast(BaseScheduler, scheduler))
        self._interrupt_count = 0
        try:
            try:
                return await scheduler.arun_until_complete(target)
            except CancelledError:
                self._raise_keyboard_interrupt_if_requested()
                raise
        finally:
            self._restore_sigint_handler(sigint_handler)


async def run_async(
    entry,
    /,
    *,
    context: contextvars.Context | None = None,
    scheduler_factory: Callable[[], AsyncSchedulerDrivingAPI] | None = None,
    debug: bool | None = None,
    handle_sigint: bool = True,
):
    """Convenience helper that runs one entry under a temporary AsyncRunner."""

    runner = AsyncRunner(
        scheduler_factory=scheduler_factory,
        context=context,
        debug=debug,
        handle_sigint=handle_sigint,
    )
    try:
        return await runner.run(entry)
    finally:
        await runner.aclose()


def run_in_asyncio(
    entry,
    /,
    *,
    context: contextvars.Context | None = None,
    scheduler_factory: Callable[[], AsyncSchedulerDrivingAPI] | None = None,
    loop_factory: Callable[[], _asyncio.AbstractEventLoop] | None = None,
    debug: bool | None = None,
    handle_sigint: bool = True,
):
    """Run one entry under an AsyncRunner owned by a temporary asyncio.Runner."""

    return compat.run_asyncio_once(
        run_async(
            entry,
            context=context,
            scheduler_factory=scheduler_factory,
            debug=debug,
            handle_sigint=handle_sigint,
        ),
        loop_factory=loop_factory,
        debug=debug,
    )


def run_asyncio_in_tealet(
    entry,
    /,
    *,
    context: contextvars.Context | None = None,
    scheduler_factory: Callable[[], SelectorScheduler] | None = None,
    loop_factory: Callable[[], _asyncio.AbstractEventLoop] | None = None,
    debug: bool | None = None,
    handle_sigint: bool = False,
):
    """Run one asyncio entry under a temporary SelectorScheduler-owned tealet runner."""

    tealet_runner = TealetRunner(
        scheduler_factory=scheduler_factory or SelectorScheduler,
        debug=debug,
        handle_sigint=handle_sigint,
    )

    def run_inside_tealet():
        tealet_runner.get_scheduler()

        def tealet_loop_factory() -> _asyncio.AbstractEventLoop:
            if loop_factory is not None:
                return loop_factory()
            loop = TealetSelectorEventLoop()
            _asyncio.set_event_loop(loop)
            return loop

        return compat.run_asyncio_once(entry, context=context, loop_factory=tealet_loop_factory, debug=debug)

    try:
        return tealet_runner.run(run_inside_tealet)
    finally:
        tealet_runner.close()
