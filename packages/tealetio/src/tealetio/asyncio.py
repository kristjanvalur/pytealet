from __future__ import annotations

import asyncio as _asyncio
import asyncio.proactor_events as _proactor_events
import contextvars
import selectors
import socket
from collections.abc import Mapping
from contextlib import suppress
from typing import Any, Callable, TypeVar, cast

from . import compat
from .locks import Event, TimeoutError
from .scheduler import (
    AsyncDrivingMixin,
    AsyncSchedulerDrivingAPI,
    BaseScheduler,
    RunnableQueueFactory,
    SyncSchedulerDrivingAPI,
    _current_scheduler,
    gather,
)
from .tasks import (
    CancelledError,
    _copy_context_without_current_task,
    get_current,
)
from .runner import BaseRunner
from .runner import Runner as TealetRunner
from .io_manager import SocketSendBuffer
from .proactor import Operation, Proactor, ProactorScheduler
from .selector import SelectorScheduler

T = TypeVar("T")

__all__ = [
    "AsyncRunner",
    "AsyncScheduler",
    "ForwardingSelector",
    "ForwardingProactor",
    "TealetProactorEventLoop",
    "TealetSelectorEventLoop",
    "asyncio_get_current",
    "run_async",
    "run_asyncio_in_tealet",
    "run_in_asyncio",
]


def asyncio_get_current() -> _asyncio.Task[Any] | None:
    """Return the current asyncio task, unless execution is inside a Task."""
    if get_current() is not None:
        return None
    return _asyncio.current_task()


class ForwardingSelector(selectors.BaseSelector):
    """Asyncio selector facade backed by a tealetio selector scheduler."""

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
        super().__init__(selector=ForwardingSelector(scheduler))


class ForwardingProactor:
    """Asyncio proactor facade backed by a tealetio proactor."""

    def __init__(self, proactor: Proactor) -> None:
        self._proactor = proactor
        self._loop: _asyncio.AbstractEventLoop | None = None
        self._closed = False

    def set_loop(self, loop: _asyncio.AbstractEventLoop) -> None:
        """Bind the asyncio loop that owns Futures created by this proactor."""

        self._loop = loop

    def close(self) -> None:
        """Detach from the host proactor without closing it."""

        self._closed = True
        self._loop = None

    def get_map(self) -> Mapping[Any, Any]:
        """Return an empty selector map for BaseEventLoop compatibility."""

        return {}

    def select(self, timeout: float | None = None) -> list[object]:
        """Wait for host proactor completions and let Futures schedule callbacks."""

        if self._closed:
            return []
        deadline = None if timeout is None else self._proactor.get_time() + timeout
        self._proactor.wait(deadline)
        return []

    def recv(self, sock: socket.socket, n: int) -> _asyncio.Future[bytes]:
        """Receive bytes through the host proactor."""

        return self._future_from_operation(self._proactor.recv(sock, n))

    def recv_into(self, sock: socket.socket, buf: Any) -> _asyncio.Future[int]:
        """Receive bytes into `buf` through the host proactor."""

        return self._future_from_operation(self._proactor.recv_into(sock, buf))

    def recvfrom(self, sock: socket.socket, bufsize: int) -> _asyncio.Future[tuple[bytes, Any]]:
        """Receive datagram bytes and address through the host proactor."""

        return self._future_from_operation(self._proactor.recvfrom(sock, bufsize))

    def recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> _asyncio.Future[tuple[int, Any]]:
        """Receive datagram bytes into `buf` through the host proactor."""

        return self._future_from_operation(self._proactor.recvfrom_into(sock, buf, nbytes))

    def send(self, sock: socket.socket, data: Any) -> _asyncio.Future[None]:
        """Send all bytes through the host proactor."""

        return self._future_from_operation(self._proactor.sendall(sock, data))

    def sendto(self, sock: socket.socket, data: Any, flags: int, address: Any) -> _asyncio.Future[int]:
        """Send datagram bytes through the host proactor."""

        if flags:
            future: _asyncio.Future[int] = self._require_loop().create_future()
            future.set_exception(NotImplementedError("sendto flags are not supported by ForwardingProactor"))
            return future
        return self._future_from_operation(self._proactor.sendto(sock, data, address))

    def accept(self, sock: socket.socket) -> _asyncio.Future[tuple[socket.socket, Any]]:
        """Accept a socket through the host proactor."""

        return self._future_from_operation(self._proactor.accept(sock))

    def connect(
        self,
        sock: socket.socket,
        address: Any,
        *,
        initial: SocketSendBuffer | None = None,
    ) -> _asyncio.Future[None] | _asyncio.Future[bool]:
        """Connect a socket through the host proactor.

        When ``initial`` is provided the future completes with ``True`` when
        connect-time send ran (including an empty buffer). Backends that ignore
        ``initial`` complete with a falsy result; callers should flush ``initial``
        themselves.
        """

        return self._future_from_operation(self._proactor.connect(sock, address, initial=initial))

    def create_socket(
        self,
        family: int,
        type: int,
        proto: int = 0,
        *,
        flags: int = 0,
        connect_to: Any | None = None,
        initial_data: SocketSendBuffer | None = None,
    ) -> _asyncio.Future[tuple[socket.socket, bool, bool]]:
        """Create a socket through the host proactor."""

        return self._future_from_operation(
            self._proactor.create_socket(
                family,
                type,
                proto,
                flags=flags,
                connect_to=connect_to,
                initial_data=initial_data,
            )
        )

    def sendfile(self, sock: socket.socket, file: Any, offset: int, blocksize: int) -> _asyncio.Future[int]:
        """Report that native proactor sendfile is not available."""

        future: _asyncio.Future[int] = self._require_loop().create_future()
        future.set_exception(_asyncio.SendfileNotAvailableError("ForwardingProactor does not support sendfile"))
        return future

    def _stop_serving(self, sock: socket.socket) -> None:
        pass

    def _future_from_operation(self, operation: Operation[T]) -> _asyncio.Future[T]:
        loop = self._require_loop()
        future: _asyncio.Future[T] = loop.create_future()

        def complete_future() -> None:
            if future.cancelled():
                return
            if operation.cancelled():
                future.cancel()
                return
            try:
                result = operation.result()
            except BaseException as exc:
                future.set_exception(exc)
            else:
                future.set_result(result)

        def complete_operation(_operation: Operation[Any]) -> None:
            try:
                loop.call_soon_threadsafe(complete_future)
            except RuntimeError:
                pass

        def cancel_operation(asyncio_future: _asyncio.Future[T]) -> None:
            if asyncio_future.cancelled():
                operation.cancel()

        if operation.done():
            complete_future()
        else:
            operation.add_done_callback(complete_operation)
            future.add_done_callback(cancel_operation)
        return future

    def _require_loop(self) -> _asyncio.AbstractEventLoop:
        if self._loop is None:
            raise RuntimeError("ForwardingProactor is not bound to an asyncio loop")
        return self._loop


class TealetProactorEventLoop(_proactor_events.BaseProactorEventLoop):
    """Asyncio proactor loop hosted by a tealetio proactor scheduler."""

    def __init__(self, proactor: Proactor | None = None) -> None:
        if proactor is None:
            current = _current_scheduler()
            if not isinstance(current, ProactorScheduler):
                raise RuntimeError("TealetProactorEventLoop requires a current ProactorScheduler")
            proactor = current.proactor
        self._tealet_proactor = proactor
        super().__init__(ForwardingProactor(proactor))

    def run_forever(self) -> None:
        """Run the loop while polling the host tealetio proactor."""

        loop_self_reading = getattr(self, "_loop_self_reading", None)
        if loop_self_reading is not None:
            loop_self_reading()
        try:
            super().run_forever()
        finally:
            self_reading_future = getattr(self, "_self_reading_future", None)
            if self_reading_future is not None:
                self_reading_future.cancel()
                self._self_reading_future = None


class AsyncScheduler(AsyncDrivingMixin, BaseScheduler, AsyncSchedulerDrivingAPI):
    """Cooperative scheduler for asyncio-hosted driving."""

    def __init__(self, *, runnable_queue_factory: RunnableQueueFactory | None = None) -> None:
        super().__init__(runnable_queue_factory=runnable_queue_factory)
        self._wakeup = _asyncio.Event()
        self._wakeup_loop: _asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: _asyncio.AbstractEventLoop) -> None:
        """Bind this scheduler to an asyncio event loop clock."""

        if self._wakeup_loop is not None and self._wakeup_loop is not loop:
            raise RuntimeError("AsyncScheduler is already bound to a different event loop")
        self._wakeup_loop = loop
        self._time = loop.time

    def _lazy_bind_running_loop(self) -> None:
        if self._wakeup_loop is None:
            self.bind_loop(_asyncio.get_running_loop())

    def _before_arun(self) -> None:
        self._lazy_bind_running_loop()

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
        """Register a readability callback on the running asyncio loop."""

        loop = _asyncio.get_running_loop()
        loop.add_reader(fd, callback, *args)

    def remove_reader(self, fd: int) -> bool:
        """Remove a readability callback from the running asyncio loop."""

        loop = _asyncio.get_running_loop()
        return loop.remove_reader(fd)

    def add_writer(self, fd: int, callback: Callable[..., object], *args: object) -> None:
        """Register a writability callback on the running asyncio loop."""

        loop = _asyncio.get_running_loop()
        loop.add_writer(fd, callback, *args)

    def remove_writer(self, fd: int) -> bool:
        """Remove a writability callback from the running asyncio loop."""

        loop = _asyncio.get_running_loop()
        return loop.remove_writer(fd)

    # -- Asyncio socket helpers --------------------------------------

    def sock_recv(self, sock: socket.socket, n: int) -> bytes:
        """Receive up to `n` bytes using asyncio socket readiness."""

        loop = _asyncio.get_running_loop()
        return self.await_(loop.sock_recv(sock, n))

    def sock_recv_into(self, sock: socket.socket, buf: Any) -> int:
        """Receive bytes into `buf` using asyncio socket readiness."""

        loop = _asyncio.get_running_loop()
        return self.await_(loop.sock_recv_into(sock, buf))

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> tuple[bytes, Any]:
        """Receive datagram bytes and address using asyncio socket readiness."""

        loop = _asyncio.get_running_loop()
        return self.await_(compat.sock_recvfrom(loop, sock, bufsize))

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> tuple[int, Any]:
        """Receive datagram bytes into `buf` using asyncio socket readiness."""

        loop = _asyncio.get_running_loop()
        return self.await_(compat.sock_recvfrom_into(loop, sock, buf, nbytes))

    def sock_sendall(self, sock: socket.socket, data: Any) -> None:
        """Send all `data` using asyncio socket readiness."""

        loop = _asyncio.get_running_loop()
        return self.await_(loop.sock_sendall(sock, data))

    def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> int:
        """Send one datagram using asyncio socket readiness."""

        loop = _asyncio.get_running_loop()
        return self.await_(compat.sock_sendto(loop, sock, data, address))

    def sock_accept(self, sock: socket.socket) -> tuple[socket.socket, Any]:
        """Accept one connection using asyncio socket readiness."""

        loop = _asyncio.get_running_loop()
        return self.await_(loop.sock_accept(sock))

    def sock_connect(self, sock: socket.socket, address: Any) -> None:
        """Connect a socket using asyncio socket readiness."""

        loop = _asyncio.get_running_loop()
        return self.await_(loop.sock_connect(sock, address))

    # -- Async waiting -------------------------------------------------

    async def _wait_async(self) -> None:
        wakeup = self._wakeup
        if wakeup.is_set():
            wakeup.clear()
            return
        deadline = self._next_timer_deadline()
        if deadline is None:
            # No scheduler timer is pending. We may still be alive because one or
            # more tealets are blocked in await_() on external asyncio
            # awaitables, so block until an explicit wakeup arrives.
            await wakeup.wait()
            wakeup.clear()
            return
        try:
            await compat.wait_until(wakeup.wait(), deadline)
        except TimeoutError:
            pass
        finally:
            wakeup.clear()

    async def _driver_wait(self) -> None:
        await self._wait_async()

    async def _driver_yield(self) -> None:
        await _asyncio.sleep(0)


class AsyncRunner(BaseRunner[AsyncSchedulerDrivingAPI]):
    """Run scheduler-backed entries from within an existing asyncio task."""

    default_factory = AsyncScheduler

    @property
    def task(self) -> _asyncio.Task[None] | None:
        """Return the background asyncio task used by this runner, if any."""

        return None

    async def __aenter__(self) -> "AsyncRunner":
        self._lazy_init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Shut down pending scheduler tasks and release runner resources."""

        if self._closed:
            return
        scheduler = self._scheduler
        try:
            if scheduler is not None:
                scheduler.bind_loop(_asyncio.get_running_loop())
                with scheduler.main_context():
                    tasks = self._shutdown_scheduler_tasks(scheduler)
                    async_scheduler = scheduler
                    shutdown_group = gather(*tasks, return_exceptions=True)
                    await async_scheduler.arun_until_complete(shutdown_group)
                    executor_shutdown = scheduler.shutdown_default_executor()
                    await async_scheduler.arun_until_complete(executor_shutdown)
        finally:
            self._finalize_close(scheduler)

    async def run(self, entry, /, *, context: contextvars.Context | None = None):
        """Run one callable or Future to completion using this async runner."""

        self._lazy_init()
        scheduler = self._require_scheduler()
        scheduler.bind_loop(_asyncio.get_running_loop())
        run_context = self._resolve_context(context)
        with scheduler.main_context():
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
    scheduler_factory: Callable[[], SyncSchedulerDrivingAPI] | None = None,
    loop_factory: Callable[[], _asyncio.AbstractEventLoop] | None = None,
    debug: bool | None = None,
    handle_sigint: bool = False,
):
    """Run one asyncio entry under a temporary sync scheduler."""

    tealet_runner = TealetRunner(
        scheduler_factory=scheduler_factory,
        debug=debug,
        handle_sigint=handle_sigint,
    )

    def run_inside_tealet():
        scheduler = tealet_runner.get_scheduler()
        if not isinstance(scheduler, BaseScheduler):
            raise RuntimeError("run_asyncio_in_tealet requires a BaseScheduler-compatible scheduler")
        base_scheduler = scheduler

        def tealet_loop_factory() -> _asyncio.AbstractEventLoop:
            if loop_factory is not None:
                return loop_factory()
            if isinstance(scheduler, ProactorScheduler):
                loop = TealetProactorEventLoop(scheduler.proactor)
            elif isinstance(scheduler, SelectorScheduler):
                loop = TealetSelectorEventLoop(scheduler)
            else:
                raise RuntimeError("run_asyncio_in_tealet requires a selector or proactor scheduler")
            _asyncio.set_event_loop(loop)
            return loop

        async def yield_to_tealet_scheduler() -> None:
            while True:
                await _asyncio.sleep(0)
                base_scheduler.sleep(0)

        async def wrapped_entry():
            yielder = _asyncio.create_task(yield_to_tealet_scheduler())
            try:
                return await entry
            finally:
                yielder.cancel()
                with suppress(CancelledError):
                    await yielder

        run_context = _copy_context_without_current_task(context)
        return compat.run_asyncio_once(
            wrapped_entry(), context=run_context, loop_factory=tealet_loop_factory, debug=debug
        )

    try:
        return tealet_runner.run(run_inside_tealet)
    finally:
        tealet_runner.close()
