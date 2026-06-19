from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import errno
import functools
import heapq
import inspect
import itertools
import selectors
import socket
import threading
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Callable, Generic, TypeVar, cast

import tealet

from .locks import Event, InvalidStateError, set_scheduler_resolver

T = TypeVar("T")
_FdCallback = tuple[Callable[..., object], tuple[object, ...], contextvars.Context]


@dataclass
class _FdCallbacks:
    reader: _FdCallback | None = None
    writer: _FdCallback | None = None

    def empty(self) -> bool:
        return self.reader is None and self.writer is None


# a thread local scheduler
_scheduler = threading.local()


class Linkable(ABC):
    """Base interface for objects that can be linked from a TealetTask."""

    @abstractmethod
    def _unlink(self, t: tealet.tealet) -> None:
        """Detach a tealet from this link target."""

    def _query_waiting(self, t: tealet.tealet) -> bool:
        return False

    def _query_runnable(self, t: tealet.tealet) -> bool:
        return False


class CoreSchedulerDrivingAPI(ABC):
    """Common control surface shared by sync and async scheduler drivers."""

    @abstractmethod
    def is_running(self) -> bool:
        """Return whether this scheduler is currently driving."""

    @abstractmethod
    def set_debug(self, enabled: bool) -> None:
        """Set scheduler debug mode flag."""

    @abstractmethod
    def get_debug(self) -> bool:
        """Return scheduler debug mode flag."""

    @abstractmethod
    def spawn(
        self,
        func: Callable[[], T],
        kwargs: dict[str, object] | None = None,
        context: contextvars.Context | None = None,
    ) -> "TealetTask":
        """Spawn a scheduler-managed task from a zero-arg callable."""


class SyncSchedulerDrivingAPI(CoreSchedulerDrivingAPI, ABC):
    """Synchronous scheduler driver API."""

    @abstractmethod
    def stop(self) -> None:
        """Stop a currently running sync driver."""

    @abstractmethod
    def run(self) -> None:
        """Run until no local runnable tasks or timers remain."""

    @abstractmethod
    def run_forever(self) -> None:
        """Run until stop() is called."""

    @abstractmethod
    def run_until_complete(self, future: "Future[T] | Callable[[], T]") -> T:
        """Run until a target future/callable completes."""


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
    async def arun_until_complete(self, future: "Future[T] | Callable[[], T]") -> T:
        """Run async scheduler loop until a target future/callable completes."""

def set_scheduler(value: "BaseScheduler | None") -> None:
    if value is None:
        if hasattr(_scheduler, "instance"):
            del _scheduler.instance
        return
    _scheduler.instance = value


def get_running_scheduler() -> "BaseScheduler":
    current = _current_scheduler()
    if current is not None and current.is_running():
        return current
    raise RuntimeError("no running scheduler")


set_scheduler_resolver(get_running_scheduler)


def _current_scheduler() -> "BaseScheduler | None":
    return getattr(_scheduler, "instance", None)


class RawTimeoutError(BaseException):
    """Internal timeout sentinel thrown into tealets by Timeout."""

    pass


TimeoutError = asyncio.TimeoutError
CancelledError = asyncio.CancelledError


def timeout(delay: float) -> "Timeout":
    """Context manager for timing out a block of code via scheduler timers."""
    when = get_running_scheduler().time() + delay
    return Timeout(when)


def timeout_at(when: float) -> "Timeout":
    """Context manager for timing out a block of code at a specific time via scheduler timers."""
    return Timeout(when)


def to_thread(func: Callable[..., T], /, *args: object, **kwargs: object) -> T:
    """Run a callable in the scheduler default thread pool and wait for its result."""

    context = contextvars.copy_context()
    call = functools.partial(context.run, func, *args, **kwargs)
    return get_running_scheduler().run_in_executor(None, call).wait()


class Channel(Linkable):
    """Rendezvous channel for sync tealet operations and optional async waits."""

    # Operation model:
    # - Unbuffered rendezvous. Each send pairs with one receive.
    # - Single waiter queue, direction inferred from signed balance.
    # - Sync operations may do immediate tealet run() based on preference.
    # - Async operations never do immediate transfer; they only wake.

    def __init__(self, preference: int = -1) -> None:
        self._waiters: deque[tealet.tealet | Event] = deque()
        self._balance = 0
        self._packets: dict[tealet.tealet | Event, tuple[bool, object]] = {}
        self.preference = preference

    # -- Configuration -------------------------------------------------

    @property
    def balance(self) -> int:
        return self._balance

    @property
    def preference(self) -> int:
        return self._preference

    @preference.setter
    def preference(self, value: int) -> None:
        if value not in (-1, 0, 1):
            raise ValueError("preference must be -1, 0, or 1")
        self._preference = value

    def _deliver(self, packet: tuple[bool, object]) -> object:
        is_exc, payload = packet
        if is_exc:
            raise payload
        return payload

    # -- Waiter bookkeeping -------------------------------------------

    def _clear_link(self, t: tealet.tealet) -> None:
        try:
            t.link = None
        except AttributeError:
            pass

    def _set_waiter_link(self, waiter: tealet.tealet | Event) -> None:
        if isinstance(waiter, Event):
            return
        self._clear_link(waiter)
        try:
            waiter.link = self
        except AttributeError:
            pass

    def _waiter_scheduler(self, waiter: tealet.tealet) -> BaseScheduler:
        if isinstance(waiter, TealetTask):
            return waiter.get_scheduler()
        return get_running_scheduler()

    def _run_on_scheduler(self, owning: BaseScheduler, callback: Callable[..., object], *args: object) -> None:
        if _current_scheduler() is owning:
            callback(*args)
            return
        owning.call_soon_threadsafe(callback, *args)

    def _wake_non_immediate(self, waiter: tealet.tealet | Event) -> None:
        if isinstance(waiter, Event):
            waiter.set()
            return
        owning = self._waiter_scheduler(waiter)
        self._run_on_scheduler(owning, owning._make_runnable, waiter)

    def _wake_sync(self, waiter: tealet.tealet | Event, prefer_immediate: bool) -> None:
        if isinstance(waiter, Event):
            waiter.set()
            return
        current = _current_scheduler()
        owning = self._waiter_scheduler(waiter)
        if prefer_immediate and current is owning:
            waiter.run()
            return
        self._run_on_scheduler(owning, owning._make_runnable, waiter)

    def _link_sender(self, waiter: tealet.tealet | Event, packet: tuple[bool, object]) -> None:
        self._packets[waiter] = packet
        self._waiters.append(waiter)
        self._balance += 1
        self._set_waiter_link(waiter)

    def _link_receiver(self, waiter: tealet.tealet | Event) -> None:
        self._waiters.append(waiter)
        self._balance -= 1
        self._set_waiter_link(waiter)

    def _unlink_waiter(self, waiter: tealet.tealet | Event) -> None:
        removed = False
        self._packets.pop(waiter, None)
        try:
            self._waiters.remove(waiter)
            removed = True
        except ValueError:
            pass
        if removed:
            if self._balance > 0:
                self._balance -= 1
            elif self._balance < 0:
                self._balance += 1
        if isinstance(waiter, tealet.tealet):
            self._clear_link(waiter)

    def _unlink(self, t: tealet.tealet) -> None:
        self._unlink_waiter(t)

    def _query_waiting(self, t: tealet.tealet) -> bool:
        return t in self._waiters

    def _query_runnable(self, t: tealet.tealet) -> bool:
        return False

    # -- Synchronous operations ---------------------------------------

    def _send_packet(self, packet: tuple[bool, object]) -> None:
        if self._balance < 0:
            receiver = self._waiters.popleft()
            self._balance += 1
            if isinstance(receiver, tealet.tealet):
                self._clear_link(receiver)
            self._packets[receiver] = packet
            self._wake_sync(receiver, prefer_immediate=self._preference < 0)
            return

        current = tealet.current()
        try:
            get_running_scheduler()._schedule(lambda: self._link_sender(current, packet))
        except BaseException as exc:
            missing = object()
            pending = self._packets.pop(current, missing)
            self._unlink_waiter(current)
            if pending is missing and isinstance(exc, RawTimeoutError):
                # Timeout-vs-delivery race: if receiver already consumed packet,
                # treat send as successful and suppress timeout.
                return
            raise

    def send(self, value: object) -> None:
        """Send one value using sync tealet rendezvous semantics.

        If a receiver is already waiting, the value is delivered immediately.
        For a waiting tealet receiver, immediate ``run()`` may occur depending
        on ``preference``. If no receiver is waiting, the current tealet blocks
        by entering the channel wait queue.
        """
        self._send_packet((False, value))

    def send_exception(self, exc: BaseException) -> None:
        """Send an exception instance to the next receiver.

        The receiver raises the delivered exception when it receives the packet.
        """
        if not isinstance(exc, BaseException):
            raise TypeError("exc must be a BaseException instance")
        self._send_packet((True, exc))

    def receive(self) -> object:
        """Receive one packet using sync tealet rendezvous semantics.

        If a sender is already waiting, the packet is consumed immediately.
        For a waiting tealet sender, immediate ``run()`` may occur depending on
        ``preference``. If no sender is waiting, the current tealet blocks by
        entering the channel wait queue.
        """
        if self._balance > 0:
            sender = self._waiters.popleft()
            self._balance -= 1
            packet = self._packets.pop(sender)
            if isinstance(sender, tealet.tealet):
                self._clear_link(sender)
            self._wake_sync(sender, prefer_immediate=self._preference > 0)
            return self._deliver(packet)

        current = tealet.current()
        try:
            get_running_scheduler()._schedule(lambda: self._link_receiver(current))
        except BaseException as exc:
            missing = object()
            packet = self._packets.pop(current, missing)
            self._unlink_waiter(current)
            if packet is not missing and isinstance(exc, RawTimeoutError):
                # Timeout-vs-delivery race: if a packet was already delivered,
                # consume the packet and suppress the timeout.
                return self._deliver(packet)
            raise

        return self._deliver(self._packets.pop(current))

    # -- Async operations ---------------------------------------------

    async def async_send(self, value: object) -> None:
        """Send one value from async code without immediate task transfer.

        Async operations always use non-immediate wake semantics. When matching
        a waiting tealet receiver, it is only made runnable. When matching an
        Event waiter, the Event is set.
        """
        packet = (False, value)
        if self._balance < 0:
            receiver = self._waiters.popleft()
            self._balance += 1
            if isinstance(receiver, tealet.tealet):
                self._clear_link(receiver)
            self._packets[receiver] = packet
            self._wake_non_immediate(receiver)
            return

        waiter = Event()
        self._link_sender(waiter, packet)
        try:
            await waiter.async_wait()
        except BaseException as exc:
            missing = object()
            pending = self._packets.pop(waiter, missing)
            self._unlink_waiter(waiter)
            if pending is missing and isinstance(exc, CancelledError):
                return
            raise

    async def async_receive(self) -> object:
        """Receive one packet from async code without immediate task transfer.

        Async operations always use non-immediate wake semantics. When matching
        a waiting tealet sender, it is only made runnable. When matching an
        Event waiter, the Event is set.
        """
        if self._balance > 0:
            sender = self._waiters.popleft()
            self._balance -= 1
            packet = self._packets.pop(sender)
            if isinstance(sender, tealet.tealet):
                self._clear_link(sender)
            self._wake_non_immediate(sender)
            return self._deliver(packet)

        waiter = Event()
        self._link_receiver(waiter)
        try:
            await waiter.async_wait()
        except BaseException as exc:
            # Async timeouts surface as cancellation at this await point.
            # We cannot reliably distinguish timeout cancellation from an
            # explicit task cancellation here. If a packet is already queued,
            # prefer delivering data over propagating cancellation.
            missing = object()
            packet = self._packets.pop(waiter, missing)
            self._unlink_waiter(waiter)
            if packet is not missing and isinstance(exc, CancelledError):
                return self._deliver(packet)
            raise

        return self._deliver(self._packets.pop(waiter))


class DeadlockError(RuntimeError):
    """Raised when the scheduler has no runnable tasks."""


class TimerHandle:
    """Cancellable callback scheduled to run in the future."""

    def __init__(
        self,
        when: float,
        callback: Callable[..., object],
        args: tuple[object, ...] = (),
        context: contextvars.Context | None = None,
    ) -> None:
        self._when = when
        self._callback = callback
        self._args = args
        self._context = context
        self._cancelled = False

    # -- Public state --------------------------------------------------

    @property
    def when(self) -> float:
        return self._when

    def cancel(self) -> None:
        self._cancelled = True

    def cancelled(self) -> bool:
        return self._cancelled

    # -- Execution -----------------------------------------------------

    def _run(self) -> None:
        if self._cancelled:
            return
        if self._context is None:
            self._callback(*self._args)
            return
        self._context.run(self._callback, *self._args)

    def __enter__(self) -> "TimerHandle":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.cancel()


class Future(Generic[T]):
    """Minimal Future for scheduler tasks."""

    def __init__(self) -> None:
        self._done = False
        self._result: T | None = None
        self._exception: BaseException | None = None
        self._event = Event()
        self._done_callbacks: list[
            tuple[Callable[[Future[T]], object], contextvars.Context | None]
        ] = []

    # -- State ---------------------------------------------------------

    def done(self) -> bool:
        return self._done

    def cancelled(self) -> bool:
        return isinstance(self._exception, CancelledError)

    def cancel(self) -> bool:
        if self._done:
            return False
        self.set_exception(CancelledError())
        return True

    # -- Completion ----------------------------------------------------

    def set_result(self, value: T) -> None:
        if self._done:
            raise InvalidStateError("Future already done")
        self._result = value
        self._done = True
        self._event.set()
        self._run_done_callbacks()

    def set_exception(self, exc: BaseException) -> None:
        if self._done:
            raise InvalidStateError("Future already done")
        if not isinstance(exc, BaseException):
            raise TypeError("exc must be a BaseException instance")
        self._exception = exc
        self._done = True
        self._event.set()
        self._run_done_callbacks()

    # -- Done callbacks -----------------------------------------------

    def add_done_callback(
        self,
        callback: Callable[[Future[T]], object],
        *,
        context: contextvars.Context | None = None,
    ) -> None:
        if self._done:
            loop = asyncio.get_running_loop()
            if context is None:
                loop.call_soon(callback, self)
            else:
                loop.call_soon(callback, self, context=context)
            return
        if context is None:
            context = contextvars.copy_context()
        self._done_callbacks.append((callback, context))

    def remove_done_callback(self, callback: Callable[[Future[T]], object]) -> int:
        removed = 0
        kept: list[tuple[Callable[[Future[T]], object], contextvars.Context | None]] = []
        for stored_callback, context in self._done_callbacks:
            if stored_callback is callback:
                removed += 1
            else:
                kept.append((stored_callback, context))
        self._done_callbacks = kept
        return removed

    def _run_done_callbacks(self) -> None:
        callbacks = self._done_callbacks[:]
        self._done_callbacks.clear()
        for callback, context in callbacks:
            if context is None:
                callback(self)
            else:
                context.run(callback, self)

    # -- Waiting and results ------------------------------------------

    def _wait(self) -> bool:
        if self._done:
            return True

        return self._event.wait()

    def wait(self) -> T:
        try:
            self._wait()
        except CancelledError:
            get_running_scheduler().call_soon(self.cancel)
            raise
        return self.result()

    async def async_wait(self) -> T:
        if not self._done:
            try:
                await self._event.async_wait()
            except CancelledError:
                get_running_scheduler().call_soon(self.cancel)
                raise
        return self.result()

    def __await__(self):
        return self.async_wait().__await__()

    def result(self) -> T:
        if not self._done:
            raise InvalidStateError("Result is not ready.")
        if self.cancelled():
            assert self._exception is not None
            raise self._exception
        if self._exception is not None:
            raise self._exception
        return self._result

    def exception(self) -> BaseException | None:
        if not self._done:
            raise InvalidStateError("Exception is not set.")
        if self.cancelled():
            assert self._exception is not None
            raise self._exception
        return self._exception


class Shield(Generic[T]):
    """Wait wrapper that avoids cancelling the wrapped future."""

    def __init__(self, future: Future[T]) -> None:
        self._future = future

    def wait(self) -> T:
        self._future._wait()
        return self._future.result()


def shield(future: Future[T]) -> Shield[T]:
    return Shield(future)


class TealetTask(tealet.tealet, Future[object]):
    """Tealet task that is also a Future for its completion result."""

    def __init__(self, owning_scheduler: BaseScheduler):
        tealet.tealet.__init__(self)
        Future.__init__(self)
        self.link: Linkable | None = None
        self._scheduler: BaseScheduler = owning_scheduler

    # -- Runtime state -------------------------------------------------

    def is_waiting(self):
        if self.link is None:
            return False
        return self.link._query_waiting(self)

    def is_runnable(self):
        if self.link is None:
            return False
        return self.link._query_runnable(self)

    def is_blocked(self):
        return self._scheduler._is_blocked(self)

    def is_running(self):
        return tealet.current() is self

    def get_scheduler(self) -> BaseScheduler:
        return self._scheduler

    # -- Scheduler transfer -------------------------------------------

    def _unlink(self):
        if self.link is not None:
            self.link._unlink(self)
        self._scheduler._unlink_pending_async_wait(self)

    def run(self):
        self._scheduler._target_run(self)

    def throw(self, exc: BaseException):
        self._scheduler._target_throw(self, exc)

    def _throw_from_scheduler(self, exc: BaseException):
        super().throw(exc)

    def cancel(self) -> bool:
        if self.done():
            return False
        self.throw(CancelledError())
        return True

    # -- Target completion --------------------------------------------

    def resolve_target(self, result, exc, exc_target):
        suppress = False
        if exc is None:
            self.set_result(result)
        elif isinstance(exc, (SystemExit, KeyboardInterrupt)):
            self.set_exception(exc)
            return super().resolve_target(result, exc, exc_target)
        else:
            self.set_exception(exc)
            suppress = True
            if exc_target is not None:
                try:
                    exc_target._unlink()
                except AttributeError:
                    pass
                return exc_target, None, suppress

        # Scheduler-owned tasks always route via scheduler target selection,
        # even if task startup immediately raises before user code returns.
        return self._scheduler._find_target(task_exit=True), None, suppress


class Timeout:
    """Timer-backed synchronous timeout context manager."""

    def __init__(self, when: float):
        self._when = when
        self._handle: TimerHandle | None = None
        self._exc = RawTimeoutError()
        self._expired = False

    # -- Public state --------------------------------------------------

    def reschedule(self, when: float):
        if not self._expired and self._handle is not None:
            self._handle.cancel()
            self._when = when
            self._handle = get_running_scheduler().call_at(self._when, self._timeout, tealet.current())

    def expired(self) -> bool:
        return self._expired

    # -- Context manager ----------------------------------------------

    def __enter__(self) -> "Timeout":
        self._handle = get_running_scheduler().call_at(self._when, self._timeout, tealet.current())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._handle is not None:
            self._handle.cancel()
        if exc_val is self._exc:
            assert self._expired is True
            raise asyncio.TimeoutError("Operation timed out") from exc_val

    def _timeout(self, target) -> None:
        self._expired = True
        target.throw(self._exc)


class BaseScheduler(Linkable, CoreSchedulerDrivingAPI):
    """Shared cooperative scheduler mechanics for concrete drivers."""

    def __init__(self) -> None:
        self._tasks: deque[tealet.tealet] = deque()
        self._task_set: set[tealet.tealet] = set()
        self._runner = None
        self._running = False
        self._debug = False
        self._stopping = False
        self._threadsafe_callbacks: deque[
            tuple[Callable[..., object], tuple[object, ...], contextvars.Context | None]
        ] = deque()
        self._threadsafe_lock = threading.Lock()
        self._pending_async_waits: set[tealet.tealet] = set()
        self._timers: list[tuple[float, int, TimerHandle]] = []
        self._timer_sequence = itertools.count()
        self._n_scheduled = 0
        self._target_count = None
        self._default_executor: concurrent.futures.ThreadPoolExecutor | None = None

    # -- Basic state ---------------------------------------------------

    def time(self) -> float:
        return time.monotonic()

    def is_running(self) -> bool:
        return self._running

    def set_debug(self, enabled: bool) -> None:
        self._debug = bool(enabled)

    def get_debug(self) -> bool:
        return self._debug

    # -- External integration APIs ------------------------------------

    def run_in_executor(
        self,
        executor: concurrent.futures.Executor | None,
        func: Callable[..., T],
        *args: object,
    ) -> Future[T]:
        if executor is None:
            if self._default_executor is None:
                self._default_executor = concurrent.futures.ThreadPoolExecutor(thread_name_prefix="tealet")
            executor = self._default_executor

        future: Future[T] = Future()

        def complete_result(value: T) -> None:
            if not future.done():
                future.set_result(value)

        def complete_exception(exc: BaseException) -> None:
            if not future.done():
                future.set_exception(exc)

        def worker() -> None:
            try:
                result = func(*args)
            except BaseException as exc:
                self.call_soon_threadsafe(complete_exception, exc)
            else:
                self.call_soon_threadsafe(complete_result, result)

        executor.submit(worker)
        return future

    def add_reader(self, fd: int, callback: Callable[..., object], *args: object) -> None:
        raise NotImplementedError("reader callbacks require an IO-capable scheduler")

    def remove_reader(self, fd: int) -> bool:
        raise NotImplementedError("reader callbacks require an IO-capable scheduler")

    def add_writer(self, fd: int, callback: Callable[..., object], *args: object) -> None:
        raise NotImplementedError("writer callbacks require an IO-capable scheduler")

    def remove_writer(self, fd: int) -> bool:
        raise NotImplementedError("writer callbacks require an IO-capable scheduler")

    def sock_recv(self, sock: socket.socket, n: int) -> bytes:
        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_recv_into(self, sock: socket.socket, buf: Any) -> int:
        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> tuple[bytes, Any]:
        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> tuple[int, Any]:
        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_sendall(self, sock: socket.socket, data: Any) -> None:
        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> int:
        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_accept(self, sock: socket.socket) -> tuple[socket.socket, Any]:
        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    def sock_connect(self, sock: socket.socket, address: Any) -> None:
        raise NotImplementedError("socket helpers require an IO-capable scheduler")

    # -- Driver state --------------------------------------------------

    def _verify_current_scheduler(self) -> None:
        if _current_scheduler() is not self:
            raise RuntimeError("operation requires this scheduler to be the current scheduler")
        if self._running:
            raise RuntimeError("Scheduler already running")

    def stop(self) -> None:
        self._stopping = True
        self._break_wait()

    # -- Callback and timer scheduling --------------------------------

    def call_soon(
        self,
        callback: Callable[..., object],
        *args: object,
        context: contextvars.Context | None = None,
    ) -> TimerHandle:
        return self.call_at(self.time(), callback, *args, context=context)

    def call_soon_threadsafe(
        self,
        callback: Callable[..., object],
        *args: object,
        context: contextvars.Context | None = None,
    ) -> None:
        if context is None:
            context = contextvars.copy_context()
        with self._threadsafe_lock:
            self._threadsafe_callbacks.append((callback, args, context))
        self._break_wait_threadsafe()

    def call_later(
        self,
        delay: float,
        callback: Callable[..., object],
        *args: object,
        context: contextvars.Context | None = None,
    ) -> TimerHandle:
        if delay < 0:
            delay = 0
        return self.call_at(self.time() + delay, callback, *args, context=context)

    def call_at(
        self,
        when: float,
        callback: Callable[..., object],
        *args: object,
        context: contextvars.Context | None = None,
    ) -> TimerHandle:
        if context is None:
            context = contextvars.copy_context()
        handle = TimerHandle(when, callback, args, context=context)
        self._enqueue_timer(when, handle)
        return handle

    def _enqueue_timer(self, when: float, handle: TimerHandle) -> None:
        heapq.heappush(self._timers, (when, next(self._timer_sequence), handle))
        self._break_wait()

    def _drain_threadsafe_callbacks(self) -> None:
        while True:
            with self._threadsafe_lock:
                if not self._threadsafe_callbacks:
                    return
                callback, args, context = self._threadsafe_callbacks.popleft()
            if context is None:
                callback(*args)
            else:
                context.run(callback, *args)

    def _run_ready_timers(self) -> None:
        self._drain_threadsafe_callbacks()
        now = self.time()
        while self._timers and self._timers[0][0] <= now:
            _, _, handle = heapq.heappop(self._timers)
            handle._run()

    def _time_to_next_timer(self) -> float | None:
        while self._timers and self._timers[0][2].cancelled():
            heapq.heappop(self._timers)
        if not self._timers:
            return None
        return max(0.0, self._timers[0][0] - self.time())

    def _has_pending_driver_work(self) -> bool:
        return False

    # -- Link and runnable state --------------------------------------

    def _is_runnable(self, t: tealet.tealet) -> bool:
        return t in self._task_set

    def _query_runnable(self, t: tealet.tealet) -> bool:
        return self._is_runnable(t)

    def _query_waiting(self, t: tealet.tealet) -> bool:
        return False

    def _is_blocked(self, t: tealet.tealet) -> bool:
        return t in self._pending_async_waits

    def _unlink_pending_async_wait(self, t: tealet.tealet) -> None:
        self._pending_async_waits.discard(t)

    def _unlink(self, t: tealet.tealet) -> None:
        removed = False
        if t in self._task_set:
            self._task_set.remove(t)
            try:
                self._tasks.remove(t)
            except ValueError:
                pass
            removed = True
        if t in self._pending_async_waits:
            self._pending_async_waits.remove(t)
            removed = True
        if removed:
            try:
                t.link = None
            except AttributeError:
                pass

    # -- Task creation and cooperative operations ---------------------

    def spawn(
        self,
        func: Callable[[], T],
        kwargs: dict[str, object] | None = None,
        context: contextvars.Context | None = None,
    ) -> TealetTask:
        if context is None:
            context = contextvars.copy_context()
        if kwargs is not None:
            raise TypeError("spawn() does not accept callable kwargs")

        def task_main(current: tealet.tealet, _arg: object):
            return context.run(func)

        t = TealetTask(self)
        t.prepare(task_main)
        self._make_runnable(t)
        return t

    def _schedule(self, enqueue=None) -> None:
        if enqueue is not None:
            enqueue()
        self._run_ready_timers()
        target = self._find_target()
        self._n_scheduled += 1
        target.switch()

    def yield_(self) -> None:
        self._schedule(lambda: self._make_runnable(tealet.current()))

    def sleep(self, delay: float) -> None:
        current = tealet.current()
        awakened = False

        def wake() -> None:
            nonlocal awakened
            awakened = True
            self._make_runnable(current)

        with self.call_later(delay, wake):
            if not awakened:
                self._schedule()

    def wait_async(self, awaitable):
        """Wait for an asyncio awaitable from a tealet task and return its result."""
        current = tealet.current()
        loop = asyncio.get_running_loop()

        if asyncio.isfuture(awaitable):
            fut = awaitable
            if fut.get_loop() is not loop:
                raise RuntimeError("wait_async future is bound to a different event loop")
        elif inspect.isawaitable(awaitable):
            fut = loop.create_task(awaitable)
        else:
            raise TypeError("awaitable must be an awaitable, Future, or Task")

        if fut.done():
            return fut.result()

        done_evt = Event()
        self._pending_async_waits.add(current)
        state = {"active": True}

        def _resume_waiter(_fut) -> None:
            if not state["active"]:
                return
            state["active"] = False
            self._pending_async_waits.discard(current)
            self._make_runnable(current)
            done_evt.set()

        fut.add_done_callback(_resume_waiter)
        try:
            try:
                done_evt.wait()
            except CancelledError:
                loop.call_soon(fut.cancel)
                raise
        finally:
            if state["active"]:
                state["active"] = False
                self._pending_async_waits.discard(current)
            fut.remove_done_callback(_resume_waiter)

        return fut.result()

    # -- Scheduler-owned transfer -------------------------------------

    def _make_runnable(self, t: tealet.tealet) -> None:
        if t in self._task_set:
            return
        try:
            t.link = self
        except AttributeError:
            pass
        if isinstance(t, TealetTask):
            t._scheduler = self
        self._tasks.append(t)
        self._task_set.add(t)
        self._break_wait()

    def _target_run(self, target: tealet.tealet) -> None:
        if target is tealet.current():
            return
        try:
            target._unlink()
        except AttributeError:
            raise RuntimeError(f"Cannot throw to this target: {target}") from None
        self._make_runnable(tealet.current())
        target.switch()

    def _target_throw(self, target: tealet.tealet, exc: BaseException) -> None:
        if target is tealet.current():
            raise exc
        try:
            target._unlink()
        except AttributeError:
            raise RuntimeError(f"Cannot throw to this target: {target}") from None
        self._make_runnable(tealet.current())
        if isinstance(target, TealetTask):
            target._throw_from_scheduler(exc)
        else:
            target.throw(exc)

    def _find_target(self, task_exit=False) -> tealet.tealet:
        if self._runner is not None and self._target_count is not None and self._n_scheduled >= self._target_count:
            result = self._runner
            try:
                result._unlink()
            except AttributeError:
                self._unlink(result)
            self._unlink(result)
        elif self._tasks:
            result = self._tasks.popleft()
            self._task_set.discard(result)
        elif not task_exit:
            raise DeadlockError("No tasks to switch to")
        else:
            result = tealet.main()
        try:
            result.link = None
        except AttributeError:
            pass
        return result

    def _pump(self, n=0) -> None:
        if self._runner is not None:
            raise RuntimeError("Scheduler already running")
        start_count = self._n_scheduled
        if n > 0:
            self._target_count = start_count + n
        self._runner = tealet.current()
        try:
            self.yield_()
            return self._n_scheduled - start_count - 1
        finally:
            self._runner = None
            self._target_count = None

    def pump(self, n=0) -> None:
        self._verify_current_scheduler()
        self._running = True
        try:
            return self._pump(n)
        finally:
            self._running = False

    # -- Concrete driver hooks ----------------------------------------

    @abstractmethod
    def _break_wait_threadsafe(self) -> None:
        """Wake a concrete driver from another thread or scheduler context."""

    @abstractmethod
    def _break_wait(self) -> None:
        """Wake a concrete driver from its owning context."""


class SelectorMixin:
    """Selector-backed readiness waits for synchronous schedulers."""

    def __init__(self) -> None:
        super().__init__()
        self._selector = selectors.DefaultSelector()
        self._fd_callbacks: defaultdict[int, _FdCallbacks] = defaultdict(_FdCallbacks)
        self._selector_wakeup_reader, self._selector_wakeup_writer = socket.socketpair()
        self._selector_wakeup_reader.setblocking(False)
        self._selector_wakeup_writer.setblocking(False)
        self._selector.register(
            self._selector_wakeup_reader.fileno(),
            selectors.EVENT_READ,
            self._selector_wakeup_reader.fileno(),
        )

    # -- Lifecycle -----------------------------------------------------

    def close(self) -> None:
        self._selector.close()
        self._selector_wakeup_reader.close()
        self._selector_wakeup_writer.close()

    # -- Readiness waits -----------------------------------------------

    def wait_readable(self, fileobj: object) -> None:
        """Block the current tealet until a file descriptor is readable."""

        fd = self._fileobj_to_fd(fileobj)
        ready = Event()
        active = True

        def wake() -> None:
            nonlocal active
            if not active:
                return
            active = False
            self.remove_reader(fd)
            ready.set()

        self.add_reader(fd, wake)

        try:
            ready.wait()
        finally:
            if active:
                active = False
                self.remove_reader(fd)

    def wait_writable(self, fileobj: object) -> None:
        """Block the current tealet until a file descriptor is writable."""

        fd = self._fileobj_to_fd(fileobj)
        ready = Event()
        active = True

        def wake() -> None:
            nonlocal active
            if not active:
                return
            active = False
            self.remove_writer(fd)
            ready.set()

        self.add_writer(fd, wake)

        try:
            ready.wait()
        finally:
            if active:
                active = False
                self.remove_writer(fd)

    # -- File descriptor callbacks -----------------------------------

    def add_reader(self, fd: int, callback: Callable[..., object], *args: object) -> None:
        fd = self._fileobj_to_fd(fd)
        entry = self._fd_callbacks[fd]
        previous = entry.reader
        entry.reader = (callback, args, contextvars.copy_context())
        try:
            self._update_selector_registration(fd)
        except Exception:
            entry.reader = previous
            if entry.empty():
                del self._fd_callbacks[fd]
            self._update_selector_registration(fd)
            raise
        self._wake_selector()

    def remove_reader(self, fd: int) -> bool:
        fd = self._fileobj_to_fd(fd)
        entry = self._fd_callbacks.get(fd)
        if entry is None or entry.reader is None:
            return False
        entry.reader = None
        if entry.empty():
            del self._fd_callbacks[fd]
        self._update_selector_registration(fd)
        return True

    def add_writer(self, fd: int, callback: Callable[..., object], *args: object) -> None:
        fd = self._fileobj_to_fd(fd)
        entry = self._fd_callbacks[fd]
        previous = entry.writer
        entry.writer = (callback, args, contextvars.copy_context())
        try:
            self._update_selector_registration(fd)
        except Exception:
            entry.writer = previous
            if entry.empty():
                del self._fd_callbacks[fd]
            self._update_selector_registration(fd)
            raise
        self._wake_selector()

    def remove_writer(self, fd: int) -> bool:
        fd = self._fileobj_to_fd(fd)
        entry = self._fd_callbacks.get(fd)
        if entry is None or entry.writer is None:
            return False
        entry.writer = None
        if entry.empty():
            del self._fd_callbacks[fd]
        self._update_selector_registration(fd)
        return True

    # -- Asyncio-style socket helpers ---------------------------------

    def sock_recv(self, sock: socket.socket, n: int) -> bytes:
        self._check_socket(sock)
        while True:
            try:
                return sock.recv(n)
            except (BlockingIOError, InterruptedError):
                self.wait_readable(sock)

    def sock_recv_into(self, sock: socket.socket, buf: Any) -> int:
        self._check_socket(sock)
        while True:
            try:
                return sock.recv_into(buf)
            except (BlockingIOError, InterruptedError):
                self.wait_readable(sock)

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> tuple[bytes, Any]:
        self._check_socket(sock)
        while True:
            try:
                return sock.recvfrom(bufsize)
            except (BlockingIOError, InterruptedError):
                self.wait_readable(sock)

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> tuple[int, Any]:
        self._check_socket(sock)
        while True:
            try:
                if nbytes:
                    return sock.recvfrom_into(buf, nbytes)
                return sock.recvfrom_into(buf)
            except (BlockingIOError, InterruptedError):
                self.wait_readable(sock)

    def sock_sendall(self, sock: socket.socket, data: Any) -> None:
        self._check_socket(sock)
        view = memoryview(data)
        total = 0
        while total < len(view):
            try:
                sent = sock.send(view[total:])
                if sent == 0:
                    self.wait_writable(sock)
                    continue
                total += sent
            except (BlockingIOError, InterruptedError):
                self.wait_writable(sock)

    def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> int:
        self._check_socket(sock)
        while True:
            try:
                return sock.sendto(data, address)
            except (BlockingIOError, InterruptedError):
                self.wait_writable(sock)

    def sock_accept(self, sock: socket.socket) -> tuple[socket.socket, Any]:
        self._check_socket(sock)
        while True:
            try:
                conn, address = sock.accept()
                conn.setblocking(False)
                return conn, address
            except (BlockingIOError, InterruptedError):
                self.wait_readable(sock)

    def sock_connect(self, sock: socket.socket, address: Any) -> None:
        self._check_socket(sock)
        try:
            sock.connect(address)
            return
        except (BlockingIOError, InterruptedError):
            pass
        except OSError as exc:
            if exc.errno not in (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
                raise

        while True:
            self.wait_writable(sock)
            err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err == 0:
                return
            if err in (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
                continue
            raise OSError(err, errno.errorcode.get(err, "socket connect failed"))

    def _check_socket(self, sock: socket.socket) -> None:
        if sock.getblocking():
            raise ValueError("socket must be non-blocking")

    # -- File descriptor registration ---------------------------------

    def _fileobj_to_fd(self, fileobj: object) -> int:
        if isinstance(fileobj, int):
            fd = fileobj
        else:
            fileno = getattr(fileobj, "fileno", None)
            if fileno is None:
                raise ValueError("file object must be an fd or have fileno()")
            fd = fileno()
        if not isinstance(fd, int) or fd < 0:
            raise ValueError("invalid file descriptor")
        return fd

    def _selector_mask_for_fd(self, fd: int) -> int:
        mask = 0
        entry = self._fd_callbacks.get(fd)
        if entry is None:
            return mask
        if entry.reader is not None:
            mask |= selectors.EVENT_READ
        if entry.writer is not None:
            mask |= selectors.EVENT_WRITE
        return mask

    def _update_selector_registration(self, fd: int) -> None:
        mask = self._selector_mask_for_fd(fd)
        try:
            self._selector.get_key(fd)
        except KeyError:
            if mask:
                self._selector.register(fd, mask, fd)
            return

        if mask:
            self._selector.modify(fd, mask, fd)
            return
        try:
            self._selector.unregister(fd)
        except (KeyError, ValueError, OSError):
            pass

    def _schedule_fd_callback(self, fd: int, event: int) -> None:
        callbacks = self._fd_callbacks.get(fd)
        if callbacks is None:
            return
        assert event in (selectors.EVENT_READ, selectors.EVENT_WRITE)
        entry = callbacks.reader if event == selectors.EVENT_READ else callbacks.writer
        if entry is None:
            return
        callback, args, context = entry
        cast(BaseScheduler, self).call_soon(callback, *args, context=context)

    # -- Driver wakeup and polling ------------------------------------

    def _drain_selector_wakeup(self) -> None:
        while True:
            try:
                if not self._selector_wakeup_reader.recv(4096):
                    return
            except BlockingIOError:
                return
            except OSError:
                return

    def _wake_selector(self) -> None:
        try:
            self._selector_wakeup_writer.send(b"\0")
        except BlockingIOError:
            pass
        except OSError:
            pass

    def _break_wait_threadsafe(self) -> None:
        self._wake_selector()

    def _break_wait(self) -> None:
        self._wake_selector()

    def _wait_thread(self) -> None:
        scheduler = cast(BaseScheduler, self)
        events = self._selector.select(timeout=scheduler._time_to_next_timer())
        wakeup_fd = self._selector_wakeup_reader.fileno()
        for key, mask in events:
            fd = key.fd
            if fd == wakeup_fd:
                self._drain_selector_wakeup()
                continue
            if mask & selectors.EVENT_READ:
                self._schedule_fd_callback(fd, selectors.EVENT_READ)
            if mask & selectors.EVENT_WRITE:
                self._schedule_fd_callback(fd, selectors.EVENT_WRITE)
            self._update_selector_registration(fd)

    def _has_pending_driver_work(self) -> bool:
        return bool(self._fd_callbacks)


class Scheduler(BaseScheduler, SyncSchedulerDrivingAPI):
    """Cooperative scheduler for synchronous driving."""

    def __init__(self) -> None:
        super().__init__()
        self._wakeup = threading.Event()

    # -- Driver wakeup -------------------------------------------------

    def _break_wait_threadsafe(self) -> None:
        self._wakeup.set()

    def _break_wait(self) -> None:
        self._wakeup.set()

    def _wait_thread(self) -> None:
        self._wakeup.wait(timeout=self._time_to_next_timer())
        self._wakeup.clear()

    # -- Sync run entry points ----------------------------------------

    def run(self) -> None:
        """Run scheduler synchronously until no runnable tasks or timers remain.
        This method is intended for single threaded context with no
        asyncio loop interaction.

        This sync runner only considers local runnable state (`_tasks`) and
        scheduled timer callbacks (`_timers`). Tealets blocked in
        `wait_async()` are not progressed here; use `arun()` for that mode.
        """
        self._verify_current_scheduler()
        self._running = True
        try:
            while self._tasks or self._timers or self._has_pending_driver_work():
                self._run_ready_timers()
                if self._tasks:
                    self._pump()
                if self._tasks or self._timers or self._has_pending_driver_work():
                    self._wait_thread()
        finally:
            self._running = False

    def run_forever(self) -> None:
        self._verify_current_scheduler()
        self._stopping = False
        self._running = True
        try:
            while not self._stopping:
                self._run_ready_timers()
                if self._tasks:
                    self._pump()
                    continue
                self._wait_thread()
        finally:
            self._running = False
            self._stopping = False

    def run_until_complete(
        self,
        future: Future[T] | Callable[[], T],
    ) -> T:
        self._verify_current_scheduler()
        if isinstance(future, Future):
            target: Future[T] = future
            if isinstance(target, TealetTask) and target.get_scheduler() is not self:
                raise RuntimeError("Future is bound to a different scheduler")
        elif callable(future):
            target = self.spawn(future)
        else:
            raise TypeError("future must be a Future or callable")

        self._stopping = False
        self._running = True
        try:
            while not target.done() and not self._stopping:
                self._run_ready_timers()
                if self._tasks:
                    self._pump()
                if not target.done() and not self._stopping:
                    self._wait_thread()
        finally:
            self._running = False
            self._stopping = False

        if not target.done():
            raise RuntimeError("Scheduler stopped before Future completed.")
        return target.result()


class SelectorScheduler(SelectorMixin, Scheduler):
    """Synchronous scheduler with selector-backed fd readiness waits."""


class AsyncScheduler(BaseScheduler, AsyncSchedulerDrivingAPI):
    """Cooperative scheduler for asyncio-hosted driving."""

    def __init__(self) -> None:
        super().__init__()
        self._wakeup = asyncio.Event()
        self._wakeup_loop: asyncio.AbstractEventLoop | None = None

    # -- Driver wakeup -------------------------------------------------

    def _break_wait_threadsafe(self) -> None:
        loop = self._wakeup_loop
        if loop is None:
            return
        try:
            if asyncio.get_running_loop() is loop:
                self._wakeup.set()
                return
        except RuntimeError:
            pass
        loop.call_soon_threadsafe(self._wakeup.set)

    def _break_wait(self) -> None:
        """Primarily used from async code.  if the Scheduler is Sleeping, then no tealet code is running, but
        Asyncio code can still wake up the Scheduler.
        """
        self._wakeup.set()

    # -- Asyncio fd callbacks ----------------------------------------

    def add_reader(self, fd: int, callback: Callable[..., object], *args: object) -> None:
        loop = asyncio.get_running_loop()
        loop.add_reader(fd, callback, *args)

    def remove_reader(self, fd: int) -> bool:
        loop = asyncio.get_running_loop()
        return loop.remove_reader(fd)

    def add_writer(self, fd: int, callback: Callable[..., object], *args: object) -> None:
        loop = asyncio.get_running_loop()
        loop.add_writer(fd, callback, *args)

    def remove_writer(self, fd: int) -> bool:
        loop = asyncio.get_running_loop()
        return loop.remove_writer(fd)

    # -- Asyncio socket helpers --------------------------------------

    def sock_recv(self, sock: socket.socket, n: int) -> bytes:
        loop = asyncio.get_running_loop()
        return self.wait_async(loop.sock_recv(sock, n))

    def sock_recv_into(self, sock: socket.socket, buf: Any) -> int:
        loop = asyncio.get_running_loop()
        return self.wait_async(loop.sock_recv_into(sock, buf))

    def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> tuple[bytes, Any]:
        loop = asyncio.get_running_loop()
        return self.wait_async(loop.sock_recvfrom(sock, bufsize))

    def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> tuple[int, Any]:
        loop = asyncio.get_running_loop()
        if nbytes:
            return self.wait_async(loop.sock_recvfrom_into(sock, buf, nbytes))
        return self.wait_async(loop.sock_recvfrom_into(sock, buf))

    def sock_sendall(self, sock: socket.socket, data: Any) -> None:
        loop = asyncio.get_running_loop()
        return self.wait_async(loop.sock_sendall(sock, data))

    def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> int:
        loop = asyncio.get_running_loop()
        return self.wait_async(loop.sock_sendto(sock, data, address))

    def sock_accept(self, sock: socket.socket) -> tuple[socket.socket, Any]:
        loop = asyncio.get_running_loop()
        return self.wait_async(loop.sock_accept(sock))

    def sock_connect(self, sock: socket.socket, address: Any) -> None:
        loop = asyncio.get_running_loop()
        return self.wait_async(loop.sock_connect(sock, address))

    # -- Async waiting -------------------------------------------------

    async def _wait_async(self) -> None:
        wakeup = self._wakeup
        if wakeup.is_set():
            wakeup.clear()
            return
        timeout = self._time_to_next_timer()
        if timeout is None:
            # No scheduler timer is pending. We may still be alive because one or
            # more tealets are blocked in wait_async() on external asyncio
            # awaitables, so block until an explicit wakeup arrives.
            await wakeup.wait()
            wakeup.clear()
            return
        try:
            async with asyncio.timeout(timeout):
                await wakeup.wait()
        except TimeoutError:
            pass
        finally:
            wakeup.clear()

    # -- Async run entry points ---------------------------------------

    async def arun(self) -> None:
        self._verify_current_scheduler()
        self._wakeup_loop = asyncio.get_running_loop()
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
        self._wakeup_loop = asyncio.get_running_loop()
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
            target = self.spawn(future)
        else:
            raise TypeError("future must be a Future or callable")

        self._wakeup_loop = asyncio.get_running_loop()
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
        return target.result()
