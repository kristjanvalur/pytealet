from __future__ import annotations

import asyncio as _asyncio
import errno
import os
import selectors
import socket
import struct
import sys
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, NoReturn, Protocol, TypeAlias, TypeGuard, TypeVar, overload

import uring_api

from . import compat
from .delivery import (
    ContinuousStepResult,
    MultishotDelivery,
    OpHandle,
    SelectorCancelHandle,
    _DeliveryHandle,
    io_cancellation_error,
)
from .io_manager import ProactorIOManager
from .poll_helpers import poll_mask_to_selector_events as _poll_mask_to_selector_events
from .poll_helpers import probe_poll_fd_now as _probe_poll_fd_now
from .scheduler import (
    AsyncDrivingMixin,
    AsyncSchedulerDrivingAPI,
    BaseScheduler,
    RunnableQueueFactory,
    SyncDrivingMixin,
    SyncSchedulerDrivingAPI,
)
from .socket_helpers import (
    configure_scheduler_socket,
    socket_from_uring_fd,
)
from .stream_diag import worker_completion_mark_emit_end, worker_completion_mark_emit_start
from .types import IoExpect, IoMore, RecvResult

T = TypeVar("T")

__all__ = [
    "DEFAULT_URING_CQ_ENTRIES",
    "DEFAULT_URING_SQ_ENTRIES",
    "AsyncProactorScheduler",
    "Proactor",
    "ProactorBase",
    "ProactorFactory",
    "ProactorScheduler",
    "RecvBufferPool",
    "SelectorProactor",
    "SyncProactorScheduler",
    "SyncUringProactor",
    "SyntheticRecvBufferPool",
    "ThreadedSelectorProactor",
    "UringProactor",
]


_ProgressCallback = Callable[[int], object]
_Clock = Callable[[], float]
# SQ 256 covers a wrk-style 256-conn burst of send / recv re-arm.
# CQ 1024 is 4× SQ so recv-multishot + send CQEs do not fill the ring
# (liburing default CQ is only 2× SQ; that is tight at 256+256).
DEFAULT_URING_SQ_ENTRIES = 256
DEFAULT_URING_CQ_ENTRIES = 1024
_DEFAULT_URING_COMPLETION_THREADS = 2
_DEFAULT_URING_COMPLETION_THREAD_NICE = -5
_DEFAULT_URING_RECV_MANY_BUFFER_SIZE = 16 * 1024
_DEFAULT_URING_RECV_MANY_BUFFER_COUNT = 256
_DEFAULT_RECVITER_BUFFER_SIZE = 16 * 1024
_DEFAULT_RECVITER_BUFFER_COUNT = 8
_DEFAULT_SELECTOR_RECV_MANY_CHUNK_SIZE = 8192
_RecvManyValue = memoryview
_RecvManyCallback = Callable[[MultishotDelivery], object]
_OneshotRecvCallback = Callable[[RecvResult | None, BaseException | None], object]
_OneshotCallback = Callable[[Any, BaseException | None], object]
_AcceptManyCallback = Callable[[MultishotDelivery], object]
_PollManyCallback = Callable[[MultishotDelivery], object]


class WakeupManager(Protocol):
    """Cross-thread wakeup primitive for proactor ``wait`` / ``wait_async``."""

    def wakeup(self) -> None:
        """Wake sync and async waiters, or latch until ``wait()`` / ``poll()``."""

    def wait(self, timeout: float | None = None) -> bool:
        """Block until ``wakeup()`` or ``timeout`` elapses."""

    def poll(self) -> bool:
        """Return whether a wakeup is pending, consuming it when true."""

    async def wait_async(self, timeout: float | None = None) -> None:
        """Await ``wakeup()`` or ``timeout`` on the running event loop."""


class EventWakeupManager:
    """Threading and asyncio event pair for proactor wait hosts."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._async_loop: _asyncio.AbstractEventLoop | None = None
        self._async_waiter: _asyncio.Event | None = None

    def wait(self, timeout: float | None = None) -> bool:
        woke = self._event.wait(timeout=timeout)
        if woke:
            self._event.clear()
        return woke

    def wakeup(self) -> None:
        self._event.set()
        waiter = self._async_waiter
        if waiter is not None:
            loop = self._async_loop
            assert loop is not None
            try:
                loop.call_soon_threadsafe(waiter.set)
            except RuntimeError:
                return

    def poll(self) -> bool:
        result = self._event.is_set()
        if result:
            self._event.clear()
        return result

    def bind_loop(self, loop: _asyncio.AbstractEventLoop) -> None:
        """Wire the asyncio loop and waiter used by ``wakeup()`` / ``wait_async()``."""

        if self._async_loop is None:
            self._async_loop = loop
        elif self._async_loop is not loop:
            raise RuntimeError("EventWakeupManager is already bound to a different event loop")
        if self._async_waiter is None:
            self._async_waiter = _asyncio.Event()

    async def wait_async(self, timeout: float | None = None) -> None:
        if self.poll():
            return

        waiter = self._async_waiter
        assert waiter is not None

        waiter.clear()
        try:
            if timeout is None:
                await waiter.wait()
            else:
                try:
                    await _asyncio.wait_for(waiter.wait(), timeout=timeout)
                except _asyncio.TimeoutError:
                    pass
        finally:
            waiter.clear()


def _sync_create_scheduler_socket(family: int, type: int, proto: int = 0) -> socket.socket:
    return configure_scheduler_socket(socket.socket(family, type, proto))


def _uring_cqe_oserror(res: int) -> OSError:
    return OSError(-res, errno.errorcode.get(-res, "io_uring operation failed"))


def _cqe_io_more(completion: Any) -> IoMore:
    flags = getattr(completion, "flags", 0)
    if flags & uring_api.IORING_CQE_F_SOCK_NONEMPTY:
        return IoMore.MORE
    return IoMore.EMPTY


def _recv_many_error_delivery(*, index: int, res: int) -> MultishotDelivery:
    return MultishotDelivery(
        index=index,
        value=memoryview(b""),
        exception=_uring_cqe_oserror(res),
        more=False,
    )


def _recv_many_enobufs_delivery(*, index: int) -> MultishotDelivery:
    return MultishotDelivery(
        index=index,
        value=memoryview(b""),
        exception=_enobufs_error(),
        more=False,
    )


def _emit_many(user_cb, delivery) -> None:
    worker_completion_mark_emit_start()
    try:
        user_cb(delivery)
    finally:
        worker_completion_mark_emit_end()


def _emit_recv_many(user_cb, delivery) -> None:
    _emit_many(user_cb, delivery)


def _cancel_or_remove_cqe(completion) -> bool:
    return completion.kind in (
        uring_api.COMPLETION_KIND_POLL_REMOVE,
        uring_api.COMPLETION_KIND_CANCEL,
    )


def _recv_many_cqe(completion, user_cb, _extra) -> None:
    """Provided-buffer recv shaper: Completion → ``MultishotDelivery``.

    ``user_data = (_recv_many_cqe, user_cb, extra)``. MORE shells copy the tuple.
    Ignore cancel/poll_remove CQEs that copy this payload.
    """

    if _cancel_or_remove_cqe(completion):
        return
    res = completion.res
    index = completion.sequence
    if res < 0:
        if res == -errno.ENOBUFS:
            delivery = _recv_many_enobufs_delivery(index=index)
        else:
            delivery = _recv_many_error_delivery(index=index, res=res)
    else:
        more = bool(completion.flags & uring_api.IORING_CQE_F_MORE)
        delivery = MultishotDelivery(index, memoryview(completion.result), None, more)
    _emit_recv_many(user_cb, delivery)


def _recv_oneshot_chunk(buffer, res, synthetic_pool):
    if res == 0:
        return memoryview(b"")
    data = bytes(buffer[:res])
    if synthetic_pool is None:
        return memoryview(data)
    return _leased_synthetic_memoryview(data, synthetic_pool)


def _recv_oneshot_cqe(completion, user_cb, extra) -> None:
    """Synthetic-pool oneshot recv shaper. ``user_data = (_recv_oneshot_cqe, cb, (buf, pool))``."""

    if _cancel_or_remove_cqe(completion):
        return
    buffer, synthetic_pool = extra
    res = completion.res
    index = completion.sequence
    if res < 0:
        _emit_recv_many(user_cb, _recv_many_error_delivery(index=index, res=res))
        return
    _emit_recv_many(
        user_cb,
        MultishotDelivery(index, _recv_oneshot_chunk(buffer, res, synthetic_pool), None, False),
    )


def _accept_many_cqe(completion, user_cb, _extra) -> None:
    """Multishot accept shaper. ``user_data = (_accept_many_cqe, user_cb, extra)``.

    MORE shells copy the tuple. Ignore cancel/poll_remove CQEs that copy this payload.
    """

    if _cancel_or_remove_cqe(completion):
        return
    res = completion.res
    index = completion.sequence
    if res < 0:
        _emit_many(user_cb, _continuous_error_delivery(_uring_cqe_oserror(res), index=index))
        return
    conn = socket_from_uring_fd(res)
    more = bool(completion.flags & uring_api.IORING_CQE_F_MORE)
    _emit_many(user_cb, MultishotDelivery(index, conn, None, more))


def _accept_many_oneshot_cqe(completion, user_cb, _extra) -> None:
    """Emulated oneshot accept_many shaper. Always ``more=False``."""

    if _cancel_or_remove_cqe(completion):
        return
    res = completion.res
    index = completion.sequence
    if res < 0:
        _emit_many(user_cb, _continuous_error_delivery(_uring_cqe_oserror(res), index=index))
        return
    conn = socket_from_uring_fd(res)
    _emit_many(user_cb, MultishotDelivery(index, conn, None, False))


def _poll_many_cqe(completion, user_cb, _extra) -> None:
    """Multishot poll shaper. ``user_data = (_poll_many_cqe, user_cb, extra)``.

    MORE shells copy the tuple. Ignore cancel/poll_remove CQEs that copy this payload.
    """

    if _cancel_or_remove_cqe(completion):
        return
    res = completion.res
    index = completion.sequence
    if res < 0:
        _emit_many(user_cb, _continuous_error_delivery(_uring_cqe_oserror(res), index=index))
        return
    more = bool(completion.flags & uring_api.IORING_CQE_F_MORE)
    _emit_many(user_cb, MultishotDelivery(index, res, None, more))


def _poll_many_oneshot_cqe(completion, user_cb, extra) -> None:
    """Emulated oneshot poll_many shaper. Re-arms until stop or error.

    ``user_data = (_poll_many_oneshot_cqe, user_cb, (holder, proactor))``.
    """

    if _cancel_or_remove_cqe(completion):
        return
    holder, proactor = extra
    res = completion.res
    index = holder._next_index
    finish_error = False

    if res >= 0 and holder.completion is not _URING_ABANDONED_LEG:
        _emit_many(user_cb, MultishotDelivery(index, res, None, True))
        holder._next_index = index + 1

    prepare_error: BaseException | None = None
    with proactor._multi_leg_lock:
        if holder.completion is _URING_ABANDONED_LEG:
            holder.completion = None
            return
        if res < 0:
            finish_error = True
        else:
            try:
                holder.completion = proactor._ring.prepare_poll(
                    holder.fd,
                    holder.mask,
                    (_poll_many_oneshot_cqe, user_cb, extra),
                )
            except BaseException as exc:
                prepare_error = exc

    if prepare_error is not None:
        holder.completion = None
        _emit_many(user_cb, _continuous_error_delivery(prepare_error, index=index))
        return
    if finish_error:
        _emit_many(user_cb, _continuous_error_delivery(_uring_cqe_oserror(res), index=index))


def _recv_cqe(completion, user_cb, extra) -> None:
    """Oneshot recv shaper. ``user_data = (_recv_cqe, user_cb, (buf,))``."""

    if _cancel_or_remove_cqe(completion):
        return
    (buf,) = extra
    res = completion.res
    if res < 0:
        user_cb(None, _uring_cqe_oserror(res))
        return
    user_cb(RecvResult(bytes(buf[:res]), _cqe_io_more(completion)), None)


def _send_all_cqe(completion, user_cb, extra) -> None:
    """send_all shaper. ``user_data = (_send_all_cqe, user_cb, (progress,))``."""

    if _cancel_or_remove_cqe(completion):
        return
    (progress,) = extra
    res = completion.res
    if res < 0:
        user_cb(None, _uring_cqe_oserror(res))
        return
    if progress is not None:
        total = completion.result if completion.result is not None else res
        try:
            progress(total)
        except BaseException as exc:
            user_cb(None, exc)
            return
    user_cb(None, None)


def _res_cqe(completion, user_cb, _extra) -> None:
    """Oneshot shaper: ``callback(completion.res)``."""

    if _cancel_or_remove_cqe(completion):
        return
    res = completion.res
    if res < 0:
        user_cb(None, _uring_cqe_oserror(res))
        return
    user_cb(res, None)


def _void_result_cqe(completion, user_cb, _extra) -> None:
    """Oneshot shaper: ``callback(None)`` on success."""

    if _cancel_or_remove_cqe(completion):
        return
    res = completion.res
    if res < 0:
        user_cb(None, _uring_cqe_oserror(res))
        return
    user_cb(None, None)


def _teardown_cqe(completion, user_cb, _extra) -> None:
    """Oneshot shaper for ``stop_poll`` / cancel-request CQEs.

    Unlike other shapers, POLL_REMOVE and CANCEL kinds *are* this completion.
    """

    res = completion.res
    if res < 0:
        user_cb(None, _uring_cqe_oserror(res))
        return
    user_cb(None, None)


def _bytes_cqe(completion, user_cb, extra) -> None:
    if _cancel_or_remove_cqe(completion):
        return
    (buf,) = extra
    res = completion.res
    if res < 0:
        user_cb(None, _uring_cqe_oserror(res))
        return
    user_cb(bytes(buf[:res]), None)


def _socket_cqe(completion, user_cb, _extra) -> None:
    if _cancel_or_remove_cqe(completion):
        return
    res = completion.res
    if res < 0:
        user_cb(None, _uring_cqe_oserror(res))
        return
    user_cb(socket_from_uring_fd(res), None)


def _recvfrom_cqe(completion, user_cb, extra) -> None:
    if _cancel_or_remove_cqe(completion):
        return
    (buf,) = extra
    res = completion.res
    if res < 0:
        user_cb(None, _uring_cqe_oserror(res))
        return
    user_cb((bytes(buf[:res]), completion.result), None)


def _recvfrom_into_cqe(completion, user_cb, _extra) -> None:
    if _cancel_or_remove_cqe(completion):
        return
    res = completion.res
    if res < 0:
        user_cb(None, _uring_cqe_oserror(res))
        return
    user_cb((res, completion.result), None)


def _stat_cqe(completion, user_cb, extra) -> None:
    if _cancel_or_remove_cqe(completion):
        return
    (buf,) = extra
    res = completion.res
    if res < 0:
        user_cb(None, _uring_cqe_oserror(res))
        return
    try:
        user_cb(_stat_result_from_statx(buf), None)
    except ValueError as exc:
        user_cb(None, exc)


def _stat_fdsize_cqe(completion, user_cb, extra) -> None:
    if _cancel_or_remove_cqe(completion):
        return
    (fd,) = extra
    res = completion.res
    if res < 0:
        user_cb(None, _uring_cqe_oserror(res))
        return
    size = completion.result
    if size is None:
        try:
            user_cb(os.fstat(fd).st_size, None)
        except OSError as exc:
            user_cb(None, exc)
        return
    user_cb(size, None)


def _continuous_error_delivery(exc: BaseException, *, index: int = 0) -> MultishotDelivery:
    return MultishotDelivery(index=index, exception=exc, more=False)


def _run_cqe_handler(completion, user_data):
    """Dispatch ``user_data = (handler, user_cb, extra)``. Untyped: handler is a CQE shaper."""

    user_data[0](completion, user_data[1], user_data[2])


def _call_sync_callback(callback: _OneshotCallback, action: Callable[[], object], *, void: bool = False) -> None:
    try:
        value = action()
    except OSError as exc:
        callback(None, exc)
        return
    callback(None if void else value, None)


class _SelectorOpHandle:
    """Selector oneshot cancel token. Opaque ``OpHandle``; not a waitable."""

    __slots__ = ("_callback", "_done", "kind")

    def __init__(self, kind: str) -> None:
        self.kind = kind
        self._callback: _OneshotCallback | None = None
        self._done = False

    def done(self) -> bool:
        return self._done


def _finish_selector_oneshot(
    handle: _SelectorOpHandle,
    result: Any = None,
    exception: BaseException | None = None,
) -> None:
    """Mark the selector token done and invoke its submit callback."""

    if handle._done:
        return
    handle._done = True
    callback = handle._callback
    if callback is None:
        return
    if exception is not None:
        callback(None, exception)
    else:
        callback(result, None)


def _spawn_operation(kind: str, callback: _OneshotCallback | None = None) -> _SelectorOpHandle:
    handle = _SelectorOpHandle(kind)
    handle._callback = callback
    return handle


def _close_raw_fd(fd: int) -> None:
    try:
        os.close(fd)
    except OSError:
        pass


def _enobufs_error() -> OSError:
    return OSError(errno.ENOBUFS, errno.errorcode.get(errno.ENOBUFS, "no buffer space"))


def _synthetic_recv_pool_is_full(buf_group: RecvBufferPool) -> bool:
    if not _is_synthetic_recv_buffer_pool(buf_group):
        return False
    return buf_group.leased_count >= buf_group.buffer_count


def _complete_recv_many_enobufs(
    handle: OpHandle,
    *,
    index: int,
) -> OpHandle:
    handle._finish_with_terminal_delivery(_recv_many_enobufs_delivery(index=index))
    return handle


_DEFAULT_ACCEPT_FLAGS = getattr(socket, "SOCK_NONBLOCK", 0) | getattr(socket, "SOCK_CLOEXEC", 0)


_DEFAULT_OPENAT_DFD = getattr(os, "AT_FDCWD", -100)


def _stat_result_from_statx(buf: bytes | bytearray | memoryview) -> os.stat_result:
    """Build ``os.stat_result`` from a completed io_uring statx buffer."""

    if len(buf) < uring_api.STATX_BUFFER_SIZE:
        raise ValueError("statx buffer must be at least STATX_BUFFER_SIZE bytes")
    mask = struct.unpack_from("<I", buf, 0)[0]
    if not (mask & uring_api.STATX_SIZE):
        raise ValueError("statx buffer does not contain STATX_SIZE fields")
    nlink, uid, gid, mode = struct.unpack_from("<IIIH", buf, 16)
    ino, size, _blocks = struct.unpack_from("<QQQ", buf, 32)
    atime_sec, atime_nsec = struct.unpack_from("<qi", buf, 64)
    ctime_sec, ctime_nsec = struct.unpack_from("<qi", buf, 96)
    mtime_sec, mtime_nsec = struct.unpack_from("<qi", buf, 112)
    _rdev_major, _rdev_minor, dev_major, dev_minor = struct.unpack_from("<IIII", buf, 128)
    dev = os.makedev(dev_major, dev_minor)
    # os.stat_result accepts a 10-field sequence; extra tuple entries mis-map attributes.
    return os.stat_result(
        (
            mode,
            ino,
            dev,
            nlink,
            uid,
            gid,
            size,
            atime_sec + atime_nsec / 1_000_000_000,
            mtime_sec + mtime_nsec / 1_000_000_000,
            ctime_sec + ctime_nsec / 1_000_000_000,
        )
    )


_UringRing: TypeAlias = uring_api.Ring
_UringCompletion: TypeAlias = uring_api.Completion


class RecvBufferPool(Protocol):
    """Receive buffer pool surface shared by uring ``BufGroup`` and ``SyntheticRecvBufferPool``.

    ``leased_count`` tracks how many receive chunks consumers still hold.
    On uring multishot, when the pool is full, ``recv_many`` surfaces
    ``errno.ENOBUFS`` through ``MultishotDelivery.exception`` and completes
    the current leg; consumers drop held views and start a fresh
    ``recv_many()`` to continue. ``SyntheticRecvBufferPool`` mirrors the same
    accounting for degraded selector and uring receive paths.

    Optional ``release_callback`` is an owner hook used by ``close()``: when set,
    ``close()`` returns the pool to its owner (for example the IO manager size
    cache) instead of destroying it. ``close()`` does not clear the hook. Soft
    returns (cache free list) keep it so a second ``close()`` is a soft no-op;
    clear the hook only immediately before intentional hard dispose (for uring
    ``BufGroup``, no-callback ``close()`` frees the kernel ring).
    """

    @property
    def buffer_size(self) -> int: ...

    @property
    def buffer_count(self) -> int: ...

    @property
    def leased_count(self) -> int: ...

    release_callback: Callable[[RecvBufferPool], object] | None

    def close(self) -> None: ...


def _supports_release_buffer() -> bool:
    """Return True when PEP 688 ``__release_buffer__`` exporters are usable."""

    return sys.version_info >= (3, 12)


def _is_synthetic_recv_buffer_pool(buf_group: RecvBufferPool) -> TypeGuard[SyntheticRecvBufferPool]:
    return isinstance(buf_group, SyntheticRecvBufferPool)


class SyntheticRecvBufferPool:
    """Lease-counted buffer pool used when PBUF rings are unavailable or on selector.

    Proactor receive paths deliver copied chunks; ``RecvIterBuffer`` tracks
    leases against this pool so ``sock_recv_iter`` backpressure matches the
    uring provided-buffer contract.
    """

    def __init__(self, buffer_size: int, buffer_count: int) -> None:
        self.buffer_size = buffer_size
        self.buffer_count = buffer_count
        self.leased_count = 0
        self.release_callback: Callable[[RecvBufferPool], object] | None = None

    def close(self) -> None:
        """Return to owner via ``release_callback``, or drop the synthetic pool.

        Does not clear ``release_callback``; the owner (for example the size
        cache) clears it only when hard-disposing. A second ``close()`` while
        free re-enters the owner and is a soft no-op.
        """

        release = self.release_callback
        if release is not None:
            release(self)

    def _note_leased(self) -> None:
        self.leased_count += 1

    def _note_unleased(self) -> None:
        if self.leased_count:
            self.leased_count -= 1

    def note_chunk_released(self) -> None:
        """Explicitly return one leased slot (tests and manual consumers)."""

        self._note_unleased()

    def lease_delivery_chunk(self, data: bytes | bytearray) -> memoryview:
        """Return a pool-leased view for one delivered receive chunk when supported."""

        return _leased_synthetic_memoryview(data, self)

    def wrap_delivery_chunk(self, data: memoryview) -> memoryview:
        """Lease an already-delivered chunk when lease accounting applies."""

        if not data or getattr(data.obj, "__release_buffer__", None) is not None:
            return data
        if not _supports_release_buffer():
            return data
        return self.lease_delivery_chunk(bytes(data))


class _LeasedChunk:
    """PEP 688 buffer exporter whose release returns a synthetic pool slot."""

    __slots__ = ("_data", "_held", "_pool")

    def __init__(self, data: bytearray, pool: SyntheticRecvBufferPool) -> None:
        self._data = data
        self._pool = pool
        self._held: memoryview | None = None

    def __buffer__(self, flags: int) -> memoryview:
        if self._held is not None:
            raise AssertionError("leased chunk buffer is already held")
        self._held = memoryview(self._data)
        return self._held

    def __release_buffer__(self, view: memoryview) -> None:
        # cyclic GC may tp_clear us before the memoryview finalizer runs
        try:
            if self._held is not view:
                raise AssertionError("released view does not match active leased chunk")
            self._held.release()
            self._held = None
            self._pool._note_unleased()
        except AttributeError:
            pass


def _selector_recv_many_chunk_view(data: bytes, buf_group: RecvBufferPool) -> memoryview:
    if not data:
        return memoryview(b"")
    if _is_synthetic_recv_buffer_pool(buf_group):
        return buf_group.lease_delivery_chunk(data)
    return memoryview(data)


def _leased_synthetic_memoryview(data: bytes | bytearray, pool: SyntheticRecvBufferPool) -> memoryview:
    payload = data if type(data) is bytearray else bytearray(data)
    if not _supports_release_buffer():
        # PEP 688 buffer exporters need Python 3.12+; callers cannot release pool
        # slots via memoryview.release() on older builds, so skip lease accounting.
        return memoryview(payload)
    pool._note_leased()
    return memoryview(_LeasedChunk(payload, pool))


_UringRingFactory = Callable[[int, int], _UringRing]


def _resolve_uring_cq_entries(sq_entries: int, cq_entries: int | None) -> int:
    if cq_entries is not None:
        return cq_entries
    return max(DEFAULT_URING_CQ_ENTRIES, sq_entries * 2)


def _default_uring_ring_factory(entries: int, flags: int, cq_entries: int | None = None) -> _UringRing:
    return uring_api.Ring(entries=entries, flags=flags, cq_entries=_resolve_uring_cq_entries(entries, cq_entries))


class Proactor(Protocol):
    """Minimal completion-oriented IO backend used by `ProactorScheduler`."""

    def close(self) -> None: ...

    def wake_wait(self) -> None: ...

    def set_delivery_exception_handler(
        self,
        handler: Callable[[dict[str, Any]], object] | None,
    ) -> None: ...

    def bind_loop(self, loop: _asyncio.AbstractEventLoop) -> None: ...

    def get_time(self) -> float: ...

    def set_clock(self, clock: _Clock) -> None: ...

    def has_pending_operations(self) -> bool: ...

    def wait(self, deadline: float | None = None) -> None: ...

    async def wait_async(self, deadline: float | None = None) -> None: ...

    def recv(
        self,
        sock: socket.socket,
        n: int,
        callback: _OneshotRecvCallback,
    ) -> OpHandle:
        """Arm a oneshot recv. ``callback(result, exception)``.

        Returns an opaque ``OpHandle`` (uring: the armed ``Completion``;
        selector: a private oneshot token). Not a waitable — park in the IO
        manager. ``n == 0`` and selector first-try success invoke ``callback``
        before this returns.
        """
        ...

    def recv_into(self, sock: socket.socket, buf: Any, callback: _OneshotCallback) -> OpHandle: ...

    def recvfrom(self, sock: socket.socket, bufsize: int, callback: _OneshotCallback) -> OpHandle: ...

    def recvfrom_into(self, sock: socket.socket, buf: Any, callback: _OneshotCallback, nbytes: int = 0) -> OpHandle: ...

    def send(
        self,
        sock: socket.socket,
        data: Any,
        callback: _OneshotCallback,
        progress: _ProgressCallback | None = None,
        *,
        expect: IoExpect = IoExpect.READY,
    ) -> OpHandle: ...

    def send_nowait(
        self,
        sock: socket.socket,
        data: Any,
        callback: _OneshotCallback | None = None,
        *,
        expect: IoExpect = IoExpect.READY,
    ) -> None:
        """Fire-and-forget sendall. ``callback(None, exc)`` on failure only."""
        ...

    def send_close_nowait(
        self,
        sock: socket.socket,
        data: Any,
        *,
        expect: IoExpect = IoExpect.READY,
    ) -> None:
        """Drain ``data`` then nowait-close ``sock``. No waitable."""
        ...

    def sendto(self, sock: socket.socket, data: Any, address: Any, callback: _OneshotCallback) -> OpHandle: ...

    def accept(self, sock: socket.socket, callback: _OneshotCallback) -> OpHandle: ...

    def accept_many(
        self,
        sock: socket.socket,
        callback: _AcceptManyCallback,
        *,
        base_sequence: int = 0,
    ) -> OpHandle:
        """Accept connections until cancelled or failed.

        Returns an opaque ``OpHandle``, not a waitable. Each callback
        receives a ``MultishotDelivery`` whose ``value`` is the accepted
        ``socket``. Call ``socket.getpeername()`` when the peer address is
        needed. Use ``ProactorIOManager.accept_many`` for accept-time reads
        and a waitable over stream-end.

        ``base_sequence`` seeds delivery ``index`` for the first accept leg
        (multishot: first kernel sequence; oneshot/selector: that single leg).
        """

        ...

    def connect(
        self,
        sock: socket.socket,
        address: Any,
        callback: _OneshotCallback,
    ) -> OpHandle:
        """Connect a socket.

        For ``AF_UNIX``, the connect completes synchronously via a brief
        blocking ``sock.connect()`` and invokes ``callback`` before return.
        Inet sockets use the backend's async path.
        ``ProactorIOManager.sock_connect`` composes connect-time send via
        ``IOWaitGroup`` instead.
        """

        ...

    def shutdown(self, sock: socket.socket, how: int, callback: _OneshotCallback) -> OpHandle:
        """Submit ``socket.shutdown(how)`` for ``sock``."""

        ...

    def close_socket(self, sock: socket.socket, callback: _OneshotCallback) -> OpHandle:
        """Submit socket close and release the Python wrapper fd."""

        ...

    def close_socket_nowait(self, sock: socket.socket) -> None:
        """Close ``sock`` without a waitable completion.

        Releases the Python wrapper immediately. On uring this detaches the fd
        and prepares a nowait close (same lazy flush as ``close_socket``).
        On selector it is ``sock.close()``. Kernel close failures are not
        reported on a waiter.
        """

        ...

    def shutdown_nowait(self, sock: socket.socket, how: int) -> None:
        """``shutdown(how)`` without a waitable.

        Uring nowait ring shutdown parks on the send-all conflict FIFO.
        Selector queues behind an in-flight send on the same fd.
        """

        ...

    def create_socket(
        self,
        family: int,
        type: int,
        callback: _OneshotCallback,
        proto: int = 0,
        *,
        flags: int = 0,
    ) -> OpHandle:
        """Create a scheduler-contract socket.

        ``ProactorIOManager.sock_create`` creates sockets directly and only
        composes connect (and optional send) via ``IOWaitGroup``. This method
        remains for direct proactor callers (including the uring socket op).
        """

        ...

    def openat(
        self,
        path: str,
        flags: int,
        callback: _OneshotCallback,
        mode: int = 0,
        *,
        dfd: int = _DEFAULT_OPENAT_DFD,
    ) -> OpHandle: ...

    def read(self, fd: int, n: int, offset: int, callback: _OneshotCallback) -> OpHandle: ...

    def read_into(self, fd: int, buf: Any, offset: int, callback: _OneshotCallback) -> OpHandle: ...

    def write(self, fd: int, data: Any, offset: int, callback: _OneshotCallback) -> OpHandle: ...

    def close_fd(self, fd: int, callback: _OneshotCallback) -> OpHandle:
        """Close a caller-owned raw file descriptor."""

        ...

    def stat(self, path: str = "", *, fd: int = -1, callback: _OneshotCallback) -> OpHandle: ...

    def stat_fdsize(self, fd: int, callback: _OneshotCallback) -> OpHandle: ...

    def recv_many(
        self,
        sock: socket.socket,
        callback: _RecvManyCallback,
        *,
        buf_group: RecvBufferPool,
        base_sequence: int = 0,
    ) -> OpHandle: ...

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> RecvBufferPool: ...

    def shared_recv_buffer_pool(self) -> RecvBufferPool: ...

    def set_shared_recv_buffer_pool(self, pool: RecvBufferPool) -> None: ...

    def poll(self, fd: int, mask: int, callback: _OneshotCallback) -> OpHandle: ...

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: _PollManyCallback,
    ) -> OpHandle:
        """Start a continuous poll stream.

        Returns an opaque handle for ``stop_poll``, not a waitable. Each
        callback receives a ``MultishotDelivery`` whose ``value`` is the
        readiness mask. Stop with ``stop_poll`` (not ``cancel`` on uring
        multishot — that posts ``ASYNC_CANCEL``; ``stop_poll`` posts
        ``POLL_REMOVE``).
        """

        ...

    def cancel(self, handle: OpHandle, callback: _OneshotCallback) -> None:
        """Cancel ``handle``. ``callback(None, exception)``.

        Posts ``ASYNC_CANCEL`` (uring) or local-terminalises (selector).
        Prefer ``stop_poll`` to stop a poll stream (``POLL_REMOVE`` on native
        uring). Does not check handle kind. Returns nothing — the callback
        is the cancel-request completion.
        """

        ...

    def cancel_nowait(self, handle: OpHandle) -> None:
        """Cancel ``handle`` without a teardown waitable.

        Uring posts ``ASYNC_CANCEL`` with skip-success (same lazy flush as
        ``close_socket_nowait``) whenever a reverse ``Completion`` exists,
        including after the target may already have completed (kernel
        ``-ENOENT`` is silent). Selector deregisters and terminalises
        locally. Prefer ``stop_poll`` for ``poll_many``. The target still
        finishes from its CQE (uring) or local terminalise (selector).
        """

        ...

    def stop_poll(self, handle: OpHandle, callback: _OneshotCallback) -> None:
        """Stop a ``poll_many`` stream. ``callback(None, exception)``.

        Same shape as ``cancel``: no returned token. Native uring posts
        ``prepare_poll_remove``; emulated oneshot abandons the reverse link
        and ``ASYNC_CANCEL``s the live poll; selector deregisters locally.
        Does not check that ``handle`` came from ``poll_many``.
        """

        ...


ProactorFactory = Callable[[], Proactor]


class ProactorBase:
    """Shared helpers for concrete proactor backends."""

    def __init__(self) -> None:
        self._closed = False
        self._clock = time.monotonic
        self._async_wait_loop: _asyncio.AbstractEventLoop | None = None
        self._async_break: Callable[[], object] | None = None
        self._shared_recv_buffer_pool: RecvBufferPool | None = None
        self._delivery_exception_handler: Callable[[dict[str, Any]], object] | None = None

    def set_delivery_exception_handler(
        self,
        handler: Callable[[dict[str, Any]], object] | None,
    ) -> None:
        """Route continuous delivery-callback failures to ``handler`` when set."""

        self._delivery_exception_handler = handler

    @overload
    def _guard_delivery_callback(
        self,
        callback: Callable[[MultishotDelivery], object],
    ) -> Callable[[MultishotDelivery], object]: ...

    @overload
    def _guard_delivery_callback(self, callback: None) -> None: ...

    def _guard_delivery_callback(
        self,
        callback: Callable[[MultishotDelivery], object] | None,
    ) -> Callable[[MultishotDelivery], object] | None:
        if callback is None:
            return None

        def guarded(delivery: MultishotDelivery) -> None:
            # User callbacks own stream-end; the guard only routes failures.
            try:
                callback(delivery)
            except BaseException as exc:
                handler = self._delivery_exception_handler
                if handler is None:
                    raise
                handler(
                    {
                        "message": "Exception in delivery callback",
                        "exception": exc,
                        "delivery": delivery,
                    }
                )

        return guarded

    def set_async_break(self, callback: Callable[[], object] | None) -> None:
        """Optional hook ``SelectorProactor.wake_wait()`` runs after selector wakeup."""

        self._async_break = callback

    def _run_async_break(self) -> None:
        break_cb = self._async_break
        if break_cb is not None:
            break_cb()

    def bind_loop(self, loop: _asyncio.AbstractEventLoop) -> None:
        """Bind this proactor to an asyncio event loop for async waits."""

        if self._async_wait_loop is None:
            self._async_wait_loop = loop
        elif self._async_wait_loop is not loop:
            raise RuntimeError(f"{type(self).__name__} is already bound to a different event loop")
        self._bind_wakeup_loop(loop)

    def _bind_wakeup_loop(self, loop: _asyncio.AbstractEventLoop) -> None:
        return

    def get_time(self) -> float:
        """Return the proactor clock value."""

        return self._clock()

    def set_clock(self, clock: _Clock) -> None:
        """Set the clock used for deadline-oriented waits."""

        self._clock = clock

    def _detach_owner_hooks(self) -> None:
        """Drop scheduler-owned bound methods so close can break ref cycles.

        ``ProactorScheduler`` installs ``self.time`` and
        ``self.call_exception_handler``; those methods keep the scheduler alive
        via ``__self__`` while the scheduler owns the proactor.
        """

        self._clock = time.monotonic
        self._delivery_exception_handler = None
        self._async_break = None

    def _timeout_until_deadline(self, deadline: float | None) -> float | None:
        if deadline is None:
            return None
        if deadline == 0:
            return 0.0
        return max(0.0, deadline - self.get_time())

    def _check_open(self) -> None:
        """Raise if this proactor has been closed.

        Selector backends call this at submit and wait. ``UringProactor`` does
        not: a closed ring already fails prepare/wait, and use-after-close is
        misuse rather than a recovery path.
        """

        if self._closed:
            raise RuntimeError("proactor is closed")

    def close_socket_nowait(self, sock: socket.socket) -> None:
        """Close ``sock`` without a waitable completion (stdlib ``sock.close()``)."""

        if sock.fileno() == -1:
            return
        sock.close()

    def shutdown_nowait(self, sock: socket.socket, how: int) -> None:
        """``socket.shutdown(how)`` on the calling thread (no waitable)."""

        if sock.fileno() == -1:
            return
        sock.shutdown(how)

    def _report_nowait_send_error(self, exc: BaseException, sock: object, *, message: str) -> None:
        handler = self._delivery_exception_handler
        if handler is None:
            return
        handler(
            {
                "message": message,
                "exception": exc,
                "proactor": self,
                "socket": sock,
            }
        )

    def _report_send_nowait_error(self, exc: BaseException, sock: object) -> None:
        self._report_nowait_send_error(exc, sock, message="send_nowait failed")

    def _report_send_close_nowait_error(self, exc: BaseException, sock: object) -> None:
        self._report_nowait_send_error(exc, sock, message="send_close_nowait failed")

    def recv_many(
        self,
        sock: socket.socket,
        callback: _RecvManyCallback,
        *,
        buf_group: RecvBufferPool,
        base_sequence: int = 0,
    ) -> OpHandle:
        raise NotImplementedError

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> RecvBufferPool:
        raise NotImplementedError(f"{type(self).__name__} does not provide receive buffer pools")

    def _default_shared_recv_buffer_pool_sizes(self) -> tuple[int, int]:
        return _DEFAULT_RECVITER_BUFFER_SIZE, _DEFAULT_RECVITER_BUFFER_COUNT

    def shared_recv_buffer_pool(self) -> RecvBufferPool:
        """Return this proactor's lazy shared provided-buffer pool."""

        pool = self._shared_recv_buffer_pool
        if pool is None:
            buffer_size, buffer_count = self._default_shared_recv_buffer_pool_sizes()
            pool = self.create_recv_buffer_pool(buffer_size, buffer_count)
            self._shared_recv_buffer_pool = pool
        return pool

    def set_shared_recv_buffer_pool(self, pool: RecvBufferPool) -> None:
        """Replace this proactor's shared provided-buffer pool."""

        self._shared_recv_buffer_pool = pool

    def _clear_shared_recv_buffer_pool(self) -> None:
        self._shared_recv_buffer_pool = None

    def _terminalise_cancelled(self, handle: _SelectorOpHandle | _DeliveryHandle) -> None:
        """Apply local cancel when the backend will not produce a completion.

        One-shot ops finish with ``OSError(ECANCELED)``. ``_DeliveryHandle``
        streams emit a terminal ``MultishotDelivery`` at ``_next_index`` (no
        done/exception). Used by selector stop and by oneshot ``stop_poll``
        (not by ordinary uring ``cancel()`` of a ``Completion``). Must not
        run while holding ``_multi_leg_lock``.
        """

        if isinstance(handle, _DeliveryHandle):
            handle._finish_with_terminal_delivery(
                _continuous_error_delivery(
                    io_cancellation_error(),
                    index=getattr(handle, "_next_index", 0),
                ),
            )
            return
        assert isinstance(handle, _SelectorOpHandle)
        _finish_selector_oneshot(handle, exception=io_cancellation_error())

    def cancel(self, handle: OpHandle, callback: _OneshotCallback) -> None:
        raise NotImplementedError

    def cancel_nowait(self, handle: OpHandle) -> None:
        raise NotImplementedError

    def stop_poll(self, handle: OpHandle, callback: _OneshotCallback) -> None:
        raise NotImplementedError

    def openat(
        self,
        path: str,
        flags: int,
        callback: _OneshotCallback,
        mode: int = 0,
        *,
        dfd: int = _DEFAULT_OPENAT_DFD,
    ) -> OpHandle:
        raise NotImplementedError

    def read(self, fd: int, n: int, offset: int, callback: _OneshotCallback) -> OpHandle:
        raise NotImplementedError

    def read_into(self, fd: int, buf: Any, offset: int, callback: _OneshotCallback) -> OpHandle:
        raise NotImplementedError

    def write(self, fd: int, data: Any, offset: int, callback: _OneshotCallback) -> OpHandle:
        raise NotImplementedError

    def close_fd(self, fd: int, callback: _OneshotCallback) -> OpHandle:
        """Close a caller-owned raw file descriptor."""

        self._check_open()
        if fd < 0:
            callback(None, None)
            return None
        _call_sync_callback(callback, lambda: _close_raw_fd(fd), void=True)
        return None

    def stat(self, path: str = "", *, fd: int = -1, callback: _OneshotCallback) -> OpHandle:
        """Return file metadata, completing synchronously via ``os.stat`` / ``os.fstat``."""

        self._check_open()
        if fd < 0 and not path:
            raise ValueError("stat() requires fd >= 0 or a non-empty path")
        if fd >= 0:
            _call_sync_callback(callback, lambda: os.fstat(fd))
        else:
            _call_sync_callback(callback, lambda: os.stat(path))
        return None

    def stat_fdsize(self, fd: int, callback: _OneshotCallback) -> OpHandle:
        """Return the byte length of an open file descriptor."""

        self._check_open()
        if fd < 0:
            raise ValueError("stat_fdsize() requires fd >= 0")
        _call_sync_callback(callback, lambda: os.fstat(fd).st_size)
        return None

    def poll(self, fd: int, mask: int, callback: _OneshotCallback) -> OpHandle:
        raise NotImplementedError

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: _PollManyCallback,
    ) -> OpHandle:
        raise NotImplementedError

    def _sync_unix_connect(
        self,
        sock: socket.socket,
        address: Any,
        callback: _OneshotCallback,
    ) -> OpHandle:
        """Complete a UNIX-domain connect synchronously and invoke ``callback``.

        io_uring ``prepare_connect`` does not accept UNIX sockaddr paths today.
        Both proactor backends use this path so chained ``connect`` legs from
        ``sock_create`` / ``sock_connect`` behave uniformly at the io_manager
        layer even when the operation finishes before the caller's ``wait()`` returns.
        """

        def finish_connect() -> None:
            sock.setblocking(True)
            try:
                sock.connect(address)
            finally:
                sock.setblocking(False)

        _call_sync_callback(callback, finish_connect, void=True)
        return None


@dataclass
class _FdSlot:
    handle: _SelectorOpHandle | _DeliveryHandle
    attempt: Callable[[], Any] | None = None
    step: Callable[[], ContinuousStepResult] | None = None


@dataclass
class _QueuedWrite:
    """One write-side op waiting behind an in-flight send on the same fd."""

    run: Callable[[], None]
    handle: _SelectorOpHandle | None = None


@dataclass
class _FdEntry:
    reader: _FdSlot | None = None
    writer: _FdSlot | None = None
    write_queue: deque[_QueuedWrite] = field(default_factory=deque)

    def empty(self) -> bool:
        return self.reader is None and self.writer is None and not self.write_queue


# Ring user_data for callback CQEs is ``(handler, user_cb, extra)``.
# ``extra`` is ``()`` or a frozen cargo tuple. Delivery is
# ``fn(completion, user_cb, extra)``.


class _UringOneshotPollHandle(_DeliveryHandle):
    """Opaque emulated oneshot poll_many token: replaceable reverse Completion.

    Native poll_many returns the armed ``Completion``. This holder exists only
    when ``IORING_POLL_MULTISHOT`` is unavailable so re-arm can replace the
    live reverse under ``_multi_leg_lock``. Not a waitable; not pooled.
    """

    __slots__ = ("_next_index", "completion", "fd", "mask")

    def __init__(self, callback: _PollManyCallback, fd: int, mask: int) -> None:
        super().__init__(callback)
        self.completion: Any = None
        self.fd = fd
        self.mask = mask
        self._next_index = 0


# Reverse-link sentinel: cancel/stop claimed the in-flight oneshot poll leg.
class _AbandonedLeg:
    __slots__ = ()
    user_data = object()


_URING_ABANDONED_LEG = _AbandonedLeg()


def _uring_reverse_is_live(completion: object | None) -> bool:
    """True if reverse points at a Completion that still holds ``user_data``.

    After CQE delivery, ``user_data`` is nerfed so the cycle is broken even
    if reverse still holds the object. ``cancel_nowait`` posts whenever a
    reverse ``Completion`` exists.
    """

    if completion is None or completion is _URING_ABANDONED_LEG:
        return False
    return getattr(completion, "user_data", None) is not None


class SelectorProactor(ProactorBase):
    """Completion-oriented proactor prototype backed by a selector."""

    def __init__(
        self,
        selector: selectors.BaseSelector | None = None,
    ) -> None:
        super().__init__()
        self._lock = threading.RLock()
        self._selector = selector if selector is not None else compat.released_default_selector()
        self._fd_slots: dict[int, _FdEntry] = {}
        self._wakeup_reader, self._wakeup_writer = socket.socketpair()
        self._wakeup_reader.setblocking(False)
        self._wakeup_writer.setblocking(False)
        self._selector.register(self._wakeup_reader.fileno(), selectors.EVENT_READ, None)

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> SyntheticRecvBufferPool:
        """Create a synthetic provided-buffer pool for ``recv_many`` / ``sock_recv_iter``."""

        return SyntheticRecvBufferPool(buffer_size, buffer_count)

    def create_buf_group(self, buffer_size: int, buffer_count: int) -> SyntheticRecvBufferPool:
        return self.create_recv_buffer_pool(buffer_size, buffer_count)

    def has_pending_operations(self) -> bool:
        """Return True if IO is waiting for backend completion."""

        with self._lock:
            return bool(self._fd_slots)

    def close(self) -> None:
        """Close selector and wakeup resources."""

        self._wake_selector()
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._clear_shared_recv_buffer_pool()
            self._selector.close()
            self._wakeup_reader.close()
            self._wakeup_writer.close()
            self._detach_owner_hooks()

    def wake_wait(self) -> None:
        """Wake a thread blocked in `wait`."""

        self._wake_selector()
        self._run_async_break()

    def _wake_selector(self) -> None:
        """Wake a thread blocked in the selector."""

        try:
            self._wakeup_writer.send(b"\0")
        except (BlockingIOError, OSError):
            pass

    def _after_selector_registration_changed(self) -> None:
        pass

    def wait(self, deadline: float | None = None) -> None:
        """Wait until `deadline` and drive ready IO."""

        with self._lock:
            self._check_open()
            self._poll(deadline)

    def _poll(self, deadline: float | None = None) -> None:
        select_released = getattr(self._selector, "select_released", None)
        wakeup_fd = self._wakeup_reader.fileno()
        while True:
            timeout = self._timeout_until_deadline(deadline)
            if select_released is None:
                events = self._selector.select(timeout)
            else:
                # Compat selector with released-lock select (see compat module).
                events = select_released(timeout, self._lock)
            progressed = False
            woke = False
            for key, mask in events:
                fd = key.fd
                if fd == wakeup_fd:
                    self._drain_wakeup()
                    woke = True
                    continue
                entry = self._fd_slots.get(fd)
                if entry is not None and entry.reader is not None and entry.reader is entry.writer:
                    if mask & (selectors.EVENT_READ | selectors.EVENT_WRITE):
                        progressed = self._step_fd_operation(fd, selectors.EVENT_READ) or progressed
                    continue
                if mask & selectors.EVENT_READ:
                    progressed = self._step_fd_operation(fd, selectors.EVENT_READ) or progressed
                if mask & selectors.EVENT_WRITE:
                    progressed = self._step_fd_operation(fd, selectors.EVENT_WRITE) or progressed
            if progressed or woke or timeout == 0 or not events:
                return

    async def wait_async(self, deadline: float | None = None) -> None:
        """Wait asynchronously until `deadline` and drive ready IO."""

        self._check_open()
        if deadline == 0:
            self.wait(0)
            return

        timeout = self._timeout_until_deadline(deadline)
        if timeout == 0:
            return

        loop = self._async_wait_loop
        assert loop is not None
        await loop.run_in_executor(None, self.wait, deadline)

    def recv(
        self,
        sock: socket.socket,
        n: int,
        callback: _OneshotRecvCallback,
    ) -> OpHandle:
        """Arm a oneshot recv. ``callback(result, exception)``.

        Selector still parks internally on a private oneshot token; that
        object is the opaque ``OpHandle``. The submit callback runs when the
        attempt completes (including a synchronous first try).
        """

        operation = _spawn_operation("recv", callback)

        def attempt() -> RecvResult:
            return RecvResult(sock.recv(n))

        self._prepare_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def recv_into(self, sock: socket.socket, buf: Any, callback: _OneshotCallback) -> OpHandle:
        """Arm a oneshot recv-into. ``callback(nbytes, exception)``."""

        operation = _spawn_operation("recv_into", callback)

        def attempt() -> int:
            return sock.recv_into(buf)

        self._prepare_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def recvfrom(self, sock: socket.socket, bufsize: int, callback: _OneshotCallback) -> OpHandle:
        """Arm a oneshot datagram recv. ``callback((data, address), exception)``."""

        operation = _spawn_operation("recvfrom", callback)

        def attempt() -> tuple[bytes, Any]:
            return sock.recvfrom(bufsize)

        self._prepare_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def recvfrom_into(self, sock: socket.socket, buf: Any, callback: _OneshotCallback, nbytes: int = 0) -> OpHandle:
        """Arm a oneshot datagram recv-into. ``callback((nbytes, address), exception)``."""

        operation = _spawn_operation("recvfrom_into", callback)

        def attempt() -> tuple[int, Any]:
            if nbytes:
                return sock.recvfrom_into(buf, nbytes)
            return sock.recvfrom_into(buf)

        self._prepare_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def send(
        self,
        sock: socket.socket,
        data: Any,
        callback: _OneshotCallback,
        progress: _ProgressCallback | None = None,
        *,
        expect: IoExpect = IoExpect.READY,
    ) -> OpHandle:
        """Arm a stream send that drains ``data``. ``callback(None, exception)``.

        ``expect`` is ignored on the selector path (the socket is already
        polled for ``EVENT_WRITE``).
        """

        del expect
        operation = _spawn_operation("send", callback)
        view = memoryview(data)
        offset = 0

        def attempt() -> None:
            nonlocal offset
            while offset < len(view):
                sent = sock.send(view[offset:])
                if sent == 0:
                    raise BlockingIOError(errno.EWOULDBLOCK, "socket send returned zero bytes")
                offset += sent
                if progress is not None:
                    progress(offset)

        def start() -> None:
            self._prepare_socket_operation(sock, selectors.EVENT_WRITE, operation, attempt)

        fd = sock.fileno()
        with self._lock:
            self._check_open()
            if self._write_busy(fd):
                self._enqueue_write(fd, start, operation)
                return operation
            start()
        return operation

    def send_nowait(
        self,
        sock: socket.socket,
        data: Any,
        callback: _OneshotCallback | None = None,
        *,
        expect: IoExpect = IoExpect.READY,
    ) -> None:
        """Fire-and-forget sendall. ``callback(None, exc)`` on failure only.

        Success is silent. With no ``callback``, failures go to the delivery
        exception handler. ``expect`` is ignored (same as ``send``).
        """

        self._check_open()
        if not data:
            return

        def on_send(_result: object, exception: BaseException | None) -> None:
            if exception is None:
                return
            if callback is not None:
                callback(None, exception)
            else:
                self._report_send_nowait_error(exception, sock)

        self.send(sock, data, on_send, expect=expect)

    def send_close_nowait(
        self,
        sock: socket.socket,
        data: Any,
        *,
        expect: IoExpect = IoExpect.READY,
    ) -> None:
        """Drain ``data`` then nowait-close ``sock``. No waitable.

        Completions stay internal. Close is queued on the write FIFO behind
        the send (same order as uring send_all then close).
        """

        self._check_open()
        if not data:
            self.close_socket_nowait(sock)
            return

        def on_error(_result: object, exception: BaseException | None) -> None:
            if exception is not None:
                self._report_send_close_nowait_error(exception, sock)

        self.send_nowait(sock, data, on_error, expect=expect)
        self.close_socket_nowait(sock)

    def sendto(self, sock: socket.socket, data: Any, address: Any, callback: _OneshotCallback) -> OpHandle:
        """Arm a datagram send. ``callback(nbytes, exception)``."""

        operation = _spawn_operation("sendto", callback)

        def attempt() -> int:
            return sock.sendto(data, address)

        self._prepare_socket_operation(sock, selectors.EVENT_WRITE, operation, attempt)
        return operation

    def accept(self, sock: socket.socket, callback: _OneshotCallback) -> OpHandle:
        """Arm a oneshot accept. ``callback(conn, exception)``."""

        operation = _spawn_operation("accept", callback)

        def attempt() -> socket.socket:
            conn, _address = sock.accept()
            configure_scheduler_socket(conn)
            return conn

        self._prepare_socket_operation(sock, selectors.EVENT_READ, operation, attempt)
        return operation

    def shutdown(self, sock: socket.socket, how: int, callback: _OneshotCallback) -> OpHandle:
        """``shutdown(how)`` after any in-flight send on this fd."""

        def run() -> None:
            _call_sync_callback(callback, lambda: sock.shutdown(how), void=True)

        self._run_or_enqueue_write(sock, run)
        return None

    def shutdown_nowait(self, sock: socket.socket, how: int) -> None:
        """Nowait ``shutdown``; queued behind an in-flight send on this fd."""

        def run() -> None:
            if sock.fileno() == -1:
                return
            try:
                sock.shutdown(how)
            except OSError as exc:
                self._report_nowait_send_error(exc, sock, message="shutdown_nowait failed")

        self._run_or_enqueue_write(sock, run)

    def close_socket_nowait(self, sock: socket.socket) -> None:
        """Nowait close; queued behind an in-flight send on this fd."""

        def run() -> None:
            if sock.fileno() == -1:
                return
            try:
                sock.close()
            except OSError:
                pass

        self._run_or_enqueue_write(sock, run)

    def close_socket(self, sock: socket.socket, callback: _OneshotCallback) -> OpHandle:
        """Close ``sock`` after any in-flight send on this fd."""

        def run() -> None:
            _call_sync_callback(callback, sock.close, void=True)

        self._run_or_enqueue_write(sock, run)
        return None

    def accept_many(
        self,
        sock: socket.socket,
        callback: _AcceptManyCallback,
        *,
        base_sequence: int = 0,
    ) -> OpHandle:
        """Accept connections and deliver each via the result callback.

        Returns a ``SelectorCancelHandle``, not a waitable. Without io_uring
        multishot accept this issues one ``accept()`` per ``accept_many`` call
        and emits ``more=False``. Callers must arm another accept
        (``StreamServer`` re-arms in a loop; ``scheduler.io.accept_many().wait()``
        returns after each leg). This differs from oneshot ``poll_many``
        fallbacks, which arm the next one-shot leg inside the proactor until
        cancel. With multishot (``UringProactor`` only) one kernel leg may
        deliver many connections until cancel, error, or terminal CQE.

        `callback` may run on any backend worker thread. Each accepted connection
        is delivered as the accepted ``socket``. Call ``socket.getpeername()`` when
        the peer address is needed.

        ``base_sequence`` is the delivery ``index`` for this accept leg.
        """

        handle = SelectorCancelHandle(
            self._guard_delivery_callback(callback),
            base_sequence=base_sequence,
        )

        def step() -> ContinuousStepResult:
            try:
                conn, _address = sock.accept()
            except (BlockingIOError, InterruptedError):
                return ContinuousStepResult(progressed=False)
            configure_scheduler_socket(conn)
            handle._emit_result(conn, more=False, index=base_sequence)
            return ContinuousStepResult(progressed=True, done=True)

        self._prepare_socket_continuous_operation(sock, selectors.EVENT_READ, handle, step)
        return handle

    def create_socket(
        self,
        family: int,
        type: int,
        callback: _OneshotCallback,
        proto: int = 0,
        *,
        flags: int = 0,
    ) -> OpHandle:
        """Create a scheduler-contract socket."""

        del flags
        _call_sync_callback(callback, lambda: _sync_create_scheduler_socket(family, type, proto))
        return None

    def connect(
        self,
        sock: socket.socket,
        address: Any,
        callback: _OneshotCallback,
    ) -> OpHandle:
        """Arm a socket connect. ``callback(None, exception)``."""

        if sock.family == socket.AF_UNIX:
            return self._sync_unix_connect(sock, address, callback)

        return self._prepare_selector_connect(sock, address, callback)

    def _prepare_selector_connect(
        self,
        sock: socket.socket,
        address: Any,
        callback: _OneshotCallback,
    ) -> OpHandle:
        started = False

        def finish_connect() -> None:
            nonlocal started
            if not started:
                started = True
                try:
                    sock.connect(address)
                except (BlockingIOError, InterruptedError):
                    raise BlockingIOError(errno.EINPROGRESS, "connect in progress") from None
                except OSError as exc:
                    if exc.errno in (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
                        raise BlockingIOError(exc.errno, exc.strerror) from None
                    raise
                return
            err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err == 0:
                return
            if err in (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY):
                raise BlockingIOError(err, errno.errorcode.get(err, "connect in progress"))
            raise OSError(err, errno.errorcode.get(err, "socket connect failed"))

        operation = _spawn_operation("connect", callback)

        def attempt() -> None:
            finish_connect()

        self._prepare_socket_operation(sock, selectors.EVENT_WRITE, operation, attempt)
        return operation

    def recv_many(
        self,
        sock: socket.socket,
        callback: _RecvManyCallback,
        *,
        buf_group: RecvBufferPool,
        base_sequence: int = 0,
    ) -> OpHandle:
        """Submit one ``recv()`` and deliver a single ``MultishotDelivery``.

        `callback` may run on any backend worker thread. This backend does not
        provide native multishot receive: one ``recv()`` result is delivered at
        ``base_sequence`` with ``more=False`` (data, empty EOF, or ``exception``),
        then the handle completes. Callers that need a byte stream must start
        a fresh ``recv_many()`` for each further chunk.

        ``buf_group`` sizes ``SyntheticRecvBufferPool`` lease accounting;
        selector receive delivers copied ``memoryview`` data per call. When the
        synthetic pool is already full, ``recv_many()`` delivers ``errno.ENOBUFS``
        immediately without submitting ``recv()``.
        """

        handle = SelectorCancelHandle(
            self._guard_delivery_callback(callback),
            base_sequence=base_sequence,
        )
        if _synthetic_recv_pool_is_full(buf_group):
            return _complete_recv_many_enobufs(handle, index=base_sequence)

        def step() -> ContinuousStepResult:
            try:
                data = sock.recv(_DEFAULT_SELECTOR_RECV_MANY_CHUNK_SIZE)
            except (BlockingIOError, InterruptedError):
                return ContinuousStepResult(progressed=False)
            except OSError as exc:
                handle._finish_with_terminal_delivery(MultishotDelivery(index=base_sequence, exception=exc, more=False))
                return ContinuousStepResult(progressed=True, done=True)
            chunk = _selector_recv_many_chunk_view(data, buf_group)
            handle._emit_result(chunk, index=base_sequence, more=False)
            return ContinuousStepResult(progressed=True, done=True)

        self._prepare_socket_continuous_operation(sock, selectors.EVENT_READ, handle, step)
        return handle

    def poll(self, fd: int, mask: int, callback: _OneshotCallback) -> OpHandle:
        """Wait until an fd reports the requested poll events."""

        operation = _spawn_operation("poll", callback)

        def attempt() -> int:
            return _probe_poll_fd_now(fd, mask)

        self._prepare_fd_operation(fd, mask, operation, attempt)
        return operation

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: _PollManyCallback,
    ) -> OpHandle:
        """Emit poll event masks whenever the fd becomes ready.

        Returns a ``SelectorCancelHandle``, not a waitable. `callback` may
        run on any backend worker thread. Stop with ``stop_poll``.
        """

        handle = SelectorCancelHandle(self._guard_delivery_callback(callback))

        def step() -> ContinuousStepResult:
            try:
                result = _probe_poll_fd_now(fd, mask)
            except BlockingIOError:
                return ContinuousStepResult(progressed=False)
            index = handle._next_index
            handle._emit_result(result, more=True, index=index)
            handle._next_index = index + 1
            return ContinuousStepResult(progressed=True)

        self._prepare_fd_continuous_operation(fd, mask, handle, step)
        return handle

    def _prepare_fd_operation(
        self,
        fd: int,
        poll_mask: int,
        operation: _SelectorOpHandle,
        attempt: Callable[[], T],
    ) -> None:
        with self._lock:
            self._check_open()
            self._check_fd(fd)
            selector_events = _poll_mask_to_selector_events(poll_mask)
            if selector_events & selectors.EVENT_READ:
                self._check_fd_slot_available(fd, selectors.EVENT_READ)
            if selector_events & selectors.EVENT_WRITE:
                self._check_fd_slot_available(fd, selectors.EVENT_WRITE)
            if self._try_complete_operation(operation, attempt):
                return
            self._reserve_fd_poll_slot(fd, selector_events, operation, attempt)
            self._update_selector_registration(fd)
        self._after_selector_registration_changed()

    def _prepare_fd_continuous_operation(
        self,
        fd: int,
        poll_mask: int,
        operation: SelectorCancelHandle,
        step: Callable[[], ContinuousStepResult],
    ) -> None:
        with self._lock:
            self._check_open()
            self._check_fd(fd)
            selector_events = _poll_mask_to_selector_events(poll_mask)
            if selector_events & selectors.EVENT_READ:
                self._check_fd_slot_available(fd, selectors.EVENT_READ)
            if selector_events & selectors.EVENT_WRITE:
                self._check_fd_slot_available(fd, selectors.EVENT_WRITE)
            self._reserve_fd_poll_slot(fd, selector_events, operation, step=step)
            if self._try_step_continuous_operation(fd, operation, step):
                return
            self._update_selector_registration(fd)
        self._after_selector_registration_changed()

    def _try_step_continuous_operation(
        self,
        fd: int,
        operation: SelectorCancelHandle,
        step: Callable[[], ContinuousStepResult],
    ) -> bool:
        """Run one continuous step synchronously. Return True when the leg ended."""

        try:
            step_result = step()
        except (BlockingIOError, InterruptedError):
            return False
        except BaseException as exc:
            self._remove_handle(operation)
            operation._finish_with_terminal_delivery(
                _continuous_error_delivery(exc, index=operation._next_index),
            )
            return True
        if step_result.done:
            self._remove_handle(operation)
            return True
        if step_result.progressed:
            self._update_selector_registration(fd)
        return False

    def _reserve_fd_poll_slot(
        self,
        fd: int,
        selector_events: int,
        operation: _SelectorOpHandle | _DeliveryHandle,
        attempt: Callable[[], Any] | None = None,
        *,
        step: Callable[[], ContinuousStepResult] | None = None,
    ) -> None:
        slot = _FdSlot(handle=operation, attempt=attempt, step=step)
        entry = self._fd_slots.setdefault(fd, _FdEntry())
        if selector_events & selectors.EVENT_READ:
            entry.reader = slot
        if selector_events & selectors.EVENT_WRITE:
            entry.writer = slot

    def _prepare_socket_operation(
        self,
        sock: socket.socket,
        event: int,
        operation: _SelectorOpHandle,
        attempt: Callable[[], T],
    ) -> None:
        with self._lock:
            self._check_open()
            self._check_socket(sock)
            fd = sock.fileno()
            self._check_fd_slot_available(fd, event)
            if self._try_complete_operation(operation, attempt):
                return
            self._reserve_fd_slot(fd, event, operation, attempt=attempt)
            self._update_selector_registration(fd)
        self._after_selector_registration_changed()

    def _prepare_socket_continuous_operation(
        self,
        sock: socket.socket,
        event: int,
        operation: SelectorCancelHandle,
        step: Callable[[], ContinuousStepResult],
    ) -> None:
        with self._lock:
            self._check_open()
            self._check_socket(sock)
            fd = sock.fileno()
            self._check_fd_slot_available(fd, event)
            self._reserve_fd_slot(fd, event, operation, step=step)
            self._update_selector_registration(fd)
        self._after_selector_registration_changed()

    def _try_complete_operation(self, operation: _SelectorOpHandle, attempt: Callable[[], T]) -> bool:
        try:
            result = attempt()
        except (BlockingIOError, InterruptedError):
            return False
        except BaseException as exc:
            _finish_selector_oneshot(operation, exception=exc)
        else:
            _finish_selector_oneshot(operation, result=result)
        return True

    def _check_fd_slot_available(self, fd: int, event: int) -> None:
        entry = self._fd_slots.get(fd)
        if entry is None:
            return
        current = entry.reader if event == selectors.EVENT_READ else entry.writer
        if current is not None:
            raise RuntimeError("IO is already pending for this fd and direction")

    def _reserve_fd_slot(
        self,
        fd: int,
        event: int,
        operation: _SelectorOpHandle | _DeliveryHandle,
        *,
        attempt: Callable[[], Any] | None = None,
        step: Callable[[], ContinuousStepResult] | None = None,
    ) -> None:
        self._check_fd_slot_available(fd, event)
        slot = _FdSlot(handle=operation, attempt=attempt, step=step)
        entry = self._fd_slots.setdefault(fd, _FdEntry())
        if event == selectors.EVENT_READ:
            entry.reader = slot
        else:
            entry.writer = slot

    def cancel(self, handle: OpHandle, callback: _OneshotCallback) -> None:
        assert isinstance(handle, (_SelectorOpHandle, _DeliveryHandle))
        try:
            self._selector_stop_handle(handle)
        except BaseException as exc:
            callback(None, exc)
            raise
        callback(None, None)

    def cancel_nowait(self, handle: OpHandle) -> None:
        assert isinstance(handle, (_SelectorOpHandle, _DeliveryHandle))
        self._selector_stop_handle(handle)

    def stop_poll(self, handle: OpHandle, callback: _OneshotCallback) -> None:
        """Stop ``poll_many``. Selector has no POLL_REMOVE SQE: local deregister."""

        try:
            self._selector_stop_handle(handle)
        except BaseException as exc:
            callback(None, exc)
            raise
        callback(None, None)

    def _selector_stop_handle(self, handle: _SelectorOpHandle | _DeliveryHandle) -> None:
        """Deregister interest and terminalise (selector has no POLL_REMOVE SQE)."""

        if isinstance(handle, _SelectorOpHandle) and handle.done():
            return
        with self._lock:
            removed = self._remove_handle(handle)
        if removed:
            self._after_selector_registration_changed()
            self._terminalise_cancelled(handle)
        elif isinstance(handle, _SelectorOpHandle):
            self._terminalise_cancelled(handle)

    def _write_busy(self, fd: int) -> bool:
        entry = self._fd_slots.get(fd)
        return entry is not None and (entry.writer is not None or bool(entry.write_queue))

    def _enqueue_write(
        self,
        fd: int,
        run: Callable[[], None],
        handle: _SelectorOpHandle | None = None,
    ) -> None:
        entry = self._fd_slots.setdefault(fd, _FdEntry())
        entry.write_queue.append(_QueuedWrite(run=run, handle=handle))

    def _run_or_enqueue_write(
        self,
        sock: socket.socket,
        run: Callable[[], None],
        handle: _SelectorOpHandle | None = None,
    ) -> None:
        """Run ``run`` now, or after the in-flight write-side op on this fd."""

        fd = sock.fileno()
        if fd == -1:
            run()
            return
        with self._lock:
            self._check_open()
            if self._write_busy(fd):
                self._enqueue_write(fd, run, handle)
                return
        run()

    def _drain_write_queue(self, fd: int) -> None:
        """Start queued write-side ops while the writer slot is free. Holds ``_lock``."""

        entry = self._fd_slots.get(fd)
        if entry is None:
            return
        while entry.write_queue and entry.writer is None:
            item = entry.write_queue.popleft()
            if item.handle is not None and item.handle.done():
                continue
            try:
                item.run()
            except Exception as exc:
                if item.handle is not None and not item.handle.done():
                    _finish_selector_oneshot(item.handle, exception=exc)

    def _remove_handle(self, handle: _SelectorOpHandle | _DeliveryHandle) -> bool:
        for fd, entry in list(self._fd_slots.items()):
            removed = False
            writer_cleared = False
            if entry.reader is not None and entry.reader.handle is handle:
                entry.reader = None
                removed = True
            if entry.writer is not None and entry.writer.handle is handle:
                entry.writer = None
                removed = True
                writer_cleared = True
            if entry.write_queue:
                kept: deque[_QueuedWrite] = deque()
                for item in entry.write_queue:
                    if item.handle is handle:
                        removed = True
                    else:
                        kept.append(item)
                entry.write_queue = kept
            if removed:
                if writer_cleared:
                    self._drain_write_queue(fd)
                    entry = self._fd_slots.get(fd)
                    if entry is None:
                        self._update_selector_registration(fd)
                        return True
                if entry.empty():
                    del self._fd_slots[fd]
                self._update_selector_registration(fd)
                return True
        return False

    def _require_fd_slot_driver(
        self,
        fd: int,
        handle: _SelectorOpHandle | _DeliveryHandle,
        slot: _FdSlot,
        *,
        continuous: bool,
    ) -> Callable[[], Any]:
        if continuous:
            step = slot.step
            if step is None:
                self._remove_handle(handle)
                label = handle.kind if isinstance(handle, _SelectorOpHandle) else type(handle).__name__
                raise RuntimeError(f"continuous operation {label!r} missing step driver on fd {fd}")
            return step
        attempt = slot.attempt
        if attempt is None:
            self._remove_handle(handle)
            assert isinstance(handle, _SelectorOpHandle)
            raise RuntimeError(f"operation {handle.kind!r} missing attempt driver on fd {fd}")
        return attempt

    def _step_fd_operation(self, fd: int, event: int) -> bool:
        entry = self._fd_slots.get(fd)
        if entry is None:
            return False
        slot = entry.reader if event == selectors.EVENT_READ else entry.writer
        if slot is None:
            return False
        handle = slot.handle
        if isinstance(handle, _SelectorOpHandle) and handle.done():
            return False
        if slot.step is not None:
            assert isinstance(handle, SelectorCancelHandle)
            step = self._require_fd_slot_driver(fd, handle, slot, continuous=True)
            return self._step_continuous_fd_operation(fd, event, handle, step)
        assert isinstance(handle, _SelectorOpHandle)
        attempt = self._require_fd_slot_driver(fd, handle, slot, continuous=False)
        try:
            result = attempt()
        except (BlockingIOError, InterruptedError):
            self._update_selector_registration(fd)
            return False
        except BaseException as exc:
            self._remove_handle(handle)
            _finish_selector_oneshot(handle, exception=exc)
        else:
            self._remove_handle(handle)
            _finish_selector_oneshot(handle, result=result)
        return True

    def _step_continuous_fd_operation(
        self,
        fd: int,
        event: int,
        handle: SelectorCancelHandle,
        step: Callable[[], ContinuousStepResult],
    ) -> bool:
        try:
            step_result = step()
        except (BlockingIOError, InterruptedError):
            self._update_selector_registration(fd)
            return False
        except BaseException as exc:
            self._remove_handle(handle)
            handle._finish_with_terminal_delivery(
                _continuous_error_delivery(exc, index=handle._next_index),
            )
            return True
        if step_result.done:
            self._remove_handle(handle)
        else:
            self._update_selector_registration(fd)
        return step_result.progressed or step_result.done

    def _selector_mask_for_fd(self, fd: int) -> int:
        entry = self._fd_slots.get(fd)
        if entry is None:
            return 0
        mask = 0
        if entry.reader is not None:
            mask |= selectors.EVENT_READ
        if entry.writer is not None:
            mask |= selectors.EVENT_WRITE
        return mask

    def _update_selector_registration(self, fd: int) -> None:
        if self._closed:
            return
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

    def _drain_wakeup(self) -> None:
        while True:
            try:
                if not self._wakeup_reader.recv(4096):
                    return
            except BlockingIOError:
                return
            except OSError:
                return

    def _check_socket(self, sock: socket.socket) -> None:
        if sock.getblocking():
            raise ValueError("socket must be non-blocking")
        if sock.fileno() < 0:
            raise ValueError("socket is closed")

    def _check_fd(self, fd: int) -> None:
        if fd < 0:
            raise ValueError("fd is closed")


class ThreadedSelectorProactor(SelectorProactor):
    """Selector proactor that polls readiness from a worker thread."""

    def __init__(
        self,
        selector: selectors.BaseSelector | None = None,
    ) -> None:
        if selector is None:
            selector = compat.released_default_selector()
        elif not hasattr(selector, "select_released"):
            raise TypeError("ThreadedSelectorProactor requires a selector with select_released()")
        super().__init__(selector)
        self._completed_wait = EventWakeupManager()
        self._worker_started = False
        self._worker_stop = threading.Event()
        self._worker = threading.Thread(target=self._worker_main, name="tealetio-selector-proactor", daemon=True)

    def _bind_wakeup_loop(self, loop: _asyncio.AbstractEventLoop) -> None:
        self._completed_wait.bind_loop(loop)

    def close(self) -> None:
        """Stop the worker thread and close selector resources."""

        self._worker_stop.set()
        self._completed_wait.wakeup()
        self._wake_selector()
        if self._closed:
            return
        if self._worker_started and threading.current_thread() is not self._worker:
            self._worker.join()
        super().close()

    def wake_wait(self) -> None:
        """Wake a thread blocked in `wait`."""

        self._completed_wait.wakeup()

    def _after_selector_registration_changed(self) -> None:
        self._wake_selector()

    def wait(self, deadline: float | None = None) -> None:
        """Wait until completed operations are signalled."""

        self._check_open()
        self._ensure_worker_started()
        if deadline == 0:
            return

        timeout = self._timeout_until_deadline(deadline)
        if timeout == 0:
            return
        self._wait_for_completed(timeout)

    async def wait_async(self, deadline: float | None = None) -> None:
        """Wait asynchronously until completed operations are signalled."""

        self._check_open()
        loop = self._async_wait_loop
        assert loop is not None
        self._ensure_worker_started()
        if deadline == 0:
            return

        timeout = self._timeout_until_deadline(deadline)
        if timeout == 0:
            return
        if self._completed_wait.poll():
            return
        await self._completed_wait.wait_async(timeout)

    def _ensure_worker_started(self) -> None:
        with self._lock:
            if self._worker_started:
                return
            self._worker_started = True
            self._worker.start()

    def _worker_main(self) -> None:
        while not self._worker_stop.is_set():
            try:
                with self._lock:
                    self._poll(None)
            except (OSError, ValueError, RuntimeError):
                return

    def _wait_for_completed(self, timeout: float | None) -> None:
        self._completed_wait.wait(timeout=timeout)


class UringProactor(ProactorBase):
    """io_uring-backed proactor.

    Default ring is ``entries=DEFAULT_URING_SQ_ENTRIES`` (256) and
    ``cq_entries=DEFAULT_URING_CQ_ENTRIES`` (1024), enough for a 256-connection
    recv-multishot plus send burst without CQ overflow. Pass ``entries=``
    and/or ``cq_entries=`` to override; omitted ``cq_entries`` is
    ``max(1024, 2 * entries)``.

    Default mode starts Python completion service threads that call
    ``ring.serve_completions()`` and deliver via ``Ring.callback``. Sync
    ``wait()`` parks on ``ring.wait_idle()`` until workers deliver and
    ``wake_wait()`` → ``ring.break_wait()`` opens that park. Async
    ``wait_async()`` parks on an ``EventWakeupManager`` (same shape as
    ``ThreadedSelectorProactor``) — workers already own CQ reaping, so the
    asyncio loop need not service the ring.

    With ``completion_threads=0`` (see also ``SyncUringProactor``), there are no
    service threads: sync ``wait()`` blocks in ``ring.wait()`` and runs the same
    completion processor on the calling thread. Async ``wait_async()`` runs that
    same ``ring.wait`` path in a thread-pool executor so the event-loop thread
    is not blocked while still reaping CQEs. ``wake_wait()`` still calls
    ``break_wait()`` (internal NOP) so a blocked inline reaper can return.
    """

    def __init__(
        self,
        entries: int = DEFAULT_URING_SQ_ENTRIES,
        flags: int = 0,
        *,
        cq_entries: int | None = None,
        ring_factory: _UringRingFactory | None = None,
        completion_threads: int = _DEFAULT_URING_COMPLETION_THREADS,
        completion_thread_nice: int | None = _DEFAULT_URING_COMPLETION_THREAD_NICE,
    ) -> None:
        if completion_threads < 0:
            raise ValueError("completion_threads must be non-negative")
        if ring_factory is None:
            ring_factory = _default_uring_ring_factory
        super().__init__()
        if ring_factory is _default_uring_ring_factory:
            self._ring = ring_factory(entries, flags, cq_entries)
        else:
            self._ring = ring_factory(entries, flags)
        try:
            self._capabilities = uring_api.probe(entries=entries, flags=flags)
        except (OSError, RuntimeError, NotImplementedError):
            self._capabilities = {}
        self._sendmsg_zc_supported = self._capabilities.get("IORING_OP_SENDMSG_ZC", False)
        self._recv_send_flags = (
            uring_api.IORING_RECVSEND_POLL_FIRST if self._capabilities.get("IORING_RECVSEND_POLL_FIRST", False) else 0
        )
        if not self._capabilities.get("IORING_RECV_MULTISHOT", False):
            # bound fallback is a valid instance override; ty types the class method (with self)
            self.recv_many = self._recv_multishot_fallback  # ty: ignore[invalid-assignment]
        if not self._capabilities.get("IORING_ACCEPT_MULTISHOT", False):
            self.accept_many = self._accept_multishot_fallback  # ty: ignore[invalid-assignment]
        # continuous *many ops prefer kernel multishot when probed; otherwise they
        # emulate the stream by preparing another one-shot SQE after each CQE
        # (oneshot poll delivery arms the next leg under ``_multi_leg_lock``).
        self._completion_thread_nice = completion_thread_nice
        # Serialise multi-leg reverse arm vs cancel/poll_remove (brief):
        # emulated oneshot poll first/next-leg, and cancel/poll_remove when
        # sampling reverse. Stream send is one send_all SQE. Ordinary
        # single-leg prepare does not take it: reverse is armed before the
        # public method returns, and cancel only runs on returned handles
        # (issuer thread).
        # Prepare may run under the lock (SQ fill / rare SQ-full flush), so a
        # stuck SQ wait can delay cancel of other multi-leg ops — temporary;
        # prefer prepare-outside-lock only if that becomes measurable.
        # Kernel multishot paths do not take this lock for arming.
        self._multi_leg_lock = threading.Lock()
        # IORING_BUF_RING is 5.19; IORING_RECV_MULTISHOT is 6.0 and requires it.
        # Synthetic pools are only for kernels without buf rings — never for multishot.
        self._provided_buffers_supported = bool(self._capabilities.get("IORING_BUF_RING", False))
        # inline: driver thread reaps via ring.wait() (callback delivers in-process).
        # threaded: workers serve_completions() → same callback off the driver;
        # driver parks on ring.wait_idle() until wake_wait → break_wait.
        self._inline_completions = completion_threads == 0
        # threaded async wait only: workers own CQ; EventWakeupManager mirrors
        # ThreadedSelectorProactor (no ring.wait on an executor thread).
        self._completed_wait: EventWakeupManager | None = None if self._inline_completions else EventWakeupManager()
        self._ring.callback = self._deliver_uring_completion
        self._ring.nowait_error_handler = self._on_nowait_error
        # bind once: avoid a mode check on every scheduler wait() / wait_async()
        self.wait = self._wait_inline if self._inline_completions else self._wait_workers
        self.wait_async = self._wait_async_inline if self._inline_completions else self._wait_async_workers
        self._service_threads = [
            threading.Thread(target=self._service_thread_main, name=f"tealetio-uring-{index}")
            for index in range(completion_threads)
        ]
        if self._inline_completions:
            return
        try:
            for thread in self._service_threads:
                thread.start()
            self._wait_until_service_started()
        except BaseException:
            self._ring.stop_serving()
            for thread in self._service_threads:
                if thread.is_alive():
                    thread.join()
            # drop proactor ↔ ring cycles (bound methods / hooks) before close
            self._ring.callback = None
            self._ring.nowait_error_handler = None
            self._ring.exception_handler = None
            self._ring.close()
            raise

    def _on_nowait_error(self, context: dict[str, Any]) -> None:
        """Route a failed nowait CQE to the delivery exception handler.

        Successful nowait CQEs are skipped by the ring (``IOSQE_CQE_SKIP_SUCCESS``
        when ``IORING_FEAT_CQE_SKIP`` is available; otherwise dropped silently).
        Only ``res < 0`` reaches this hook.
        """

        handler = self._delivery_exception_handler
        if handler is None:
            return
        res = context.get("res", 0)
        errno_val = -int(res) if isinstance(res, int) and res < 0 else errno.EIO
        exc = OSError(errno_val, errno.errorcode.get(errno_val, "nowait operation failed"))
        handler(
            {
                "message": context.get("message", "Nowait operation failed"),
                "exception": exc,
                "proactor": self,
                "res": context.get("res"),
                "flags": context.get("flags"),
                "kind": context.get("kind"),
                "fd": context.get("fd"),
            }
        )

    def set_delivery_exception_handler(
        self,
        handler: Callable[[dict[str, Any]], object] | None,
    ) -> None:
        """Wire ring and proactor handlers for continuous delivery failures."""

        super().set_delivery_exception_handler(handler)
        self._ring.exception_handler = handler

    @property
    def ring(self) -> _UringRing:
        """Return the low-level `uring_api.Ring` object owned by this proactor."""

        return self._ring

    @property
    def capabilities(self) -> dict[str, bool]:
        """Return the io_uring capability probe for this proactor's ring parameters.

        Populated once at construction from ``uring_api.probe(entries=..., flags=...)``.
        """

        return dict(self._capabilities)

    def _prepare_seeded(self, construct, *args, sequence=0):
        """Construct, seed the first-leg index, then fill the SQE.

        Staging copies ``completion.sequence`` when the CQE is harvested
        (drain lock, no GIL). Seeding after ``prepare_*`` races with
        auto_submit workers and SQPOLL: the SQE can complete before the
        store. uring-api: seed after construct, then ``Ring.prepare``.
        """

        completion = construct(*args)
        completion.sequence = sequence
        self._ring.prepare(completion)
        return completion

    def _arm_uring(self, callback, prepare, *args, shaper=_res_cqe, extra=()):
        try:
            return prepare(*args, (shaper, callback, extra))
        except BaseException as exc:
            callback(None, exc)
            raise

    def _abandon_emulated_oneshot_leg(self, handle: _UringOneshotPollHandle):
        """Under ``_multi_leg_lock``: set reverse link abandoned, return Completion for ASYNC_CANCEL.

        Caller holds the lock. Sets ``completion`` to ``_URING_ABANDONED_LEG`` so
        next-leg arming stops until a CQE clears the sentinel. Returns the
        previous Completion for ``prepare_cancel``, or None if unarmed /
        already abandoned.

        Used for oneshot poll_many stop: ASYNC_CANCEL may lose to a success
        CQE; the sentinel is what prevents that success path from re-arming
        the next leg.
        """

        completion = handle.completion
        if completion is None or completion is _URING_ABANDONED_LEG:
            return None
        handle.completion = _URING_ABANDONED_LEG
        return completion

    def cancel(self, handle: OpHandle, callback: _OneshotCallback) -> None:
        # issuer-thread only; stop poll_many with stop_poll, not cancel
        if isinstance(handle, _UringOneshotPollHandle):
            with self._multi_leg_lock:
                abandoned = self._abandon_emulated_oneshot_leg(handle)
            if abandoned is None:
                callback(None, None)
                return
            self._terminalise_cancelled(handle)
            self._arm_uring(callback, self._ring.prepare_cancel, abandoned, shaper=_teardown_cqe)
            return

        # recv-many / accept-many / native poll-many token is the armed Completion
        if handle is None or handle is _URING_ABANDONED_LEG:
            callback(None, None)
            return
        target: Any = handle
        self._arm_uring(callback, self._ring.prepare_cancel, target, shaper=_teardown_cqe)

    def cancel_nowait(self, handle: OpHandle) -> None:
        # Post ASYNC_CANCEL when a Completion exists. Recv/accept-many / native
        # poll-many token is the Completion. Do not probe done() / reverse-idle:
        # the kernel answers -ENOENT if the target already finished. Abandoned
        # is not a Completion. Prefer ``stop_poll`` for poll_many.
        if isinstance(handle, _UringOneshotPollHandle):
            with self._multi_leg_lock:
                abandoned = self._abandon_emulated_oneshot_leg(handle)
            if abandoned is None:
                return
            self._terminalise_cancelled(handle)
            self._ring.prepare_cancel_nowait(abandoned)
            return
        if handle is None or handle is _URING_ABANDONED_LEG:
            return
        target: Any = handle
        self._ring.prepare_cancel_nowait(target)

    def stop_poll(self, handle: OpHandle, callback: _OneshotCallback) -> None:
        """Stop ``poll_many``. ``callback(None, exception)``.

        Native handle is the armed poll ``Completion``: post ``POLL_REMOVE``.
        Emulated oneshot handle: abandon reverse, emit stream-end, then
        ``ASYNC_CANCEL`` the live poll. Does not check handle kind.
        """

        if isinstance(handle, _UringOneshotPollHandle):
            with self._multi_leg_lock:
                abandoned = self._abandon_emulated_oneshot_leg(handle)
            if abandoned is None:
                callback(None, None)
                return
            self._terminalise_cancelled(handle)
            self._arm_uring(callback, self._ring.prepare_cancel, abandoned, shaper=_teardown_cqe)
            return
        self._arm_uring(callback, self._ring.prepare_poll_remove, handle, shaper=_teardown_cqe)

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int) -> RecvBufferPool:
        """Create a provided-buffer group, or synthetic pool without ``IORING_BUF_RING``.

        Gated by probe ``IORING_BUF_RING`` (5.19). That pre-dates
        ``IORING_RECV_MULTISHOT`` (6.0), so multishot never uses synthetic pools.
        """

        if self._provided_buffers_supported:
            return self._ring.create_buf_group(buffer_size, buffer_count)
        # no buf rings => no multishot either; one-shot fallback may still use synthetic
        assert not self._capabilities.get("IORING_RECV_MULTISHOT", False)
        return SyntheticRecvBufferPool(buffer_size, buffer_count)

    def create_buf_group(self, buffer_size: int, buffer_count: int) -> RecvBufferPool:
        return self.create_recv_buffer_pool(buffer_size, buffer_count)

    def _default_shared_recv_buffer_pool_sizes(self) -> tuple[int, int]:
        return _DEFAULT_URING_RECV_MANY_BUFFER_SIZE, _DEFAULT_URING_RECV_MANY_BUFFER_COUNT

    def _service_thread_main(self) -> None:
        self._apply_completion_thread_nice()
        self._ring.serve_completions()

    def _apply_completion_thread_nice(self) -> None:
        nice = self._completion_thread_nice
        if nice is None or not hasattr(os, "setpriority"):
            return
        try:
            os.setpriority(os.PRIO_PROCESS, 0, nice)
        except (AttributeError, OSError, PermissionError, ValueError):
            return

    def _wait_until_service_started(self) -> None:
        deadline = time.monotonic() + 1.0
        while (
            not self._ring.running
            and any(thread.is_alive() for thread in self._service_threads)
            and time.monotonic() < deadline
        ):
            time.sleep(0.001)
        if not self._ring.running:
            raise RuntimeError("uring completion service failed to start")

    def has_pending_operations(self) -> bool:
        """Return True if the ring still has in-flight waitable Completions.

        Not operation-lifetime: a oneshot ``poll_many`` can read False
        between legs (CQE packaged before the next prepare). ``run()`` /
        ``arun()`` use this as their IO idle signal (best-effort).
        """

        return self._ring.pending_count() > 0

    def close(self) -> None:
        """Close the owned `io_uring` ring."""

        if self._closed:
            return
        self._closed = True
        self._clear_shared_recv_buffer_pool()
        if self._service_threads:
            self._ring.stop_serving()
            for thread in self._service_threads:
                thread.join()
        else:
            # interrupt a driver blocked in ring.wait() before close
            try:
                self._ring.break_wait()
            except (OSError, RuntimeError, ValueError):
                pass
        self.wake_wait()
        # drop proactor ↔ ring cycles (bound methods / hooks) before close
        self._ring.callback = None
        self._ring.nowait_error_handler = None
        self._ring.exception_handler = None
        self._ring.close()
        # drop scheduler.time / call_exception_handler bound methods
        self._detach_owner_hooks()

    def _bind_wakeup_loop(self, loop: _asyncio.AbstractEventLoop) -> None:
        completed = self._completed_wait
        if completed is not None:
            completed.bind_loop(loop)

    def wake_wait(self) -> None:
        """Unblock sync ``wait`` and threaded async ``wait_async``.

        Always calls ``ring.break_wait()``: opens ``wait_idle`` (threaded sync)
        or submits an internal NOP when completion service is idle (inline
        ``ring.wait()`` on an empty CQ). With service workers the NOP is
        skipped. Threaded mode also signals ``EventWakeupManager`` for
        ``wait_async`` parkers that do not sit on the ring.
        """

        try:
            self._ring.break_wait()
        except (OSError, RuntimeError, ValueError):
            pass
        completed = self._completed_wait
        if completed is not None:
            completed.wakeup()

    def _wait_inline(self, deadline: float | None = None) -> None:
        """Block in ``ring.wait``; delivery runs via the registered ring callback.

        ``ring.wait`` flushes prepared SQEs itself when this thread may submit —
        no separate ``ring.submit()`` before wait.

        Wait after ``close()`` is undefined (misuse), not a recovery path.
        Submit methods likewise skip ``_check_open()``; the closed ring fails.
        """

        # deadline==0: one non-blocking harvest (selector wait(0) analogue)
        # callback mode: wait delivers non-empty batches and returns None
        self._ring.wait(self._timeout_until_deadline(deadline))

    def _wait_workers(self, deadline: float | None = None) -> None:
        """Park on ``ring.wait_idle`` while completion workers own CQ reaping.

        The ring idle park allows many ``wake_wait`` / ``break_wait`` signallers
        but only one concurrent waiter — the proactor driver. Do not park a
        second host (or dual ``wait`` / ``wait_async`` threads) on the same ring.

        Always flush prepared SQEs first (including ``deadline == 0`` / expired
        timeout) so workers blocked in ``wait_cqe`` see new work, cancels, and
        poll_removes. Then park only when there is time left.

        Wait after ``close()`` is undefined (misuse); same as ``_wait_inline``.
        """

        # issuer flush even on non-blocking poll (lazy prepare)
        self._ring.submit()
        if deadline == 0:
            return

        timeout = self._timeout_until_deadline(deadline)
        if timeout == 0:
            return
        self._ring.wait_idle(timeout)

    async def _wait_async_inline(self, deadline: float | None = None) -> None:
        """Inline mode: reap CQEs via ``ring.wait`` on an executor thread.

        There is no completion service thread, so async hosts must still run
        the inline ``wait`` binding (not a pure event park).

        When wait runs on an executor under SINGLE_ISSUER, that thread cannot
        flush — publish on the issuer here before hopping. Same-thread
        ``wait(0)`` relies on ``ring.wait``'s own flush.
        """

        if deadline == 0:
            self.wait(0)
            return
        timeout = self._timeout_until_deadline(deadline)
        if timeout == 0:
            return
        # issuer flush before non-issuer executor may call ring.wait
        self._ring.submit()
        loop = self._async_wait_loop
        assert loop is not None
        await loop.run_in_executor(None, self.wait, deadline)

    async def _wait_async_workers(self, deadline: float | None = None) -> None:
        """Threaded mode: park on ``EventWakeupManager`` only.

        Workers own CQ reaping; the asyncio loop only needs a cross-thread
        wakeup when ``wake_wait()`` runs (via ``call_soon_threadsafe`` →
        ``break_wait`` path, or direct ``wake_wait``).

        Always flush prepared SQEs first (same as ``_wait_workers``), including
        zero-timeout polls, then park only when there is time left.
        """

        self._ring.submit()
        if deadline == 0:
            return
        timeout = self._timeout_until_deadline(deadline)
        if timeout == 0:
            return
        completed = self._completed_wait
        assert completed is not None
        await completed.wait_async(timeout)

    def recv(
        self,
        sock: socket.socket,
        n: int,
        callback: _OneshotRecvCallback,
    ) -> OpHandle:
        """Arm a oneshot recv. ``callback(result, exception)``.

        Result is ``RecvResult``: payload plus ``IoMore`` for the next oneshot
        recv (uring ``SOCK_NONEMPTY``). Continuous ``recv_many`` does not
        surface this hint. Returns the armed ``Completion``, or ``None`` when
        ``callback`` already ran (``n == 0``).
        """

        if n == 0:
            callback(RecvResult(b""), None)
            return None
        data = memoryview(bytearray(n))
        try:
            return self._ring.prepare_recv(
                sock.fileno(),
                data,
                self._recv_send_flags,
                (_recv_cqe, callback, (data,)),
            )
        except BaseException as exc:
            callback(None, exc)
            raise

    def recv_into(self, sock: socket.socket, buf: Any, callback: _OneshotCallback) -> OpHandle:
        """Arm a oneshot recv-into. ``callback(nbytes, exception)``."""

        return self._arm_uring(
            callback,
            self._ring.prepare_recv,
            sock.fileno(),
            buf,
            self._recv_send_flags,
        )

    def recvfrom(self, sock: socket.socket, bufsize: int, callback: _OneshotCallback) -> OpHandle:
        """Arm a oneshot datagram recv. ``callback((data, address), exception)``."""

        data = memoryview(bytearray(bufsize))
        return self._arm_uring(
            callback,
            self._ring.prepare_recvmsg,
            sock.fileno(),
            data,
            self._recv_send_flags,
            shaper=_recvfrom_cqe,
            extra=(data,),
        )

    def recvfrom_into(self, sock: socket.socket, buf: Any, callback: _OneshotCallback, nbytes: int = 0) -> OpHandle:
        """Arm a oneshot datagram recv-into. ``callback((nbytes, address), exception)``."""

        data = memoryview(buf)
        if nbytes < 0:
            raise ValueError("negative buffersize in recvfrom_into")
        if nbytes > len(data):
            raise ValueError("nbytes is greater than the length of the buffer")
        if nbytes:
            data = data[:nbytes]
        return self._arm_uring(
            callback,
            self._ring.prepare_recvmsg,
            sock.fileno(),
            data,
            self._recv_send_flags,
            shaper=_recvfrom_into_cqe,
        )

    def send(
        self,
        sock: socket.socket,
        data: Any,
        callback: _OneshotCallback,
        progress: _ProgressCallback | None = None,
        *,
        expect: IoExpect = IoExpect.READY,
    ) -> OpHandle:
        """Arm a stream send that drains ``data``. ``callback(None, exception)``.

        Uses ``uring-api`` ``send_all`` (copying send; C re-arms partial CQEs).
        ``expect`` applies to the first SQE only. ``READY`` omits
        ``POLL_FIRST`` (try send now). ``BLOCK`` sets it when probed.
        Later legs always use ``POLL_FIRST`` in C when probed.
        """

        if not data:
            callback(None, None)
            return None
        flags = self._send_sqe_flags(expect=expect)
        completion = self._ring.construct_send_all(sock.fileno(), data, flags, (_send_all_cqe, callback, (progress,)))
        self._ring.prepare(completion)
        return completion

    def send_nowait(
        self,
        sock: socket.socket,
        data: Any,
        callback: _OneshotCallback | None = None,
        *,
        expect: IoExpect = IoExpect.READY,
    ) -> None:
        """Fire-and-forget ``send_all``. ``callback(None, exc)`` on failure only.

        Success is silent. With ``callback``, ``skip_success`` delivers errors
        through the CQE shaper. With no callback, ``skip_all`` (errors to
        ``nowait_error_handler``). Follow with ``shutdown_nowait`` /
        ``close_socket_nowait`` on the same fd (uring conflict FIFO; selector
        write FIFO).
        """

        if not data:
            return
        flags = self._send_sqe_flags(expect=expect)
        user_data = None if callback is None else (_send_all_cqe, callback, (None,))
        completion = self._ring.construct_send_all(sock.fileno(), data, flags, user_data)
        completion.skip_success = True
        if callback is None:
            completion.skip_all = True
        self._ring.prepare(completion)

    def send_close_nowait(
        self,
        sock: socket.socket,
        data: Any,
        *,
        expect: IoExpect = IoExpect.READY,
    ) -> None:
        """Drain ``data`` then nowait-close ``sock``. No waitable.

        Nowait ``send_all`` and nowait close are prepared together; close parks
        on the send-all conflict FIFO. Later errors go to the delivery
        exception handler. Do not submit another send on ``sock`` until this
        drain has finished (the socket is closing anyway).
        """

        if not data:
            self.close_socket_nowait(sock)
            return
        flags = self._send_sqe_flags(expect=expect)
        fd = sock.detach()
        if fd == -1:
            return
        # detach first so Python cannot close an in-flight fd. construct/prepare
        # failures after this are usage or internal errors; we do not recover the fd.
        completion = self._ring.construct_send_all(fd, data, flags)
        completion.skip_all = True
        close = self._ring.construct_close_nowait(fd)
        self._ring.prepare([completion, close])

    def sendto(self, sock: socket.socket, data: Any, address: Any, callback: _OneshotCallback) -> OpHandle:
        """Arm a datagram send. ``callback(nbytes, exception)``."""

        prepare = (
            self._ring.prepare_sendmsg_zc
            if self._sendmsg_zc_supported and sock.family != socket.AF_UNIX
            else self._ring.prepare_sendto
        )
        return self._arm_uring(
            callback,
            prepare,
            sock.fileno(),
            data,
            address,
            self._recv_send_flags,
        )

    def accept(self, sock: socket.socket, callback: _OneshotCallback) -> OpHandle:
        """Arm a oneshot accept. ``callback(conn, exception)``."""

        return self._arm_uring(
            callback,
            self._ring.prepare_accept,
            sock.fileno(),
            _DEFAULT_ACCEPT_FLAGS,
            shaper=_socket_cqe,
        )

    def shutdown(self, sock: socket.socket, how: int, callback: _OneshotCallback) -> OpHandle:
        """Submit ``socket.shutdown(how)`` for ``sock``."""

        if sock.fileno() == -1:
            callback(None, OSError(errno.EBADF, "Bad file descriptor"))
            return None
        return self._arm_uring(callback, self._ring.prepare_shutdown, sock.fileno(), how, shaper=_void_result_cqe)

    def close_socket(self, sock: socket.socket, callback: _OneshotCallback) -> OpHandle:
        """Submit socket close and release the Python wrapper fd."""

        fd = sock.detach()
        if fd == -1:
            callback(None, None)
            return None
        return self._arm_uring(callback, self._ring.prepare_close, fd, shaper=_void_result_cqe)

    def close_socket_nowait(self, sock: socket.socket) -> None:
        """Detach ``sock`` and prepare a nowait close. Returns ``None``.

        The ring stamps ``IOSQE_CQE_SKIP_SUCCESS`` when the kernel supports
        it; successful closes produce no CQE. Failures go to
        ``ring.nowait_error_handler``.
        """

        fd = sock.detach()
        if fd == -1:
            return
        self._ring.prepare_close_nowait(fd)

    def shutdown_nowait(self, sock: socket.socket, how: int) -> None:
        """Nowait ring ``shutdown``. Parks on the same-fd send-all conflict FIFO."""

        fd = sock.fileno()
        if fd == -1:
            return
        self._ring.prepare_shutdown_nowait(fd, how)

    def close_fd(self, fd: int, callback: _OneshotCallback) -> OpHandle:
        """Submit raw fd close for caller-owned descriptors (for example from ``openat``)."""

        if fd < 0:
            callback(None, None)
            return None
        return self._arm_uring(callback, self._ring.prepare_close, fd, shaper=_void_result_cqe)

    def accept_many(
        self,
        sock: socket.socket,
        callback: _AcceptManyCallback,
        *,
        base_sequence: int = 0,
    ) -> OpHandle:
        """Accept connections and deliver each via the result callback.

        Returns an opaque ``OpHandle`` (armed ``Completion``), not a waitable.
        Uses multishot accept when the runtime probe accepts it; otherwise
        prepares one oneshot accept and emits ``more=False`` so callers re-arm.
        `callback` may run on any uring completion service thread.

        Each accepted connection is delivered as the accepted ``socket``. Call
        ``socket.getpeername()`` when the peer address is needed. Use
        ``ProactorIOManager.accept_many`` for accept-time reads and a waitable
        over stream-end.

        ``base_sequence`` seeds the first-leg index on the constructed handle
        before the SQE is filled, so continuous arms can continue after eager
        accepts.
        """

        # POLL_FIRST + accept_multishot is unsupported. Prepare-fail raises
        # before a handle is published. user_data is (handler, user_cb, extra);
        # the armed Completion is the OpHandle.
        return self._prepare_seeded(
            self._ring.construct_accept_multishot,
            sock.fileno(),
            _DEFAULT_ACCEPT_FLAGS,
            (_accept_many_cqe, callback, ()),
            sequence=base_sequence,
        )

    def _accept_multishot_fallback(
        self,
        sock: socket.socket,
        callback: _AcceptManyCallback,
        *,
        base_sequence: int = 0,
    ) -> OpHandle:
        # emulated accept_many: one accept, emit more=False; callers re-arm
        # (for example StreamServer).
        cb = self._guard_delivery_callback(callback)
        return self._prepare_seeded(
            self._ring.construct_accept,
            sock.fileno(),
            _DEFAULT_ACCEPT_FLAGS,
            (_accept_many_oneshot_cqe, cb, ()),
            sequence=base_sequence,
        )

    def create_socket(
        self,
        family: int,
        type: int,
        callback: _OneshotCallback,
        proto: int = 0,
        *,
        flags: int = 0,
    ) -> OpHandle:
        """Create a scheduler-contract socket."""

        if self._capabilities.get("IORING_OP_SOCKET", False):
            socket_type = type | flags | _DEFAULT_ACCEPT_FLAGS
            return self._arm_uring(
                callback,
                self._ring.prepare_socket,
                family,
                socket_type,
                proto,
                0,
                shaper=_socket_cqe,
            )
        _call_sync_callback(callback, lambda: _sync_create_scheduler_socket(family, type, proto))
        return None

    def connect(
        self,
        sock: socket.socket,
        address: Any,
        callback: _OneshotCallback,
    ) -> OpHandle:
        """Arm a socket connect. ``callback(None, exception)``."""

        if sock.family == socket.AF_UNIX:
            return self._sync_unix_connect(sock, address, callback)

        return self._arm_uring(callback, self._ring.prepare_connect, sock.fileno(), address, shaper=_void_result_cqe)

    def openat(
        self,
        path: str,
        flags: int,
        callback: _OneshotCallback,
        mode: int = 0,
        *,
        dfd: int = _DEFAULT_OPENAT_DFD,
    ) -> OpHandle:
        """Submit an io_uring openat operation and return the opened fd on success."""

        return self._arm_uring(callback, self._ring.prepare_openat, dfd, path, flags, mode)

    def read(self, fd: int, n: int, offset: int, callback: _OneshotCallback) -> OpHandle:
        """Submit a positioned file read that completes with the bytes read."""

        data = memoryview(bytearray(n))
        return self._arm_uring(callback, self._ring.prepare_read, fd, data, offset, shaper=_bytes_cqe, extra=(data,))

    def read_into(self, fd: int, buf: Any, offset: int, callback: _OneshotCallback) -> OpHandle:
        """Submit a positioned file read into a caller-provided buffer."""

        return self._arm_uring(callback, self._ring.prepare_read, fd, buf, offset)

    def write(self, fd: int, data: Any, offset: int, callback: _OneshotCallback) -> OpHandle:
        """Submit a positioned file write and return the byte count written."""

        return self._arm_uring(callback, self._ring.prepare_write, fd, data, offset)

    def stat(self, path: str = "", *, fd: int = -1, callback: _OneshotCallback) -> OpHandle:
        """Return file metadata via io_uring statx when probed, else blocking ``os.stat``."""

        if fd < 0 and not path:
            raise ValueError("stat() requires fd >= 0 or a non-empty path")
        if not self._capabilities.get("IORING_OP_STATX", False) or not hasattr(self._ring, "prepare_statx"):
            return super().stat(path, fd=fd, callback=callback)

        buf = bytearray(uring_api.STATX_BUFFER_SIZE)
        if fd >= 0:
            dfd = fd
            stat_path = ""
            stat_flags = uring_api.AT_EMPTY_PATH
        else:
            dfd = uring_api.AT_FDCWD
            stat_path = path
            stat_flags = 0
        return self._arm_uring(
            callback,
            self._ring.prepare_statx,
            dfd,
            stat_path,
            stat_flags,
            uring_api.STATX_BASIC_STATS,
            buf,
            shaper=_stat_cqe,
            extra=(memoryview(buf),),
        )

    def stat_fdsize(self, fd: int, callback: _OneshotCallback) -> OpHandle:
        """Return file byte length via io_uring statx_fdsize when probed, else blocking ``os.fstat``.

        When statx_fdsize completes without a parsed size, the completion handler
        falls back to blocking ``os.fstat`` on the uring completion thread. That
        path should be rare; the blocking prepare-time fallback via ``super()`` is
        used when statx is unavailable.
        """

        if fd < 0:
            raise ValueError("stat_fdsize() requires fd >= 0")
        if not self._capabilities.get("IORING_OP_STATX", False) or not hasattr(self._ring, "prepare_statx_fdsize"):
            return super().stat_fdsize(fd, callback)
        return self._arm_uring(callback, self._ring.prepare_statx_fdsize, fd, shaper=_stat_fdsize_cqe, extra=(fd,))

    # instance may rebind recv_many to the bound fallback; ty then unions that
    # with this def and rejects the override (extra self on the unbound form)
    def recv_many(  # ty: ignore[invalid-method-override]
        self,
        sock: socket.socket,
        callback: _RecvManyCallback,
        *,
        buf_group: RecvBufferPool,
        base_sequence: int = 0,
    ) -> OpHandle:
        """Start a cancellable receive stream that completes on EOF.

        Returns an opaque ``OpHandle`` (armed ``Completion`` when native
        recv-multishot is used), not a waitable. Chunks go to ``callback``.

        `callback` may run on any uring completion service thread.

        When multishot provided-buffer receive is available, each callback
        receives ``MultishotDelivery`` with stream ``index`` (``completion.sequence``,
        seeded by ``base_sequence`` before the SQE is filled), leased ``memoryview`` data in
        ``value``, optional ``exception``, and ``more``. Callback delivery may
        arrive out of order across completion threads; consumers that need
        stream order must reorder by index themselves. Chunk sizes come from the
        operation's ``BufGroup`` pool. Holding live views can pin provided
        buffers and stall further receives. ``errno.ENOBUFS`` is delivered
        through ``exception`` at the terminal ``index`` and completes the current
        leg. ``more=False`` with non-empty data means the leg stopped before EOF;
        consumers drop held views and start a fresh ``recv_many()`` with
        ``base_sequence`` set to ``index + 1``.

        When multishot receive is unavailable but ``buf_group`` is a real
        provided-buffer pool (``IORING_BUF_RING`` without multishot: 5.19–5.x),
        the proactor prepares one ``recv_buf`` and delivers a leased
        ``BufView`` per leg. With a ``SyntheticRecvBufferPool`` (no buf rings;
        also no multishot), it falls back to oneshot ``recv`` and leases
        copied chunks against the synthetic pool before delivery.

        ``buf_group`` must be a provided-buffer pool from
        ``create_recv_buffer_pool()`` or ``shared_recv_buffer_pool()``.
        """

        # POLL_FIRST + recv_multishot is unsupported. Prepare-fail raises
        # before a handle is published. user_data is (handler, user_cb, extra);
        # the armed Completion is the OpHandle.
        return self._prepare_seeded(
            self._ring.construct_recv_multishot,
            sock.fileno(),
            buf_group,
            0,
            (_recv_many_cqe, callback, ()),
            sequence=base_sequence,
        )

    def _recv_multishot_fallback(
        self,
        sock: socket.socket,
        callback: _RecvManyCallback,
        *,
        buf_group: RecvBufferPool,
        base_sequence: int = 0,
    ) -> OpHandle:
        cb = self._guard_delivery_callback(callback)
        if _is_synthetic_recv_buffer_pool(buf_group):
            if _synthetic_recv_pool_is_full(buf_group):
                _emit_recv_many(cb, _recv_many_enobufs_delivery(index=base_sequence))
                return None
            buffer = bytearray(_DEFAULT_SELECTOR_RECV_MANY_CHUNK_SIZE)
            return self._prepare_seeded(
                self._ring.construct_recv,
                sock.fileno(),
                buffer,
                self._recv_send_flags,
                (_recv_oneshot_cqe, cb, (buffer, buf_group)),
                sequence=base_sequence,
            )
        return self._prepare_seeded(
            self._ring.construct_recv_buf,
            sock.fileno(),
            buf_group,
            self._recv_send_flags,
            (_recv_many_cqe, cb, ()),
            sequence=base_sequence,
        )

    def poll(self, fd: int, mask: int, callback: _OneshotCallback) -> OpHandle:
        """Submit a one-shot io_uring poll operation."""

        # mask and fd go straight to io_uring; bad values show up as CQE errors.
        # selector validates masks (select() fd lists) and fd>=0; no per-fd exclusivity.
        return self._arm_uring(callback, self._ring.prepare_poll, fd, mask)

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: _PollManyCallback,
    ) -> OpHandle:
        """Start a continuous io_uring poll operation.

        Returns an opaque handle for ``stop_poll``, not a waitable. Uses
        multishot poll when the runtime probe accepts it (handle is the armed
        ``Completion``); otherwise falls back to preparing another one-shot
        ``prepare_poll()`` after each readiness CQE (handle is a reverse-link
        holder; first-leg and next-leg prepare under ``_multi_leg_lock``).
        `callback` may run on any uring completion service thread.
        """

        # mask handling matches poll(); no pre-validation on the uring path.
        cb = self._guard_delivery_callback(callback)
        if self._capabilities.get("IORING_POLL_MULTISHOT", False):
            return self._prepare_seeded(
                self._ring.construct_poll_multishot,
                fd,
                mask,
                (_poll_many_cqe, cb, ()),
            )

        holder = _UringOneshotPollHandle(cb, fd, mask)
        extra = (holder, self)
        user_data = (_poll_many_oneshot_cqe, cb, extra)
        prepare_error: BaseException | None = None
        with self._multi_leg_lock:
            try:
                holder.completion = self._ring.prepare_poll(fd, mask, user_data)
            except BaseException as exc:
                prepare_error = exc
        if prepare_error is not None:
            _emit_many(cb, _continuous_error_delivery(prepare_error, index=0))
            raise prepare_error
        return holder

    def _deliver_uring_completion(self, completion: _UringCompletion) -> None:
        # take_user_data() breaks op↔completion cycles (multishot shell/terminal
        # contract: uring-api docs). Tuple user_data is ``(handler, user_cb, extra)``.
        op = completion.take_user_data()
        delivered = False
        if op is not None:
            assert type(op) is tuple
            _run_cqe_handler(completion, op)
            delivered = True
        # threaded mode: workers deliver off the driver; open wait_idle via break_wait.
        # inline mode: the driver is already inside wait() processing this CQE.
        if not self._inline_completions and not delivered and not self.has_pending_operations():
            self.wake_wait()

    def _send_sqe_flags(self, *, expect: IoExpect) -> int:
        """POLL_FIRST on the first send_all SQE only when the caller expects to block."""

        if not self._recv_send_flags:
            return 0
        if expect is IoExpect.READY:
            return 0
        return self._recv_send_flags

    def _raise_unsupported(self, operation: str) -> NoReturn:
        raise NotImplementedError(f"UringProactor does not yet support {operation} operations")


def _default_proactor_factory() -> Proactor:
    if uring_api.is_available():
        return UringProactor()  # ty: ignore[invalid-return-type]
    return SelectorProactor()


class SyncUringProactor(UringProactor):
    """Single-threaded ``UringProactor``: ``wait()`` is ``ring.wait`` + deliver.

    Intended for benchmarks and debugging against the threaded default. Same
    prepare path and callback model; no completion service threads and no
    cross-thread delivery hop on the sync driver.
    """

    def __init__(
        self,
        entries: int = DEFAULT_URING_SQ_ENTRIES,
        flags: int = 0,
        *,
        cq_entries: int | None = None,
        ring_factory: _UringRingFactory | None = None,
        completion_thread_nice: int | None = _DEFAULT_URING_COMPLETION_THREAD_NICE,
    ) -> None:
        super().__init__(
            entries,
            flags,
            cq_entries=cq_entries,
            ring_factory=ring_factory,
            completion_threads=0,
            completion_thread_nice=completion_thread_nice,
        )


class ProactorScheduler(BaseScheduler):
    """Shared proactor-backed cooperative scheduling mechanics."""

    def __init__(
        self,
        proactor_factory: ProactorFactory | None = None,
        *,
        runnable_queue_factory: RunnableQueueFactory | None = None,
    ) -> None:
        super().__init__(runnable_queue_factory=runnable_queue_factory)
        factory = proactor_factory if proactor_factory is not None else _default_proactor_factory
        self._proactor: Proactor | None = factory()
        self._proactor.set_clock(self.time)
        self._proactor.set_delivery_exception_handler(self.call_exception_handler)
        self._io: ProactorIOManager | None = ProactorIOManager(self, self._proactor)

    @property
    def io(self) -> ProactorIOManager:
        """Return the blocking IO facade for this scheduler."""

        io = self._io
        assert io is not None
        return io

    @property
    def proactor(self) -> Proactor:
        """Return the proactor backend owned by this scheduler."""

        proactor = self._proactor
        assert proactor is not None
        return proactor

    def close(self) -> None:
        """Close proactor and scheduler-owned resources.

        Exceptional ``IOWaiter.wait()`` exits and ``Proactor.cancel()`` submit
        async ring-cancel / ``poll_remove`` teardown legs without awaiting them.
        ``UringProactor.has_pending_operations()`` may stay true briefly until
        those CQEs complete. Pump ``proactor.wait()`` or ``wait()`` on returned
        teardown operations when strict ring quiescence is required before
        ``UringProactor.close()``.
        """

        io = self._io
        proactor = self._proactor
        # drop ownership edges first so a second close() is a no-op edge-wise
        # and so the closed cluster is not one external root away from free
        self._io = None
        self._proactor = None
        if io is not None:
            io.close()
        if proactor is not None:
            proactor.close()
        BaseScheduler.close(self)

    # -- Driver wakeup -------------------------------------------------
    # Internals use _proactor and assert: post-close is a contract bug, not API misuse.
    # RuntimeError("… closed") lives on public .proactor / .io only.

    def _break_wait(self) -> None:
        proactor = self._proactor
        assert proactor is not None
        proactor.wake_wait()

    def _poll_io(self) -> None:
        proactor = self._proactor
        assert proactor is not None
        proactor.wait(0)

    def _wait_thread(self) -> None:
        deadline = self._next_timer_deadline()
        proactor = self._proactor
        assert proactor is not None
        proactor.wait(deadline)

    def _has_pending_driver_work(self) -> bool:
        proactor = self._proactor
        assert proactor is not None
        return proactor.has_pending_operations() or BaseScheduler._has_pending_driver_work(self)


class SyncProactorScheduler(SyncDrivingMixin, ProactorScheduler, SyncSchedulerDrivingAPI):
    """Synchronous scheduler whose IO wait point is a proactor backend."""

    async def _driver_wait(self) -> None:
        self._wait_thread()


class AsyncProactorScheduler(AsyncDrivingMixin, ProactorScheduler, AsyncSchedulerDrivingAPI):
    """Async-hosted scheduler whose IO wait point is a proactor backend."""

    def __init__(
        self,
        proactor_factory: ProactorFactory | None = None,
        *,
        runnable_queue_factory: RunnableQueueFactory | None = None,
    ) -> None:
        super().__init__(proactor_factory=proactor_factory, runnable_queue_factory=runnable_queue_factory)
        self._wakeup_loop: _asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: _asyncio.AbstractEventLoop) -> None:
        """Bind this scheduler to an asyncio event loop clock and completion wakeups."""

        if self._wakeup_loop is not None and self._wakeup_loop is not loop:
            raise RuntimeError("AsyncProactorScheduler is already bound to a different event loop")
        self._wakeup_loop = loop
        self._time = loop.time
        proactor = self._proactor
        assert proactor is not None
        proactor.bind_loop(loop)

    def _lazy_bind_running_loop(self) -> None:
        if self._wakeup_loop is None:
            self.bind_loop(_asyncio.get_running_loop())

    def _before_arun(self) -> None:
        self._lazy_bind_running_loop()

    def close(self) -> None:
        """Close proactor and scheduler-owned resources."""

        super().close()

    async def _driver_wait(self) -> None:
        self._lazy_bind_running_loop()
        deadline = self._next_timer_deadline()
        proactor = self._proactor
        assert proactor is not None
        await proactor.wait_async(deadline)
