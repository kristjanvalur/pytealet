from __future__ import annotations

import errno
import gc
import os
import select
import socket
import threading
from typing import Any

import pytest

from tealetio import set_scheduler
import tealetio.io_manager as io_manager_mod
import tealetio.io_waiter as io_waiter_module
from tealetio.io_manager import (
    DEFAULT_MAX_FREE_RECV_BUFFER_POOLS,
    ProactorIOManager,
    ServerIO,
    _finish_or_close_socket,
)
from tealetio.io_waiter import (
    IOWaiter,
    IOWaiterSync,
    IOWaitGroup,
    IOWaitGroupChild,
    InvalidStateError,
)
from tealetio.delivery import (
    MultishotDelivery,
    OpHandle,
    SelectorCancelHandle,
    io_cancellation_error,
    is_io_cancellation,
)
from tealetio.types import IoExpect, RecvResult
from tealetio.proactor import SyncProactorScheduler, UringProactor
from io_fakes import StubProactor, StubScheduler
from uring_fakes import (
    SCHEDULER_INTEGRATION_FACTORIES,
    _DeferredCreateSocketUringRing,
    _ensure_deferred_connect_completed,
    _patch_uring_capabilities,
    _wait_for_uring,
)


def _fd_closed(fd: int) -> bool:
    try:
        os.fstat(fd)
    except OSError:
        return True
    return False


def _manager(proactor: _MockProactor) -> ProactorIOManager:
    return ProactorIOManager(StubScheduler(), proactor)  # type: ignore[arg-type]


def _pending_waiter(io: ProactorIOManager) -> IOWaiter:
    return IOWaiter(io)


def _eager_accept_conn() -> socket.socket:
    """Accepted socket whose peer is already closed (preread recv sees EOF)."""

    conn, peer = socket.socketpair()
    peer.close()
    conn.setblocking(False)
    return conn


def _eager_accept_conn_open_peer() -> tuple[socket.socket, socket.socket]:
    """Accepted socket with peer kept open so oneshot recv does not see EOF."""

    conn, peer = socket.socketpair()
    conn.setblocking(False)
    peer.setblocking(False)
    return conn, peer


def _nonblocking_listener(*, backlog: int = 8) -> socket.socket:
    """Idle non-blocking listen socket (no pending connections)."""

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(backlog)
    listener.setblocking(False)
    return listener


def _eager_accept_arm(
    sock: socket.socket,
    callback: Any,
    conn: socket.socket | None = None,
    *,
    more: bool = True,
) -> SelectorCancelHandle:
    handle = SelectorCancelHandle(callback)
    handle._emit_result(conn if conn is not None else _eager_accept_conn(), more=more)
    return handle


class _PendingOneshot:
    """Test double for a not-yet-complete oneshot submit."""

    __slots__ = ("callback", "done", "exception")

    def __init__(self, callback=None) -> None:
        self.callback = callback
        self.done = False
        self.exception = None

    def complete(self, result=None, exception=None) -> None:
        if self.done:
            return
        self.done = True
        self.exception = exception
        if self.callback is None:
            return
        if exception is not None:
            self.callback(None, exception)
        else:
            self.callback(result, None)


class _MockProactor:
    def __init__(self, *, recv_result: bytes = b"mock") -> None:
        self._recv_result = recv_result
        self.cancel_nowait_calls: list[object] = []
        self.recv_calls: list[tuple[socket.socket, int]] = []
        self.recv_many_calls: list[socket.socket] = []
        self.poll_calls: list[tuple[int, int]] = []
        self.send_calls: list[tuple[socket.socket, Any]] = []
        self.send_expects: list[object] = []
        self.send_nowait_calls: list[tuple[socket.socket, Any, object]] = []
        self.send_close_nowait_calls: list[tuple[socket.socket, Any, object]] = []
        self.shutdown_nowait_calls: list[tuple[socket.socket, int]] = []
        self.create_socket_calls: list[tuple[Any, ...]] = []
        self.last_create_socket: socket.socket | None = None
        self.connect_calls: list[tuple[socket.socket, Any]] = []
        self.last_connect_socket: socket.socket | None = None
        self.openat_calls: list[tuple[str, int, int]] = []
        self.close_fd_calls: list[int] = []
        # keep accept-time peer ends alive so mock preread does not see EOF
        self._held_peers: list[socket.socket] = []

    def recv(self, sock: socket.socket, n: int, callback) -> object:
        self.recv_calls.append((sock, n))
        callback(RecvResult(self._recv_result), None)
        return None

    def recv_many(self, sock, callback, *, buf_group, base_sequence=0):
        del buf_group
        self.recv_many_calls.append(sock)
        self.last_recv_many_base_sequence = base_sequence
        return SelectorCancelHandle(callback, base_sequence=base_sequence)

    def _terminalise(self, operation: object) -> None:
        if isinstance(operation, SelectorCancelHandle):
            operation._finish_with_terminal_delivery(
                MultishotDelivery(
                    index=operation._next_index,
                    exception=io_cancellation_error(),
                    more=False,
                )
            )
            return
        if isinstance(operation, _PendingOneshot):
            operation.complete(exception=io_cancellation_error())

    def cancel(self, handle: OpHandle, callback) -> None:
        self._terminalise(handle)
        callback(None, None)

    def cancel_nowait(self, handle: OpHandle) -> None:
        self.cancel_nowait_calls.append(handle)
        self._terminalise(handle)

    def stop_poll(self, handle, callback) -> object:
        self._terminalise(handle)
        callback(None, None)
        return None

    def recycle_operation(self, operation: object) -> None:
        # freelist no-op; counts calls for IOHandle tests
        recycled = getattr(self, "recycle_calls", None)
        if recycled is None:
            self.recycle_calls = []
            recycled = self.recycle_calls
        recycled.append(operation)

    def create_recv_buffer_pool(self, buffer_size: int, buffer_count: int):
        from tealetio.proactor import SyntheticRecvBufferPool

        return SyntheticRecvBufferPool(buffer_size, buffer_count)

    def shared_recv_buffer_pool(self):
        return self.create_recv_buffer_pool(8192, 4)

    def accept(self, sock: socket.socket, callback) -> object:
        del sock
        conn, peer = socket.socketpair()
        self._held_peers.append(peer)
        conn.setblocking(False)
        os.set_inheritable(conn.fileno(), False)
        callback(conn, None)
        return None

    def poll(self, fd: int, mask: int, callback) -> object:
        self.poll_calls.append((fd, mask))
        callback(mask, None)
        return None

    def openat(self, path: str, flags: int, callback, mode: int = 0) -> object:
        self.openat_calls.append((path, flags, mode))
        callback(901, None)
        return None

    def create_socket(
        self,
        family: int,
        type: int,
        callback,
        proto: int = 0,
        *,
        flags: int = 0,
    ) -> object:
        self.create_socket_calls.append((family, type, proto, flags))
        sock = socket.socket(family, type, proto)
        sock.setblocking(False)
        os.set_inheritable(sock.fileno(), False)
        self.last_create_socket = sock
        callback(sock, None)
        return None

    def connect(
        self,
        sock: socket.socket,
        address: Any,
        callback,
    ) -> object:
        self.connect_calls.append((sock, address))
        self.last_connect_socket = sock
        callback(None, None)
        return None

    def send(
        self,
        sock: socket.socket,
        data: Any,
        callback,
        progress: Any = None,
        *,
        expect: object = None,
    ) -> object:
        del progress
        self.send_calls.append((sock, data))
        self.send_expects.append(expect)
        callback(None, None)
        return None

    def send_nowait(
        self,
        sock: socket.socket,
        data: Any,
        callback: Any = None,
        *,
        expect: object = None,
    ) -> None:
        self.send_nowait_calls.append((sock, data, callback, expect))

    def send_close_nowait(
        self,
        sock: socket.socket,
        data: Any,
        *,
        expect: object = None,
    ) -> None:
        self.send_close_nowait_calls.append((sock, data, expect))
        if sock.fileno() != -1:
            sock.close()

    def shutdown_nowait(self, sock: socket.socket, how: int) -> None:
        self.shutdown_nowait_calls.append((sock, how))
        sock.shutdown(how)

    def poll_many(
        self,
        fd: int,
        mask: int,
        callback: Any,
    ) -> SelectorCancelHandle:
        # leave open until stop_poll/cancel (IOHandle.close)
        return SelectorCancelHandle(callback)

    def shutdown(self, sock: socket.socket, how: int, callback) -> object:
        try:
            sock.shutdown(how)
        except OSError as exc:
            callback(None, exc)
            return None
        callback(None, None)
        return None

    def close_socket(self, sock: socket.socket, callback) -> object:
        try:
            sock.close()
        except OSError as exc:
            callback(None, exc)
            return None
        callback(None, None)
        return None

    def close_socket_nowait(self, sock: socket.socket) -> None:
        sock.close()

    def close_fd(self, fd: int, callback) -> object:
        self.close_fd_calls.append(fd)
        callback(None, None)
        return None


class TestProactorIOManager:
    def test_basic_scheduler_io_raises(self):
        from tealetio.scheduler import BasicScheduler

        scheduler = BasicScheduler()
        with pytest.raises(RuntimeError, match="scheduler with IO support"):
            scheduler.io


class TestRecvBufferPoolCache:
    def test_default_max_free_recv_buffer_pools_is_1024(self) -> None:
        assert DEFAULT_MAX_FREE_RECV_BUFFER_POOLS == 1024
        io = ProactorIOManager(StubScheduler(), _MockProactor())  # type: ignore[arg-type]
        assert io._recv_pool_cache.max_free == 1024

    def test_acquire_reuses_released_pools(self) -> None:
        io = _manager(_MockProactor())
        first = io.acquire_recv_buffer_pool()
        first.close()
        reused = io.acquire_recv_buffer_pool()
        assert reused is first
        reused.close()
        other = io.create_recv_buffer_pool(8192, 4)
        assert other is not first
        other.close()
        assert io.acquire_recv_buffer_pool() is first

    def test_idle_cap_rejects_extra_free_pools(self) -> None:
        io = ProactorIOManager(
            StubScheduler(),
            _MockProactor(),
            max_free_recv_buffer_pools=2,
        )  # type: ignore[arg-type]
        cache = io._recv_pool_cache
        a = io.acquire_recv_buffer_pool()
        b = io.acquire_recv_buffer_pool()
        c = io.acquire_recv_buffer_pool()
        a.close()
        b.close()
        c.close()
        assert a.release_callback is cache.release_callback
        assert b.release_callback is cache.release_callback
        assert c.release_callback is None
        assert cache.free_count == 2
        assert io.acquire_recv_buffer_pool() is not c

    def test_max_free_none_does_not_evict(self) -> None:
        io = ProactorIOManager(
            StubScheduler(),
            _MockProactor(),
            max_free_recv_buffer_pools=None,
        )  # type: ignore[arg-type]
        cache = io._recv_pool_cache
        pools = [io.acquire_recv_buffer_pool() for _ in range(8)]
        for pool in pools:
            pool.close()
        assert cache.free_count == 8
        assert cache.max_free is None
        reused = {io.acquire_recv_buffer_pool() for _ in range(8)}
        assert reused == set(pools)

    def test_close_disposes_cached_free_pools(self) -> None:
        io = _manager(_MockProactor())
        cache = io._recv_pool_cache
        pool = io.acquire_recv_buffer_pool()
        pool.close()
        assert pool.release_callback is cache.release_callback
        io.close()
        assert pool.release_callback is None
        assert cache.free_count == 0

    def test_late_release_after_manager_close_is_freed(self) -> None:
        io = _manager(_MockProactor())
        cache = io._recv_pool_cache
        pool = io.acquire_recv_buffer_pool()
        io.close()
        pool.close()
        assert pool.release_callback is None

    def test_double_close_returns_pool_to_cache_only_once(self) -> None:
        io = _manager(_MockProactor())
        cache = io._recv_pool_cache
        pool = io.acquire_recv_buffer_pool()
        pool.close()
        pool.close()
        assert cache.free_count == 1
        assert pool.release_callback is cache.release_callback
        first = io.acquire_recv_buffer_pool()
        second = io.acquire_recv_buffer_pool()
        assert first is pool
        assert second is not pool

    def test_double_release_recv_buffer_pool_is_idempotent(self) -> None:
        io = _manager(_MockProactor())
        cache = io._recv_pool_cache
        pool = io.acquire_recv_buffer_pool()
        io.release_recv_buffer_pool(pool)
        io.release_recv_buffer_pool(pool)
        assert cache.free_count == 1
        assert pool.release_callback is cache.release_callback
        assert io.acquire_recv_buffer_pool() is pool

    def test_acquire_release_is_safe_from_many_threads(self) -> None:
        io = _manager(_MockProactor())
        errors: list[BaseException] = []

        def hammer() -> None:
            try:
                for _ in range(200):
                    pool = io.acquire_recv_buffer_pool()
                    pool.close()
            except BaseException as exc:
                errors.append(exc)

        threads = [threading.Thread(target=hammer) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert errors == []
        max_free = io._recv_pool_cache.max_free
        assert max_free is not None
        assert io._recv_pool_cache.free_count <= max_free


class TestAbortiveClose:
    def test_abortive_close_closes_fd(self) -> None:
        from tealetio.socket_helpers import abortive_close

        conn, peer = socket.socketpair()
        peer.close()
        abortive_close(conn)
        assert conn.fileno() == -1
        abortive_close(conn)


class TestProactorIOManagerAcceptMany:
    @pytest.mark.parametrize("recv_size", [0, -1])
    def test_accept_many_rejects_invalid_recv_size(self, recv_size: int) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        server = _nonblocking_listener()
        try:
            with pytest.raises(ValueError):
                io.accept_many(server, lambda _: None, recv_size=recv_size)
        finally:
            server.close()

    def test_accept_many_wires_plain_callback_with_recv_size(self) -> None:
        class _CaptureProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                self.last_callback = callback
                return SelectorCancelHandle()

        proactor = _CaptureProactor()
        io = _manager(proactor)
        server = _nonblocking_listener()
        try:
            io.accept_many(server, lambda _: None)
            assert proactor.last_callback is not None
            io.accept_many(server, lambda _: None, recv_size=64)
            assert proactor.last_callback is not None
        finally:
            server.close()

    def test_accept_many_recv_size_submits_recv_from_io_manager_callback(self) -> None:
        peers: list[socket.socket] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                conn, peer = _eager_accept_conn_open_peer()
                peers.append(peer)
                return _eager_accept_arm(sock, callback, conn)

        delivered: list[tuple[socket.socket, bytes | None]] = []
        proactor = _EagerAcceptProactor(recv_result=b"peek")
        io = _manager(proactor)
        server = _nonblocking_listener()
        try:
            io.accept_many(
                server,
                lambda delivery: delivered.append(delivery),
                recv_size=8,
            )
            assert proactor.recv_calls == [(delivered[0][0], 8)]
            assert delivered == [(delivered[0][0], b"peek")]
        finally:
            for conn, _data in delivered:
                conn.close()
            for peer in peers:
                peer.close()
            server.close()

    def test_accept_many_recv_size_submits_recv_when_data_already_queued(self) -> None:
        peers: list[socket.socket] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                conn, peer = _eager_accept_conn_open_peer()
                peer.sendall(b"hello")
                peers.append(peer)
                return _eager_accept_arm(sock, callback, conn)

        delivered: list[tuple[socket.socket, bytes | None]] = []
        proactor = _EagerAcceptProactor(recv_result=b"from-proactor")
        io = _manager(proactor)
        server = _nonblocking_listener()
        try:
            io.accept_many(
                server,
                lambda delivery: delivered.append(delivery),
                recv_size=8,
            )
            assert proactor.recv_calls == [(delivered[0][0], 8)]
            assert delivered == [(delivered[0][0], b"from-proactor")]
        finally:
            for conn, _data in delivered:
                conn.close()
            for peer in peers:
                peer.close()
            server.close()

    def test_accept_many_caps_oversized_recv_size(self) -> None:
        from tealetio.delivery import normalize_accept_recv_size

        assert normalize_accept_recv_size(2**16 + 1) == 2**16

    @pytest.mark.parametrize("recv_timeout", [0, -1])
    def test_accept_many_rejects_invalid_recv_timeout(self, recv_timeout: float) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        server = _nonblocking_listener()
        try:
            with pytest.raises(ValueError):
                io.accept_many(
                    server,
                    lambda _: None,
                    recv_size=64,
                    recv_timeout=recv_timeout,
                )
        finally:
            server.close()

    def test_accept_many_recv_timeout_requires_recv_size(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        server = _nonblocking_listener()
        try:
            with pytest.raises(ValueError, match="recv_timeout requires recv_size"):
                io.accept_many(server, lambda _: None, recv_timeout=1.0)
        finally:
            server.close()

    def test_accept_many_recv_timeout_posts_cancelled_error_to_scheduler(self) -> None:
        class _PendingRecvProactor(_MockProactor):
            def __init__(self) -> None:
                super().__init__()
                self.pending_recvs: list[_PendingOneshot] = []

            def recv(self, sock: socket.socket, n: int, callback) -> object:
                self.recv_calls.append((sock, n))
                pending = _PendingOneshot(callback)
                self.pending_recvs.append(pending)
                return pending

        peers: list[socket.socket] = []

        class _EagerAcceptProactor(_PendingRecvProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                conn, peer = _eager_accept_conn_open_peer()
                peers.append(peer)
                return _eager_accept_arm(sock, callback, conn)

        recv_errors: list[tuple[socket.socket, BaseException]] = []
        proactor = _EagerAcceptProactor()
        scheduler = StubScheduler()
        io = ProactorIOManager(scheduler, proactor)  # type: ignore[arg-type]
        server = _nonblocking_listener()
        try:
            io.accept_many(
                server,
                lambda _: (_ for _ in ()).throw(AssertionError("accept callback")),
                recv_size=8,
                recv_timeout=0.5,
                on_recv_error=lambda conn, exc: recv_errors.append((conn, exc)),
            )
            recv_op = proactor.pending_recvs[0]
            scheduler.fire_timers()
            assert is_io_cancellation(recv_op.exception)
            assert len(recv_errors) == 1
            assert is_io_cancellation(recv_errors[0][1])
            assert recv_errors[0][0].fileno() == -1
        finally:
            for peer in peers:
                peer.close()
            server.close()

    def test_accept_many_recv_timeout_cancels_pending_recv(self) -> None:
        class _PendingRecvProactor(_MockProactor):
            def __init__(self) -> None:
                super().__init__()
                self.pending_recvs: list[_PendingOneshot] = []

            def recv(self, sock: socket.socket, n: int, callback) -> object:
                self.recv_calls.append((sock, n))
                pending = _PendingOneshot(callback)
                self.pending_recvs.append(pending)
                return pending

        peers: list[socket.socket] = []

        class _EagerAcceptProactor(_PendingRecvProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                conn, peer = _eager_accept_conn_open_peer()
                peers.append(peer)
                return _eager_accept_arm(sock, callback, conn)

        delivered: list[tuple[socket.socket, bytes | None]] = []
        proactor = _EagerAcceptProactor()
        scheduler = StubScheduler()
        io = ProactorIOManager(scheduler, proactor)  # type: ignore[arg-type]
        server = _nonblocking_listener()
        try:
            io.accept_many(
                server,
                lambda delivery: delivered.append(delivery),
                recv_size=8,
                recv_timeout=0.5,
            )
            assert len(proactor.pending_recvs) == 1
            assert len(scheduler.timer_handles) == 1
            recv_op = proactor.pending_recvs[0]
            assert not recv_op.done
            scheduler.fire_timers()
            assert is_io_cancellation(recv_op.exception)
            assert delivered == []
            conn, _size = proactor.recv_calls[0]
            assert conn.fileno() == -1
        finally:
            for peer in peers:
                peer.close()
            server.close()

    def test_accept_many_recv_timeout_skips_arm_when_recv_already_done(self) -> None:
        class _DeferredArmScheduler(StubScheduler):
            def __init__(self) -> None:
                super().__init__()
                self.deferred: list[tuple[Any, tuple[object, ...]]] = []

            def call_soon_threadsafe(self, callback, *args: object, **kwargs: object) -> None:
                del kwargs
                self.deferred.append((callback, args))

        peers: list[socket.socket] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                conn, peer = _eager_accept_conn_open_peer()
                peers.append(peer)
                return _eager_accept_arm(sock, callback, conn)

        delivered: list[tuple[socket.socket, bytes | None]] = []
        proactor = _EagerAcceptProactor(recv_result=b"peek")
        scheduler = _DeferredArmScheduler()
        io = ProactorIOManager(scheduler, proactor)  # type: ignore[arg-type]
        server = _nonblocking_listener()
        try:
            io.accept_many(
                server,
                lambda delivery: delivered.append(delivery),
                recv_size=8,
                recv_timeout=0.5,
            )
            arm_callbacks: list[Any] = []

            def drain_deferred() -> None:
                while scheduler.deferred:
                    callback, args = scheduler.deferred.pop(0)
                    if callback.__name__ == "arm":
                        arm_callbacks.append(callback)
                    callback(*args)

            drain_deferred()
            assert len(arm_callbacks) == 1
            assert not scheduler.timer_handles
            scheduler.fire_timers()
            assert len(delivered) == 1
            assert delivered[0][1] == b"peek"
        finally:
            for conn, _data in delivered:
                conn.close()
            for peer in peers:
                peer.close()
            server.close()

    def test_accept_many_recv_timeout_cancelled_when_recv_completes(self) -> None:
        peers: list[socket.socket] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                conn, peer = _eager_accept_conn_open_peer()
                peers.append(peer)
                return _eager_accept_arm(sock, callback, conn)

        delivered: list[tuple[socket.socket, bytes | None]] = []
        proactor = _EagerAcceptProactor(recv_result=b"peek")
        scheduler = StubScheduler()
        io = ProactorIOManager(scheduler, proactor)  # type: ignore[arg-type]
        server = _nonblocking_listener()
        try:
            io.accept_many(
                server,
                lambda delivery: delivered.append(delivery),
                recv_size=8,
                recv_timeout=0.5,
            )
            assert not scheduler.timer_handles
            scheduler.fire_timers()
            assert len(delivered) == 1
            assert delivered[0][1] == b"peek"
        finally:
            for conn, _data in delivered:
                conn.close()
            for peer in peers:
                peer.close()
            server.close()

    def test_accept_many_on_recv_error_closes_after_callback(self) -> None:
        captured_errors: list[tuple[socket.socket, BaseException]] = []
        peers: list[socket.socket] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                conn, peer = _eager_accept_conn_open_peer()
                peers.append(peer)
                return _eager_accept_arm(sock, callback, conn)

            def recv(self, sock: socket.socket, n: int, callback) -> object:
                callback(None, OSError("recv failed"))
                return None

        proactor = _EagerAcceptProactor()
        io = _manager(proactor)
        server = _nonblocking_listener()
        try:
            io.accept_many(
                server,
                lambda _: (_ for _ in ()).throw(AssertionError("accept callback")),
                recv_size=64,
                on_recv_error=lambda conn, exc: captured_errors.append((conn, exc)),
            )
            assert len(captured_errors) == 1
            conn, exc = captured_errors[0]
            assert str(exc) == "recv failed"
            assert conn.fileno() == -1
        finally:
            for peer in peers:
                peer.close()
            server.close()

    def test_accept_many_recv_error_without_hook_closes_silently(self) -> None:
        closed: list[socket.socket] = []
        peers: list[socket.socket] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                conn, peer = _eager_accept_conn_open_peer()
                peers.append(peer)
                closed.append(conn)
                return _eager_accept_arm(sock, callback, conn)

            def recv(self, sock: socket.socket, n: int, callback) -> object:
                callback(None, OSError("recv failed"))
                return None

        proactor = _EagerAcceptProactor()
        io = _manager(proactor)
        server = _nonblocking_listener()
        try:
            io.accept_many(
                server,
                lambda delivery: (_ for _ in ()).throw(AssertionError("accept callback")),
                recv_size=64,
            )
            assert closed[0].fileno() == -1
        finally:
            for peer in peers:
                peer.close()
            server.close()

    def test_accept_many_reports_callback_exception(self) -> None:
        handler_errors: list[BaseException] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                return _eager_accept_arm(sock, callback)

        scheduler = StubScheduler()
        scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
        io = ProactorIOManager(scheduler, _EagerAcceptProactor())  # type: ignore[arg-type]
        server = _nonblocking_listener()
        try:
            io.accept_many(server, lambda _: (_ for _ in ()).throw(ValueError("accept failed")))
            assert len(handler_errors) == 1
            assert str(handler_errors[0]) == "accept failed"
        finally:
            server.close()

    def test_accept_many_streams_reports_callback_exception(self) -> None:
        handler_errors: list[BaseException] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                return _eager_accept_arm(sock, callback)

        scheduler = StubScheduler()
        scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
        io = ProactorIOManager(scheduler, _EagerAcceptProactor())  # type: ignore[arg-type]
        server = _nonblocking_listener()
        try:
            io.accept_many_streams(
                server,
                lambda _: (_ for _ in ()).throw(ValueError("streams failed")),
            )
            assert len(handler_errors) == 1
            assert str(handler_errors[0]) == "streams failed"
        finally:
            server.close()

    def test_accept_many_reports_on_recv_error_hook_exception(self) -> None:
        handler_errors: list[BaseException] = []
        peers: list[socket.socket] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                conn, peer = _eager_accept_conn_open_peer()
                peers.append(peer)
                return _eager_accept_arm(sock, callback, conn)

            def recv(self, sock: socket.socket, n: int, callback) -> object:
                callback(None, OSError("recv failed"))
                return None

        scheduler = StubScheduler()
        scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
        io = ProactorIOManager(scheduler, _EagerAcceptProactor())  # type: ignore[arg-type]
        server = _nonblocking_listener()
        try:
            io.accept_many(
                server,
                lambda _: None,
                recv_size=64,
                on_recv_error=lambda _conn, _exc: (_ for _ in ()).throw(RuntimeError("hook failed")),
            )
            assert len(handler_errors) == 1
            assert str(handler_errors[0]) == "hook failed"
        finally:
            for peer in peers:
                peer.close()
            server.close()

    def test_accept_many_streams_opens_recv_many_before_marshalled_callback(self) -> None:
        class _QueueingScheduler(StubScheduler):
            def __init__(self) -> None:
                super().__init__()
                self.queued: list[tuple[Any, tuple[object, ...]]] = []

            def call_soon_threadsafe(self, callback, *args: object, **kwargs: object) -> None:
                del kwargs
                self.queued.append((callback, args))

        peers: list[socket.socket] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                # keep peer open so stream open arms continuous recv_many (no eager EOF)
                conn, peer = socket.socketpair()
                peers.append(peer)
                conn.setblocking(False)
                return _eager_accept_arm(sock, callback, conn)

        proactor = _EagerAcceptProactor()
        scheduler = _QueueingScheduler()
        io = ProactorIOManager(scheduler, proactor)  # type: ignore[arg-type]
        server = _nonblocking_listener()
        handled: list[object] = []
        try:
            io.accept_many_streams(server, lambda streams: handled.append(streams))
            assert len(scheduler.queued) == 1
            scheduler.queued[0][0]()
            assert proactor.recv_many_calls
            assert handled
            _reader, writer = handled[0]
            writer.close()
        finally:
            for peer in peers:
                peer.close()
            server.close()

    def test_accept_many_streams_owner_open_arms_recv_many_after_marshal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("TEALETIO_ACCEPT_OPEN_STREAMS", "owner")

        class _QueueingScheduler(StubScheduler):
            def __init__(self) -> None:
                super().__init__()
                self.queued: list[tuple[Any, tuple[object, ...]]] = []

            def call_soon_threadsafe(self, callback, *args: object, **kwargs: object) -> None:
                del kwargs
                self.queued.append((callback, args))

        peers: list[socket.socket] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                conn, peer = socket.socketpair()
                peers.append(peer)
                conn.setblocking(False)
                return _eager_accept_arm(sock, callback, conn)

        proactor = _EagerAcceptProactor()
        scheduler = _QueueingScheduler()
        io = ProactorIOManager(scheduler, proactor)  # type: ignore[arg-type]
        server = _nonblocking_listener()
        handled: list[object] = []
        try:
            io.accept_many_streams(server, lambda streams: handled.append(streams))
            assert len(scheduler.queued) == 1
            assert proactor.recv_many_calls == []
            scheduler.queued[0][0]()
            assert proactor.recv_many_calls
            assert handled
            _reader, writer = handled[0]
            writer.close()
        finally:
            for peer in peers:
                peer.close()
            server.close()

    def test_accept_many_streams_closes_socket_when_stream_factory_raises(self) -> None:
        accepted: list[socket.socket] = []

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                conn = _eager_accept_conn()
                accepted.append(conn)
                return _eager_accept_arm(sock, callback, conn)

        def boom(_io: Any, _sock: socket.socket, **kwargs: Any) -> tuple[Any, Any]:
            raise ValueError("stream failed")

        io = _manager(_EagerAcceptProactor())
        server = _nonblocking_listener()
        try:
            with pytest.raises(ValueError, match="stream failed"):
                io.accept_many_streams(server, lambda _: None, stream_factory=boom)
            assert len(accepted) == 1
            assert accepted[0].fileno() == -1
        finally:
            server.close()

    def test_accept_many_streams_propagates_marshal_failure_without_closing_socket(self) -> None:
        accepted: list[socket.socket] = []
        peers: list[socket.socket] = []

        class _ShutdownScheduler(StubScheduler):
            def call_soon_threadsafe(self, callback, *args: object, **kwargs: object) -> None:
                del callback, args, kwargs
                raise RuntimeError("scheduler shut down")

        class _EagerAcceptProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                # keep peer open so stream open does not hit eager EOF marshal
                conn, peer = socket.socketpair()
                peers.append(peer)
                conn.setblocking(False)
                accepted.append(conn)
                return _eager_accept_arm(sock, callback, conn)

        io = ProactorIOManager(_ShutdownScheduler(), _EagerAcceptProactor())  # type: ignore[arg-type]
        server = _nonblocking_listener()
        try:
            with pytest.raises(RuntimeError, match="scheduler shut down"):
                io.accept_many_streams(server, lambda _: None)
            assert len(accepted) == 1
            assert accepted[0].fileno() != -1
            accepted[0].close()
        finally:
            for peer in peers:
                peer.close()
            server.close()

    def test_accept_many_streams_uses_bare_socket_callback(self) -> None:
        class _CaptureProactor(_MockProactor):
            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                self.last_callback = callback
                return SelectorCancelHandle()

        proactor = _CaptureProactor()
        io = _manager(proactor)
        server = _nonblocking_listener()
        try:
            io.accept_many_streams(server, lambda _: None)
            assert proactor.last_callback is not None
        finally:
            server.close()


class TestProactorIOManagerAcceptSubmit:
    def _listen_with_backlog(self, n: int) -> tuple[socket.socket, list[socket.socket]]:
        listener = _nonblocking_listener(backlog=max(n, 8))
        clients: list[socket.socket] = []
        for _ in range(n):
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(listener.getsockname())
            clients.append(client)
        return listener, clients

    def test_accept_many_submits_without_draining_ready_backlog(self) -> None:
        class _CaptureProactor(_MockProactor):
            def __init__(self) -> None:
                super().__init__()
                self.accept_many_calls = 0
                self.last_base_sequence = -1

            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                self.accept_many_calls += 1
                self.last_base_sequence = base_sequence
                return SelectorCancelHandle(callback)

        proactor = _CaptureProactor()
        io = _manager(proactor)
        listener, clients = self._listen_with_backlog(3)
        accepted: list[socket.socket] = []
        try:
            waiter = io.accept_many(
                listener,
                lambda delivery: accepted.append(delivery[0]),
            )
            assert isinstance(waiter, IOWaiter)
            assert proactor.accept_many_calls == 1
            assert proactor.last_base_sequence == 0
            assert accepted == []
            conn, _address = listener.accept()
            conn.close()
        finally:
            for client in clients:
                client.close()
            listener.close()

    def test_accept_many_empty_queue_submits(self) -> None:
        class _CaptureProactor(_MockProactor):
            def __init__(self) -> None:
                super().__init__()
                self.accept_many_calls = 0
                self.last_base_sequence = -1

            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                self.accept_many_calls += 1
                self.last_base_sequence = base_sequence
                return SelectorCancelHandle(callback)

        proactor = _CaptureProactor()
        io = _manager(proactor)
        listener = _nonblocking_listener()
        accepted: list[object] = []
        try:
            waiter = io.accept_many(listener, lambda d: accepted.append(d))
            assert isinstance(waiter, IOWaiter)
            assert proactor.accept_many_calls == 1
            assert proactor.last_base_sequence == 0
            assert accepted == []
        finally:
            listener.close()

    def test_accept_many_recv_size_submits_without_eager_preread(self) -> None:
        class _CaptureProactor(_MockProactor):
            def __init__(self, **kwargs: Any) -> None:
                super().__init__(**kwargs)
                self.accept_many_calls = 0
                self.last_base_sequence = -1

            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                self.accept_many_calls += 1
                self.last_base_sequence = base_sequence
                return SelectorCancelHandle(callback)

        proactor = _CaptureProactor(recv_result=b"hi")
        io = _manager(proactor)
        listener, clients = self._listen_with_backlog(2)
        accepted: list[tuple[socket.socket, bytes | None]] = []
        try:
            io.accept_many(
                listener,
                lambda d: accepted.append(d),
                recv_size=64,
            )
            assert proactor.accept_many_calls == 1
            assert proactor.last_base_sequence == 0
            assert accepted == []
            assert proactor.recv_calls == []
        finally:
            for client in clients:
                client.close()
            listener.close()

    def test_accept_many_streams_submits_without_draining(self) -> None:
        class _CaptureProactor(_MockProactor):
            def __init__(self) -> None:
                super().__init__()
                self.accept_many_calls = 0
                self.last_base_sequence = -1

            def accept_many(self, sock: socket.socket, callback=None, *, base_sequence: int = 0):
                self.accept_many_calls += 1
                self.last_base_sequence = base_sequence
                return SelectorCancelHandle(callback)

            def recv_many(self, sock, callback, *, buf_group, base_sequence=0):
                del callback, buf_group, base_sequence
                self.recv_many_calls.append(sock)
                return SelectorCancelHandle()

        proactor = _CaptureProactor()
        io = _manager(proactor)
        listener, clients = self._listen_with_backlog(2)
        streams: list[tuple[Any, Any]] = []
        try:
            waiter = io.accept_many_streams(
                listener,
                lambda pair: streams.append(pair),
            )
            assert isinstance(waiter, IOWaiter)
            assert proactor.accept_many_calls == 1
            assert proactor.last_base_sequence == 0
            assert streams == []
        finally:
            for client in clients:
                client.close()
            listener.close()

    def test_sock_accept_delegates_to_proactor(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        listener, clients = self._listen_with_backlog(1)
        try:
            waiter = io.sock_accept(listener)
            assert isinstance(waiter, IOWaiter)
            conn, initial = waiter.wait()
            try:
                assert initial is None
                assert not conn.getblocking()
            finally:
                conn.close()
            conn, _address = listener.accept()
            conn.close()
        finally:
            for client in clients:
                client.close()
            listener.close()


class TestProactorIOManagerRecvManySubmit:
    def test_recv_many_submits_without_draining_ready_bytes(self) -> None:
        class _CaptureProactor(_MockProactor):
            def __init__(self) -> None:
                super().__init__()
                self.recv_many_calls = 0
                self.last_base_sequence = -1

            def recv_many(self, sock, callback, *, buf_group, base_sequence=0):
                del callback, buf_group
                self.recv_many_calls += 1
                self.last_base_sequence = base_sequence
                return SelectorCancelHandle()

            def shared_recv_buffer_pool(self):
                return self.create_recv_buffer_pool(4, 8)

        proactor = _CaptureProactor()
        io = _manager(proactor)
        reader, writer = socket.socketpair()
        reader.setblocking(False)
        writer.setblocking(False)
        writer.sendall(b"abcdef")
        seen: list[bytes] = []
        try:
            operation = io._recv_many(
                reader,
                lambda d: seen.append(bytes(d.value) if d.value is not None else b""),
            )
            assert isinstance(operation, SelectorCancelHandle)
            assert proactor.recv_many_calls == 1
            assert proactor.last_base_sequence == 0
            assert seen == []
            assert reader.recv(6) == b"abcdef"
        finally:
            reader.close()
            writer.close()

    def test_recv_many_empty_socket_submits(self) -> None:
        class _CaptureProactor(_MockProactor):
            def __init__(self) -> None:
                super().__init__()
                self.recv_many_calls = 0
                self.last_base_sequence = -1

            def recv_many(self, sock, callback, *, buf_group, base_sequence=0):
                del callback, buf_group
                self.recv_many_calls += 1
                self.last_base_sequence = base_sequence
                return SelectorCancelHandle()

        proactor = _CaptureProactor()
        io = _manager(proactor)
        reader, writer = socket.socketpair()
        reader.setblocking(False)
        writer.setblocking(False)
        try:
            operation = io._recv_many(reader, lambda _d: None)
            assert isinstance(operation, SelectorCancelHandle)
            assert proactor.recv_many_calls == 1
            assert proactor.last_base_sequence == 0
        finally:
            reader.close()
            writer.close()

    def test_recv_many_queued_eof_still_submits_continuous(self) -> None:
        class _CaptureProactor(_MockProactor):
            def __init__(self) -> None:
                super().__init__()
                self.recv_many_calls = 0

            def recv_many(self, sock, callback, *, buf_group, base_sequence=0):
                del sock, callback, buf_group, base_sequence
                self.recv_many_calls += 1
                return SelectorCancelHandle()

            def shared_recv_buffer_pool(self):
                return self.create_recv_buffer_pool(64, 4)

        proactor = _CaptureProactor()
        io = _manager(proactor)
        reader, writer = socket.socketpair()
        reader.setblocking(False)
        writer.sendall(b"hi")
        writer.close()
        try:
            io._recv_many(reader, lambda _d: None)
            assert proactor.recv_many_calls == 1
        finally:
            reader.close()


class TestProactorIOManagerSockCreateStreams:
    def test_sock_create_streams_composes_create_connect_and_send(self, monkeypatch: pytest.MonkeyPatch) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        address = ("127.0.0.1", 9)
        waiter = io.sock_create_streams(
            socket.AF_INET,
            socket.SOCK_STREAM,
            connect_to=address,
            initial_data=b"hi",
        )
        assert isinstance(waiter, IOWaitGroup)
        reader, writer = waiter.wait()
        try:
            assert proactor.create_socket_calls == []
            assert len(proactor.connect_calls) == 1
            assert len(proactor.send_calls) == 1
        finally:
            writer.close()

    def test_sock_create_streams_uses_io_wait_group(self) -> None:
        from tealetio.streams import StreamReader, StreamWriter

        proactor = _MockProactor()
        io = _manager(proactor)
        waiter = io.sock_create_streams(
            socket.AF_INET,
            socket.SOCK_STREAM,
            connect_to=("127.0.0.1", 9),
        )
        assert isinstance(waiter, IOWaitGroup)
        writer = None
        try:
            reader, writer = waiter.wait()
            assert isinstance(reader, StreamReader)
            assert isinstance(writer, StreamWriter)
        finally:
            if writer is not None:
                writer.close()

    def test_sock_create_streams_closes_socket_when_stream_factory_raises(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)

        def boom(_io: Any, _sock: socket.socket, **kwargs: Any) -> tuple[Any, Any]:
            raise ValueError("stream failed")

        waiter = io.sock_create_streams(
            socket.AF_INET,
            socket.SOCK_STREAM,
            connect_to=("127.0.0.1", 9),
            stream_factory=boom,
        )
        with pytest.raises(ValueError, match="stream failed"):
            waiter.wait()

    def test_sock_create_streams_closes_socket_when_connect_submit_raises(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        seen: list[socket.socket] = []

        def raising_connect(sock: socket.socket, address: Any, callback) -> object:
            del address, callback
            seen.append(sock)
            raise RuntimeError("proactor is closed")

        proactor.connect = raising_connect  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="proactor is closed"):
            io.sock_create_streams(
                socket.AF_INET,
                socket.SOCK_STREAM,
                connect_to=("127.0.0.1", 9),
            )
        assert len(seen) == 1
        assert seen[0].fileno() == -1


class TestProactorIOManagerDirect:
    def test_io_waiter_wait_returns_immediate_result(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = socket.socketpair()[0]
        try:
            assert io.sock_recv(sock, 4).wait() == b"mock"
        finally:
            sock.close()

    def test_io_waiter_forget_drops_interest_without_cancelling(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        token = object()
        waiter = IOWaiter(io)
        waiter.bind(token)
        waiter.forget()
        assert token not in proactor.cancel_nowait_calls
        with pytest.raises(AssertionError):
            waiter.wait()
        waiter.forget()

    def test_io_waiter_forget_allows_backend_completion(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        conn, peer = socket.socketpair()
        peer.close()
        try:
            assert io.sock_close(conn) is None
            assert conn.fileno() == -1
        finally:
            if conn.fileno() != -1:
                conn.close()

    def test_sock_close_uses_proactor_nowait(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        conn, peer = socket.socketpair()
        peer.close()
        try:
            assert io.sock_shutdown(conn, socket.SHUT_WR) is None
            assert io.sock_close(conn) is None
            assert conn.fileno() == -1
        finally:
            if conn.fileno() != -1:
                conn.close()

    def test_sock_close_raises_from_nowait(self) -> None:
        proactor = _MockProactor()

        def boom(sock: socket.socket) -> None:
            del sock
            raise OSError(errno.EBADF, "bad fd")

        proactor.close_socket_nowait = boom  # type: ignore[method-assign]
        io = _manager(proactor)
        conn, peer = socket.socketpair()
        peer.close()
        try:
            with pytest.raises(OSError, match="bad fd"):
                io.sock_close(conn)
        finally:
            if conn.fileno() != -1:
                conn.close()

    def test_sock_recv_delegates_to_proactor(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        sock, peer = socket.socketpair()
        sock.setblocking(False)
        peer.setblocking(False)
        try:
            waiter = io.sock_recv(sock, 4)
            assert isinstance(waiter, IOWaiter)
            assert waiter.wait() == b"mock"
            assert proactor.recv_calls == [(sock, 4)]
        finally:
            sock.close()
            peer.close()

    def test_sock_recv_submits_when_data_already_queued(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        sock, peer = socket.socketpair()
        sock.setblocking(False)
        peer.setblocking(False)
        try:
            peer.sendall(b"abcd")
            waiter = io.sock_recv(sock, 4)
            assert isinstance(waiter, IOWaiter)
            assert waiter.wait() == b"mock"
            assert proactor.recv_calls == [(sock, 4)]
            assert sock.recv(4) == b"abcd"
        finally:
            sock.close()
            peer.close()

    def test_sock_sendall_always_uses_proactor_send(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        sock, peer = socket.socketpair()
        sock.setblocking(False)
        peer.setblocking(False)
        try:
            waiter = io.sock_sendall(sock, b"hello")
            assert not isinstance(waiter, IOWaiterSync)
            waiter.wait()
            assert proactor.send_calls == [(sock, b"hello")]
            assert proactor.send_expects == [IoExpect.READY]
        finally:
            sock.close()
            peer.close()

    def test_sock_send_nowait_passes_through(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        sock, peer = socket.socketpair()
        try:
            def on_error(_result: object, _exc: BaseException | None) -> None:
                return None

            assert io.sock_send_nowait(sock, b"hello", on_error) is None
            assert len(proactor.send_nowait_calls) == 1
            assert proactor.send_nowait_calls[0][0] is sock
            assert proactor.send_nowait_calls[0][1] == b"hello"
            assert proactor.send_nowait_calls[0][2] is on_error
        finally:
            sock.close()
            peer.close()

    def test_sock_send_close_passes_through(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        sock, peer = socket.socketpair()
        try:
            assert io.sock_send_close(sock, b"hello") is None
            assert len(proactor.send_close_nowait_calls) == 1
            assert proactor.send_close_nowait_calls[0][0] is sock
            assert proactor.send_close_nowait_calls[0][1] == b"hello"
            assert sock.fileno() == -1
        finally:
            if sock.fileno() != -1:
                sock.close()
            peer.close()

    def test_sock_sendall_waiter_add_done_callback_registers_after_return(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        sock, peer = socket.socketpair()
        sock.setblocking(False)
        peer.setblocking(False)
        completed: list[int] = []
        try:
            waiter = io.sock_sendall(sock, b"hello")
            waiter.add_done_callback(lambda: completed.append(1))
            waiter.forget()
            assert completed == [1]
            assert proactor.send_calls == [(sock, b"hello")]
        finally:
            sock.close()
            peer.close()

    def test_sock_sendall_waiter_add_done_callback_runs_on_failure(self, monkeypatch: pytest.MonkeyPatch):
        proactor = _MockProactor()
        io = _manager(proactor)
        sock, peer = socket.socketpair()
        sock.setblocking(False)
        peer.setblocking(False)
        completed: list[int] = []
        try:

            def boom(sock: socket.socket, data: Any, callback, progress: Any = None, **_kwargs: object) -> object:
                del data, progress
                callback(None, OSError("send failed"))
                return None

            proactor.send = boom  # type: ignore[method-assign]
            waiter = io.sock_sendall(sock, b"hello")
            waiter.add_done_callback(lambda: completed.append(1))
            with pytest.raises(OSError, match="send failed"):
                waiter.wait()
            assert completed == [1]
        finally:
            sock.close()
            peer.close()

    def test_sock_sendall_empty_payload_add_done_callback_runs_after_return(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = socket.socketpair()[0]
        phase: list[str] = []
        try:

            def send(target_sock: socket.socket, data: Any, callback, progress: Any = None, **_kwargs: object) -> object:
                del data, progress
                phase.append("send")
                callback(None, None)
                return None

            proactor.send = send  # type: ignore[method-assign]
            waiter = io.sock_sendall(sock, b"")
            phase.append("returned")
            waiter.add_done_callback(lambda: phase.append("done"))
            assert phase == ["send", "returned", "done"]
            assert proactor.send_calls == []
        finally:
            sock.close()

    def test_poll_delegates_to_proactor(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        assert io.poll(7, 3).wait() == 3
        assert proactor.poll_calls == [(7, 3)]

    def test_sock_create_applies_scheduler_socket_contract(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        waiter = io.sock_create(socket.AF_INET, socket.SOCK_STREAM)
        assert isinstance(waiter, IOWaiterSync)
        assert waiter.poll() is True
        sock = waiter.wait()
        try:
            import fcntl

            flags = fcntl.fcntl(sock.fileno(), fcntl.F_GETFL)
            assert flags & os.O_NONBLOCK
            assert not os.get_inheritable(sock.fileno())
            assert proactor.create_socket_calls == []
        finally:
            sock.close()

    def test_io_waiter_sync_raises_stored_exception(self) -> None:
        waiter = IOWaiterSync.failed(OSError("create failed"))
        assert waiter.poll() is True
        with pytest.raises(OSError, match="create failed"):
            waiter.wait()
        seen: list[int] = []
        waiter.add_done_callback(lambda: seen.append(1))
        assert seen == [1]

    def test_sock_create_composes_connect_without_operation_factory(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        address = ("127.0.0.1", 9)
        waiter = io.sock_create(
            socket.AF_INET,
            socket.SOCK_STREAM,
            connect_to=address,
        )
        assert isinstance(waiter, IOWaitGroup)
        sock = waiter.wait()
        try:
            assert proactor.create_socket_calls == []
            assert proactor.connect_calls == [(sock, address)]
        finally:
            sock.close()

    def test_sock_create_composes_connect_and_send_without_operation_factory(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        address = ("127.0.0.1", 9)
        waiter = io.sock_create(
            socket.AF_INET,
            socket.SOCK_STREAM,
            connect_to=address,
            initial_data=b"hi",
        )
        assert isinstance(waiter, IOWaitGroup)
        sock = waiter.wait()
        try:
            assert proactor.create_socket_calls == []
            assert proactor.connect_calls == [(sock, address)]
            assert len(proactor.send_calls) == 1
            assert proactor.send_calls[0][0] is sock
        finally:
            sock.close()

    def test_sock_connect_without_initial_returns_io_waiter(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = socket.socketpair()[0]
        try:
            waiter = io.sock_connect(sock, ("127.0.0.1", 9))
            assert isinstance(waiter, IOWaiter)
            waiter.wait()
            assert proactor.send_calls == []
        finally:
            sock.close()

    def test_sock_connect_composes_send_after_connect(self, monkeypatch: pytest.MonkeyPatch) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = socket.socketpair()[0]
        try:
            waiter = io.sock_connect(sock, ("127.0.0.1", 9), initial=b"hi")
            assert isinstance(waiter, IOWaitGroup)
            waiter.wait()
            assert len(proactor.send_calls) == 1
            assert bytes(proactor.send_calls[0][1]) == b"hi"
        finally:
            sock.close()

    def test_sock_connect_initial_send_uses_proactor(self) -> None:
        """After connect, initial bytes use ``sock_sendall`` → ``proactor.send``."""

        proactor = _MockProactor()
        io = _manager(proactor)
        sock, peer = socket.socketpair()
        sock.setblocking(False)
        peer.setblocking(False)
        try:
            waiter = io.sock_connect(sock, ("127.0.0.1", 9), initial=b"hi")
            assert isinstance(waiter, IOWaitGroup)
            waiter.wait()
            assert proactor.send_calls == [(sock, b"hi")]
        finally:
            sock.close()
            peer.close()

    def test_sock_accept_without_recv_returns_io_waiter(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        listen = _nonblocking_listener()
        try:
            waiter = io.sock_accept(listen)
            assert isinstance(waiter, IOWaiter)
            conn, initial = waiter.wait()
            try:
                assert initial is None
                assert proactor.recv_calls == []
            finally:
                conn.close()
        finally:
            listen.close()

    def test_sock_accept_composes_recv_after_accept(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        listen = _nonblocking_listener()
        try:
            waiter = io.sock_accept(listen, 64)
            assert isinstance(waiter, IOWaitGroup)
            conn, data = waiter.wait()
            try:
                assert data == b"mock"
                assert proactor.recv_calls == [(conn, 64)]
            finally:
                conn.close()
        finally:
            listen.close()

    def test_sock_accept_closes_connection_when_recv_attach_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        listen = _nonblocking_listener()
        accepted: list[socket.socket] = []
        peers: list[socket.socket] = []

        def accept_capture(sock: socket.socket, callback) -> object:
            conn, peer = _eager_accept_conn_open_peer()
            peers.append(peer)
            accepted.append(conn)
            callback(conn, None)
            return None

        proactor.accept = accept_capture  # type: ignore[method-assign]

        real_attach = IOWaitGroup.attach

        attach_count = [0]

        def attach_fail_recv(self: IOWaitGroup[Any], operation: IOWaiter[Any], **kwargs: Any) -> Any:
            if isinstance(operation, IOWaiter):
                attach_count[0] += 1
                if attach_count[0] > 1:
                    raise RuntimeError("attach failed")
            return real_attach(self, operation, **kwargs)

        monkeypatch.setattr(IOWaitGroup, "attach", attach_fail_recv)
        try:
            waiter = io.sock_accept(listen, 64)
            assert isinstance(waiter, IOWaitGroup)
            with pytest.raises(RuntimeError, match="attach failed"):
                waiter.wait()
            assert len(accepted) == 1
            assert accepted[0].fileno() == -1
        finally:
            for peer in peers:
                peer.close()
            listen.close()

    def test_sock_accept_delivers_empty_initial_read_as_eof(self) -> None:
        proactor = _MockProactor(recv_result=b"")
        io = _manager(proactor)
        listen = _nonblocking_listener()
        try:
            waiter = io.sock_accept(listen, 64)
            conn, data = waiter.wait()
            try:
                assert data == b""
                assert proactor.recv_calls == [(conn, 64)]
            finally:
                conn.close()
        finally:
            listen.close()

    def test_sock_create_rejects_initial_data_without_connect_to(self):
        proactor = _MockProactor()
        io = _manager(proactor)
        with pytest.raises(ValueError, match="initial_data requires connect_to"):
            io.sock_create(
                socket.AF_INET,
                socket.SOCK_STREAM,
                initial_data=b"hi",
            )

    def test_sock_create_closes_socket_when_connect_fails(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        seen: list[socket.socket] = []

        def failing_connect(
            sock: socket.socket,
            address: Any,
            callback,
        ) -> object:
            del address
            seen.append(sock)
            callback(None, OSError("connect failed"))
            return None

        proactor.connect = failing_connect  # type: ignore[method-assign]
        waiter = io.sock_create(
            socket.AF_INET,
            socket.SOCK_STREAM,
            connect_to=("127.0.0.1", 9),
        )
        assert isinstance(waiter, IOWaitGroup)
        with pytest.raises(OSError, match="connect failed"):
            waiter.wait()
        assert len(seen) == 1
        assert seen[0].fileno() == -1

    def test_sock_create_closes_socket_when_connect_submit_raises(self) -> None:
        """Stdlib-created sock is not group-owned until attach; submit raise must close it."""

        proactor = _MockProactor()
        io = _manager(proactor)
        seen: list[socket.socket] = []

        def raising_connect(sock: socket.socket, address: Any, callback) -> object:
            del address, callback
            seen.append(sock)
            raise RuntimeError("proactor is closed")

        proactor.connect = raising_connect  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="proactor is closed"):
            io.sock_create(
                socket.AF_INET,
                socket.SOCK_STREAM,
                connect_to=("127.0.0.1", 9),
            )
        assert len(seen) == 1
        assert seen[0].fileno() == -1

    def test_sock_connect_leaves_socket_open_when_send_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)

        def failing_send(
            sock: socket.socket,
            data: Any,
            callback,
            progress: Any = None,
            **_kwargs: object,
        ) -> object:
            del data, progress
            callback(None, OSError("send failed"))
            return None

        proactor.send = failing_send  # type: ignore[method-assign]
        sock = socket.socketpair()[0]
        try:
            waiter = io.sock_connect(sock, ("127.0.0.1", 9), initial=b"hi")
            assert isinstance(waiter, IOWaitGroup)
            with pytest.raises(OSError, match="send failed"):
                waiter.wait()
            assert sock.fileno() != -1
        finally:
            sock.close()

    def test_sock_create_closes_socket_when_send_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)

        def failing_send(
            sock: socket.socket,
            data: Any,
            callback,
            progress: Any = None,
            **_kwargs: object,
        ) -> object:
            del data, progress
            callback(None, OSError("send failed"))
            return None

        proactor.send = failing_send  # type: ignore[method-assign]
        waiter = io.sock_create(
            socket.AF_INET,
            socket.SOCK_STREAM,
            connect_to=("127.0.0.1", 9),
            initial_data=b"hi",
        )
        assert isinstance(waiter, IOWaitGroup)
        with pytest.raises(OSError, match="send failed"):
            waiter.wait()
        assert proactor.last_connect_socket is not None
        assert proactor.last_connect_socket.fileno() == -1

    def test_io_waiter_accept_finishes_without_wrapping_operation(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        waiter = IOWaiter(io)
        waiter.bind(object())
        waiter.complete(None, None)
        assert waiter.wait() is None

    def test_io_waiter_exceptional_exit_cancels_via_proactor(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import tealetio.io_waiter as io_waiter_module

        proactor = _MockProactor()
        io = _manager(proactor)
        token = object()
        waiter = IOWaiter(io)
        waiter.bind(token)

        original_event = io_waiter_module.CrossThreadEvent

        class RaisingEvent(original_event):
            def swait(self) -> bool:
                raise KeyboardInterrupt()

        monkeypatch.setattr(io_waiter_module, "CrossThreadEvent", RaisingEvent)

        with pytest.raises(KeyboardInterrupt):
            waiter.wait()

        assert proactor.cancel_nowait_calls == [token]

    def test_poll_many_returns_io_handle(self) -> None:
        from tealetio.io_waiter import IOHandle

        proactor = _MockProactor()
        io = _manager(proactor)
        handle = io.poll_many(5, 1, lambda _d: None)
        assert isinstance(handle, IOHandle)
        assert handle.closed is False
        handle.close()
        assert handle.closed is True

    def test_io_handle_close_idempotent_while_still_open(self) -> None:
        from tealetio.io_waiter import IOHandle
        from tealetio.delivery import MultishotDelivery, SelectorCancelHandle

        proactor = _MockProactor()
        remove_calls: list[object] = []

        def stop_poll_leave_open(handle, callback) -> object:
            remove_calls.append(handle)
            callback(None, None)
            return None

        proactor.stop_poll = stop_poll_leave_open  # type: ignore[method-assign]
        io = _manager(proactor)
        handle = io.poll_many(5, 1, lambda _d: None)
        assert isinstance(handle, IOHandle)
        assert handle.closed is False
        handle.close()
        assert handle.closed is False
        assert len(remove_calls) == 1
        handle.close()  # second close while open: no second stop_poll
        assert len(remove_calls) == 1
        token = remove_calls[0]
        assert isinstance(token, SelectorCancelHandle)
        token._finish_with_terminal_delivery(
            MultishotDelivery(index=token._next_index, exception=io_cancellation_error(), more=False)
        )
        assert handle.closed is True


class TestProactorIOManagerDeferredCompose:
    @pytest.fixture(autouse=True)
    def _patch_uring_probe_capabilities(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_uring_capabilities(monkeypatch)

    def test_sock_accept_cancel_during_pending_recv_closes_connection(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import tealetio.io_waiter as io_waiter_module

        proactor = _MockProactor()
        io = _manager(proactor)
        listen = _nonblocking_listener()
        pending_recv: list[_PendingOneshot] = []
        accepted_conn: list[socket.socket] = []

        def pending_recv_operation(sock: socket.socket, n: int, callback) -> object:
            accepted_conn.append(sock)
            pending = _PendingOneshot(callback)
            pending_recv.append(pending)
            return pending

        proactor.recv = pending_recv_operation  # type: ignore[method-assign]
        original_swait = io_waiter_module.CrossThreadEvent.swait

        def swait_and_abort(self: Any) -> None:
            raise TimeoutError("abort wait")

        monkeypatch.setattr(io_waiter_module.CrossThreadEvent, "swait", swait_and_abort)
        try:
            # mock accept keeps peer open so preread would-block and attaches pending recv
            waiter = io.sock_accept(listen, 64)
            assert isinstance(waiter, IOWaitGroup)
            assert len(accepted_conn) == 1
            conn = accepted_conn[0]
            assert len(pending_recv) == 1
            with pytest.raises(TimeoutError, match="abort wait"):
                waiter.wait()
            assert is_io_cancellation(pending_recv[0].exception)
            assert conn.fileno() == -1
        finally:
            listen.close()
            io_waiter_module.CrossThreadEvent.swait = original_swait

    def test_sock_create_cancel_during_pending_connect_closes_socket(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import tealetio.io_waiter as io_waiter_module

        proactor = UringProactor(ring_factory=_DeferredCreateSocketUringRing)
        scheduler = SyncProactorScheduler(lambda: proactor)
        set_scheduler(scheduler)
        original_swait = io_waiter_module.CrossThreadEvent.swait

        def staged_swait(self: Any) -> None:
            _wait_for_uring(proactor, lambda: len(proactor.ring.pending_connect) == 1)
            raise TimeoutError("abort wait")

        monkeypatch.setattr(io_waiter_module.CrossThreadEvent, "swait", staged_swait)
        try:
            waiter = scheduler.io.sock_create(
                socket.AF_INET,
                socket.SOCK_STREAM,
                connect_to=("127.0.0.1", 9),
                initial_data=b"hi",
            )
            assert isinstance(waiter, IOWaitGroup)
            with pytest.raises(TimeoutError, match="abort wait"):
                waiter.wait()
            # cancel CQEs may be in-flight on completion workers (free-threaded);
            # wait until close_on_fail has run rather than a single drain.
            assert proactor.ring.submitted_socket == []
            assert proactor.ring.submitted_connect
            leaked_fd = proactor.ring.submitted_connect[0][0]
            _wait_for_uring(proactor, lambda: _fd_closed(leaked_fd))
        finally:
            scheduler.close()
            proactor.close()
            io_waiter_module.CrossThreadEvent.swait = original_swait

    def test_sock_create_cancel_during_pending_send_closes_socket(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import tealetio.io_waiter as io_waiter_module

        # force proactor send leg so the deferred ring can hold the CQE
        proactor = UringProactor(ring_factory=_DeferredCreateSocketUringRing)
        scheduler = SyncProactorScheduler(lambda: proactor)
        set_scheduler(scheduler)
        original_swait = io_waiter_module.CrossThreadEvent.swait

        def staged_swait(self: Any) -> None:
            _wait_for_uring(proactor, lambda: len(proactor.ring.pending_connect) == 1)
            _ensure_deferred_connect_completed(proactor.ring)
            _wait_for_uring(proactor, lambda: len(proactor.ring.pending_connect_send) == 1)
            raise TimeoutError("abort wait")

        monkeypatch.setattr(io_waiter_module.CrossThreadEvent, "swait", staged_swait)
        try:
            waiter = scheduler.io.sock_create(
                socket.AF_INET,
                socket.SOCK_STREAM,
                connect_to=("127.0.0.1", 9),
                initial_data=b"hi",
            )
            assert isinstance(waiter, IOWaitGroup)
            with pytest.raises(TimeoutError, match="abort wait"):
                waiter.wait()
            # cancel CQEs may be in-flight on completion workers (free-threaded);
            # wait until close_on_fail has run rather than a single drain.
            assert proactor.ring.submitted_socket == []
            assert proactor.ring.submitted_connect
            leaked_fd = proactor.ring.submitted_connect[0][0]
            _wait_for_uring(proactor, lambda: _fd_closed(leaked_fd))
        finally:
            scheduler.close()
            proactor.close()
            io_waiter_module.CrossThreadEvent.swait = original_swait


class TestIOWaitablePoll:
    def test_io_waiter_poll_tracks_operation_completion(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        listen = _nonblocking_listener()
        pending: list[_PendingOneshot] = []

        def pending_accept(sock: socket.socket, callback) -> object:
            handle = _PendingOneshot(callback)
            pending.append(handle)
            return handle

        proactor.accept = pending_accept  # type: ignore[method-assign]
        waiter = io.sock_accept(listen)
        try:
            assert waiter.poll() is False
            conn, _peer = socket.socketpair()
            pending[0].complete(conn)
            assert waiter.poll() is True
            accepted, initial = waiter.wait()
            try:
                assert initial is None
                assert accepted is conn
            finally:
                accepted.close()
            assert waiter.poll() is False
        finally:
            listen.close()

    def test_io_waiter_poll_is_false_after_forget(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        token = object()
        def recv_stub(_sock: socket.socket, _n: int, callback) -> object:
            del callback
            return token

        proactor.recv = recv_stub  # type: ignore[method-assign, assignment]
        sock, peer = socket.socketpair()
        sock.setblocking(False)
        peer.setblocking(False)
        try:
            waiter = io.sock_recv(sock, 4)
            assert isinstance(waiter, IOWaiter)
            waiter.forget()
            assert waiter.poll() is False
        finally:
            sock.close()
            peer.close()

    def test_io_wait_group_poll_tracks_group_completion(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        group = IOWaitGroup[str](io)
        group.attach(_pending_waiter(io))
        assert group.poll() is False
        group.finish("done")
        assert group.poll() is True
        assert group.wait() == "done"


class TestIOWaitGroup:
    def test_group_wait_uses_single_cross_thread_event_for_multi_leg_compose(self) -> None:
        import tealetio.io_waiter as io_waiter_module

        proactor = _MockProactor()
        io = _manager(proactor)
        event_count = 0
        original_event = io_waiter_module.CrossThreadEvent
        pending_connect: list[IOWaiter[None]] = []

        class TrackingEvent(original_event):
            def __init__(self, scheduler: Any) -> None:
                nonlocal event_count
                event_count += 1
                super().__init__(scheduler)

            def swait(self) -> None:
                connect = pending_connect[0]
                if not connect.done():
                    connect.complete(None)
                super().swait()

        io_waiter_module.CrossThreadEvent = TrackingEvent  # type: ignore[misc]
        try:
            create: IOWaiter[socket.socket] = IOWaiter(io)
            connect: IOWaiter[None] = IOWaiter(io)
            pending_connect.append(connect)

            group = IOWaitGroup[socket.socket](io)

            def advance_create(child: IOWaitGroupChild[socket.socket]) -> None:
                sock = child.value()
                group.attach(
                    connect,
                    advance=lambda _connect_child: group.finish(sock),
                )

            group.attach(create, advance=advance_create)
            create.complete(proactor.last_create_socket)
            assert group._completion is None
            assert group.wait() is proactor.last_create_socket
            assert event_count == 1
        finally:
            io_waiter_module.CrossThreadEvent = original_event

    def test_group_add_done_callback_runs_when_finish_completes(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        completed: list[int] = []
        group = IOWaitGroup[str](io)
        group.add_done_callback(lambda: completed.append(1))
        group.finish("done")
        assert completed == [1]
        assert group.wait() == "done"

    def test_group_add_done_callback_runs_immediately_when_already_done(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        completed: list[int] = []
        group = IOWaitGroup[str](io)
        group.finish("done")
        group.add_done_callback(lambda: completed.append(1))
        assert completed == [1]

    def test_group_attach_sync_completed_waiter_clears_members(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        waiter: IOWaiter[socket.socket] = IOWaiter(io)
        waiter.complete(sock)
        group = IOWaitGroup[socket.socket](io)
        group.attach(waiter, advance=lambda child: group.finish(child.value()))
        assert group._members == set()
        assert group.wait() == sock
        sock.close()

    def test_group_child_value_is_one_shot(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        waiter: IOWaiter[int] = IOWaiter(io)
        group = IOWaitGroup[int](io)
        child = group.attach(waiter, advance=lambda _leg: None)
        waiter.complete(7)
        assert child.value() == 7
        with pytest.raises(InvalidStateError, match="already consumed"):
            child.value()

    def test_group_child_value_not_ready_raises_invalid_state(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        waiter: IOWaiter[int] = IOWaiter(io)
        group = IOWaitGroup[int](io)
        child = group.attach(waiter, advance=lambda _leg: None)
        with pytest.raises(InvalidStateError, match="not ready"):
            child.value()

    def test_group_child_on_cleanup_runs_when_value_not_consumed(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        waiter: IOWaiter[int] = IOWaiter(io)
        seen: list[tuple[bool, int | None]] = []
        group = IOWaitGroup[int](io)
        group.attach(
            waiter,
            on_cleanup=lambda fail, value: seen.append((fail, value)),
            advance=lambda _child: group.finish(0),
        )
        waiter.complete(9)
        del group
        gc.collect()
        assert seen == [(False, 9)]

    def test_group_child_on_cleanup_runs_on_waiter_error(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        waiter: IOWaiter[None] = IOWaiter(io)
        seen: list[tuple[bool, Any]] = []
        group = IOWaitGroup[None](io)
        group.attach(waiter, on_cleanup=lambda fail, value: seen.append((fail, value)))
        waiter.complete(None, OSError("connect failed"))
        with pytest.raises(OSError, match="connect failed"):
            group.wait()
        assert seen == [(True, None)]

    def test_group_chained_attach_completes_group(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        first: IOWaiter[None] = IOWaiter(io)
        second: IOWaiter[None] = IOWaiter(io)
        group = IOWaitGroup[str](io)

        def advance_first(_child: IOWaitGroupChild[None]) -> None:
            group.attach(second, advance=lambda _second: group.finish("done"))

        group.attach(first, advance=advance_first)
        first.complete(None)
        second.complete(None)
        assert group.wait() == "done"

    def test_group_late_advance_after_finish_is_rejected(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        first: IOWaiter[None] = IOWaiter(io)
        group = IOWaitGroup[str](io)

        def advance_first(_child: IOWaitGroupChild[None]) -> None:
            group.finish("done")
            with pytest.raises(RuntimeError, match="IOWaitGroup is closed"):
                group.attach(_pending_waiter(io))

        group.attach(first, advance=advance_first)
        first.complete(None)
        assert group.wait() == "done"

    def test_group_finish_returns_false_after_wait_interrupt(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        group = IOWaitGroup[str](io)
        group.attach(_pending_waiter(io))

        import tealetio.io_waiter as io_waiter_module

        original_swait = io_waiter_module.CrossThreadEvent.swait

        def swait_and_abort(self: Any) -> None:
            raise TimeoutError("abort wait")

        io_waiter_module.CrossThreadEvent.swait = swait_and_abort  # type: ignore[method-assign]
        try:
            with pytest.raises(TimeoutError, match="abort wait"):
                group.wait()
            assert group.finish("late") is False
        finally:
            io_waiter_module.CrossThreadEvent.swait = original_swait

    def test_finish_or_close_socket_closes_on_rejected_delivery(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        group = IOWaitGroup[tuple[socket.socket, bytes]](io)
        conn, _peer = socket.socketpair()
        group._closed = True
        _finish_or_close_socket(group, conn, (conn, b"hi"))
        assert conn.fileno() == -1

    def test_group_wait_survives_finish_during_ready_registration(self) -> None:
        import tealetio.io_waiter as io_waiter_module

        proactor = _MockProactor()
        io = _manager(proactor)
        group = IOWaitGroup[str](io)
        group.attach(_pending_waiter(io))

        original_event = io_waiter_module.CrossThreadEvent

        class RacingEvent(original_event):
            def __init__(self, scheduler: Any) -> None:
                super().__init__(scheduler)
                group.finish("raced")

        io_waiter_module.CrossThreadEvent = RacingEvent  # type: ignore[misc]
        try:
            assert group.wait() == "raced"
        finally:
            io_waiter_module.CrossThreadEvent = original_event

    def test_group_wait_returns_result_when_delivery_races_interrupt(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        group = IOWaitGroup[str](io)
        group.attach(_pending_waiter(io))

        import tealetio.io_waiter as io_waiter_module

        original_swait = io_waiter_module.CrossThreadEvent.swait

        def swait_finish_then_abort(self: Any) -> None:
            group.finish("delivered")
            raise TimeoutError("abort wait")

        io_waiter_module.CrossThreadEvent.swait = swait_finish_then_abort  # type: ignore[method-assign]
        try:
            assert group.wait() == "delivered"
        finally:
            io_waiter_module.CrossThreadEvent.swait = original_swait

    def test_io_waiter_returns_result_when_delivery_races_interrupt(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        listen = _nonblocking_listener()
        pending: list[_PendingOneshot] = []

        def pending_accept(sock: socket.socket, callback) -> object:
            handle = _PendingOneshot(callback)
            pending.append(handle)
            return handle

        proactor.accept = pending_accept  # type: ignore[method-assign]

        import tealetio.io_waiter as io_waiter_module

        original_swait = io_waiter_module.CrossThreadEvent.swait
        waiter = io.sock_accept(listen)

        def swait_complete_then_abort(self: Any) -> None:
            conn, _peer = socket.socketpair()
            pending[0].complete(conn)
            raise TimeoutError("abort wait")

        io_waiter_module.CrossThreadEvent.swait = swait_complete_then_abort  # type: ignore[method-assign]
        try:
            conn, initial = waiter.wait()
            try:
                assert initial is None
                assert conn.fileno() != -1
            finally:
                conn.close()
        finally:
            listen.close()
            io_waiter_module.CrossThreadEvent.swait = original_swait

    def test_group_wait_cancels_active_waiters_on_exception(self) -> None:
        proactor = _MockProactor()
        io = _manager(proactor)
        token = object()
        waiter: IOWaiter[None] = IOWaiter(io)
        waiter.bind(token)
        group = IOWaitGroup[None](io)
        group.attach(waiter)

        import tealetio.io_waiter as io_waiter_module

        original_swait = io_waiter_module.CrossThreadEvent.swait

        def swait_and_abort(self: Any) -> None:
            raise TimeoutError()

        io_waiter_module.CrossThreadEvent.swait = swait_and_abort  # type: ignore[method-assign]
        try:
            with pytest.raises(TimeoutError):
                group.wait()
            assert token in proactor.cancel_nowait_calls
        finally:
            io_waiter_module.CrossThreadEvent.swait = original_swait


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
class TestProactorIOManagerIntegration:
    @pytest.fixture
    def scheduler(self, scheduler_factory) -> SyncProactorScheduler:
        sched = scheduler_factory()
        set_scheduler(sched)
        yield sched
        sched.close()

    def test_scheduler_exposes_io_facade(self, scheduler: SyncProactorScheduler) -> None:
        io = scheduler.io
        _: ServerIO = io
        assert io.proactor is scheduler.proactor

    def test_scheduler_io_forwards_sock_recv(self, scheduler: SyncProactorScheduler) -> None:
        client, server = socket.socketpair()
        try:
            client.setblocking(False)
            server.setblocking(False)
            client.sendall(b"ping")

            def exercise() -> bytes:
                return scheduler.io.sock_recv(server, 4).wait()

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"ping"

            def read_zero() -> bytes:
                return scheduler.io.sock_recv(server, 0).wait()

            assert scheduler.run_until_complete(scheduler.spawn(read_zero)) == b""
        finally:
            client.close()
            server.close()

# -- Manager poll_many / accept_many composition (from test_io_operation_waiters) --

def test_poll_many_marshals_callback_and_sets_closed_on_terminal() -> None:
    delivered: list[int] = []

    class _PollProactor(StubProactor):
        def poll_many(self, fd, mask, callback=None):
            from tealetio.delivery import SelectorCancelHandle

            handle = SelectorCancelHandle(callback)
            handle._emit_result(3, more=True, index=0)
            handle._finish_with_terminal_delivery(MultishotDelivery(index=1, value=0, more=False))
            return handle

        def stop_poll(self, handle, callback):
            callback(None, None)
            return None

    io = ProactorIOManager(StubScheduler(), _PollProactor())  # type: ignore[arg-type]
    handle = io.poll_many(5, 1, lambda delivery: delivered.append(delivery.value))

    assert delivered == [3, 0]
    assert handle.closed is True


def test_accept_many_terminal_error_finishes_waiter() -> None:
    error = OSError("accept failed")
    handler_errors: list[BaseException] = []

    class _AcceptProactor(StubProactor):
        def accept_many(self, sock, callback=None, *, base_sequence: int = 0):
            handle = SelectorCancelHandle(callback)
            handle._finish_with_terminal_delivery(MultishotDelivery(exception=error, more=False))
            return handle

    scheduler = StubScheduler()
    scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
    io = ProactorIOManager(scheduler, _AcceptProactor())  # type: ignore[arg-type]
    server = _nonblocking_listener()
    try:
        waiter = io.accept_many(server, lambda _: None)
        assert handler_errors == []
        assert waiter.done()
        assert waiter.exception() is error
    finally:
        server.close()


def test_accept_many_callback_exception_finishes_terminal_leg() -> None:
    handler_errors: list[BaseException] = []

    class _AcceptProactor(StubProactor):
        def accept_many(self, sock, callback=None, *, base_sequence: int = 0):
            conn, peer = socket.socketpair()
            peer.close()
            handle = SelectorCancelHandle(callback)
            handle._emit_result(conn, more=False)
            return handle

    scheduler = StubScheduler()
    scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
    io = ProactorIOManager(scheduler, _AcceptProactor())  # type: ignore[arg-type]
    server = _nonblocking_listener()
    try:
        waiter = io.accept_many(server, lambda _: (_ for _ in ()).throw(ValueError("accept failed")))
        assert len(handler_errors) == 1
        assert str(handler_errors[0]) == "accept failed"
        assert waiter.done()
        assert waiter.exception() is None
    finally:
        server.close()


def test_accept_many_streams_terminal_error_finishes_waiter() -> None:
    error = OSError("accept failed")
    handler_errors: list[BaseException] = []

    class _AcceptProactor(StubProactor):
        def accept_many(self, sock, callback=None, *, base_sequence: int = 0):
            handle = SelectorCancelHandle(callback)
            handle._finish_with_terminal_delivery(MultishotDelivery(exception=error, more=False))
            return handle

    scheduler = StubScheduler()
    scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
    io = ProactorIOManager(scheduler, _AcceptProactor())  # type: ignore[arg-type]
    server = _nonblocking_listener()
    try:
        waiter = io.accept_many_streams(server, lambda _: None)
        assert handler_errors == []
        assert waiter.done()
        assert waiter.exception() is error
    finally:
        server.close()


def test_accept_many_defers_finish_until_terminal_count() -> None:
    """CountFinalizer defers IOWaiter settle until every sequenced leg has run."""
    error = OSError("accept failed")
    handler_errors: list[BaseException] = []
    user_calls: list[object] = []

    class _AcceptProactor(StubProactor):
        def accept_many(self, sock, callback=None, *, base_sequence: int = 0):
            return SelectorCancelHandle(callback)

    scheduler = StubScheduler()
    scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
    io = ProactorIOManager(scheduler, _AcceptProactor())  # type: ignore[arg-type]
    server = _nonblocking_listener()
    try:
        waiter = io.accept_many(server, user_calls.append)
        handle = waiter._handle
        assert handle is not None
        handle._finish_with_terminal_delivery(MultishotDelivery(index=2, exception=error, more=False))
        assert handler_errors == []
        assert user_calls == []
        assert not waiter.done()

        handle._emit_result(None, index=0, more=True)
        assert not waiter.done()
        handle._emit_result(None, index=1, more=True)
        assert waiter.done()
        assert waiter.exception() is error
        assert user_calls == []
    finally:
        server.close()


def test_accept_many_streams_defers_finish_until_terminal_count() -> None:
    """Same deferred-finish contract as accept_many; stragglers must not open streams."""
    error = OSError("accept failed")
    handler_errors: list[BaseException] = []
    user_calls: list[object] = []

    class _AcceptProactor(StubProactor):
        def accept_many(self, sock, callback=None, *, base_sequence: int = 0):
            return SelectorCancelHandle(callback)

    scheduler = StubScheduler()
    scheduler.set_exception_handler(lambda context: handler_errors.append(context["exception"]))
    io = ProactorIOManager(scheduler, _AcceptProactor())  # type: ignore[arg-type]
    server = _nonblocking_listener()
    try:
        waiter = io.accept_many_streams(server, user_calls.append)
        handle = waiter._handle
        assert handle is not None
        handle._finish_with_terminal_delivery(MultishotDelivery(index=2, exception=error, more=False))
        assert handler_errors == []
        assert user_calls == []
        assert not waiter.done()

        handle._emit_result(None, index=0, more=True)
        assert not waiter.done()
        handle._emit_result(None, index=1, more=True)
        assert waiter.done()
        assert waiter.exception() is error
        assert user_calls == []
    finally:
        server.close()


def test_poll_many_terminal_error_sets_handle_closed() -> None:
    error = OSError("poll failed")
    seen: list[BaseException | None] = []

    class _PollProactor(StubProactor):
        def poll_many(self, fd, mask, callback=None):
            from tealetio.delivery import SelectorCancelHandle

            handle = SelectorCancelHandle(callback)
            handle._finish_with_terminal_delivery(MultishotDelivery(exception=error, more=False))
            return handle

        def stop_poll(self, handle, callback):
            callback(None, None)
            return None

    io = ProactorIOManager(StubScheduler(), _PollProactor())  # type: ignore[arg-type]
    handle = io.poll_many(5, 1, lambda d: seen.append(d.exception))
    assert handle.closed is True
    assert seen == [error]


def test_marshal_continuous_delivery_uses_eager_emit() -> None:
    delivered: list[socket.socket] = []

    class _EagerProactor(StubProactor):
        def accept_many(self, sock, callback=None, *, base_sequence: int = 0):
            conn, peer = socket.socketpair()
            peer.close()
            handle = SelectorCancelHandle(callback)
            handle._emit_result(conn, more=False)
            return handle

    io = ProactorIOManager(StubScheduler(), _EagerProactor())  # type: ignore[arg-type]
    server = _nonblocking_listener()
    try:
        io.accept_many(
            server,
            lambda delivery: delivered.append(delivery[0]),
        )
    finally:
        server.close()

    assert len(delivered) == 1


def test_poll_many_handle_close_is_idempotent_after_terminal() -> None:
    class _PollProactor(StubProactor):
        def poll_many(self, fd, mask, callback=None):
            from tealetio.delivery import SelectorCancelHandle

            handle = SelectorCancelHandle(callback)
            handle._finish_with_terminal_delivery(MultishotDelivery(value=7, more=False))
            return handle

        def stop_poll(self, handle, callback):
            raise AssertionError("close after terminal must not stop_poll")

    io = ProactorIOManager(StubScheduler(), _PollProactor())  # type: ignore[arg-type]
    handle = io.poll_many(5, 1, lambda _delivery: None)
    assert handle.closed is True
    handle.close()
    handle.close()

# -- IOWaiter park (from test_io_waiter_continuous) --

class _WaiterParkProactor(StubProactor):
    pass


def test_iowaiter_wait_parks_on_accept(monkeypatch) -> None:
    io = ProactorIOManager(StubScheduler(), _WaiterParkProactor())  # type: ignore[arg-type]
    scheduled: list[object] = []
    waiter = IOWaiter(io)
    waiter.bind(object())

    class _FakeReady:
        def set(self) -> None:
            scheduled.append("wake")

        def swait(self) -> bool:
            waiter.complete(None, None)
            return True

    monkeypatch.setattr(io_waiter_module, "CrossThreadEvent", lambda _scheduler: _FakeReady())

    waiter._wait_self()

    assert scheduled == ["wake"]
    assert waiter.done()
