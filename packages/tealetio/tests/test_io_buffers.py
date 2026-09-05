from __future__ import annotations

import errno
import socket
import threading
from typing import Any

import pytest
from uring_fakes import SCHEDULER_INTEGRATION_FACTORIES

import tealetio.io_buffers as io_buffers_module
import tealetio.proactor as proactor_module
from tealetio import Event
from tealetio.io_buffers import RECV_MANY_BUFFER_PRESSURE, RecvIterBuffer, SendBuffer
from tealetio.io_waiter import IOWaiter
from tealetio.delivery import MultishotDelivery, OpHandle, SelectorCancelHandle, io_cancellation_error
from tealetio.proactor import SelectorProactor, SyncProactorScheduler
from tealetio.scheduler import set_scheduler


def _held_waiter(io) -> IOWaiter:
    """Unfinished waiter tests complete later via ``complete``."""

    return IOWaiter(io).bind(object())


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
class TestSendBuffer:
    @pytest.fixture
    def scheduler(self, scheduler_factory) -> SyncProactorScheduler:
        sched = scheduler_factory()
        set_scheduler(sched)
        yield sched
        sched.close()

    def test_write_flush_delivers_in_order(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            send_buffer = scheduler.io._open_send_buffer(writer)

            def exercise() -> bytes:
                send_buffer.write(b"ab")
                send_buffer.write(b"cd")
                send_buffer.flush()
                return scheduler.io.sock_recv(reader, 4).wait()

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"abcd"
        finally:
            reader.close()
            writer.close()

    def test_writes_while_busy_coalesce_into_one_leg(self, scheduler: SyncProactorScheduler) -> None:
        """Line-sized writes while a send is in flight join into one next leg."""

        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            first = _held_waiter(scheduler.io)
            real_sendall = scheduler.io.sock_sendall
            seen: list[bytes] = []

            def staged_sendall(sock: socket.socket, data, progress=None) -> IOWaiter[None]:
                del sock, progress
                cargo = bytes(data)
                seen.append(cargo)
                if len(seen) == 1:
                    return first
                waiter = _held_waiter(scheduler.io)
                waiter.complete(None, None)
                return waiter

            scheduler.io.sock_sendall = staged_sendall  # type: ignore[method-assign]
            # min_write=0 so the first write starts a leg immediately
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler, min_write=0)
            send_buffer.write(b"HEAD\n")
            for line in (b"a\n", b"b\n", b"c\n"):
                send_buffer.write(line)
            assert len(seen) == 1
            assert seen[0] == b"HEAD\n"
            # in flight + coalesced backlog
            assert send_buffer.pending_bytes == len(b"HEAD\n") + len(b"a\nb\nc\n")
            first.complete(None, None)
            assert seen == [b"HEAD\n", b"a\nb\nc\n"]
            send_buffer.flush()
            assert send_buffer.pending_bytes == 0
        finally:
            scheduler.io.sock_sendall = real_sendall  # type: ignore[method-assign]
            reader.close()
            writer.close()

    def test_first_write_hands_bytes_object_to_io_without_copy(self, scheduler: SyncProactorScheduler) -> None:
        """Empty backlog takes possession of bytes; take_pending detaches it as-is."""

        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            seen: list[object] = []
            real_sendall = scheduler.io.sock_sendall

            def capture_sendall(sock: socket.socket, data, progress=None):
                del sock, progress
                seen.append(data)
                waiter = _held_waiter(scheduler.io)
                waiter.complete(None, None)
                return waiter

            scheduler.io.sock_sendall = capture_sendall  # type: ignore[method-assign]
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler, min_write=0)
            payload = b"x" * 4096
            send_buffer.write(payload)
            assert len(seen) == 1
            assert seen[0] is payload
        finally:
            scheduler.io.sock_sendall = real_sendall  # type: ignore[method-assign]
            reader.close()
            writer.close()

    def test_mutable_first_write_is_snapshotted_from_caller_buffer(
        self, scheduler: SyncProactorScheduler
    ) -> None:
        """bytearray/memoryview writes must not alias the caller's buffer."""

        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            seen: list[object] = []
            real_sendall = scheduler.io.sock_sendall

            def capture_sendall(sock: socket.socket, data, progress=None):
                del sock, progress
                seen.append(data)
                waiter = _held_waiter(scheduler.io)
                waiter.complete(None, None)
                return waiter

            scheduler.io.sock_sendall = capture_sendall  # type: ignore[method-assign]

            # Idle hold then flush: mutate after write, before the leg is taken.
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler, min_write=1000)
            payload = bytearray(b"hello-world")
            send_buffer.write(payload)
            payload[:] = b"X" * len(payload)
            send_buffer.flush()
            assert len(seen) == 1
            assert seen[0] is not payload
            assert isinstance(seen[0], (bytes, bytearray))
            assert bytes(seen[0]) == b"hello-world"

            # memoryview path: same snapshot contract
            seen.clear()
            mv_src = bytearray(b"view-payload")
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler, min_write=0)
            send_buffer.write(memoryview(mv_src))
            mv_src[:] = b"Y" * len(mv_src)
            assert len(seen) == 1
            assert seen[0] is not mv_src
            assert bytes(seen[0]) == b"view-payload"
        finally:
            scheduler.io.sock_sendall = real_sendall  # type: ignore[method-assign]
            reader.close()
            writer.close()

    def test_on_leg_complete_sends_tiny_backlog(self, scheduler: SyncProactorScheduler) -> None:
        """After a leg completes, any pending ships even below min_write (flush-safe)."""

        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            first = _held_waiter(scheduler.io)
            seen: list[bytes] = []

            def staged_sendall(sock: socket.socket, data, progress=None) -> IOWaiter[None]:
                del sock, progress
                seen.append(bytes(data))
                if len(seen) == 1:
                    return first
                waiter = _held_waiter(scheduler.io)
                waiter.complete(None, None)
                return waiter

            real_sendall = scheduler.io.sock_sendall
            scheduler.io.sock_sendall = staged_sendall  # type: ignore[method-assign]
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler, min_write=1000)
            send_buffer.write(b"x" * 1000)  # starts first leg
            send_buffer.write(b"tiny")  # below min_write; coalesces while busy
            assert len(seen) == 1
            first.complete(None, None)
            assert seen == [b"x" * 1000, b"tiny"]
            assert send_buffer.pending_bytes == 0
        finally:
            scheduler.io.sock_sendall = real_sendall  # type: ignore[method-assign]
            reader.close()
            writer.close()

    def test_idle_writes_hold_until_min_write_or_flush(self, scheduler: SyncProactorScheduler) -> None:
        """Small idle writes must not start a leg until min_write or flush."""

        reader, writer = socket.socketpair()
        try:
            real_sendall = scheduler.io.sock_sendall
            seen: list[bytes] = []

            def capture_sendall(sock: socket.socket, data, progress=None) -> IOWaiter[None]:
                del sock, progress
                cargo = bytes(data)
                seen.append(cargo)
                waiter = _held_waiter(scheduler.io)
                waiter.complete(None, None)
                return waiter

            scheduler.io.sock_sendall = capture_sendall  # type: ignore[method-assign]
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler, min_write=100)
            for _ in range(9):
                send_buffer.write(b"0123456789")
            assert seen == []
            assert send_buffer.pending_bytes == 90
            send_buffer.write(b"0123456789")
            assert seen == [b"0123456789" * 10]
            assert send_buffer.pending_bytes == 0

            send_buffer.write(b"short")
            assert seen == [b"0123456789" * 10]
            assert send_buffer.pending_bytes == 5
            send_buffer.flush()
            assert seen == [b"0123456789" * 10, b"short"]
            assert send_buffer.pending_bytes == 0
        finally:
            scheduler.io.sock_sendall = real_sendall  # type: ignore[method-assign]
            reader.close()
            writer.close()

    def test_flush_waits_for_callback_driven_completion(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler)

            def sender() -> None:
                send_buffer.write(b"hello")
                send_buffer.flush()

            def receiver() -> bytes:
                return scheduler.io.sock_recv(reader, 5).wait()

            scheduler.spawn(sender)
            assert scheduler.run_until_complete(scheduler.spawn(receiver)) == b"hello"
        finally:
            reader.close()
            writer.close()

    def test_drain_returns_immediately_below_high_water(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            pending = _held_waiter(scheduler.io)
            real_sendall = scheduler.io.sock_sendall

            def pending_sendall(sock: socket.socket, data, progress=None) -> IOWaiter[None]:
                del data, progress
                return pending

            scheduler.io.sock_sendall = pending_sendall  # type: ignore[method-assign]
            send_buffer = SendBuffer(
                sock=writer,
                io=scheduler.io,
                scheduler=scheduler,
                high_water=1024,
                low_water=256,
                min_write=0,
            )
            send_buffer.write(b"x" * 100)
            send_buffer.drain()
            assert send_buffer.pending_bytes == 100
            pending.complete(None, None)
            send_buffer.flush()
            assert send_buffer.pending_bytes == 0
        finally:
            scheduler.io.sock_sendall = real_sendall  # type: ignore[method-assign]
            reader.close()
            writer.close()

    def test_drain_blocks_until_low_water(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            pending_ops: list[IOWaiter] = []
            real_sendall = scheduler.io.sock_sendall

            def staged_sendall(sock: socket.socket, data, progress=None) -> IOWaiter[None]:
                del progress
                waiter = _held_waiter(scheduler.io)
                pending_ops.append(waiter)
                return waiter

            scheduler.io.sock_sendall = staged_sendall  # type: ignore[method-assign]
            send_buffer = SendBuffer(
                sock=writer,
                io=scheduler.io,
                scheduler=scheduler,
                high_water=10,
                low_water=2,
                min_write=0,
            )

            def exercise() -> None:
                send_buffer.write(b"123456")
                send_buffer.write(b"123456")
                assert send_buffer.pending_bytes == 12

                started = Event()
                done = Event()

                def drainer() -> None:
                    started.set()
                    send_buffer.drain()
                    done.set()

                scheduler.spawn(drainer)
                started.swait()
                assert not done.is_set()
                assert len(pending_ops) == 1

                pending_ops[0].complete(None, None)
                assert not done.is_set()
                assert send_buffer.pending_bytes == 6

                pending_ops[1].complete(None, None)
                done.swait()
                assert send_buffer.pending_bytes == 0

            scheduler.run_until_complete(scheduler.spawn(exercise))
        finally:
            scheduler.io.sock_sendall = real_sendall  # type: ignore[method-assign]
            reader.close()
            writer.close()

    def test_pending_bytes_include_in_flight_chunk(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            pending = _held_waiter(scheduler.io)
            real_sendall = scheduler.io.sock_sendall

            def pending_sendall(sock: socket.socket, data, progress=None) -> IOWaiter[None]:
                del data, progress
                return pending

            scheduler.io.sock_sendall = pending_sendall  # type: ignore[method-assign]
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler, min_write=0)
            send_buffer.write(b"hello")
            assert send_buffer.pending_bytes == 5
            pending.complete(None, None)
            send_buffer.flush()
            assert send_buffer.pending_bytes == 0
        finally:
            scheduler.io.sock_sendall = real_sendall  # type: ignore[method-assign]
            reader.close()
            writer.close()

    def test_set_write_buffer_limits_rejects_invalid_pair(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler)
            with pytest.raises(ValueError, match="high"):
                send_buffer.set_write_buffer_limits(high=4, low=8)
        finally:
            reader.close()
            writer.close()

    def test_write_eof_defers_shutdown_until_queue_drains(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            pending_ops: list[IOWaiter] = []
            real_sendall = scheduler.io.sock_sendall
            real_shutdown = scheduler.io.sock_shutdown

            def staged_sendall(sock: socket.socket, data, progress=None) -> IOWaiter[None]:
                del progress
                waiter = _held_waiter(scheduler.io)
                pending_ops.append(waiter)
                return waiter

            shutdown_calls: list[int] = []

            def track_shutdown(sock: socket.socket, how: int):
                shutdown_calls.append(how)
                return real_shutdown(sock, how)

            scheduler.io.sock_sendall = staged_sendall  # type: ignore[method-assign]
            scheduler.io.sock_shutdown = track_shutdown  # type: ignore[method-assign]
            # default min_write holds "ab"; write_eof must force-send before SHUT_WR
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler)

            def exercise() -> None:
                send_buffer.write(b"ab")
                assert not pending_ops
                send_buffer.write_eof()
                assert send_buffer.eof_pending
                assert not send_buffer.write_eof_done
                assert shutdown_calls == []
                assert len(pending_ops) == 1
                pending_ops[0].complete(None, None)
                assert send_buffer.write_eof_done
                assert shutdown_calls == [socket.SHUT_WR]

            scheduler.run_until_complete(scheduler.spawn(exercise))
        finally:
            scheduler.io.sock_sendall = real_sendall  # type: ignore[method-assign]
            scheduler.io.sock_shutdown = real_shutdown  # type: ignore[method-assign]
            reader.close()
            writer.close()

    def test_write_eof_on_idle_socket_shuts_down_immediately(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            shutdown_calls: list[int] = []
            real_shutdown = scheduler.io.sock_shutdown

            def track_shutdown(sock: socket.socket, how: int):
                shutdown_calls.append(how)
                return real_shutdown(sock, how)

            scheduler.io.sock_shutdown = track_shutdown  # type: ignore[method-assign]
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler)

            def exercise() -> None:
                send_buffer.write_eof()
                assert send_buffer.write_eof_done
                assert shutdown_calls == [socket.SHUT_WR]

            scheduler.run_until_complete(scheduler.spawn(exercise))
        finally:
            scheduler.io.sock_shutdown = real_shutdown  # type: ignore[method-assign]
            reader.close()
            writer.close()

    def test_write_eof_is_idempotent(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            shutdown_calls: list[int] = []
            real_shutdown = scheduler.io.sock_shutdown

            def track_shutdown(sock: socket.socket, how: int):
                shutdown_calls.append(how)
                return real_shutdown(sock, how)

            scheduler.io.sock_shutdown = track_shutdown  # type: ignore[method-assign]
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler)

            def exercise() -> None:
                send_buffer.write_eof()
                send_buffer.write_eof()
                assert send_buffer.write_eof_done
                assert shutdown_calls == [socket.SHUT_WR]

            scheduler.run_until_complete(scheduler.spawn(exercise))
        finally:
            scheduler.io.sock_shutdown = real_shutdown  # type: ignore[method-assign]
            reader.close()
            writer.close()

    def test_write_after_write_eof_raises(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler)

            def exercise() -> None:
                send_buffer.write_eof()

            scheduler.run_until_complete(scheduler.spawn(exercise))
            with pytest.raises(RuntimeError, match="write\\(\\) after write_eof"):
                send_buffer.write(b"x")
        finally:
            reader.close()
            writer.close()

    def test_write_submit_failure_restores_pending_chunk(self, scheduler: SyncProactorScheduler) -> None:
        _reader, writer = socket.socketpair()
        try:
            real_sendall = scheduler.io.sock_sendall

            def raising_sendall(sock: socket.socket, data, progress=None) -> IOWaiter[None]:
                del sock, data, progress
                raise OSError("submit failed")

            scheduler.io.sock_sendall = raising_sendall  # type: ignore[method-assign]
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler, min_write=0)

            with pytest.raises(OSError, match="submit failed"):
                send_buffer.write(b"ab")
            assert send_buffer.pending_bytes == 2
            assert not send_buffer._active
        finally:
            scheduler.io.sock_sendall = real_sendall  # type: ignore[method-assign]
            writer.close()

    def test_chained_submit_failure_restores_pending_and_sticks_error(self, scheduler: SyncProactorScheduler) -> None:
        _reader, writer = socket.socketpair()
        try:
            pending_ops: list[IOWaiter] = []
            submit_calls = 0
            real_sendall = scheduler.io.sock_sendall

            def staged_sendall(sock: socket.socket, data, progress=None) -> IOWaiter[None]:
                nonlocal submit_calls
                submit_calls += 1
                if submit_calls == 1:
                    waiter = _held_waiter(scheduler.io)
                    pending_ops.append(waiter)
                    return waiter
                raise OSError("chained submit failed")

            scheduler.io.sock_sendall = staged_sendall  # type: ignore[method-assign]
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler, min_write=0)
            send_buffer.write(b"ab")
            send_buffer.write(b"cd")
            with pytest.raises(OSError, match="chained submit failed"):
                pending_ops[0].complete(None, None)
            assert send_buffer.pending_bytes == 2
            with pytest.raises(OSError, match="chained submit failed"):
                send_buffer.flush()
        finally:
            scheduler.io.sock_sendall = real_sendall  # type: ignore[method-assign]
            writer.close()

    def test_nested_eager_complete_does_not_reprepend_already_sent_chunk(
        self, scheduler: SyncProactorScheduler
    ) -> None:
        """Eager IOWaiterSync runs _on_leg_complete nested; do not restore sent bytes."""

        from tealetio.io_waiter import IOWaiterSync

        _reader, writer = socket.socketpair()
        real_sendall = scheduler.io.sock_sendall
        real_complete = SendBuffer._on_leg_complete
        try:

            def sync_sendall(sock: socket.socket, data, progress=None):
                del sock, data, progress
                return IOWaiterSync(None)

            def boom_complete(self: SendBuffer) -> None:
                real_complete(self)
                raise RuntimeError("handler failed")

            scheduler.io.sock_sendall = sync_sendall  # type: ignore[method-assign]
            SendBuffer._on_leg_complete = boom_complete  # type: ignore[method-assign]
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler, min_write=0)
            with pytest.raises(RuntimeError, match="handler failed"):
                send_buffer.write(b"ab")
            # bytes were accepted by sock_sendall; must not re-queue them
            assert send_buffer.pending_bytes == 0
            assert send_buffer._pending is None
        finally:
            scheduler.io.sock_sendall = real_sendall  # type: ignore[method-assign]
            SendBuffer._on_leg_complete = real_complete  # type: ignore[method-assign]
            writer.close()

    def test_close_drains_pending_after_in_flight_leg(self, scheduler: SyncProactorScheduler) -> None:
        _reader, writer = socket.socketpair()
        try:
            writer.setblocking(False)
            pending_ops: list[IOWaiter] = []
            real_sendall = scheduler.io.sock_sendall

            def staged_sendall(sock: socket.socket, data, progress=None) -> IOWaiter[None]:
                del progress
                waiter = _held_waiter(scheduler.io)
                pending_ops.append(waiter)
                return waiter

            scheduler.io.sock_sendall = staged_sendall  # type: ignore[method-assign]
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler, min_write=0)

            def exercise() -> None:
                send_buffer.write(b"ab")
                send_buffer.write(b"cd")
                send_buffer.close()
                assert len(pending_ops) == 1
                pending_ops[0].complete(None, None)
                assert len(pending_ops) == 2
                pending_ops[1].complete(None, None)
                send_buffer.flush()
                assert send_buffer.pending_bytes == 0

            scheduler.run_until_complete(scheduler.spawn(exercise))
        finally:
            scheduler.io.sock_sendall = real_sendall  # type: ignore[method-assign]
            writer.close()

    def test_write_after_close_raises(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            send_buffer = scheduler.io._open_send_buffer(writer)
            send_buffer.close()
            with pytest.raises(RuntimeError, match="SendBuffer is closed"):
                send_buffer.write(b"x")
        finally:
            reader.close()
            writer.close()

    def test_cancelled_send_sets_sticky_error(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            pending = _held_waiter(scheduler.io)
            real_sendall = scheduler.io.sock_sendall

            def pending_sendall(sock: socket.socket, data, progress=None) -> IOWaiter[None]:
                del data, progress
                return pending

            scheduler.io.sock_sendall = pending_sendall  # type: ignore[method-assign]
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler, min_write=0)

            def sender() -> None:
                send_buffer.write(b"hello")
                pending.complete(None, io_cancellation_error())
                send_buffer.flush()

            with pytest.raises(OSError) as exc_info:
                scheduler.run_until_complete(scheduler.spawn(sender))
            assert exc_info.value.errno == errno.ECANCELED
            with pytest.raises(OSError) as exc_info:
                send_buffer.write(b"again")
            assert exc_info.value.errno == errno.ECANCELED
        finally:
            scheduler.io.sock_sendall = real_sendall  # type: ignore[method-assign]
            reader.close()
            writer.close()

    def test_sticky_send_error_propagates(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            send_buffer = scheduler.io._open_send_buffer(writer)
            writer.close()

            def sender() -> None:
                send_buffer.write(b"hello")
                send_buffer.flush()

            with pytest.raises((OSError, ValueError)):
                scheduler.run_until_complete(scheduler.spawn(sender))
            with pytest.raises((OSError, ValueError)):
                send_buffer.write(b"again")
        finally:
            reader.close()

# -- RecvIterBuffer (moved from test_proactor) --

_RecvManySeen = MultishotDelivery


def _recv_chunk(index: int, data: bytes, *, more: bool = True) -> _RecvManySeen:
    return MultishotDelivery(index=index, value=memoryview(data), more=more)


def _enobufs_chunk(leg_index: int = 0) -> _RecvManySeen:
    return MultishotDelivery(
        index=leg_index,
        value=memoryview(b""),
        exception=OSError(errno.ENOBUFS, errno.errorcode.get(errno.ENOBUFS, "no buffer space")),
        more=False,
    )


def _recv_error_chunk(index: int, exc: BaseException, *, more: bool = False) -> _RecvManySeen:
    return MultishotDelivery(index=index, exception=exc, more=more)


_RECVITER_TEST_SOCK = socket.socketpair()[0]



class _RecvIterTestPool:
    buffer_size = 16 * 1024
    buffer_count = 8
    leased_count = 0
    release_callback = None

    def close(self) -> None:
        if self.release_callback is not None:
            self.release_callback(self)


def _recviter_test_pool() -> _RecvIterTestPool:
    return _RecvIterTestPool()


class _RecvIterTestProactor:
    def __init__(self) -> None:
        self.recv_many_bases: list[int] = []

    def recv_many(
        self,
        sock: socket.socket,
        callback: Any,
        *,
        buf_group: Any,
        base_sequence: int = 0,
    ) -> OpHandle:
        del sock, buf_group
        self.recv_many_bases.append(base_sequence)
        handle = SelectorCancelHandle(callback, base_sequence=base_sequence)
        return handle

    def cancel(self, handle: Any, callback) -> None:
        handle._finish_with_terminal_delivery(
            MultishotDelivery(
                index=handle._next_index,
                exception=io_cancellation_error(),
                more=False,
            )
        )
        callback(None, None)

    def cancel_nowait(self, handle: Any) -> None:
        self.cancel(handle, lambda *_: None)


def _recviter_test_proactor() -> _RecvIterTestProactor:
    return _RecvIterTestProactor()


def _recviter_buffer(*, proactor: _RecvIterTestProactor, buffer_pool: Any) -> io_buffers_module.RecvIterBuffer:
    return io_buffers_module.RecvIterBuffer(sock=_RECVITER_TEST_SOCK, proactor=proactor, buffer_pool=buffer_pool)


def _iter_recv_stream(stream: Any):
    yield from stream


def _recviter_bytes(stream: Any) -> list[tuple[int, bytes]]:
    seen: list[tuple[int, bytes]] = []
    for index, chunk in _iter_recv_stream(stream):
        if index < 0:
            continue
        seen.append((index, bytes(chunk)))
        if type(chunk) is memoryview:
            chunk.release()
    return seen


def _assert_recviter_pressure(item: tuple[int, Any] | None) -> None:
    assert item is not None
    index, chunk = item
    assert index == RECV_MANY_BUFFER_PRESSURE
    assert type(chunk) is memoryview
    assert len(chunk) == 0


def _exercise_recviter_buffer(exercise: Any) -> Any:
    scheduler = SyncProactorScheduler()
    set_scheduler(scheduler)
    try:
        return scheduler.run_until_complete(scheduler.spawn(exercise))
    finally:
        scheduler.close()


@pytest.mark.skipif(
    not proactor_module._supports_release_buffer(), reason="leased selector chunks require Python 3.12+"
)

def test_recviter_buffer_reorders_out_of_order_chunks():
    def exercise() -> list[tuple[int, memoryview | None]]:
        buffer = io_buffers_module.RecvIterBuffer(
            sock=_RECVITER_TEST_SOCK, proactor=_recviter_test_proactor(), buffer_pool=_recviter_test_pool()
        )
        buffer.on_result(_recv_chunk(1, b"b"))
        buffer.on_result(_recv_chunk(0, b"a"))
        return [buffer.take_next(), buffer.take_next()]

    first, second = _exercise_recviter_buffer(exercise)
    assert first is not None and first[0] == 0 and bytes(first[1]) == b"a"
    assert second is not None and second[0] == 1 and bytes(second[1]) == b"b"


def test_recviter_buffer_resume_waits_until_low_water_mark():
    class _Pool:
        release_callback = None

        def close(self) -> None:
            if self.release_callback is not None:
                self.release_callback(self)

        buffer_count = 4
        leased_count = 4

        def note_chunk_released(self) -> None:
            if self.leased_count:
                self.leased_count -= 1

    def exercise() -> list[int]:
        proactor = _recviter_test_proactor()
        pool = _Pool()
        buffer = _recviter_buffer(proactor=proactor, buffer_pool=pool)
        buffer.on_result(_recv_chunk(0, b"a"))
        buffer.on_result(_recv_chunk(1, b"b"))
        buffer.on_result(_enobufs_chunk(2))
        _assert_recviter_pressure(buffer.take_next())
        buffer.consume_pressure_resume()
        assert proactor.recv_many_bases == [0]
        first = buffer.take_next()
        assert first is not None and first[0] == 0
        pool.note_chunk_released()
        assert proactor.recv_many_bases == [0]
        second = buffer.take_next()
        assert second is not None and second[0] == 1
        pool.note_chunk_released()
        assert proactor.recv_many_bases == [0]
        pool.note_chunk_released()
        buffer.consume_pressure_resume()
        assert proactor.recv_many_bases == [0, 2]
        buffer.on_result(_recv_chunk(2, b"", more=False))
        assert buffer.take_next() is None
        return proactor.recv_many_bases

    assert _exercise_recviter_buffer(exercise) == [0, 2]


def test_recviter_buffer_enobufs_finishes_recv_many_leg():
    def exercise() -> bool:
        proactor = _recviter_test_proactor()
        buffer = _recviter_buffer(proactor=proactor, buffer_pool=_recviter_test_pool())
        operation = buffer._current_operation
        assert operation is not None
        buffer.on_result(_enobufs_chunk(0))
        return buffer._current_operation is None

    assert _exercise_recviter_buffer(exercise)


def test_recviter_buffer_close_cancel_finishes_before_heaped_straggler():
    """Live-op close cancels at the next expected index; first take_next is ECANCELED.

    A heaped later chunk is flushed after that numeric cancel into ``_ready``.
    Calling ``take_next`` again after the raise is undefined.
    """

    def exercise() -> None:
        buffer = io_buffers_module.RecvIterBuffer(
            sock=_RECVITER_TEST_SOCK, proactor=_recviter_test_proactor(), buffer_pool=_recviter_test_pool()
        )
        buffer.on_result(_recv_chunk(1, b"straggler"))
        buffer.close()
        try:
            item = buffer.take_next()
        except OSError as exc:
            assert exc.errno == errno.ECANCELED
            return
        raise AssertionError(f"expected ECANCELED from close cancel, got {item!r}")

    _exercise_recviter_buffer(exercise)


def test_recviter_buffer_close_after_enobufs_posts_sequenced_cancel():
    """No live op after ENOBUFS: close posts ECANCELED at the next expected index."""

    def exercise() -> None:
        buffer = io_buffers_module.RecvIterBuffer(
            sock=_RECVITER_TEST_SOCK, proactor=_recviter_test_proactor(), buffer_pool=_recviter_test_pool()
        )
        buffer.on_result(_recv_chunk(0, b"a"))
        buffer.on_result(_enobufs_chunk(1))
        assert buffer._current_operation is None
        _assert_recviter_pressure(buffer.take_next())
        buffer.close()
        first = buffer.take_next()
        assert first is not None and first[0] == 0 and bytes(first[1]) == b"a"
        try:
            item = buffer.take_next()
        except OSError as exc:
            assert exc.errno == errno.ECANCELED
            return
        raise AssertionError(f"expected sequenced ECANCELED after ENOBUFS close, got {item!r}")

    _exercise_recviter_buffer(exercise)


def test_recviter_buffer_close_during_recv_many_install_cancels_returned_op():
    """Close while recv_many is on the stack cancels the returned op after it returns."""

    def exercise() -> None:
        proactor = _recviter_test_proactor()
        cancelled: list[object] = []
        orig_cancel = proactor.cancel_nowait

        def track_cancel(operation: Any) -> None:
            cancelled.append(operation)
            orig_cancel(operation)

        proactor.cancel_nowait = track_cancel  # type: ignore[method-assign]
        buffer = _recviter_buffer(proactor=proactor, buffer_pool=_recviter_test_pool())
        orig_recv_many = buffer._recv_many

        def recv_and_close(*args: Any, **kwargs: Any):
            buffer.close()
            return orig_recv_many(*args, **kwargs)

        buffer._recv_many = recv_and_close
        buffer.on_result(_recv_chunk(0, b"a"))
        buffer.on_result(_enobufs_chunk(1))
        _assert_recviter_pressure(buffer.take_next())
        first = buffer.take_next()
        assert first is not None and first[0] == 0 and bytes(first[1]) == b"a"
        assert buffer._closed
        assert len(cancelled) == 1
        try:
            item = buffer.take_next()
        except OSError as exc:
            assert exc.errno == errno.ECANCELED
            return
        raise AssertionError(f"expected ECANCELED after install-close, got {item!r}")

    _exercise_recviter_buffer(exercise)


def test_recviter_buffer_close_wakes_take_next_after_leg_finished():
    """close still cancels when the last leg finished but the stream has not."""

    def exercise() -> None:
        proactor = _recviter_test_proactor()
        buffer = _recviter_buffer(proactor=proactor, buffer_pool=_recviter_test_pool())
        operation = buffer._current_operation
        assert operation is not None
        buffer.on_result(_recv_chunk(0, b"x", more=False))
        buffer.close()
        first = buffer.take_next()
        assert first is not None and first[0] == 0 and bytes(first[1]) == b"x"
        try:
            buffer.take_next()
        except OSError as exc:
            assert exc.errno == errno.ECANCELED
            return
        raise AssertionError("expected ECANCELED after draining post-close data")

    _exercise_recviter_buffer(exercise)


def test_recviter_buffer_enobufs_when_closed_delivers_cancel():
    def exercise() -> tuple[MultishotDelivery | None, bool]:
        buffer = io_buffers_module.RecvIterBuffer(
            sock=_RECVITER_TEST_SOCK, proactor=_recviter_test_proactor(), buffer_pool=_recviter_test_pool()
        )
        buffer._closed = True
        buffer.on_result(_enobufs_chunk(0))
        ready_item = buffer._ready[0] if buffer._ready else None
        return ready_item, buffer._pressure_pending

    delivery, pressure_pending = _exercise_recviter_buffer(exercise)
    assert delivery is not None
    assert delivery.exception is not None
    assert delivery.exception.errno == errno.ECANCELED
    assert not pressure_pending


def test_recviter_buffer_close_prevents_pressure_resume_resubmit():
    class _Pool:
        release_callback = None

        def close(self) -> None:
            if self.release_callback is not None:
                self.release_callback(self)

        buffer_count = 4
        leased_count = 0

    def exercise() -> list[int]:
        proactor = _recviter_test_proactor()
        pool = _Pool()
        buffer = _recviter_buffer(proactor=proactor, buffer_pool=pool)
        buffer.on_result(_recv_chunk(0, b"a", more=False))
        assert buffer.take_next() is not None
        buffer.close()
        buffer.consume_pressure_resume()
        return list(proactor.recv_many_bases)

    assert _exercise_recviter_buffer(exercise) == [0, 1]


def test_recviter_buffer_post_close_data_terminal_does_not_schedule_resubmit():
    """Late more=False-with-data after close must not arm resubmit bookkeeping."""

    def exercise() -> tuple[int, bool]:
        proactor = _recviter_test_proactor()
        buffer = _recviter_buffer(proactor=proactor, buffer_pool=_recviter_test_pool())
        buffer.close()
        try:
            buffer.take_next()
        except OSError as exc:
            assert exc.errno == errno.ECANCELED
        next_base = buffer._next_base
        current = buffer._current_operation
        # straggler terminal with data (would resubmit if open)
        buffer.on_result(_recv_chunk(0, b"late", more=False))
        same_current = buffer._current_operation is current
        return buffer._next_base - next_base, same_current

    delta, same_current = _exercise_recviter_buffer(exercise)
    assert delta == 0
    assert same_current is True


def test_recviter_buffer_start_recv_many_after_close_is_noop():
    class _Pool:
        release_callback = None

        def close(self) -> None:
            if self.release_callback is not None:
                self.release_callback(self)

        buffer_count = 4
        leased_count = 0

    def exercise() -> list[int]:
        proactor = _recviter_test_proactor()
        pool = _Pool()
        buffer = _recviter_buffer(proactor=proactor, buffer_pool=pool)
        buffer.on_result(_recv_chunk(0, b"a", more=False))
        assert buffer.take_next() is not None
        buffer.close()
        buffer._start_recv_many(base_sequence=9)
        return list(proactor.recv_many_bases)

    assert _exercise_recviter_buffer(exercise) == [0, 1]


def test_recviter_buffer_pressure_token_precedes_queued_views():
    def exercise() -> list[tuple[int, memoryview | None] | None]:
        buffer = io_buffers_module.RecvIterBuffer(
            sock=_RECVITER_TEST_SOCK, proactor=_recviter_test_proactor(), buffer_pool=_recviter_test_pool()
        )
        buffer.on_result(_recv_chunk(0, b"a"))
        buffer.on_result(_recv_chunk(1, b"b"))
        buffer.on_result(_enobufs_chunk(2))
        return [buffer.take_next(), buffer.take_next(), buffer.take_next()]

    token, first, second = _exercise_recviter_buffer(exercise)
    _assert_recviter_pressure(token)
    assert first is not None and first[0] == 0 and bytes(first[1]) == b"a"
    assert second is not None and second[0] == 1 and bytes(second[1]) == b"b"


def test_recviter_buffer_eof_stops_iteration():
    def exercise() -> list[tuple[int, memoryview | None] | None]:
        buffer = io_buffers_module.RecvIterBuffer(
            sock=_RECVITER_TEST_SOCK, proactor=_recviter_test_proactor(), buffer_pool=_recviter_test_pool()
        )
        buffer.on_result(_recv_chunk(0, b"done"))
        buffer.on_result(_recv_chunk(1, b"", more=False))
        return [buffer.take_next(), buffer.take_next()]

    first, second = _exercise_recviter_buffer(exercise)
    assert first is not None and first[0] == 0 and bytes(first[1]) == b"done"
    assert second is None


def test_recviter_buffer_delivers_buffered_chunks_before_stream_error():
    def exercise() -> list[object]:
        buffer = io_buffers_module.RecvIterBuffer(
            sock=_RECVITER_TEST_SOCK, proactor=_recviter_test_proactor(), buffer_pool=_recviter_test_pool()
        )
        buffer.on_result(_recv_chunk(0, b"a"))
        buffer.on_result(_recv_chunk(1, b"b"))
        buffer.on_result(_recv_error_chunk(2, OSError("recv failed")))
        results: list[object] = [buffer.take_next(), buffer.take_next()]
        try:
            buffer.take_next()
        except OSError as exc:
            results.append(exc)
        else:
            results.append(None)
        return results

    first, second, third = _exercise_recviter_buffer(exercise)
    assert first is not None and first[0] == 0 and bytes(first[1]) == b"a"
    assert second is not None and second[0] == 1 and bytes(second[1]) == b"b"
    assert isinstance(third, OSError)
    assert str(third) == "recv failed"


def test_recviter_buffer_yields_memoryviews():
    def exercise() -> tuple[int, memoryview | None] | None:
        buffer = io_buffers_module.RecvIterBuffer(
            sock=_RECVITER_TEST_SOCK, proactor=_recviter_test_proactor(), buffer_pool=_recviter_test_pool()
        )
        buffer.on_result(_recv_chunk(0, b"a"))
        return buffer.take_next()

    item = _exercise_recviter_buffer(exercise)
    assert item is not None
    index, chunk = item
    assert index == 0
    assert type(chunk) is memoryview
    assert bytes(chunk) == b"a"


def test_recviter_buffer_take_next_waits_for_cross_thread_delivery(monkeypatch):
    """Regression: recv completion threads must wake a blocked take_next()."""

    ready_to_wait = threading.Event()

    def exercise() -> tuple[int, memoryview]:
        buffer = io_buffers_module.RecvIterBuffer(
            sock=_RECVITER_TEST_SOCK, proactor=_recviter_test_proactor(), buffer_pool=_recviter_test_pool()
        )
        real_swait = buffer._pevent.swait

        def swait_and_signal() -> bool:
            ready_to_wait.set()
            return real_swait()

        monkeypatch.setattr(buffer._pevent, "swait", swait_and_signal)

        def producer() -> None:
            assert ready_to_wait.wait(timeout=1.0)
            buffer.on_result(_recv_chunk(0, b"late"))

        threading.Thread(target=producer, daemon=True).start()
        item = buffer.take_next()
        assert item is not None
        index, chunk = item
        assert type(chunk) is memoryview
        return index, chunk

    index, chunk = _exercise_recviter_buffer(exercise)
    assert index == 0
    assert bytes(chunk) == b"late"


def test_recviter_buffer_resumes_on_pressure_while_waiting(monkeypatch):
    """Regression: ENOBUFS while blocked must start a fresh recv_many when no views remain."""

    ready_to_wait = threading.Event()

    class _Pool:
        release_callback = None

        def close(self) -> None:
            if self.release_callback is not None:
                self.release_callback(self)

        buffer_count = 4
        leased_count = 1

        def note_chunk_released(self) -> None:
            if self.leased_count:
                self.leased_count -= 1

    def exercise() -> tuple[tuple[int, memoryview], list[int]]:
        proactor = _recviter_test_proactor()
        pool = _Pool()
        buffer = _recviter_buffer(proactor=proactor, buffer_pool=pool)

        buffer.on_result(_recv_chunk(0, b"a"))
        first = buffer.take_next()
        assert first is not None and first[0] == 0 and bytes(first[1]) == b"a"

        real_swait = buffer._pevent.swait

        def swait_and_signal() -> bool:
            ready_to_wait.set()
            return real_swait()

        monkeypatch.setattr(buffer._pevent, "swait", swait_and_signal)

        def producer() -> None:
            assert ready_to_wait.wait(timeout=1.0)
            buffer.on_result(_enobufs_chunk(1))

        threading.Thread(target=producer, daemon=True).start()
        pressure = buffer.take_next()
        _assert_recviter_pressure(pressure)
        pool.note_chunk_released()
        buffer.consume_pressure_resume()
        assert proactor.recv_many_bases == [0, 1]
        buffer.on_result(_recv_chunk(1, b"b"))
        second = buffer.take_next()
        assert second is not None and second[0] == 1 and bytes(second[1]) == b"b"
        return second, proactor.recv_many_bases

    second, bases = _exercise_recviter_buffer(exercise)
    assert second[0] == 1 and bytes(second[1]) == b"b"
    assert bases == [0, 1]


def test_recviter_buffer_single_slot_pool_requires_one_free_before_resume():
    class _Pool:
        release_callback = None

        def close(self) -> None:
            if self.release_callback is not None:
                self.release_callback(self)

        buffer_count = 1
        leased_count = 1

        def note_chunk_released(self) -> None:
            if self.leased_count:
                self.leased_count -= 1

    def exercise() -> list[int]:
        proactor = _recviter_test_proactor()
        pool = _Pool()
        buffer = _recviter_buffer(proactor=proactor, buffer_pool=pool)
        buffer.on_result(_recv_chunk(0, b"a"))
        buffer.on_result(_enobufs_chunk(1))
        first = buffer.take_next()
        _assert_recviter_pressure(first)
        second = buffer.take_next()
        assert second is not None and second[0] == 0
        pool.note_chunk_released()
        buffer.consume_pressure_resume()
        assert proactor.recv_many_bases == [0, 1]
        buffer.on_result(_recv_chunk(1, b"", more=False))
        assert buffer.take_next() is None
        return proactor.recv_many_bases

    assert _exercise_recviter_buffer(exercise) == [0, 1]


def test_recviter_buffer_resumes_when_low_water_mark_reached():
    class _Pool:
        release_callback = None

        def close(self) -> None:
            if self.release_callback is not None:
                self.release_callback(self)

        buffer_count = 4
        leased_count = 4

        def note_chunk_released(self) -> None:
            if self.leased_count:
                self.leased_count -= 1

    def exercise() -> list[int]:
        proactor = _recviter_test_proactor()
        pool = _Pool()
        buffer = _recviter_buffer(proactor=proactor, buffer_pool=pool)
        buffer.on_result(_recv_chunk(0, b"a"))
        buffer.on_result(_recv_chunk(1, b"b"))
        buffer.on_result(_enobufs_chunk(2))
        token = buffer.take_next()
        _assert_recviter_pressure(token)
        first = buffer.take_next()
        assert first is not None and first[0] == 0
        pool.note_chunk_released()
        assert proactor.recv_many_bases == [0]
        second = buffer.take_next()
        assert second is not None and second[0] == 1
        pool.note_chunk_released()
        assert proactor.recv_many_bases == [0]
        pool.note_chunk_released()
        buffer.consume_pressure_resume()
        assert proactor.recv_many_bases == [0, 2]
        buffer.on_result(_recv_chunk(2, b"", more=False))
        assert buffer.take_next() is None
        return proactor.recv_many_bases

    assert _exercise_recviter_buffer(exercise) == [0, 2]


def test_recviter_buffer_defers_resume_until_all_queued_chunks_yielded():
    class _Pool:
        release_callback = None

        def close(self) -> None:
            if self.release_callback is not None:
                self.release_callback(self)

        buffer_count = 4
        leased_count = 4

        def note_chunk_released(self) -> None:
            if self.leased_count:
                self.leased_count -= 1

    def exercise() -> tuple[list[tuple[int, memoryview]], list[int]]:
        proactor = _recviter_test_proactor()
        pool = _Pool()
        buffer = _recviter_buffer(proactor=proactor, buffer_pool=pool)
        buffer.on_result(_recv_chunk(0, b"a"))
        buffer.on_result(_recv_chunk(1, b"b"))
        buffer.on_result(_enobufs_chunk(2))
        token = buffer.take_next()
        _assert_recviter_pressure(token)
        assert proactor.recv_many_bases == [0]
        first = buffer.take_next()
        assert first is not None and first[0] == 0 and bytes(first[1]) == b"a"
        pool.note_chunk_released()
        assert proactor.recv_many_bases == [0]
        second = buffer.take_next()
        assert second is not None and second[0] == 1 and bytes(second[1]) == b"b"
        pool.note_chunk_released()
        assert proactor.recv_many_bases == [0]
        pool.note_chunk_released()
        buffer.consume_pressure_resume()
        assert proactor.recv_many_bases == [0, 2]
        buffer.on_result(_recv_chunk(2, b"", more=False))
        eof = buffer.take_next()
        assert eof is None
        return [first, second], proactor.recv_many_bases

    chunks, bases = _exercise_recviter_buffer(exercise)
    assert [(index, bytes(chunk)) for index, chunk in chunks] == [(0, b"a"), (1, b"b")]
    assert bases == [0, 2]


def test_recviter_buffer_defers_resume_until_next_take_after_yielding_chunk():
    class _Pool:
        release_callback = None

        def close(self) -> None:
            if self.release_callback is not None:
                self.release_callback(self)

        buffer_count = 2
        leased_count = 2

        def note_chunk_released(self) -> None:
            if self.leased_count:
                self.leased_count -= 1

    def exercise() -> tuple[tuple[int, memoryview | None] | None, list[int]]:
        proactor = _recviter_test_proactor()
        pool = _Pool()
        buffer = _recviter_buffer(proactor=proactor, buffer_pool=pool)
        buffer.on_result(_recv_chunk(0, b"a"))
        buffer.on_result(_enobufs_chunk(1))
        token = buffer.take_next()
        _assert_recviter_pressure(token)
        assert proactor.recv_many_bases == [0]
        first = buffer.take_next()
        assert first is not None and first[0] == 0 and bytes(first[1]) == b"a"
        pool.note_chunk_released()
        assert proactor.recv_many_bases == [0]
        pool.note_chunk_released()
        buffer.consume_pressure_resume()
        assert proactor.recv_many_bases == [0, 1]
        buffer.on_result(_recv_chunk(1, b"", more=False))
        second = buffer.take_next()
        assert proactor.recv_many_bases == [0, 1]
        return second, proactor.recv_many_bases

    eof, bases = _exercise_recviter_buffer(exercise)
    assert eof is None
    assert bases == [0, 1]


def test_recviter_buffer_resubmits_when_leg_stops_with_data():
    class _Pool:
        release_callback = None

        def close(self) -> None:
            if self.release_callback is not None:
                self.release_callback(self)

        buffer_count = 4
        leased_count = 0

    def exercise() -> list[int]:
        proactor = _recviter_test_proactor()
        pool = _Pool()
        buffer = _recviter_buffer(proactor=proactor, buffer_pool=pool)
        buffer.on_result(_recv_chunk(0, b"a", more=False))
        first = buffer.take_next()
        assert first is not None and first[0] == 0 and bytes(first[1]) == b"a"
        buffer.consume_pressure_resume()
        assert proactor.recv_many_bases == [0, 1]
        buffer.on_result(_recv_chunk(1, b"", more=False))
        assert buffer.take_next() is None
        return proactor.recv_many_bases

    assert _exercise_recviter_buffer(exercise) == [0, 1]


def test_recviter_buffer_pressure_when_initial_recv_many_hits_full_synthetic_pool() -> None:
    def exercise() -> bool:
        proactor = SelectorProactor()
        reader, _writer = socket.socketpair()
        pool = proactor_module.SyntheticRecvBufferPool(8192, 2)
        pool.leased_count = 2
        try:
            reader.setblocking(False)
            buffer = io_buffers_module.RecvIterBuffer(sock=reader, buffer_pool=pool, proactor=proactor)
            _assert_recviter_pressure(buffer.take_next())
            # nested same-thread ENOBUFS during start must clear for resume, not leave a done op
            assert buffer._current_operation is None
            pool.leased_count = 0
            buffer.consume_pressure_resume()
            assert buffer._current_operation is not None
            return True
        finally:
            reader.close()
            proactor.close()

    assert _exercise_recviter_buffer(exercise) is True


def test_recviter_buffer_preserves_global_sequence_across_enobufs_resubmit():
    class _Pool:
        release_callback = None

        def close(self) -> None:
            if self.release_callback is not None:
                self.release_callback(self)

        buffer_count = 4
        leased_count = 4

        def note_chunk_released(self) -> None:
            if self.leased_count:
                self.leased_count -= 1

    def exercise() -> list[tuple[int, bytes]]:
        proactor = _recviter_test_proactor()
        pool = _Pool()
        buffer = _recviter_buffer(proactor=proactor, buffer_pool=pool)
        buffer.on_result(_recv_chunk(0, b"a"))
        buffer.on_result(_recv_chunk(1, b"b"))
        buffer.on_result(_enobufs_chunk(2))
        _assert_recviter_pressure(buffer.take_next())
        first = buffer.take_next()
        assert first is not None and first[0] == 0
        pool.note_chunk_released()
        second = buffer.take_next()
        assert second is not None and second[0] == 1
        pool.note_chunk_released()
        pool.note_chunk_released()
        buffer.consume_pressure_resume()
        assert proactor.recv_many_bases == [0, 2]
        buffer.on_result(_recv_chunk(2, b"c"))
        buffer.on_result(_recv_chunk(3, b"", more=False))
        third = buffer.take_next()
        assert third is not None and third[0] == 2 and bytes(third[1]) == b"c"
        assert buffer.take_next() is None
        return [(0, b"a"), (1, b"b"), (2, b"c")]

    assert _exercise_recviter_buffer(exercise) == [(0, b"a"), (1, b"b"), (2, b"c")]


def test_recviter_buffer_defers_resume_while_reorder_heap_has_gap():
    class _Pool:
        release_callback = None

        def close(self) -> None:
            if self.release_callback is not None:
                self.release_callback(self)

        buffer_count = 4
        leased_count = 0

    def exercise() -> list[int]:
        proactor = _recviter_test_proactor()
        pool = _Pool()
        buffer = _recviter_buffer(proactor=proactor, buffer_pool=pool)
        buffer.on_result(_recv_chunk(1, b"b"))
        buffer.on_result(_recv_chunk(2, b"c"))
        buffer.on_result(_enobufs_chunk(3))
        buffer.on_result(_recv_chunk(0, b"a"))
        _assert_recviter_pressure(buffer.take_next())
        assert proactor.recv_many_bases == [0]
        first = buffer.take_next()
        assert first is not None and first[0] == 0 and bytes(first[1]) == b"a"
        assert proactor.recv_many_bases == [0]
        second = buffer.take_next()
        assert second is not None and second[0] == 1 and bytes(second[1]) == b"b"
        assert proactor.recv_many_bases == [0]
        third = buffer.take_next()
        assert third is not None and third[0] == 2 and bytes(third[1]) == b"c"
        buffer.on_result(_recv_chunk(3, b"", more=False))
        assert buffer.take_next() is None
        return proactor.recv_many_bases

    assert _exercise_recviter_buffer(exercise) == [0, 3]


