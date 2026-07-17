from __future__ import annotations

import socket

import pytest

from tealetio import Event
from tealetio.io_waiter import IOWaiter
from tealetio.operations import Operation
from tealetio.proactor import SyncProactorScheduler
from tealetio.scheduler import set_scheduler
from tealetio.io_buffers import SendBuffer
import errno

from tealetio.operations import io_cancellation_error, is_io_cancellation
from uring_fakes import SCHEDULER_INTEGRATION_FACTORIES


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
            pending = Operation[None](kind="send", fileobj=writer)
            real_sendall = scheduler.io.sock_sendall

            def pending_sendall(sock: socket.socket, data, progress=None) -> IOWaiter[None]:
                del data, progress
                return IOWaiter(scheduler.io, pending)

            scheduler.io.sock_sendall = pending_sendall  # type: ignore[method-assign]
            send_buffer = SendBuffer(
                sock=writer,
                io=scheduler.io,
                scheduler=scheduler,
                high_water=1024,
                low_water=256,
            )
            send_buffer.write(b"x" * 100)
            send_buffer.drain()
            assert send_buffer.pending_bytes == 100
            pending._finish(result=None)
            send_buffer.flush()
            assert send_buffer.pending_bytes == 0
        finally:
            scheduler.io.sock_sendall = real_sendall  # type: ignore[method-assign]
            reader.close()
            writer.close()

    def test_drain_blocks_until_low_water(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            pending_ops: list[Operation[None]] = []
            real_sendall = scheduler.io.sock_sendall

            def staged_sendall(sock: socket.socket, data, progress=None) -> IOWaiter[None]:
                del progress
                operation = Operation[None](kind="send", fileobj=sock)
                pending_ops.append(operation)
                return IOWaiter(scheduler.io, operation)

            scheduler.io.sock_sendall = staged_sendall  # type: ignore[method-assign]
            send_buffer = SendBuffer(
                sock=writer,
                io=scheduler.io,
                scheduler=scheduler,
                high_water=10,
                low_water=2,
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

                pending_ops[0]._finish(result=None)
                assert not done.is_set()
                assert send_buffer.pending_bytes == 6

                pending_ops[1]._finish(result=None)
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
            pending = Operation[None](kind="send", fileobj=writer)
            real_sendall = scheduler.io.sock_sendall

            def pending_sendall(sock: socket.socket, data, progress=None) -> IOWaiter[None]:
                del data, progress
                return IOWaiter(scheduler.io, pending)

            scheduler.io.sock_sendall = pending_sendall  # type: ignore[method-assign]
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler)
            send_buffer.write(b"hello")
            assert send_buffer.pending_bytes == 5
            pending._finish(result=None)
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
            pending_ops: list[Operation[None]] = []
            real_sendall = scheduler.io.sock_sendall
            real_shutdown = scheduler.io.sock_shutdown

            def staged_sendall(sock: socket.socket, data, progress=None) -> IOWaiter[None]:
                del progress
                operation = Operation[None](kind="send", fileobj=sock)
                pending_ops.append(operation)
                return IOWaiter(scheduler.io, operation)

            shutdown_calls: list[int] = []

            def track_shutdown(sock: socket.socket, how: int):
                shutdown_calls.append(how)
                return real_shutdown(sock, how)

            scheduler.io.sock_sendall = staged_sendall  # type: ignore[method-assign]
            scheduler.io.sock_shutdown = track_shutdown  # type: ignore[method-assign]
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler)

            def exercise() -> None:
                send_buffer.write(b"ab")
                send_buffer.write_eof()
                assert send_buffer.eof_pending
                assert not send_buffer.write_eof_done
                assert shutdown_calls == []
                pending_ops[0]._finish(result=None)
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
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler)

            with pytest.raises(OSError, match="submit failed"):
                send_buffer.write(b"ab")
            assert send_buffer.pending_bytes == 2
            assert not send_buffer._active
        finally:
            scheduler.io.sock_sendall = real_sendall  # type: ignore[method-assign]
            writer.close()

    def test_chained_submit_failure_restores_pending_and_sticks_error(
        self, scheduler: SyncProactorScheduler
    ) -> None:
        _reader, writer = socket.socketpair()
        try:
            pending_ops: list[Operation[None]] = []
            submit_calls = 0
            real_sendall = scheduler.io.sock_sendall

            def staged_sendall(sock: socket.socket, data, progress=None) -> IOWaiter[None]:
                nonlocal submit_calls
                submit_calls += 1
                if submit_calls == 1:
                    operation = Operation[None](kind="send", fileobj=sock)
                    pending_ops.append(operation)
                    return IOWaiter(scheduler.io, operation)
                raise OSError("chained submit failed")

            scheduler.io.sock_sendall = staged_sendall  # type: ignore[method-assign]
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler)
            send_buffer.write(b"ab")
            send_buffer.write(b"cd")
            with pytest.raises(OSError, match="chained submit failed"):
                pending_ops[0]._finish(result=None)
            assert send_buffer.pending_bytes == 2
            with pytest.raises(OSError, match="chained submit failed"):
                send_buffer.flush()
        finally:
            scheduler.io.sock_sendall = real_sendall  # type: ignore[method-assign]
            writer.close()

    def test_close_drains_pending_after_in_flight_leg(self, scheduler: SyncProactorScheduler) -> None:
        _reader, writer = socket.socketpair()
        try:
            writer.setblocking(False)
            pending_ops: list[Operation[None]] = []
            real_sendall = scheduler.io.sock_sendall

            def staged_sendall(sock: socket.socket, data, progress=None) -> IOWaiter[None]:
                del progress
                operation = Operation[None](kind="send", fileobj=sock)
                pending_ops.append(operation)
                return IOWaiter(scheduler.io, operation)

            scheduler.io.sock_sendall = staged_sendall  # type: ignore[method-assign]
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler)

            def exercise() -> None:
                send_buffer.write(b"ab")
                send_buffer.write(b"cd")
                send_buffer.close()
                assert len(pending_ops) == 1
                pending_ops[0]._finish(result=None)
                assert len(pending_ops) == 2
                pending_ops[1]._finish(result=None)
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
            pending = Operation[None](kind="send", fileobj=writer)
            real_sendall = scheduler.io.sock_sendall

            def pending_sendall(sock: socket.socket, data, progress=None) -> IOWaiter[None]:
                del data, progress
                return IOWaiter(scheduler.io, pending)

            scheduler.io.sock_sendall = pending_sendall  # type: ignore[method-assign]
            send_buffer = scheduler.io._open_send_buffer(writer)

            def sender() -> None:
                send_buffer.write(b"hello")
                pending._finish(exception=io_cancellation_error())
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