from __future__ import annotations

import socket

import pytest

from tealetio.io_waiter import IOWaiter
from tealetio.operations import Operation
from tealetio.proactor import SyncProactorScheduler
from tealetio.scheduler import set_scheduler
from tealetio.send_buffer import SendBuffer
from tealetio.tasks import CancelledError
from uring_fakes import SCHEDULER_INTEGRATION_FACTORIES


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
class TestSendBuffer:
    @pytest.fixture
    def scheduler(self, scheduler_factory) -> SyncProactorScheduler:
        sched = scheduler_factory()
        set_scheduler(sched)
        yield sched
        sched.close()

    def test_write_drain_delivers_in_order(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            send_buffer = scheduler.io._open_send_buffer(writer)

            def exercise() -> bytes:
                send_buffer.write(b"ab")
                send_buffer.write(b"cd")
                send_buffer.drain()
                return scheduler.io.sock_recv(reader, 4).wait()

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"abcd"
        finally:
            reader.close()
            writer.close()

    def test_drain_waits_for_callback_driven_completion(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            send_buffer = SendBuffer(sock=writer, io=scheduler.io, scheduler=scheduler)

            def sender() -> None:
                send_buffer.write(b"hello")
                send_buffer.drain()

            def receiver() -> bytes:
                return scheduler.io.sock_recv(reader, 5).wait()

            scheduler.spawn(sender)
            assert scheduler.run_until_complete(scheduler.spawn(receiver)) == b"hello"
        finally:
            reader.close()
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
                pending._finish(exception=CancelledError(), cancelled=True)
                send_buffer.drain()

            with pytest.raises(CancelledError):
                scheduler.run_until_complete(scheduler.spawn(sender))
            with pytest.raises(CancelledError):
                send_buffer.write(b"again")
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
                send_buffer.drain()

            with pytest.raises((OSError, ValueError)):
                scheduler.run_until_complete(scheduler.spawn(sender))
            with pytest.raises((OSError, ValueError)):
                send_buffer.write(b"again")
        finally:
            reader.close()