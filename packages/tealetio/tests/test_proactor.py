from __future__ import annotations

import selectors
import socket
from concurrent.futures import CancelledError

import pytest

from tealetio import TimeoutError, set_scheduler, timeout
from tealetio.proactor import InvalidStateError, Operation, ProactorScheduler, SelectorProactor


def _wait_until_done(proactor: SelectorProactor, *operations: Operation[object]) -> list[Operation[object]]:
    completed = [operation for operation in operations if operation.done()]
    pending = {operation for operation in operations if not operation.done()}
    while pending:
        for operation in proactor.wait(1.0):
            completed.append(operation)
            pending.discard(operation)
    return completed


class TestOperation:
    def test_operation_result_requires_completion(self):
        operation: Operation[int] = Operation(kind="test")

        with pytest.raises(InvalidStateError, match="result"):
            operation.result()
        with pytest.raises(InvalidStateError, match="exception"):
            operation.exception()

    def test_operation_callbacks_run_on_completion(self):
        operation: Operation[int] = Operation(kind="test")
        seen: list[int] = []

        operation.add_done_callback(lambda op: seen.append(op.result()))
        operation._set_result(42)
        operation.add_done_callback(lambda op: seen.append(op.result() + 1))

        assert seen == [42, 43]

    def test_operation_cancel_completes_with_cancelled_error(self):
        operation: Operation[int] = Operation(kind="test")

        assert operation.cancel() is True
        assert operation.done() is True
        assert operation.cancelled() is True
        assert operation.exception()

        with pytest.raises(CancelledError):
            operation.result()


class TestSelectorProactor:
    def test_recv_completes_after_selector_wait(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            operation = proactor.recv(reader, 5)
            assert operation.done() is False

            writer.send(b"hello")
            assert proactor.wait(1.0) == [operation]
            assert operation.result() == b"hello"
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_recv_into_completes_buffer(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            buf = bytearray(5)

            operation = proactor.recv_into(reader, buf)
            writer.send(b"world")

            assert proactor.wait(1.0) == [operation]
            assert operation.result() == 5
            assert bytes(buf) == b"world"
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_sendall_can_complete_immediately(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            operation = proactor.sendall(writer, b"hello")

            assert operation.done() is True
            assert operation.result() is None
            assert reader.recv(5) == b"hello"
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_accept_and_connect_complete_after_pumping(self):
        proactor = SelectorProactor()
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        accepted: socket.socket | None = None
        try:
            server.setblocking(False)
            client.setblocking(False)
            server.bind(("127.0.0.1", 0))
            server.listen()

            accept_operation = proactor.accept(server)
            connect_operation = proactor.connect(client, server.getsockname())
            completed = _wait_until_done(proactor, accept_operation, connect_operation)
            accepted, address = accept_operation.result()

            assert accept_operation in completed
            assert connect_operation in completed
            assert address[0] == "127.0.0.1"
            assert connect_operation.result() is None
        finally:
            if accepted is not None:
                accepted.close()
            client.close()
            server.close()
            proactor.close()

    def test_datagram_helpers(self):
        proactor = SelectorProactor()
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            receiver.setblocking(False)
            sender.setblocking(False)
            receiver.bind(("127.0.0.1", 0))
            buf = bytearray(5)

            receive_operation = proactor.recvfrom_into(receiver, buf)
            send_operation = proactor.sendto(sender, b"hello", receiver.getsockname())
            _wait_until_done(proactor, receive_operation, send_operation)

            count, address = receive_operation.result()
            assert count == 5
            assert bytes(buf) == b"hello"
            assert address[1] == sender.getsockname()[1]
            assert send_operation.result() == 5

            receive_bytes_operation = proactor.recvfrom(receiver, 5)
            sender.sendto(b"again", receiver.getsockname())
            assert proactor.wait(1.0) == [receive_bytes_operation]
            data, address = receive_bytes_operation.result()
            assert data == b"again"
            assert address[1] == sender.getsockname()[1]
        finally:
            sender.close()
            receiver.close()
            proactor.close()

    def test_operation_cancel_removes_selector_registration(self):
        selector = selectors.SelectSelector()
        proactor = SelectorProactor(selector=selector)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.recv(reader, 1)

            assert selector.get_key(reader.fileno()).events == selectors.EVENT_READ
            assert operation.cancel() is True
            with pytest.raises(KeyError):
                selector.get_key(reader.fileno())
            assert operation.cancelled() is True
            with pytest.raises(CancelledError):
                operation.result()
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_rejects_multiple_pending_operations_for_same_direction(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            proactor.recv(reader, 1)

            with pytest.raises(RuntimeError, match="already pending"):
                proactor.recv(reader, 1)
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_uses_provided_selector(self):
        selector = selectors.SelectSelector()
        proactor = SelectorProactor(selector=selector)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            operation = proactor.recv(reader, 1)
            assert selector.get_key(reader.fileno()).events == selectors.EVENT_READ

            writer.send(b"x")
            assert proactor.wait(1.0) == [operation]
            with pytest.raises(KeyError):
                selector.get_key(reader.fileno())
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_rejects_blocking_socket(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        try:
            with pytest.raises(ValueError, match="non-blocking"):
                proactor.recv(reader, 1)
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_break_wait_does_not_notify_callback(self):
        seen: list[str] = []
        proactor = SelectorProactor(wakeup_callback=lambda: seen.append("wake"))
        try:
            proactor.break_wait()
            assert proactor.wait(0.0) == []
            assert seen == []
        finally:
            proactor.close()

    def test_set_wakeup_callback_replaces_callback(self):
        seen: list[str] = []
        proactor = SelectorProactor(wakeup_callback=lambda: seen.append("old"))
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.recv(reader, 1)
            seen.clear()

            proactor.set_wakeup_callback(lambda: seen.append("new"))
            writer.send(b"x")

            assert proactor.wait(1.0) == [operation]
            assert seen == ["new"]
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_completion_notifies_callback(self):
        seen: list[str] = []
        proactor = SelectorProactor(wakeup_callback=lambda: seen.append("wake"))
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.recv(reader, 1)
            seen.clear()

            writer.send(b"x")

            assert proactor.wait(1.0) == [operation]
            assert seen == ["wake"]
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_cancel_wakes_wait_without_notifying_callback(self):
        seen: list[str] = []
        proactor = SelectorProactor(wakeup_callback=lambda: seen.append("wake"))
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.recv(reader, 1)
            seen.clear()

            assert operation.cancel() is True
            proactor.wait(0.0)
            assert seen == []
        finally:
            reader.close()
            writer.close()
            proactor.close()


class TestProactorScheduler:
    def test_uses_proactor_factory(self):
        selector = selectors.SelectSelector()
        created: list[SelectorProactor] = []

        def factory() -> SelectorProactor:
            proactor = SelectorProactor(selector=selector)
            created.append(proactor)
            return proactor

        scheduler = ProactorScheduler(factory)
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            def receive() -> bytes:
                return scheduler.sock_recv(reader, 5)

            def send() -> None:
                scheduler.sleep(0.001)
                scheduler.sock_sendall(writer, b"hello")

            task = scheduler.spawn(receive)
            scheduler.spawn(send)

            assert len(created) == 1
            assert scheduler.proactor is created[0]
            assert scheduler.run_until_complete(task) == b"hello"
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_socket_helpers(self):
        scheduler = ProactorScheduler()
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            buf = bytearray(5)

            def exchange() -> tuple[int, bytes]:
                scheduler.sock_sendall(writer, b"world")
                count = scheduler.sock_recv_into(reader, buf)
                return count, bytes(buf)

            task = scheduler.spawn(exchange)

            assert scheduler.run_until_complete(task) == (5, b"world")
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_accept_and_connect(self):
        scheduler = ProactorScheduler()
        set_scheduler(scheduler)
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server.setblocking(False)
            client.setblocking(False)
            server.bind(("127.0.0.1", 0))
            server.listen()

            def accept_and_read() -> bytes:
                conn, _address = scheduler.sock_accept(server)
                try:
                    return scheduler.sock_recv(conn, 4)
                finally:
                    conn.close()

            def connect_and_send() -> None:
                scheduler.sock_connect(client, server.getsockname())
                scheduler.sock_sendall(client, b"ping")

            task = scheduler.spawn(accept_and_read)
            scheduler.spawn(connect_and_send)

            assert scheduler.run_until_complete(task) == b"ping"
        finally:
            client.close()
            server.close()
            scheduler.close()

    def test_datagram_helpers(self):
        scheduler = ProactorScheduler()
        set_scheduler(scheduler)
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            receiver.setblocking(False)
            sender.setblocking(False)
            receiver.bind(("127.0.0.1", 0))
            buf = bytearray(5)

            def receive() -> tuple[int, object]:
                return scheduler.sock_recvfrom_into(receiver, buf)

            def send() -> int:
                scheduler.sleep(0.001)
                return scheduler.sock_sendto(sender, b"hello", receiver.getsockname())

            receive_task = scheduler.spawn(receive)
            send_task = scheduler.spawn(send)

            count, address = scheduler.run_until_complete(receive_task)
            assert count == 5
            assert bytes(buf) == b"hello"
            assert address[1] == sender.getsockname()[1]
            assert send_task.result() == 5
        finally:
            sender.close()
            receiver.close()
            scheduler.close()

    def test_wait_operation_timeout_cancels_operation(self):
        scheduler = ProactorScheduler()
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = scheduler.proactor.recv(reader, 1)

            def wait_with_timeout() -> bool:
                with pytest.raises(TimeoutError):
                    with timeout(0.001):
                        scheduler.wait_operation(operation)
                return operation.cancelled() and not scheduler.proactor.has_pending_operations()

            task = scheduler.spawn(wait_with_timeout)

            assert scheduler.run_until_complete(task) is True
        finally:
            reader.close()
            writer.close()
            scheduler.close()
