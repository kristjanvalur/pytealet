from __future__ import annotations

import asyncio
import selectors
import socket
import threading
from concurrent.futures import CancelledError
from types import SimpleNamespace
from typing import Any

import pytest

import tealetio.proactor as proactor_module
from tealetio import TimeoutError, set_scheduler, timeout
from tealetio.proactor import (
    AsyncProactorScheduler,
    InvalidStateError,
    Operation,
    ProactorScheduler,
    SelectorProactor,
    SyncProactorScheduler,
    ThreadedSelectorProactor,
    UringProactor,
)


def _wait_until_done(proactor: SelectorProactor, *operations: Operation[Any]) -> list[Operation[Any]]:
    completed = [operation for operation in operations if operation.done()]
    pending = {operation for operation in operations if not operation.done()}
    while pending:
        proactor.wait(proactor.get_time() + 1.0)
        for operation in list(pending):
            if operation.done():
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

        operation.cancel()
        assert operation.done() is True
        assert operation.cancelled() is True
        assert operation.exception()

        with pytest.raises(CancelledError):
            operation.result()


class TestSelectorProactor:
    def test_clock_can_be_replaced(self):
        proactor = SelectorProactor()
        try:
            proactor.set_clock(lambda: 42.0)

            assert proactor.get_time() == 42.0
        finally:
            proactor.close()

    def test_recv_completes_after_selector_wait(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            operation = proactor.recv(reader, 5)
            assert operation.done() is False

            writer.send(b"hello")
            proactor.wait(proactor.get_time() + 1.0)
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

            proactor.wait(proactor.get_time() + 1.0)
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

    def test_ready_recv_completes_immediately_without_selector_registration(self):
        selector = selectors.SelectSelector()
        proactor = SelectorProactor(selector=selector)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            writer.send(b"hello")

            operation = proactor.recv(reader, 5)

            assert operation.done() is True
            assert operation.result() == b"hello"
            with pytest.raises(KeyError):
                selector.get_key(reader.fileno())
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
            proactor.wait(proactor.get_time() + 1.0)
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
            operation.cancel()
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
            proactor.wait(proactor.get_time() + 1.0)
            assert operation.done() is True
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
        proactor = SelectorProactor(completion_callback=lambda: seen.append("wake"))
        try:
            proactor.break_wait()
            proactor.wait(0)
            assert seen == []
        finally:
            proactor.close()

    def test_set_completion_callback_replaces_callback(self):
        seen: list[str] = []
        proactor = SelectorProactor(completion_callback=lambda: seen.append("old"))
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.recv(reader, 1)
            seen.clear()

            proactor.set_completion_callback(lambda: seen.append("new"))
            writer.send(b"x")

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.done() is True
            assert seen == ["new"]
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_completion_notifies_callback(self):
        seen: list[str] = []
        proactor = SelectorProactor(completion_callback=lambda: seen.append("wake"))
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.recv(reader, 1)
            seen.clear()

            writer.send(b"x")

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.done() is True
            assert seen == ["wake"]
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_cancel_wakes_wait_without_notifying_callback(self):
        seen: list[str] = []
        proactor = SelectorProactor(completion_callback=lambda: seen.append("wake"))
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.recv(reader, 1)
            seen.clear()

            operation.cancel()
            proactor.wait(0)
            assert seen == []
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_wait_async_completes_operation(self):
        async def run() -> bytes:
            proactor = SelectorProactor()
            reader, writer = socket.socketpair()
            try:
                reader.setblocking(False)
                writer.setblocking(False)
                operation = proactor.recv(reader, 5)
                waiter = asyncio.create_task(proactor.wait_async(proactor.get_time() + 1.0))
                await asyncio.sleep(0)

                writer.send(b"hello")

                await waiter
                return operation.result()
            finally:
                reader.close()
                writer.close()
                proactor.close()

        assert asyncio.run(run()) == b"hello"

    def test_wait_async_falls_back_when_loop_cannot_watch_fds(self, monkeypatch):
        async def run() -> bytes:
            proactor = SelectorProactor()
            reader, writer = socket.socketpair()
            try:
                loop = asyncio.get_running_loop()

                def add_reader_unavailable(*args: object) -> None:
                    raise NotImplementedError

                monkeypatch.setattr(loop, "add_reader", add_reader_unavailable)
                reader.setblocking(False)
                writer.setblocking(False)
                operation = proactor.recv(reader, 5)
                waiter = asyncio.create_task(proactor.wait_async(proactor.get_time() + 1.0))
                await asyncio.sleep(0)

                writer.send(b"hello")

                await asyncio.wait_for(waiter, 1.0)
                return operation.result()
            finally:
                reader.close()
                writer.close()
                proactor.close()

        assert asyncio.run(run()) == b"hello"

    def test_wait_async_timeout_returns_without_completions(self):
        async def run() -> bool:
            proactor = SelectorProactor()
            reader, writer = socket.socketpair()
            try:
                reader.setblocking(False)
                writer.setblocking(False)
                operation = proactor.recv(reader, 1)
                await proactor.wait_async(proactor.get_time() + 0.001)
                return operation.done()
            finally:
                reader.close()
                writer.close()
                proactor.close()

        assert asyncio.run(run()) is False

    def test_wait_async_break_wait_returns_without_completions(self):
        async def run() -> None:
            proactor = SelectorProactor()
            try:
                waiter = asyncio.create_task(proactor.wait_async(proactor.get_time() + 1.0))
                await asyncio.sleep(0)

                proactor.break_wait()

                await waiter
            finally:
                proactor.close()

        asyncio.run(run())


class TestThreadedSelectorProactor:
    def test_defaults_to_epoll_selector_when_available(self):
        proactor = ThreadedSelectorProactor()
        try:
            if hasattr(selectors, "EpollSelector"):
                assert isinstance(proactor._selector, selectors.EpollSelector)
            assert hasattr(proactor._selector, "select_released")
        finally:
            proactor.close()

    def test_requires_selector_with_select_released(self):
        with pytest.raises(TypeError, match="select_released"):
            ThreadedSelectorProactor(selector=selectors.SelectSelector())

    def test_worker_thread_signals_completion(self):
        callback_threads: list[int] = []
        callback_called = threading.Event()
        main_thread = threading.get_ident()

        def on_completion() -> None:
            callback_threads.append(threading.get_ident())
            callback_called.set()

        proactor = ThreadedSelectorProactor(completion_callback=on_completion)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.recv(reader, 5)
            proactor.wait(0)
            assert operation.done() is False

            writer.send(b"hello")

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() == b"hello"
            assert callback_called.wait(1.0) is True
            assert callback_threads
            assert callback_threads[0] != main_thread
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_immediate_completion_returns_completed_operation_without_queueing(self):
        proactor = ThreadedSelectorProactor()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            operation = proactor.sendall(writer, b"hello")

            assert operation.done() is True
            proactor.wait(0)
            assert operation.result() is None
            assert reader.recv(5) == b"hello"
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_break_wait_does_not_notify_callback(self):
        seen: list[str] = []
        proactor = ThreadedSelectorProactor(completion_callback=lambda: seen.append("wake"))
        try:
            proactor.wait(0)
            proactor.break_wait()

            proactor.wait(proactor.get_time() + 0.01)
            assert seen == []
        finally:
            proactor.close()

    def test_submit_wakes_worker_before_mutating_selector(self):
        proactor = ThreadedSelectorProactor()
        reader, writer = socket.socketpair()
        operation: Operation[bytes] | None = None
        error: BaseException | None = None

        def submit() -> None:
            nonlocal operation, error
            try:
                operation = proactor.recv(reader, 1)
            except BaseException as exc:  # pragma: no cover - assertion reports it
                error = exc

        try:
            reader.setblocking(False)
            writer.setblocking(False)
            proactor.wait(0)

            thread = threading.Thread(target=submit)
            thread.start()
            thread.join(1.0)

            assert thread.is_alive() is False
            assert error is None
            assert operation is not None

            writer.send(b"x")

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() == b"x"
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_cancel_wakes_worker_before_mutating_selector(self):
        proactor = ThreadedSelectorProactor()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.recv(reader, 1)
            proactor.wait(0)

            thread = threading.Thread(target=operation.cancel)
            thread.start()
            thread.join(1.0)

            assert thread.is_alive() is False
            assert operation.cancelled() is True
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_set_completion_callback_wakes_worker_before_locking(self):
        proactor = ThreadedSelectorProactor()
        try:
            proactor.wait(0)

            thread = threading.Thread(target=lambda: proactor.set_completion_callback(lambda: None))
            thread.start()
            thread.join(1.0)

            assert thread.is_alive() is False
        finally:
            proactor.close()

    def test_async_scheduler_drives_threaded_backend(self):
        async def run() -> bytes:
            scheduler = AsyncProactorScheduler(ThreadedSelectorProactor)
            set_scheduler(scheduler)
            reader, writer = socket.socketpair()
            try:
                reader.setblocking(False)
                writer.setblocking(False)

                def receive() -> bytes:
                    return scheduler.sock_recv(reader, 5)

                task = scheduler.spawn(receive)
                await asyncio.sleep(0)
                writer.send(b"hello")

                return await scheduler.arun_until_complete(task)
            finally:
                reader.close()
                writer.close()
                scheduler.close()

        assert asyncio.run(run()) == b"hello"


class _FakeUringRing:
    def __init__(self, entries: int, flags: int) -> None:
        self.entries = entries
        self.flags = flags
        self.fd = 99
        self.features = 123
        self.sq_entries = entries
        self.cq_entries = entries * 2
        self.closed = False
        self.running = False
        self.callback = None
        self.start_count = 0
        self.stop_count = 0
        self.break_count = 0
        self.completions: list[SimpleNamespace] = []
        self.accepted_peers: list[socket.socket] = []
        self.submitted_recv: list[tuple[int, object, object]] = []
        self.submitted_recvmsg: list[tuple[int, object, object]] = []
        self.submitted_send: list[tuple[int, object, object]] = []
        self.submitted_sendto: list[tuple[int, object, object, object]] = []
        self.submitted_accept: list[tuple[int, object]] = []
        self.submitted_connect: list[tuple[int, object, object]] = []

    def close(self) -> None:
        self.stop()
        for peer in self.accepted_peers:
            peer.close()
        self.accepted_peers.clear()
        self.closed = True

    def start(self) -> None:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.running = True
        self.start_count += 1

    def stop(self) -> None:
        self.running = False
        self.stop_count += 1

    def break_wait(self) -> None:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.break_count += 1

    def submit_recv(self, fd: int, buf: Any, user_data: object = None) -> None:
        if self.closed:
            raise RuntimeError("ring is closed")
        view = memoryview(buf)
        operation = getattr(user_data, "operation", None)
        payload = b"world" if getattr(operation, "kind", None) == "recv_into" else b"hello"
        if len(view) >= len(payload):
            view[: len(payload)] = payload
        self.submitted_recv.append((fd, buf, user_data))
        self._deliver(SimpleNamespace(user_data=user_data, res=5, flags=0, result=5))

    def submit_send(self, fd: int, data: Any, user_data: object = None) -> None:
        if self.closed:
            raise RuntimeError("ring is closed")
        payload = bytes(data)
        self.submitted_send.append((fd, data, user_data))
        self._deliver(SimpleNamespace(user_data=user_data, res=len(payload), flags=0, result=len(payload)))

    def submit_recvmsg(self, fd: int, buf: Any, user_data: object = None) -> None:
        if self.closed:
            raise RuntimeError("ring is closed")
        payload = b"again" if getattr(getattr(user_data, "operation", None), "kind", None) == "recvfrom" else b"hello"
        memoryview(buf)[: len(payload)] = payload
        self.submitted_recvmsg.append((fd, buf, user_data))
        self._deliver(SimpleNamespace(user_data=user_data, res=len(payload), flags=0, result=("127.0.0.1", 54321)))

    def submit_sendto(self, fd: int, data: Any, address: Any, user_data: object = None) -> None:
        if self.closed:
            raise RuntimeError("ring is closed")
        payload = bytes(data)
        self.submitted_sendto.append((fd, data, address, user_data))
        self._deliver(SimpleNamespace(user_data=user_data, res=len(payload), flags=0, result=len(payload)))

    def submit_accept(self, fd: int, user_data: object = None) -> None:
        if self.closed:
            raise RuntimeError("ring is closed")
        conn, peer = socket.socketpair()
        self.accepted_peers.append(peer)
        self.submitted_accept.append((fd, user_data))
        self._deliver(SimpleNamespace(user_data=user_data, res=conn.fileno(), flags=0, result=(conn.detach(), "peer")))

    def submit_connect(self, fd: int, address: Any, user_data: object = None) -> None:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_connect.append((fd, address, user_data))
        self._deliver(SimpleNamespace(user_data=user_data, res=0, flags=0, result=None))

    def wait(self, timeout: float | None = None) -> SimpleNamespace | None:
        if not self.completions:
            return None
        return self.completions.pop(0)

    def _deliver(self, completion: SimpleNamespace) -> None:
        if self.running and self.callback is not None:
            self.callback(completion)
        else:
            self.completions.append(completion)


class _DeferredUringRing(_FakeUringRing):
    def submit_recv(self, fd: int, buf: Any, user_data: object = None) -> None:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_recv.append((fd, buf, user_data))

    def complete_recv(self, data: bytes = b"hello") -> None:
        _fd, buf, user_data = self.submitted_recv[-1]
        memoryview(buf)[: len(data)] = data
        self._deliver(SimpleNamespace(user_data=user_data, res=len(data), flags=0, result=len(data)))


class TestUringProactor:
    def test_initializes_ring_with_entries_and_flags(self):
        created: list[_FakeUringRing] = []

        def ring_factory(entries: int, flags: int) -> _FakeUringRing:
            ring = _FakeUringRing(entries, flags)
            created.append(ring)
            return ring

        proactor = UringProactor(entries=32, flags=7, ring_factory=ring_factory)
        try:
            assert proactor.ring is created[0]
            assert proactor.ring.fd == 99
            assert proactor.ring.sq_entries == 32
            assert proactor.ring.cq_entries == 64
            assert created[0].flags == 7
        finally:
            proactor.close()

        assert created[0].closed is True

    def test_clock_can_be_replaced(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            proactor.set_clock(lambda: 42.0)

            assert proactor.get_time() == 42.0
        finally:
            proactor.close()

    def test_wait_respects_deadline_without_pending_operations(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            proactor.set_clock(lambda: 100.0)

            proactor.wait(100.0)
            proactor.wait(None)
        finally:
            proactor.close()

    def test_break_wait_releases_blocking_wait(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        released = threading.Event()
        thread = threading.Thread(target=lambda: (proactor.wait(proactor.get_time() + 10.0), released.set()))
        try:
            thread.start()
            proactor.break_wait()
            thread.join(1.0)

            assert thread.is_alive() is False
            assert released.is_set()
        finally:
            proactor.close()

    def test_break_wait_coalesces_until_wait_consumes_it(self):
        proactor = UringProactor(ring_factory=_DeferredUringRing)
        reader, writer = socket.socketpair()
        released = threading.Event()
        try:
            reader.setblocking(False)
            proactor.recv(reader, 5)
            proactor.break_wait()
            proactor.break_wait()

            proactor.wait(proactor.get_time() + 1.0)
            thread = threading.Thread(target=lambda: (proactor.wait(proactor.get_time() + 10.0), released.set()))
            thread.start()
            thread.join(0.05)
            assert thread.is_alive() is True

            proactor.break_wait()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert released.is_set()
        finally:
            proactor.close()
            reader.close()
            writer.close()

    def test_wait_async_respects_deadline(self):
        async def run() -> None:
            proactor = UringProactor(ring_factory=_FakeUringRing)
            try:
                await proactor.wait_async(proactor.get_time())
            finally:
                proactor.close()

        asyncio.run(run())

    def test_break_wait_wakes_proactor_waiters_without_ring_wakeup(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            proactor.break_wait()

            assert isinstance(proactor.ring, _FakeUringRing)
            assert proactor.ring.break_count == 0
        finally:
            proactor.close()

    def test_ring_callback_signals_completion(self):
        callback_called = threading.Event()

        def on_completion() -> None:
            callback_called.set()

        proactor = UringProactor(ring_factory=_FakeUringRing, completion_callback=on_completion)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            operation = proactor.recv(reader, 5)

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() == b"hello"
            assert callback_called.wait(1.0) is True
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_wait_async_completes_from_callback_wakeup(self, monkeypatch):
        async def run() -> bytes:
            created: list[_DeferredUringRing] = []

            def ring_factory(entries: int, flags: int) -> _DeferredUringRing:
                ring = _DeferredUringRing(entries, flags)
                created.append(ring)
                return ring

            proactor = UringProactor(ring_factory=ring_factory)
            reader, writer = socket.socketpair()
            try:
                loop = asyncio.get_running_loop()
                call_soon_threadsafe_calls: list[object] = []
                original_call_soon_threadsafe = loop.call_soon_threadsafe

                def call_soon_threadsafe(callback, *args, context=None):
                    call_soon_threadsafe_calls.append(callback)
                    return original_call_soon_threadsafe(callback, *args, context=context)

                def run_in_executor(*args, **kwargs):
                    raise AssertionError("UringProactor.wait_async should not use an executor")

                monkeypatch.setattr(loop, "call_soon_threadsafe", call_soon_threadsafe)
                monkeypatch.setattr(loop, "run_in_executor", run_in_executor)
                reader.setblocking(False)
                operation = proactor.recv(reader, 5)
                waiter = asyncio.create_task(proactor.wait_async(proactor.get_time() + 1.0))
                await asyncio.sleep(0)

                thread = threading.Thread(target=created[0].complete_recv)
                thread.start()

                await asyncio.wait_for(waiter, 1.0)
                thread.join(1.0)
                assert thread.is_alive() is False
                assert call_soon_threadsafe_calls
                return operation.result()
            finally:
                reader.close()
                writer.close()
                proactor.close()

        assert asyncio.run(run()) == b"hello"

    def test_recv_completes_from_ring_completion(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            operation = proactor.recv(reader, 5)

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() == b"hello"
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_recv_into_completes_from_ring_completion(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            buf = bytearray(5)
            operation = proactor.recv_into(reader, buf)

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() == 5
            assert bytes(buf) == b"world"
            assert isinstance(proactor.ring, _FakeUringRing)
            submitted = proactor.ring.submitted_recv[0]
            assert submitted[0] == reader.fileno()
            assert submitted[1] is buf
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_cancelled_operation_consumes_pending_token_on_ring_completion(self):
        proactor = UringProactor(ring_factory=_DeferredUringRing)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            operation = proactor.recv(reader, 5)

            operation.cancel()
            assert operation.cancelled() is True
            assert proactor.has_pending_operations() is True
            proactor.wait(proactor.get_time() + 1.0)

            released = threading.Event()
            thread = threading.Thread(target=lambda: (proactor.wait(proactor.get_time() + 10.0), released.set()))
            thread.start()
            thread.join(0.05)
            assert thread.is_alive() is True

            assert isinstance(proactor.ring, _DeferredUringRing)
            proactor.ring.complete_recv()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert released.is_set()
            assert proactor.has_pending_operations() is False
            with pytest.raises(CancelledError):
                operation.result()
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_sendall_completes_from_ring_completion(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        try:
            writer.setblocking(False)
            payload = b"hello"
            operation = proactor.sendall(writer, payload)

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() is None
            assert isinstance(proactor.ring, _FakeUringRing)
            submitted = proactor.ring.submitted_send[0][1]
            assert isinstance(submitted, memoryview)
            assert submitted.obj is payload
            assert bytes(submitted) == b"hello"
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_recvfrom_into_completes_from_ring_completion(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            receiver.setblocking(False)
            buf = bytearray(5)
            operation = proactor.recvfrom_into(receiver, buf)

            proactor.wait(proactor.get_time() + 1.0)
            count, address = operation.result()
            assert count == 5
            assert bytes(buf) == b"hello"
            assert address == ("127.0.0.1", 54321)
            assert isinstance(proactor.ring, _FakeUringRing)
            submitted = proactor.ring.submitted_recvmsg[0]
            assert submitted[0] == receiver.fileno()
            assert submitted[1] is not buf
            assert submitted[1].obj is buf
        finally:
            receiver.close()
            proactor.close()

    def test_recvfrom_into_rejects_invalid_nbytes(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            receiver.setblocking(False)
            with pytest.raises(ValueError, match="negative buffersize"):
                proactor.recvfrom_into(receiver, bytearray(5), -1)
            with pytest.raises(ValueError, match="nbytes is greater"):
                proactor.recvfrom_into(receiver, bytearray(5), 6)
            assert isinstance(proactor.ring, _FakeUringRing)
            assert proactor.ring.submitted_recvmsg == []
        finally:
            receiver.close()
            proactor.close()

    def test_recvfrom_allocates_buffer_for_ring_completion(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            receiver.setblocking(False)
            operation = proactor.recvfrom(receiver, 5)

            proactor.wait(proactor.get_time() + 1.0)
            data, address = operation.result()
            assert data == b"again"
            assert address == ("127.0.0.1", 54321)
        finally:
            receiver.close()
            proactor.close()

    def test_sendto_completes_from_ring_completion(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sender.setblocking(False)
            payload = b"hello"
            address = ("127.0.0.1", 12345)
            operation = proactor.sendto(sender, payload, address)

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() == 5
            assert isinstance(proactor.ring, _FakeUringRing)
            submitted = proactor.ring.submitted_sendto[0]
            assert submitted[0] == sender.fileno()
            assert submitted[1].obj is payload
            assert submitted[2] == address
        finally:
            sender.close()
            proactor.close()

    def test_accept_completes_from_ring_completion(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        server = socket.socket()
        conn = None
        try:
            server.setblocking(False)
            operation = proactor.accept(server)

            proactor.wait(proactor.get_time() + 1.0)
            conn, address = operation.result()
            assert address == "peer"
            assert conn.getblocking() is False
            assert isinstance(proactor.ring, _FakeUringRing)
            submitted = proactor.ring.submitted_accept[0]
            assert submitted[0] == server.fileno()
        finally:
            if conn is not None:
                conn.close()
            server.close()
            proactor.close()

    def test_connect_completes_from_ring_completion(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.setblocking(False)
            address = ("127.0.0.1", 12345)
            operation = proactor.connect(sock, address)

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() is None
            assert isinstance(proactor.ring, _FakeUringRing)
            submitted = proactor.ring.submitted_connect[0]
            assert submitted[0] == sock.fileno()
            assert submitted[1] == address
        finally:
            sock.close()
            proactor.close()

    def test_operations_reject_closed_proactor(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        try:
            proactor.close()

            with pytest.raises(RuntimeError, match="closed"):
                proactor.recv(reader, 1)
            with pytest.raises(RuntimeError, match="closed"):
                proactor.sendall(writer, b"")
            with pytest.raises(RuntimeError, match="closed"):
                proactor.wait(0)
        finally:
            reader.close()
            writer.close()


class TestProactorScheduler:
    def test_proactor_scheduler_is_abstract(self):
        with pytest.raises(TypeError, match="abstract"):
            ProactorScheduler()

    def test_scheduler_clock_drives_proactor_clock(self):
        scheduler = SyncProactorScheduler()
        try:
            scheduler._time = lambda: 24.0

            assert scheduler.proactor.get_time() == 24.0
        finally:
            scheduler.close()

    def test_uses_proactor_factory(self):
        selector = selectors.SelectSelector()
        created: list[SelectorProactor] = []

        def factory() -> SelectorProactor:
            proactor = SelectorProactor(selector=selector)
            created.append(proactor)
            return proactor

        scheduler = SyncProactorScheduler(factory)
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
        scheduler = SyncProactorScheduler()
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
        scheduler = SyncProactorScheduler()
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
        scheduler = SyncProactorScheduler()
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
        scheduler = SyncProactorScheduler()
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

    def test_wait_operation_wakes_event_on_scheduler_thread_from_uring_callback(self, monkeypatch):
        event_set_threads: list[int] = []
        original_event = proactor_module.ThreadsafeEvent

        class TrackingEvent(original_event):
            def _set(self) -> None:
                event_set_threads.append(threading.get_ident())
                super()._set()

        created: list[_DeferredUringRing] = []

        def ring_factory(entries: int, flags: int) -> _DeferredUringRing:
            ring = _DeferredUringRing(entries, flags)
            created.append(ring)
            return ring

        def proactor_factory() -> UringProactor:
            return UringProactor(ring_factory=ring_factory)

        monkeypatch.setattr(proactor_module, "ThreadsafeEvent", TrackingEvent)
        scheduler = SyncProactorScheduler(proactor_factory)
        set_scheduler(scheduler)
        scheduler_thread = threading.get_ident()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)

            def receive() -> bytes:
                return scheduler.sock_recv(reader, 5)

            def complete_from_worker() -> None:
                scheduler.sleep(0.001)
                thread = threading.Thread(target=created[0].complete_recv)
                thread.start()
                thread.join(1.0)
                assert thread.is_alive() is False

            receive_task = scheduler.spawn(receive)
            scheduler.spawn(complete_from_worker)

            assert scheduler.run_until_complete(receive_task) == b"hello"
            assert event_set_threads == [scheduler_thread]
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_async_proactor_scheduler_drives_io_without_blocking_asyncio(self):
        async def run() -> bytes:
            scheduler = AsyncProactorScheduler()
            set_scheduler(scheduler)
            reader, writer = socket.socketpair()
            try:
                reader.setblocking(False)
                writer.setblocking(False)

                def receive() -> bytes:
                    return scheduler.sock_recv(reader, 5)

                task = scheduler.spawn(receive)
                await asyncio.sleep(0)
                writer.send(b"hello")

                return await scheduler.arun_until_complete(task)
            finally:
                reader.close()
                writer.close()
                scheduler.close()

        assert asyncio.run(run()) == b"hello"

    def test_async_proactor_scheduler_installs_loop_completion_callback(self, monkeypatch):
        async def run() -> bool:
            stored_callback = None

            class TrackingProactor(SelectorProactor):
                def set_completion_callback(self, callback):
                    nonlocal stored_callback
                    stored_callback = callback
                    super().set_completion_callback(callback)

            scheduler = AsyncProactorScheduler(TrackingProactor)
            try:
                loop = asyncio.get_running_loop()
                calls = 0
                original_call_soon_threadsafe = loop.call_soon_threadsafe

                def call_soon_threadsafe(callback, *args, context=None):
                    nonlocal calls
                    calls += 1
                    return original_call_soon_threadsafe(callback, *args, context=context)

                monkeypatch.setattr(loop, "call_soon_threadsafe", call_soon_threadsafe)
                scheduler.bind_loop(loop)
                assert stored_callback is not None
                stored_callback()
                await asyncio.sleep(0)
                return calls == 1
            finally:
                scheduler.close()

        assert asyncio.run(run()) is True
