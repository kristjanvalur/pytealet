from __future__ import annotations

import asyncio
import errno
import os
import select
import selectors
import socket
import threading
import time
from concurrent.futures import CancelledError
from types import SimpleNamespace
from typing import Any

import pytest
import uring_api

import tealetio.proactor as proactor_module
from tealetio import TimeoutError, set_scheduler, timeout
from tealetio.proactor import (
    AsyncProactorScheduler,
    ContinuousOperation,
    InvalidStateError,
    Operation,
    ProactorScheduler,
    RECV_MANY_BUFFER_PRESSURE,
    SelectorProactor,
    SyncProactorScheduler,
    ThreadedSelectorProactor,
    UringProactor,
)


def _recv_many_bytes(seen: list[tuple[int, memoryview]]) -> list[tuple[int, bytes]]:
    return [(index, bytes(data)) for index, data in seen]


def _recv_many_bytes_sorted(seen: list[tuple[int, memoryview]]) -> list[tuple[int, bytes]]:
    return sorted((index, bytes(data)) for index, data in seen if index != RECV_MANY_BUFFER_PRESSURE)


def test_recvall_adopt_chunk_keeps_all_chunk_views():
    chunks: dict[int, memoryview | bytes] = {}
    pending: set[int] = set()

    proactor_module._recvall_adopt_chunk(chunks, pending, 0, memoryview(b"a"))
    proactor_module._recvall_adopt_chunk(chunks, pending, 1, memoryview(b"b"))
    proactor_module._recvall_adopt_chunk(chunks, pending, 2, memoryview(b"c"))

    assert type(chunks[0]) is memoryview
    assert type(chunks[1]) is memoryview
    assert type(chunks[2]) is memoryview
    assert pending == {0, 1, 2}


def test_recvall_relieve_pressure_converts_all_live_views():
    chunks: dict[int, memoryview | bytes] = {
        0: memoryview(b"a"),
        1: memoryview(b"b"),
        2: memoryview(b"c"),
    }
    pending: set[int] = {0, 1, 2}

    proactor_module._recvall_relieve_pressure(chunks, pending)

    assert chunks == {0: b"a", 1: b"b", 2: b"c"}
    assert not pending


def test_recvall_release_pending_views_clears_pending_chunk_references():
    chunks: dict[int, memoryview | bytes] = {0: b"done", 1: memoryview(b"tail")}
    pending: set[int] = {1}

    proactor_module._recvall_release_pending_views(chunks, pending)

    assert not pending
    assert 0 in chunks
    assert 1 not in chunks


def _exercise_recvgen_buffer(exercise: Any) -> Any:
    scheduler = SyncProactorScheduler()
    set_scheduler(scheduler)
    try:
        return scheduler.run_until_complete(scheduler.spawn(exercise))
    finally:
        scheduler.close()


def _multishot_test_completion(*, sequence: int = 0, more: bool = True) -> SimpleNamespace:
    return SimpleNamespace(
        multishot=True,
        flags=uring_api.IORING_CQE_F_MORE if more else 0,
        sequence=sequence,
    )


def _multishot_test_entry() -> proactor_module._UringEntry:
    operation = ContinuousOperation(
        kind="recv_many",
        fileobj=socket.socketpair()[0],
        proactor=None,
        result_callback=lambda _result: None,
    )
    return proactor_module._UringEntry(
        operation=operation,
        complete=lambda *_args: None,
        multishot_leg=proactor_module._MultishotLegState(),
    )


def test_uring_entry_omits_multishot_leg_for_one_shot_operations():
    operation = Operation(kind="recv", fileobj=socket.socketpair()[0], proactor=None)
    entry = proactor_module._UringEntry(operation=operation, complete=lambda *_args: None)
    assert entry.multishot_leg is None


def test_completions_to_process_passes_through_non_multishot():
    entry = _multishot_test_entry()
    completion = SimpleNamespace(multishot=False)
    assert entry.completions_to_process(completion) == (completion, None)


def test_completions_to_process_defers_out_of_order_termination():
    entry = _multishot_test_entry()
    terminal = _multishot_test_completion(sequence=2, more=False)
    assert entry.completions_to_process(terminal) == (None, None)
    assert entry.multishot_leg is not None
    assert entry.multishot_leg.pending_final is terminal


def test_completions_to_process_flushes_stored_termination():
    entry = _multishot_test_entry()
    terminal = _multishot_test_completion(sequence=2, more=False)
    entry.completions_to_process(terminal)
    first = _multishot_test_completion(sequence=0)
    assert entry.completions_to_process(first) == (first, None)
    second = _multishot_test_completion(sequence=1)
    assert entry.completions_to_process(second) == (second, terminal)


def test_ordered_ingest_buffer_returns_empty_when_out_of_order():
    buffer = proactor_module._OrderedIngestBuffer[str]()
    assert buffer.ingest(1, "b") == []


def test_ordered_ingest_buffer_returns_single_ready_item():
    buffer = proactor_module._OrderedIngestBuffer[str]()
    assert buffer.ingest(0, "a") == [(0, "a")]


def test_ordered_ingest_buffer_unclogs_pending_items():
    buffer = proactor_module._OrderedIngestBuffer[str]()
    assert buffer.ingest(1, "b") == []
    assert buffer.ingest(2, "c") == []
    assert buffer.ingest(0, "a") == [(0, "a"), (1, "b"), (2, "c")]


def test_recvgen_buffer_reorders_out_of_order_chunks():
    def exercise() -> list[tuple[int, bytes]]:
        buffer = proactor_module._RecvGenBuffer()
        buffer.on_result((1, memoryview(b"b")))
        buffer.on_result((0, memoryview(b"a")))
        return [buffer.take_next(), buffer.take_next()]

    first, second = _exercise_recvgen_buffer(exercise)
    assert first == (0, b"a")
    assert second == (1, b"b")


def test_recvgen_buffer_pressure_converts_all_held_views():
    def exercise() -> list[tuple[int, bytes]]:
        buffer = proactor_module._RecvGenBuffer()
        buffer.on_result((0, memoryview(b"a")))
        buffer.on_result((1, memoryview(b"b")))
        buffer.on_result((RECV_MANY_BUFFER_PRESSURE, memoryview(b"")))
        return [buffer.take_next(), buffer.take_next()]

    (index0, chunk0), (index1, chunk1) = _exercise_recvgen_buffer(exercise)
    assert index0 == 0
    assert chunk0 == b"a"
    assert index1 == 1
    assert chunk1 == b"b"


def test_recvgen_buffer_eof_stops_iteration():
    def exercise() -> list[tuple[int, bytes] | None]:
        buffer = proactor_module._RecvGenBuffer()
        buffer.on_result((0, memoryview(b"done")))
        buffer.on_result((1, memoryview(b"")))
        return [buffer.take_next(), buffer.take_next()]

    first, second = _exercise_recvgen_buffer(exercise)
    assert first == (0, b"done")
    assert second is None


def test_recvgen_buffer_allow_memview_yields_memoryviews():
    def exercise() -> tuple[int, memoryview | bytes | None]:
        buffer = proactor_module._RecvGenBuffer(allow_memview=True)
        buffer.on_result((0, memoryview(b"a")))
        return buffer.take_next()

    index, chunk = _exercise_recvgen_buffer(exercise)
    assert index == 0
    assert type(chunk) is memoryview
    assert bytes(chunk) == b"a"


def test_recvgen_buffer_take_next_waits_for_cross_thread_delivery(monkeypatch):
    """Regression: recv completion threads must wake a blocked take_next()."""

    ready_to_wait = threading.Event()

    def exercise() -> tuple[int, bytes]:
        buffer = proactor_module._RecvGenBuffer()
        real_swait = buffer._event.swait

        def swait_and_signal() -> bool:
            ready_to_wait.set()
            return real_swait()

        monkeypatch.setattr(buffer._event, "swait", swait_and_signal)

        def producer() -> None:
            assert ready_to_wait.wait(timeout=1.0)
            buffer.on_result((0, memoryview(b"late")))

        threading.Thread(target=producer, daemon=True).start()
        item = buffer.take_next()
        assert item is not None
        index, chunk = item
        assert type(chunk) is bytes
        return index, chunk

    assert _exercise_recvgen_buffer(exercise) == (0, b"late")


def test_recvgen_buffer_allow_memview_pressure_token_precedes_flushed_queue():
    def exercise() -> list[tuple[int, memoryview | bytes | None] | None]:
        buffer = proactor_module._RecvGenBuffer(allow_memview=True)
        buffer.on_result((0, memoryview(b"a")))
        buffer.on_result((1, memoryview(b"b")))
        buffer.on_result((RECV_MANY_BUFFER_PRESSURE, memoryview(b"")))
        return [buffer.take_next(), buffer.take_next(), buffer.take_next()]

    token, first, second = _exercise_recvgen_buffer(exercise)
    assert token == (RECV_MANY_BUFFER_PRESSURE, None)
    assert first == (0, b"a")
    assert second == (1, b"b")


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


def _wait_for_uring(proactor: UringProactor, predicate, timeout: float = 1.0) -> None:
    deadline = proactor.get_time() + timeout
    while not predicate():
        if proactor.get_time() >= deadline:
            raise TimeoutError("timed out waiting for uring condition")
        proactor.wait(min(deadline, proactor.get_time() + 0.05))


def _default_uring_capabilities(**overrides: bool) -> dict[str, bool]:
    capabilities = {
        "available": True,
        "IORING_ACCEPT_MULTISHOT": True,
        "IORING_RECV_MULTISHOT": True,
        "IORING_POLL_MULTISHOT": True,
        "IORING_OP_SEND_ZC": True,
    }
    capabilities.update(overrides)
    return capabilities


def _patch_uring_capabilities(monkeypatch: pytest.MonkeyPatch, **overrides: bool) -> None:
    monkeypatch.setattr(
        uring_api,
        "probe",
        lambda *args, **kwargs: _default_uring_capabilities(**overrides),
    )


def _force_uring_multishot_probes(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_uring_capabilities(monkeypatch)


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

    def test_continuous_operation_emits_results_before_completion(self):
        operation: ContinuousOperation[int] = ContinuousOperation(kind="test")
        seen: list[int] = []

        operation.add_result_callback(seen.append)
        operation._emit_result(1)
        operation._emit_result(2)
        operation._set_result(None)

        assert seen == [1, 2]
        assert operation.done() is True
        assert operation.result() is None
        with pytest.raises(InvalidStateError, match="already done"):
            operation.add_result_callback(seen.append)


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
            assert accepted.getblocking() is False
            assert os.get_inheritable(accepted.fileno()) is False
            assert connect_operation.result() is None
        finally:
            if accepted is not None:
                accepted.close()
            client.close()
            server.close()
            proactor.close()

    def test_accept_many_emits_connections_until_cancelled(self):
        proactor = SelectorProactor()
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clients: list[socket.socket] = []
        accepted: list[tuple[socket.socket, Any]] = []
        try:
            server.setblocking(False)
            server.bind(("127.0.0.1", 0))
            server.listen()

            operation = proactor.accept_many(server, accepted.append)
            for _index in range(2):
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.setblocking(False)
                try:
                    client.connect(server.getsockname())
                except (BlockingIOError, InterruptedError):
                    pass
                clients.append(client)

            while len(accepted) < 2:
                proactor.wait(proactor.get_time() + 1.0)

            assert operation.done() is False
            assert [address[0] for _conn, address in accepted] == ["127.0.0.1", "127.0.0.1"]
            assert [conn.getblocking() for conn, _address in accepted] == [False, False]
            assert [os.get_inheritable(conn.fileno()) for conn, _address in accepted] == [False, False]
            operation.cancel()
            assert operation.cancelled() is True
        finally:
            for conn, _address in accepted:
                conn.close()
            for client in clients:
                client.close()
            server.close()
            proactor.close()

    def test_recv_many_emits_chunks_and_completes_on_eof(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        seen: list[tuple[int, memoryview]] = []
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.recv_many(reader, seen.append)

            writer.send(b"hello")
            while not seen:
                proactor.wait(proactor.get_time() + 1.0)
            writer.send(b"world")
            while len(seen) < 2:
                proactor.wait(proactor.get_time() + 1.0)
            writer.shutdown(socket.SHUT_WR)
            while not operation.done():
                proactor.wait(proactor.get_time() + 1.0)

            assert all(isinstance(data, memoryview) for _, data in seen)
            assert _recv_many_bytes(seen) == [(0, b"hello"), (1, b"world"), (2, b"")]
            assert operation.result() is None
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_poll_completes_when_fd_becomes_readable(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.poll(reader.fileno(), select.POLLIN)
            assert operation.done() is False

            writer.send(b"x")
            _wait_until_done(proactor, operation)

            assert operation.result() & select.POLLIN
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_poll_many_emits_readiness_until_cancelled(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        seen: list[int] = []
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.poll_many(reader.fileno(), select.POLLIN, seen.append)

            writer.send(b"a")
            while not seen:
                proactor.wait(proactor.get_time() + 1.0)
            assert seen[-1] & select.POLLIN

            operation.cancel()
            assert operation.cancelled() is True
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_recvall_collects_chunks_and_reports_progress(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        progress: list[int] = []
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.recvall(reader, progress.append)

            writer.send(b"hello")
            while not progress:
                proactor.wait(proactor.get_time() + 1.0)
            writer.send(b"world")
            writer.shutdown(socket.SHUT_WR)
            while not operation.done():
                proactor.wait(proactor.get_time() + 1.0)

            assert operation.result() == b"helloworld"
            assert progress == [5, 10]
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_sendall_reports_progress(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        progress: list[int] = []
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            operation = proactor.sendall(writer, b"hello", progress.append)

            assert operation.result() is None
            assert progress == [5]
            assert reader.recv(5) == b"hello"
        finally:
            reader.close()
            writer.close()
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

    def test_wait_without_pending_operations_waits_for_timeout(self):
        proactor = SelectorProactor()
        try:
            start = time.monotonic()
            proactor.wait(proactor.get_time() + 0.02)
            assert time.monotonic() - start >= 0.01
        finally:
            proactor.close()

    def test_wait_without_pending_operations_returns_on_break_wait(self):
        proactor = SelectorProactor()
        released = threading.Event()
        try:
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

    def test_wait_async_without_pending_operations_waits_for_timeout(self):
        async def run() -> float:
            proactor = SelectorProactor()
            try:
                proactor.bind_loop(asyncio.get_running_loop())
                start = time.monotonic()
                await proactor.wait_async(proactor.get_time() + 0.02)
                return time.monotonic() - start
            finally:
                proactor.close()

        assert asyncio.run(run()) >= 0.01

    def test_wait_async_requires_bound_loop(self):
        async def run() -> None:
            proactor = SelectorProactor()
            try:
                with pytest.raises(AssertionError):
                    await proactor.wait_async(proactor.get_time() + 0.001)
            finally:
                proactor.close()

        asyncio.run(run())

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
                proactor.bind_loop(asyncio.get_running_loop())
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

    def test_wait_async_uses_proactor_selector_in_executor(self, monkeypatch):
        async def run() -> bytes:
            proactor = SelectorProactor()
            reader, writer = socket.socketpair()
            try:
                loop = asyncio.get_running_loop()
                proactor.bind_loop(loop)

                def add_reader_unavailable(*args: object) -> None:
                    raise AssertionError("wait_async should not register fds on the asyncio loop")

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
                proactor.bind_loop(asyncio.get_running_loop())
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
                proactor.bind_loop(asyncio.get_running_loop())
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

    def test_wait_without_pending_operations_returns_on_break_wait(self):
        proactor = ThreadedSelectorProactor()
        released = threading.Event()
        try:
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

    def test_wait_async_requires_bound_loop(self):
        async def run() -> None:
            proactor = ThreadedSelectorProactor()
            try:
                with pytest.raises(AssertionError):
                    await proactor.wait_async(proactor.get_time() + 0.001)
            finally:
                proactor.close()

        asyncio.run(run())

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


class _FakeBufGroup:
    def __init__(self, ring: "_FakeUringRing", buffer_size: int, buffer_count: int) -> None:
        self.ring = ring
        self.buffer_size = buffer_size
        self.buffer_count = buffer_count


class _NativeBufViewPool:
    """Retain real rings and sockets so leased BufView completions stay valid."""

    def __init__(self) -> None:
        self._rings: list[uring_api.Ring] = []
        self._sockets: list[socket.socket] = []

    def view_for(self, data: bytes) -> uring_api.BufView:
        reader, writer = socket.socketpair()
        reader.setblocking(False)
        writer.setblocking(False)
        ring = uring_api.Ring(entries=2)
        group = ring.create_buf_group(max(1024, len(data)), 4)
        writer.send(data)
        ring.submit_recv_buf(reader.fileno(), group)
        completion = ring.wait(1.0)
        if completion is None or completion.res != len(data):
            reader.close()
            writer.close()
            ring.close()
            raise RuntimeError("failed to synthesize BufView completion for fake recv_many")
        payload = completion.result
        if not isinstance(payload, uring_api.BufView):
            reader.close()
            writer.close()
            ring.close()
            raise RuntimeError("recv_buf completion did not return BufView")
        self._rings.append(ring)
        self._sockets.extend((reader, writer))
        return payload


_NATIVE_BUF_VIEW_POOL: _NativeBufViewPool | None = None


def _native_buf_view_for(data: bytes) -> uring_api.BufView:
    global _NATIVE_BUF_VIEW_POOL
    if not uring_api.is_available():
        raise RuntimeError("io_uring is required to synthesize BufView recv_many completions")
    if _NATIVE_BUF_VIEW_POOL is None:
        _NATIVE_BUF_VIEW_POOL = _NativeBufViewPool()
    return _NATIVE_BUF_VIEW_POOL.view_for(data)


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
        self.serve_count = 0
        self.stop_serving_count = 0
        self._stop_serving_event = threading.Event()
        self.break_count = 0
        self.completions: list[SimpleNamespace] = []
        self.accepted_peers: list[socket.socket] = []
        self.submitted_recv: list[tuple[int, object, object]] = []
        self.submitted_recv_multishot: list[tuple[int, _FakeBufGroup, object]] = []
        self.buf_groups: list[_FakeBufGroup] = []
        self.submitted_recvmsg: list[tuple[int, object, object]] = []
        self.submitted_send: list[tuple[int, object, object]] = []
        self.submitted_sendto: list[tuple[int, object, object, object]] = []
        self.submitted_accept: list[tuple[int, object, int]] = []
        self.submitted_accept_multishot: list[tuple[int, object, int]] = []
        self.submitted_connect: list[tuple[int, object, object]] = []
        self.submitted_cancel: list[object] = []
        self.submitted_poll: list[tuple[int, int, object]] = []
        self.submitted_poll_multishot: list[tuple[int, int, object]] = []
        self.submitted_poll_remove: list[object] = []
        self.pending_recv: list[SimpleNamespace] = []
        self.pending_recv_multishot: list[SimpleNamespace] = []
        self.pending_accept_multishot: list[SimpleNamespace] = []
        self.pending_poll_multishot: list[SimpleNamespace] = []
        self.pending_poll_oneshot: list[SimpleNamespace] = []
        self.pending_accept_oneshot: list[SimpleNamespace] = []
        self.pending_recv_oneshot: list[SimpleNamespace] = []
        self.recv_multishot_sequence = 0

    def _completion(
        self,
        user_data: object,
        kind: int = uring_api.COMPLETION_KIND_RECV,
        res: int = 0,
        flags: int = 0,
        result: object = None,
        sequence: int = 0,
        *,
        multishot: bool = False,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            user_data=user_data,
            kind=kind,
            res=res,
            flags=flags,
            result=result,
            sequence=sequence,
            multishot=multishot,
        )

    def close(self) -> None:
        self.stop_serving()
        for peer in self.accepted_peers:
            peer.close()
        self.accepted_peers.clear()
        self.closed = True

    def serve_completions(self) -> None:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.running = True
        self.serve_count += 1
        self._stop_serving_event.wait()
        self.running = False

    def stop_serving(self) -> None:
        self._stop_serving_event.set()
        self.stop_serving_count += 1

    def reset_serving(self) -> None:
        self._stop_serving_event.clear()

    def break_wait(self) -> None:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.break_count += 1

    def submit_recv(self, fd: int, buf: Any, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        view = memoryview(buf)
        operation = getattr(user_data, "operation", None)
        kind = getattr(operation, "kind", None)
        self.submitted_recv.append((fd, buf, user_data))
        if kind == "recv_many":
            completion = self._completion(user_data, res=0, result=0)
            self.pending_recv_oneshot.append(completion)
            return completion
        payload = b"world" if kind == "recv_into" else b"hello"
        if len(view) >= len(payload):
            view[: len(payload)] = payload
        completion = self._completion(user_data, res=len(payload), result=len(payload))
        self.pending_recv.append(completion)
        self._deliver(completion)
        return completion

    def complete_recv_oneshot(self, data: bytes) -> None:
        completion = self.pending_recv_oneshot.pop(0)
        entry = completion.user_data
        view = memoryview(entry.data)
        if data:
            view[: len(data)] = data
            completion.res = len(data)
            completion.result = len(data)
        else:
            completion.res = 0
            completion.result = 0
        self._deliver(completion)

    def create_buf_group(self, buffer_size: int, buffer_count: int) -> _FakeBufGroup:
        if self.closed:
            raise RuntimeError("ring is closed")
        buf_group = _FakeBufGroup(self, buffer_size, buffer_count)
        self.buf_groups.append(buf_group)
        return buf_group

    def submit_recv_multishot(
        self,
        fd: int,
        buf_group: _FakeBufGroup,
        user_data: object = None,
        flags: int = 0,
    ) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_recv_multishot.append((fd, buf_group, user_data))
        self.recv_multishot_sequence = 0
        completion = self._completion(user_data, kind=uring_api.COMPLETION_KIND_RECV_MULTISHOT, multishot=True)
        self.pending_recv_multishot.append(completion)
        return completion

    def complete_recv_multishot_enobufs(self, *, sequence: int | None = None) -> None:
        pending = self.pending_recv_multishot[-1]
        if sequence is None:
            sequence = self.recv_multishot_sequence
            self.recv_multishot_sequence += 1
        completion = self._completion(
            pending.user_data,
            kind=uring_api.COMPLETION_KIND_RECV_MULTISHOT,
            res=-errno.ENOBUFS,
            flags=0,
            result=None,
            sequence=sequence,
            multishot=True,
        )
        self._deliver(completion)

    def complete_recv_multishot(self, data: bytes, *, more: bool = True, sequence: int | None = None) -> None:
        pending = self.pending_recv_multishot[-1]
        if sequence is None:
            sequence = self.recv_multishot_sequence
            self.recv_multishot_sequence += 1
        if not data:
            payload = None
            res = 0
        elif uring_api.is_available():
            payload = _native_buf_view_for(data)
            res = len(data)
        else:
            payload = data
            res = len(data)
        completion = self._completion(
            pending.user_data,
            kind=uring_api.COMPLETION_KIND_RECV_MULTISHOT,
            res=res,
            flags=uring_api.IORING_CQE_F_MORE if more else 0,
            result=payload,
            sequence=sequence,
            multishot=True,
        )
        self._deliver(completion)

    def submit_send(self, fd: int, data: Any, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        payload = bytes(data)
        self.submitted_send.append((fd, data, user_data))
        completion = self._completion(
            user_data, kind=uring_api.COMPLETION_KIND_SEND, res=len(payload), result=len(payload)
        )
        self._deliver(completion)
        return completion

    def submit_recvmsg(self, fd: int, buf: Any, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        payload = b"again" if getattr(getattr(user_data, "operation", None), "kind", None) == "recvfrom" else b"hello"
        memoryview(buf)[: len(payload)] = payload
        self.submitted_recvmsg.append((fd, buf, user_data))
        completion = self._completion(
            user_data,
            kind=uring_api.COMPLETION_KIND_RECVMSG,
            res=len(payload),
            result=("127.0.0.1", 54321),
        )
        self._deliver(completion)
        return completion

    def submit_sendto(self, fd: int, data: Any, address: Any, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        payload = bytes(data)
        self.submitted_sendto.append((fd, data, address, user_data))
        completion = self._completion(
            user_data,
            kind=uring_api.COMPLETION_KIND_SENDTO,
            res=len(payload),
            result=len(payload),
        )
        self._deliver(completion)
        return completion

    def submit_accept(self, fd: int, user_data: object = None, flags: int = 0) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        conn, peer = socket.socketpair()
        self.accepted_peers.append(peer)
        self.submitted_accept.append((fd, user_data, flags))
        completion = self._completion(
            user_data,
            kind=uring_api.COMPLETION_KIND_ACCEPT,
            res=conn.fileno(),
            result=(conn.detach(), "peer"),
        )
        operation = getattr(user_data, "operation", None)
        if getattr(operation, "kind", None) == "accept_many":
            self.pending_accept_oneshot.append(completion)
            return completion
        self._deliver(completion)
        return completion

    def complete_accept_oneshot(self) -> None:
        completion = self.pending_accept_oneshot.pop(0)
        self._deliver(completion)

    def submit_accept_multishot(self, fd: int, user_data: object = None, flags: int = 0) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_accept_multishot.append((fd, user_data, flags))
        self.accept_multishot_sequence = 0
        completion = self._completion(user_data, kind=uring_api.COMPLETION_KIND_ACCEPT, multishot=True)
        self.pending_accept_multishot.append(completion)
        return completion

    def complete_accept_multishot(
        self,
        address: object = "peer",
        *,
        more: bool = True,
        sequence: int | None = None,
    ) -> None:
        pending = self.pending_accept_multishot[-1]
        if sequence is None:
            sequence = getattr(self, "accept_multishot_sequence", 0)
            self.accept_multishot_sequence = sequence + 1
        conn, peer = socket.socketpair()
        self.accepted_peers.append(peer)
        completion = self._completion(
            pending.user_data,
            kind=uring_api.COMPLETION_KIND_ACCEPT,
            res=conn.fileno(),
            flags=uring_api.IORING_CQE_F_MORE if more else 0,
            result=(conn.detach(), address),
            sequence=sequence,
            multishot=True,
        )
        self._deliver(completion)

    def submit_connect(self, fd: int, address: Any, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_connect.append((fd, address, user_data))
        completion = self._completion(user_data, kind=uring_api.COMPLETION_KIND_CONNECT, res=0, result=None)
        self._deliver(completion)
        return completion

    def submit_cancel(self, completion: SimpleNamespace) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_cancel.append(completion)
        cancel_completion = self._completion(completion, kind=uring_api.COMPLETION_KIND_CANCEL, res=0, result=None)
        self._deliver(cancel_completion)
        return cancel_completion

    def submit_poll(self, fd: int, mask: int, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_poll.append((fd, mask, user_data))
        completion = self._completion(user_data, kind=uring_api.COMPLETION_KIND_POLL, res=mask, result=mask)
        operation = getattr(user_data, "operation", None)
        if getattr(operation, "kind", None) == "poll_many":
            self.pending_poll_oneshot.append(completion)
            return completion
        self._deliver(completion)
        return completion

    def complete_poll_oneshot(self, res: int = select.POLLIN) -> None:
        completion = self.pending_poll_oneshot.pop(0)
        completion.res = res
        completion.result = res
        self._deliver(completion)

    def submit_poll_multishot(self, fd: int, mask: int, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_poll_multishot.append((fd, mask, user_data))
        completion = self._completion(user_data, kind=uring_api.COMPLETION_KIND_POLL_MULTISHOT, multishot=True)
        self.pending_poll_multishot.append(completion)
        return completion

    def complete_poll_multishot(
        self,
        res: int = select.POLLIN,
        *,
        more: bool = True,
        sequence: int = 0,
    ) -> None:
        pending = self.pending_poll_multishot[-1]
        completion = self._completion(
            pending.user_data,
            kind=uring_api.COMPLETION_KIND_POLL_MULTISHOT,
            res=res,
            flags=uring_api.IORING_CQE_F_MORE if more else 0,
            sequence=sequence,
            multishot=True,
        )
        self._deliver(completion)

    def submit_poll_remove(self, completion: SimpleNamespace) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_poll_remove.append(completion)
        remove_completion = self._completion(completion, kind=uring_api.COMPLETION_KIND_POLL_REMOVE, res=0)
        self._deliver(remove_completion)
        return remove_completion

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
    def submit_recv(self, fd: int, buf: Any, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        self.submitted_recv.append((fd, buf, user_data))
        completion = self._completion(user_data)
        self.pending_recv.append(completion)
        return completion

    def complete_recv(self, data: bytes = b"hello") -> None:
        _fd, buf, user_data = self.submitted_recv[-1]
        memoryview(buf)[: len(data)] = data
        completion = self.pending_recv[-1]
        completion.res = len(data)
        completion.flags = 0
        completion.result = len(data)
        self._deliver(completion)


class _ZeroCopyFakeUringRing(_FakeUringRing):
    def __init__(self, entries: int, flags: int) -> None:
        super().__init__(entries, flags)
        self.submitted_send_zc: list[tuple[int, object, object]] = []

    def submit_send_zc(self, fd: int, data: Any, user_data: object = None) -> SimpleNamespace:
        if self.closed:
            raise RuntimeError("ring is closed")
        payload = bytes(data)
        self.submitted_send_zc.append((fd, data, user_data))
        completion = self._completion(
            user_data,
            kind=uring_api.COMPLETION_KIND_SEND_ZC,
            res=len(payload),
            result=len(payload),
        )
        self._deliver(completion)
        return completion


class _BackpressuredUringRing(_DeferredUringRing):
    def __init__(self, entries: int = 8, flags: int = 0) -> None:
        super().__init__(entries, flags)
        self.fail_next_recv = False
        self.fail_next_cancel = False

    def submit_recv(self, fd: int, buf: Any, user_data: object = None) -> None:
        if self.fail_next_recv:
            self.fail_next_recv = False
            raise uring_api.SubmissionQueueFull("no submission queue entries available")
        return super().submit_recv(fd, buf, user_data)

    def submit_cancel(self, completion: SimpleNamespace) -> SimpleNamespace:
        if self.fail_next_cancel:
            self.fail_next_cancel = False
            raise uring_api.SubmissionQueueFull("no submission queue entries available")
        return super().submit_cancel(completion)


class TestUringProactor:
    @pytest.fixture(autouse=True)
    def _default_multishot_probes(self, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> None:
        if "falls_back" in request.node.name:
            return
        _force_uring_multishot_probes(monkeypatch)

    def test_capabilities_cached_from_single_probe(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[tuple[int, int]] = []

        def tracking_capabilities(*args: object, **kwargs: object) -> dict[str, bool]:
            calls.append((kwargs.get("entries", args[0] if args else 8), kwargs.get("flags", args[1] if len(args) > 1 else 0)))
            return {
                "available": True,
                "IORING_RECV_MULTISHOT": True,
                "IORING_OP_SEND_ZC": False,
            }

        monkeypatch.setattr(uring_api, "probe", tracking_capabilities)
        proactor = UringProactor(ring_factory=_FakeUringRing, entries=16, flags=1 << 12)
        try:
            assert calls == [(16, 1 << 12)]
            assert proactor.capabilities["available"] is True
            assert proactor.capabilities["IORING_RECV_MULTISHOT"] is True
            assert proactor.capabilities["IORING_OP_SEND_ZC"] is False
            assert proactor.capabilities is not proactor._capabilities
        finally:
            proactor.close()

    def test_wait_without_pending_operations_waits_for_timeout(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            start = time.monotonic()
            proactor.wait(proactor.get_time() + 0.02)
            assert time.monotonic() - start >= 0.01
        finally:
            proactor.close()

    def test_wait_without_pending_operations_returns_on_break_wait(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        released = threading.Event()
        try:
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

    def test_wait_async_without_pending_operations_returns_on_break_wait(self, monkeypatch):
        async def run() -> None:
            proactor = UringProactor(ring_factory=_FakeUringRing)
            try:
                loop = asyncio.get_running_loop()
                proactor.bind_loop(loop)

                def call_soon_threadsafe(*args, **kwargs):
                    raise AssertionError("same-thread break_wait should set the asyncio event directly")

                monkeypatch.setattr(loop, "call_soon_threadsafe", call_soon_threadsafe)
                waiter = asyncio.create_task(proactor.wait_async(proactor.get_time() + 10.0))
                await asyncio.sleep(0)
                assert waiter.done() is False

                proactor.break_wait()

                await asyncio.wait_for(waiter, 1.0)
            finally:
                proactor.close()

        asyncio.run(run())

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

    def test_starts_default_completion_threads(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            assert isinstance(proactor.ring, _FakeUringRing)
            deadline = time.monotonic() + 1.0
            while proactor.ring.serve_count < 2 and time.monotonic() < deadline:
                time.sleep(0.01)
            assert proactor.ring.serve_count == 2
        finally:
            proactor.close()

    def test_starts_configured_completion_threads(self):
        proactor = UringProactor(
            ring_factory=_FakeUringRing,
            completion_threads=3,
        )
        try:
            assert isinstance(proactor.ring, _FakeUringRing)
            deadline = time.monotonic() + 1.0
            while proactor.ring.serve_count < 3 and time.monotonic() < deadline:
                time.sleep(0.01)
            assert proactor.ring.serve_count == 3
        finally:
            proactor.close()

    def test_validates_completion_thread_configuration(self):
        with pytest.raises(ValueError, match="completion_threads must be at least 1"):
            UringProactor(ring_factory=_FakeUringRing, completion_threads=0)

    def test_applies_default_completion_thread_nice(self, monkeypatch: pytest.MonkeyPatch):
        calls: list[tuple[int, int, int]] = []

        def setpriority(which: int, who: int, priority: int) -> None:
            calls.append((which, who, priority))

        monkeypatch.setattr(proactor_module.os, "setpriority", setpriority)

        proactor = UringProactor(
            ring_factory=_FakeUringRing,
            completion_threads=1,
        )
        try:
            deadline = time.monotonic() + 1.0
            while not calls and time.monotonic() < deadline:
                time.sleep(0.01)
            assert calls == [(proactor_module.os.PRIO_PROCESS, 0, -5)]
        finally:
            proactor.close()

    def test_applies_configured_completion_thread_nice(self, monkeypatch: pytest.MonkeyPatch):
        calls: list[tuple[int, int, int]] = []

        def setpriority(which: int, who: int, priority: int) -> None:
            calls.append((which, who, priority))

        monkeypatch.setattr(proactor_module.os, "setpriority", setpriority)

        proactor = UringProactor(
            ring_factory=_FakeUringRing,
            completion_threads=1,
            completion_thread_nice=-5,
        )
        try:
            deadline = time.monotonic() + 1.0
            while not calls and time.monotonic() < deadline:
                time.sleep(0.01)
            assert calls == [(proactor_module.os.PRIO_PROCESS, 0, -5)]
        finally:
            proactor.close()

    def test_clock_can_be_replaced(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            proactor.set_clock(lambda: 42.0)

            assert proactor.get_time() == 42.0
        finally:
            proactor.close()

    def test_wait_returns_at_elapsed_deadline_without_pending_operations(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            proactor.set_clock(lambda: 100.0)

            proactor.wait(100.0)
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
                proactor.bind_loop(asyncio.get_running_loop())

                await proactor.wait_async(proactor.get_time())
            finally:
                proactor.close()

        asyncio.run(run())

    def test_wait_async_requires_bound_loop(self):
        async def run() -> None:
            proactor = UringProactor(ring_factory=_FakeUringRing)
            try:
                with pytest.raises(AssertionError):
                    await proactor.wait_async(proactor.get_time() + 0.001)
            finally:
                proactor.close()

        asyncio.run(run())

    def test_bind_loop_prepares_async_wait_state(self):
        async def run() -> bool:
            proactor = UringProactor(ring_factory=_FakeUringRing)
            try:
                loop = asyncio.get_running_loop()
                assert proactor._async_wait_event is None

                proactor.bind_loop(loop)

                assert proactor._async_wait_loop is loop
                assert proactor._async_wait_thread_id == threading.get_ident()
                return proactor._async_wait_event is not None
            finally:
                proactor.close()

        assert asyncio.run(run()) is True

    def test_bind_loop_rejects_different_event_loop(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        first_loop = asyncio.new_event_loop()
        second_loop = asyncio.new_event_loop()
        try:
            proactor.bind_loop(first_loop)

            with pytest.raises(RuntimeError, match="already bound to a different event loop"):
                proactor.bind_loop(second_loop)
        finally:
            first_loop.close()
            second_loop.close()
            proactor.close()

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
                proactor.bind_loop(loop)
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

    def test_uring_entry_keeps_pending_completion_handle(self):
        proactor = UringProactor(ring_factory=_DeferredUringRing)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            proactor.recv(reader, 5)
            assert isinstance(proactor.ring, _DeferredUringRing)
            _fd, _buf, entry = proactor.ring.submitted_recv[-1]

            assert entry.completion is proactor.ring.pending_recv[-1]
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
            assert proactor.ring.submitted_cancel == [proactor.ring.pending_recv[-1]]
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

    def test_multishot_more_completion_keeps_uring_entry_active(self):
        proactor = UringProactor(ring_factory=_DeferredUringRing)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            operation = proactor.recv(reader, 5)
            assert isinstance(proactor.ring, _DeferredUringRing)
            _fd, buf, entry = proactor.ring.submitted_recv[-1]
            memoryview(buf)[:5] = b"hello"

            proactor.ring._deliver(
                SimpleNamespace(
                    user_data=entry,
                    kind=uring_api.COMPLETION_KIND_RECV,
                    res=5,
                    flags=uring_api.IORING_CQE_F_MORE,
                    result=5,
                    multishot=False,
                )
            )

            assert operation.result() == b"hello"
            assert entry.active is True
            assert proactor.has_pending_operations() is True

            proactor.ring._deliver(
                SimpleNamespace(
                    user_data=entry,
                    kind=uring_api.COMPLETION_KIND_RECV,
                    res=-errno.ECANCELED,
                    flags=0,
                    result=None,
                    multishot=False,
                )
            )

            assert entry.active is False
            assert proactor.has_pending_operations() is False
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_submission_queue_full_defers_and_retries_after_completion(self):
        proactor = UringProactor(ring_factory=_BackpressuredUringRing)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            first = proactor.recv(reader, 5)
            assert isinstance(proactor.ring, _BackpressuredUringRing)
            assert len(proactor.ring.submitted_recv) == 1

            proactor.ring.fail_next_recv = True
            second = proactor.recv(reader, 5)
            assert second.done() is False
            assert proactor.has_pending_operations() is True
            assert len(proactor.ring.submitted_recv) == 1

            proactor.ring.complete_recv(b"first")
            assert first.result() == b"first"
            assert len(proactor.ring.submitted_recv) == 2

            proactor.ring.complete_recv(b"again")
            assert second.result() == b"again"
            assert proactor.has_pending_operations() is False
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_cancel_removes_deferred_submission(self):
        proactor = UringProactor(ring_factory=_BackpressuredUringRing)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            first = proactor.recv(reader, 5)
            assert isinstance(proactor.ring, _BackpressuredUringRing)

            proactor.ring.fail_next_recv = True
            second = proactor.recv(reader, 5)
            second.cancel()

            assert second.cancelled() is True
            proactor.ring.complete_recv(b"first")
            assert first.result() == b"first"
            assert len(proactor.ring.submitted_recv) == 1
            assert proactor.has_pending_operations() is False
            with pytest.raises(CancelledError):
                second.result()
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_cancel_submission_queue_full_defers_cancel_request(self):
        proactor = UringProactor(ring_factory=_BackpressuredUringRing)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            operation = proactor.recv(reader, 5)
            assert isinstance(proactor.ring, _BackpressuredUringRing)

            proactor.ring.fail_next_cancel = True
            operation.cancel()

            assert operation.cancelled() is True
            assert proactor.ring.submitted_cancel == []
            assert proactor.has_pending_operations() is True

            proactor.ring.complete_recv(b"hello")

            assert proactor.ring.submitted_cancel == [proactor.ring.pending_recv[-1]]
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

    def test_sendall_uses_send_zc_when_probe_supports_it(self, monkeypatch):
        _patch_uring_capabilities(monkeypatch, IORING_OP_SEND_ZC=True)
        proactor = UringProactor(ring_factory=_ZeroCopyFakeUringRing)
        reader, writer = socket.socketpair()
        try:
            writer.setblocking(False)
            payload = b"hello"
            operation = proactor.sendall(writer, payload)

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() is None
            assert isinstance(proactor.ring, _ZeroCopyFakeUringRing)
            assert proactor.ring.submitted_send == []
            submitted = proactor.ring.submitted_send_zc[0][1]
            assert isinstance(submitted, memoryview)
            assert submitted.obj is payload
            assert bytes(submitted) == b"hello"
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_sendall_uses_send_when_probe_lacks_send_zc(self, monkeypatch):
        _patch_uring_capabilities(monkeypatch, IORING_OP_SEND_ZC=False)
        proactor = UringProactor(ring_factory=_ZeroCopyFakeUringRing)
        reader, writer = socket.socketpair()
        try:
            writer.setblocking(False)
            operation = proactor.sendall(writer, b"hello")

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() is None
            assert isinstance(proactor.ring, _ZeroCopyFakeUringRing)
            assert len(proactor.ring.submitted_send) == 1
            assert proactor.ring.submitted_send_zc == []
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
            assert os.get_inheritable(conn.fileno()) is False
            assert isinstance(proactor.ring, _FakeUringRing)
            submitted = proactor.ring.submitted_accept[0]
            assert submitted[0] == server.fileno()
            assert submitted[2] & socket.SOCK_NONBLOCK
            assert submitted[2] & socket.SOCK_CLOEXEC
        finally:
            if conn is not None:
                conn.close()
            server.close()
            proactor.close()

    def test_poll_uses_submit_poll(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.poll(reader.fileno(), select.POLLIN)
            assert isinstance(proactor.ring, _FakeUringRing)
            assert len(proactor.ring.submitted_poll) == 1
            assert proactor.ring.submitted_poll[0][:2] == (reader.fileno(), select.POLLIN)
            assert operation.done() is True
            assert operation.result() == select.POLLIN
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_poll_many_uses_multishot_poll(self, monkeypatch):
        _patch_uring_capabilities(monkeypatch, IORING_POLL_MULTISHOT=True)
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        seen: list[int] = []
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.poll_many(reader.fileno(), select.POLLIN, seen.append)
            assert isinstance(proactor.ring, _FakeUringRing)
            assert proactor.ring.submitted_poll_multishot
            proactor.ring.complete_poll_multishot(select.POLLIN, more=False)
            _wait_for_uring(proactor, lambda: seen == [select.POLLIN] and operation.done())
            assert operation.result() is None
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_poll_many_cancel_uses_poll_remove(self, monkeypatch):
        _patch_uring_capabilities(monkeypatch, IORING_POLL_MULTISHOT=True)
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.poll_many(reader.fileno(), select.POLLIN, lambda _mask: None)
            handle = proactor.ring.pending_poll_multishot[-1]
            operation.cancel()
            _wait_for_uring(proactor, lambda: proactor.ring.submitted_poll_remove == [handle])
            _wait_for_uring(proactor, lambda: not proactor.has_pending_operations())
            assert operation.cancelled() is True
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_poll_many_falls_back_to_oneshot_poll_and_resubmits(self, monkeypatch):
        _patch_uring_capabilities(monkeypatch, IORING_POLL_MULTISHOT=False)
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        seen: list[int] = []
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.poll_many(reader.fileno(), select.POLLIN, seen.append)
            assert proactor.ring.submitted_poll_multishot == []
            assert len(proactor.ring.submitted_poll) == 1
            proactor.ring.complete_poll_oneshot(select.POLLIN)
            _wait_for_uring(proactor, lambda: seen == [select.POLLIN])
            _wait_for_uring(proactor, lambda: len(proactor.ring.submitted_poll) == 2)
            assert operation.done() is False
            operation.cancel()
            assert operation.cancelled() is True
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_poll_many_cancel_uses_cancel_in_oneshot_fallback(self, monkeypatch):
        _patch_uring_capabilities(monkeypatch, IORING_POLL_MULTISHOT=False)
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.poll_many(reader.fileno(), select.POLLIN, lambda _mask: None)
            pending = proactor.ring.pending_poll_oneshot[-1]
            operation.cancel()
            _wait_for_uring(proactor, lambda: pending in proactor.ring.submitted_cancel)
            assert proactor.ring.submitted_poll_remove == []
            assert operation.cancelled() is True
        finally:
            reader.close()
            writer.close()
            proactor.close()

    @pytest.mark.skipif(not uring_api.is_available(), reason="io_uring is required")
    def test_native_poll_completes_when_fd_becomes_readable(self):
        proactor = UringProactor()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.poll(reader.fileno(), select.POLLIN)
            writer.send(b"x")
            _wait_for_uring(proactor, operation.done)
            assert operation.result() & select.POLLIN
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_accept_many_falls_back_to_oneshot_accept_and_resubmits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_uring_capabilities(monkeypatch, IORING_ACCEPT_MULTISHOT=False)
        proactor = UringProactor(ring_factory=_FakeUringRing)
        server = socket.socket()
        accepted: list[tuple[socket.socket, Any]] = []
        try:
            server.setblocking(False)
            operation = proactor.accept_many(server, accepted.append)
            assert proactor.ring.submitted_accept_multishot == []
            assert len(proactor.ring.submitted_accept) == 1
            proactor.ring.complete_accept_oneshot()
            _wait_for_uring(proactor, lambda: len(accepted) == 1)
            _wait_for_uring(proactor, lambda: len(proactor.ring.submitted_accept) == 2)
            assert operation.done() is False
            operation.cancel()
            assert operation.cancelled() is True
        finally:
            for conn, _address in accepted:
                conn.close()
            server.close()
            proactor.close()

    def test_accept_many_uses_multishot_accept(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        server = socket.socket()
        accepted: list[tuple[socket.socket, Any]] = []
        try:
            server.setblocking(False)
            operation = proactor.accept_many(server, accepted.append)
            assert isinstance(proactor.ring, _FakeUringRing)
            submitted = proactor.ring.submitted_accept_multishot[0]
            assert submitted[0] == server.fileno()
            assert submitted[2] & socket.SOCK_NONBLOCK
            assert submitted[2] & socket.SOCK_CLOEXEC

            proactor.ring.complete_accept_multishot("peer-1")
            proactor.wait(proactor.get_time() + 1.0)

            assert operation.done() is False
            assert accepted[0][1] == "peer-1"
            assert accepted[0][0].getblocking() is False
            assert os.get_inheritable(accepted[0][0].fileno()) is False
        finally:
            for conn, _address in accepted:
                conn.close()
            server.close()
            proactor.close()

    def test_recv_many_uses_custom_buf_group_factory(self):
        created: list[tuple[int, int]] = []

        def factory(ring: _FakeUringRing) -> _FakeBufGroup:
            created.append((8, 4))
            return ring.create_buf_group(8, 4)

        proactor = UringProactor(ring_factory=_FakeUringRing, buf_group_factory=factory)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            proactor.recv_many(reader, lambda _result: None)
            assert created == [(8, 4)]
            submitted = proactor.ring.submitted_recv_multishot[0]
            assert submitted[1].buffer_size == 8
            assert submitted[1].buffer_count == 4
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_recv_many_falls_back_to_oneshot_recv_and_finishes_on_eof(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_uring_capabilities(monkeypatch, IORING_RECV_MULTISHOT=False)
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        seen: list[tuple[int, memoryview]] = []
        try:
            reader.setblocking(False)
            operation = proactor.recv_many(reader, seen.append)
            assert proactor.ring.submitted_recv_multishot == []
            assert len(proactor.ring.submitted_recv) == 1
            proactor.ring.complete_recv_oneshot(b"hello")
            _wait_for_uring(proactor, lambda: _recv_many_bytes(seen) == [(0, b"hello")])
            _wait_for_uring(proactor, lambda: len(proactor.ring.submitted_recv) == 2)
            proactor.ring.complete_recv_oneshot(b"")
            _wait_for_uring(proactor, lambda: operation.done())
            assert _recv_many_bytes(seen) == [(0, b"hello"), (1, b"")]
            assert proactor._recv_many_buf_group is None
        finally:
            reader.close()
            writer.close()
            proactor.close()

    @pytest.mark.skipif(not uring_api.is_available(), reason="io_uring is required for BufView recv_many completions")
    def test_recv_many_uses_multishot_recv_and_finishes_on_eof(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        seen: list[tuple[int, memoryview]] = []
        try:
            reader.setblocking(False)
            operation = proactor.recv_many(reader, seen.append)
            assert isinstance(proactor.ring, _FakeUringRing)
            submitted = proactor.ring.submitted_recv_multishot[0]
            assert submitted[0] == reader.fileno()
            assert submitted[1] is proactor._recv_many_buf_group
            assert submitted[1].buffer_size == 16 * 1024
            assert submitted[1].buffer_count == 256

            proactor.ring.complete_recv_multishot(b"hello")
            proactor.wait(proactor.get_time() + 1.0)
            proactor.ring.complete_recv_multishot(b"", more=False)
            proactor.wait(proactor.get_time() + 1.0)

            assert _recv_many_bytes(seen) == [(0, b"hello"), (1, b"")]
            assert operation.done() is True
            assert operation.result() is None
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_recv_many_retries_after_enobufs_and_preserves_stream_sequence(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        seen: list[tuple[int, memoryview]] = []
        try:
            reader.setblocking(False)
            operation = proactor.recv_many(reader, seen.append)
            ring = proactor.ring
            ring.complete_recv_multishot(b"a", more=True, sequence=0)
            ring.complete_recv_multishot(b"b", more=True, sequence=1)
            ring.complete_recv_multishot_enobufs(sequence=2)
            assert seen[-1][0] == RECV_MANY_BUFFER_PRESSURE
            assert len(ring.submitted_recv_multishot) == 2
            ring.complete_recv_multishot(b"c", more=True, sequence=0)
            ring.complete_recv_multishot(b"", more=False, sequence=1)
            assert _recv_many_bytes(seen) == [
                (0, b"a"),
                (1, b"b"),
                (RECV_MANY_BUFFER_PRESSURE, b""),
                (2, b"c"),
                (3, b""),
            ]
            assert operation.done() is True
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_recv_many_accumulates_stream_sequence_across_repeated_enobufs(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        seen: list[tuple[int, memoryview]] = []
        try:
            reader.setblocking(False)
            operation = proactor.recv_many(reader, seen.append)
            ring = proactor.ring
            ring.complete_recv_multishot(b"a", more=True, sequence=0)
            ring.complete_recv_multishot(b"b", more=True, sequence=1)
            ring.complete_recv_multishot_enobufs(sequence=2)
            ring.complete_recv_multishot(b"c", more=True, sequence=0)
            ring.complete_recv_multishot_enobufs(sequence=1)
            assert len(ring.submitted_recv_multishot) == 3
            ring.complete_recv_multishot(b"d", more=True, sequence=0)
            ring.complete_recv_multishot(b"", more=False, sequence=1)
            assert _recv_many_bytes(seen) == [
                (0, b"a"),
                (1, b"b"),
                (RECV_MANY_BUFFER_PRESSURE, b""),
                (2, b"c"),
                (RECV_MANY_BUFFER_PRESSURE, b""),
                (3, b"d"),
                (4, b""),
            ]
            assert operation.done() is True
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_recv_many_defers_finalize_when_terminal_arrives_before_earlier_chunks(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        seen: list[tuple[int, memoryview]] = []
        try:
            reader.setblocking(False)
            operation = proactor.recv_many(reader, seen.append)
            ring = proactor.ring
            ring.complete_recv_multishot(b"", more=False, sequence=2)
            ring.complete_recv_multishot(b"hello", sequence=0)
            ring.complete_recv_multishot(b"world", sequence=1)
            assert _recv_many_bytes_sorted(seen) == [(0, b"hello"), (1, b"world"), (2, b"")]
            assert operation.done() is True
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_recv_many_defers_enobufs_when_it_arrives_before_earlier_chunks(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        seen: list[tuple[int, memoryview]] = []
        try:
            reader.setblocking(False)
            operation = proactor.recv_many(reader, seen.append)
            ring = proactor.ring
            ring.complete_recv_multishot_enobufs(sequence=2)
            ring.complete_recv_multishot(b"a", more=True, sequence=0)
            ring.complete_recv_multishot(b"b", more=True, sequence=1)
            ring.complete_recv_multishot(b"c", more=False, sequence=0)
            assert _recv_many_bytes_sorted(seen) == [(0, b"a"), (1, b"b"), (2, b"c")]
            assert (RECV_MANY_BUFFER_PRESSURE, b"") in _recv_many_bytes(seen)
            assert operation.done() is True
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_recvall_defers_finalize_when_terminal_burst_arrives_out_of_order(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            operation = proactor.recvall(reader, None)
            ring = proactor.ring
            ring.complete_recv_multishot(b"", more=False, sequence=2)
            ring.complete_recv_multishot(b"hello", sequence=0)
            ring.complete_recv_multishot(b"world", sequence=1)
            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() == b"helloworld"
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_recvgen_reorders_out_of_order_multishot_chunks(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)

            def receive_chunks() -> list[tuple[int, bytes]]:
                return list(scheduler.sock_recvgen(reader))

            def deliver_chunks() -> None:
                ring = scheduler.proactor.ring
                ring.complete_recv_multishot(b"b", more=True, sequence=1)
                ring.complete_recv_multishot(b"a", more=True, sequence=0)
                ring.complete_recv_multishot(b"", more=False, sequence=2)

            task = scheduler.spawn(receive_chunks)
            scheduler.spawn(deliver_chunks)

            assert scheduler.run_until_complete(task) == [(0, b"a"), (1, b"b")]
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_recvgen_survives_buffer_pressure_and_continues_receive(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)

            def receive_chunks() -> list[tuple[int, bytes]]:
                return list(scheduler.sock_recvgen(reader))

            def deliver_chunks() -> None:
                ring = scheduler.proactor.ring
                ring.complete_recv_multishot(b"a", more=True, sequence=0)
                ring.complete_recv_multishot(b"b", more=True, sequence=1)
                ring.complete_recv_multishot(b"c", more=True, sequence=2)
                ring.complete_recv_multishot_enobufs(sequence=3)
                ring.complete_recv_multishot(b"d", more=False, sequence=0)

            task = scheduler.spawn(receive_chunks)
            scheduler.spawn(deliver_chunks)

            assert scheduler.run_until_complete(task) == [(0, b"a"), (1, b"b"), (2, b"c"), (3, b"d")]
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_recvgen_allow_memview_pressure_token_and_continues_receive(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)

            def receive_chunks() -> tuple[bool, list[tuple[int, bytes]]]:
                got_memview = False
                got_pressure = False
                seen: list[tuple[int, bytes]] = []
                for index, chunk in scheduler.sock_recvgen(reader, allow_memview=True):
                    if index == RECV_MANY_BUFFER_PRESSURE:
                        got_pressure = True
                        continue
                    if type(chunk) is memoryview:
                        got_memview = True
                    seen.append((index, bytes(chunk)))
                return got_memview and got_pressure, seen

            def deliver_chunks() -> None:
                ring = scheduler.proactor.ring
                ring.complete_recv_multishot(b"a", more=True, sequence=0)
                scheduler.sleep(0.05)
                ring.complete_recv_multishot(b"b", more=True, sequence=1)
                ring.complete_recv_multishot(b"c", more=True, sequence=2)
                ring.complete_recv_multishot_enobufs(sequence=3)
                ring.complete_recv_multishot(b"d", more=False, sequence=0)

            task = scheduler.spawn(receive_chunks)
            scheduler.spawn(deliver_chunks)

            saw_memview_and_pressure, seen = scheduler.run_until_complete(task)
            assert saw_memview_and_pressure
            assert seen == [(0, b"a"), (1, b"b"), (2, b"c"), (3, b"d")]
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_recvall_survives_buffer_pressure_and_continues_receive(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            operation = proactor.recvall(reader, None)
            ring = proactor.ring
            ring.complete_recv_multishot(b"a", more=True, sequence=0)
            ring.complete_recv_multishot(b"b", more=True, sequence=1)
            ring.complete_recv_multishot(b"c", more=True, sequence=2)
            ring.complete_recv_multishot_enobufs(sequence=3)
            ring.complete_recv_multishot(b"d", more=False, sequence=0)
            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() == b"abcd"
        finally:
            reader.close()
            writer.close()
            proactor.close()

    @pytest.mark.skipif(not uring_api.is_available(), reason="io_uring is required for BufView recv_many completions")
    def test_recvall_collects_many_out_of_order_chunks(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            operation = proactor.recvall(reader, None)
            assert isinstance(proactor.ring, _FakeUringRing)

            for sequence, byte in enumerate(b"abcdefghijklmnop"):
                proactor.ring.complete_recv_multishot(bytes((byte,)), sequence=sequence)
                proactor.wait(proactor.get_time() + 1.0)
            proactor.ring.complete_recv_multishot(b"", more=False, sequence=16)
            proactor.wait(proactor.get_time() + 1.0)

            assert operation.result() == b"abcdefghijklmnop"
        finally:
            reader.close()
            writer.close()
            proactor.close()

    @pytest.mark.skipif(not uring_api.is_available(), reason="io_uring is required for BufView recv_many completions")
    def test_recvall_collects_uring_multishot_receive_and_reports_progress(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        progress: list[int] = []
        try:
            reader.setblocking(False)
            operation = proactor.recvall(reader, progress.append)
            assert isinstance(proactor.ring, _FakeUringRing)

            proactor.ring.complete_recv_multishot(b"world", sequence=1)
            proactor.wait(proactor.get_time() + 1.0)
            proactor.ring.complete_recv_multishot(b"hello", sequence=0)
            proactor.wait(proactor.get_time() + 1.0)
            proactor.ring.complete_recv_multishot(b"", more=False, sequence=2)
            proactor.wait(proactor.get_time() + 1.0)

            assert operation.result() == b"helloworld"
            assert progress == [5, 10]
        finally:
            reader.close()
            writer.close()
            proactor.close()

    @pytest.mark.requires_native_uring_recv_multishot
    def test_native_recv_many_cancel_after_data_before_sender_close(self):
        proactor = UringProactor()
        reader, writer = socket.socketpair()
        seen: list[tuple[int, memoryview]] = []
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.recv_many(reader, seen.append)

            writer.send(b"hello")
            _wait_for_uring(proactor, lambda: _recv_many_bytes(seen) == [(0, b"hello")])

            operation.cancel()
            _wait_for_uring(proactor, lambda: not proactor.has_pending_operations())

            assert operation.cancelled() is True
            assert _recv_many_bytes(seen) == [(0, b"hello")]
            with pytest.raises(CancelledError):
                operation.result()
        finally:
            reader.close()
            writer.close()
            proactor.close()

    @pytest.mark.requires_native_uring_recv_multishot
    def test_native_recvall_cancel_after_data_before_sender_close(self):
        proactor = UringProactor()
        reader, writer = socket.socketpair()
        progress: list[int] = []
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.recvall(reader, progress.append)

            writer.send(b"hello")
            _wait_for_uring(proactor, lambda: progress == [5])

            operation.cancel()
            _wait_for_uring(proactor, lambda: not proactor.has_pending_operations())

            assert operation.cancelled() is True
            assert progress == [5]
            with pytest.raises(CancelledError):
                operation.result()
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_sendall_reports_uring_progress(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        progress: list[int] = []
        try:
            writer.setblocking(False)
            operation = proactor.sendall(writer, b"hello", progress.append)

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() is None
            assert progress == [5]
        finally:
            reader.close()
            writer.close()
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

    def test_sock_recvgen_yields_chunks_in_stream_order(self):
        scheduler = SyncProactorScheduler()
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            def receive_chunks() -> list[tuple[int, bytes]]:
                return list(scheduler.sock_recvgen(reader))

            def send_chunks() -> None:
                scheduler.sock_sendall(writer, b"hello")
                scheduler.sleep(0.05)
                scheduler.sock_sendall(writer, b"world")
                writer.shutdown(socket.SHUT_WR)

            task = scheduler.spawn(receive_chunks)
            scheduler.spawn(send_chunks)

            assert scheduler.run_until_complete(task) == [(0, b"hello"), (1, b"world")]
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
            bound_loops = []

            class TrackingProactor(SelectorProactor):
                def bind_loop(self, loop):
                    bound_loops.append(loop)
                    super().bind_loop(loop)

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
                assert bound_loops == [loop]
                assert stored_callback is not None
                stored_callback()
                await asyncio.sleep(0)
                return calls == 1
            finally:
                scheduler.close()

        assert asyncio.run(run()) is True
