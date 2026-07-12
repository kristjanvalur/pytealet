from __future__ import annotations

import asyncio
import errno
import fcntl
import io
import os
import select
import struct
import selectors
import socket
import tempfile
import threading
import time
from tealetio.tasks import CancelledError
from types import SimpleNamespace
from collections.abc import Callable
from typing import Any, Callable, cast

import pytest
import uring_api

from uring_fakes import (
    _BackpressuredPollUringRing,
    _BackpressuredUringRing,
    _DeferredConnectUringRing,
    _DeferredCreateSocketUringRing,
    _DeferredSocketUringRing,
    _DeferredUringRing,
    _FailOnResubmitUringRing,
    _FailingConnectUringRing,
    _FailingSubmitUringRing,
    _FakeUringRing,
    _force_uring_multishot_probes,
    _pack_fake_statx_buffer,
    _patch_uring_capabilities,
    PROACTOR_CONTRACT_FACTORIES,
    PROACTOR_UNIT_TEST_FACTORIES,
    SCHEDULER_INTEGRATION_FACTORIES,
)

import tealetio.poll_helpers as poll_helpers_module
import tealetio.proactor as proactor_module
import tealetio.recv_iter as recv_iter_module
from tealetio import TimeoutError, set_scheduler, timeout
from tealetio.io_waiter import IOWaiter
from tealetio.operations import InvalidStateError
from tealetio.proactor import (
    AsyncProactorScheduler,
    ContinuousOperation,
    Operation,
    ProactorScheduler,
    RECV_MANY_BUFFER_PRESSURE,
    SelectorProactor,
    SyncProactorScheduler,
    ThreadedSelectorProactor,
    UringProactor,
    UringSubmissionStats,
)


_RecvManySeen = tuple[int, memoryview | Callable[[], None]]


def _assert_scheduler_socket_fd(sock: socket.socket) -> None:
    """Uring socket fds are non-blocking and close-on-exec; wrapper state must match."""

    assert sock.getblocking() is False
    flags = fcntl.fcntl(sock.fileno(), fcntl.F_GETFL)
    assert flags & os.O_NONBLOCK
    assert not os.get_inheritable(sock.fileno())


def _io_sock_create(
    proactor: UringProactor,
    family: int,
    type: int,
    proto: int = 0,
    *,
    flags: int = 0,
    connect_to: Any | None = None,
    initial_data: Any | None = None,
) -> socket.socket:
    scheduler = SyncProactorScheduler(lambda: proactor)
    set_scheduler(scheduler)
    try:
        return scheduler.run_until_complete(
            scheduler.spawn(
                lambda: scheduler.io.sock_create(
                    family,
                    type,
                    proto,
                    flags=flags,
                    connect_to=connect_to,
                    initial_data=initial_data,
                ).wait()
            )
        )
    finally:
        scheduler.close()


def _recv_many_auto_resume_callback(seen: list[_RecvManySeen]) -> Callable[[_RecvManySeen], None]:
    def on_result(result: _RecvManySeen) -> None:
        seen.append(result)
        if result[0] == RECV_MANY_BUFFER_PRESSURE and callable(result[1]):
            result[1]()

    return on_result


def _recv_many_bytes(seen: list[_RecvManySeen]) -> list[tuple[int, bytes]]:
    return [(index, bytes(data)) for index, data in seen if index >= 0]


def _recv_many_bytes_sorted(seen: list[_RecvManySeen]) -> list[tuple[int, bytes]]:
    return sorted((index, bytes(data)) for index, data in seen if index >= 0)


def _noop_recv_many_resume() -> Callable[[], None]:
    return lambda: None


class _RecvIterTestPool:
    buffer_size = 16 * 1024
    buffer_count = 8
    leased_count = 0


def _recviter_test_pool() -> _RecvIterTestPool:
    return _RecvIterTestPool()


def _iter_recv_stream(stream: Any):
    yield from stream


def _recviter_bytes(stream: Any) -> list[tuple[int, bytes]]:
    return [(index, bytes(chunk)) for index, chunk in _iter_recv_stream(stream) if index >= 0]


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
        result_callback=lambda _result: None,
    )
    return proactor_module._UringEntry(
        operation=operation,
        complete=lambda *_args: None,
        multishot=True,
    )


def test_uring_entry_omits_multishot_leg_for_one_shot_operations():
    operation = Operation(kind="recv", fileobj=socket.socketpair()[0])
    entry = proactor_module._UringEntry(operation=operation, complete=lambda *_args: None)
    assert entry.multishot_leg is None


def test_completions_to_process_passes_through_non_multishot():
    entry = _multishot_test_entry()
    completion = SimpleNamespace(multishot=False)
    assert entry.completions_to_process(completion) == (completion,)


def test_completions_to_process_defers_out_of_order_termination():
    entry = _multishot_test_entry()
    terminal = _multishot_test_completion(sequence=2, more=False)
    assert entry.completions_to_process(terminal) == ()
    assert entry.multishot_leg is not None
    assert entry.multishot_leg.pending_final is terminal


def test_completions_to_process_flushes_stored_termination():
    entry = _multishot_test_entry()
    terminal = _multishot_test_completion(sequence=2, more=False)
    entry.completions_to_process(terminal)
    first = _multishot_test_completion(sequence=0)
    assert entry.completions_to_process(first) == (first,)
    second = _multishot_test_completion(sequence=1)
    assert entry.completions_to_process(second) == (second, terminal)


def _drain_ordered_ingest_buffer(buffer: recv_iter_module._OrderedIngestBuffer[str]) -> list[tuple[int, str]]:
    ready: list[tuple[int, str]] = []
    while True:
        item = buffer.pop()
        if item is None:
            break
        ready.append(item)
    return ready


def test_ordered_ingest_buffer_push_defers_out_of_order_items():
    buffer = recv_iter_module._OrderedIngestBuffer[str]()
    buffer.push((1, "b"))
    assert len(buffer) == 1
    assert not buffer
    assert buffer.pop() is None
    assert buffer.pushpop((2, "c")) is None
    assert len(buffer) == 2
    assert not buffer


def test_ordered_ingest_buffer_pushpop_returns_next_ready_item():
    buffer = recv_iter_module._OrderedIngestBuffer[str]()
    assert buffer.pushpop((0, "a")) == (0, "a")
    assert not buffer


def test_ordered_ingest_buffer_unclogs_pending_items():
    buffer = recv_iter_module._OrderedIngestBuffer[str]()
    buffer.push((1, "b"))
    buffer.push((2, "c"))
    ready = [buffer.pushpop((0, "a")), *_drain_ordered_ingest_buffer(buffer)]
    assert ready == [(0, "a"), (1, "b"), (2, "c")]


def test_ordered_ingest_buffer_bool_only_when_next_index_is_on_heap():
    buffer = recv_iter_module._OrderedIngestBuffer[str]()
    buffer.push((2, "c"))
    buffer.push((1, "b"))
    assert len(buffer) == 2
    assert not buffer

    ready = [buffer.pushpop((0, "a")), *_drain_ordered_ingest_buffer(buffer)]
    assert ready == [(0, "a"), (1, "b"), (2, "c")]
    assert not buffer

    waiting = recv_iter_module._OrderedIngestBuffer[str]()
    waiting.pushpop((0, "a"))
    waiting.push((1, "b"))
    assert waiting
    assert waiting.pop() == (1, "b")


def test_ordered_ingest_buffer_reset_restores_next_index():
    buffer = recv_iter_module._OrderedIngestBuffer[str](start=5)
    buffer.pushpop((5, "a"))
    buffer.push((7, "c"))
    buffer.reset()
    assert buffer.next_index == 0
    assert not buffer
    assert buffer.pushpop((0, "z")) == (0, "z")

    buffer.reset(start=10)
    assert buffer.next_index == 10
    assert buffer.pushpop((10, "x")) == (10, "x")


@pytest.mark.skipif(
    not proactor_module._supports_release_buffer(), reason="leased selector chunks require Python 3.12+"
)
def test_selector_leased_memoryview_release_returns_pool_slot():
    pool = proactor_module._SelectorBufGroup(1024, 4)
    view = proactor_module._leased_selector_memoryview(b"abc", pool)
    assert pool.leased_count == 1
    assert bytes(view) == b"abc"
    view.release()
    assert pool.leased_count == 0


@pytest.mark.skipif(
    not proactor_module._supports_release_buffer(), reason="leased selector chunks require Python 3.12+"
)
def test_selector_buf_group_pressure_threshold_matches_recviter_policy():
    pool = proactor_module._SelectorBufGroup(1024, 4)
    required_free = max(1, pool.buffer_count // 2)
    assert required_free == 2
    views = [proactor_module._leased_selector_memoryview(b"x", pool) for _ in range(3)]
    assert pool.buffer_count - pool.leased_count < required_free
    for view in views:
        view.release()
    assert pool.buffer_count - pool.leased_count >= required_free


def test_recviter_buffer_reorders_out_of_order_chunks():
    def exercise() -> list[tuple[int, memoryview | None]]:
        buffer = proactor_module._RecvIterBuffer(buf_group=_recviter_test_pool())
        buffer.on_result((1, memoryview(b"b")))
        buffer.on_result((0, memoryview(b"a")))
        return [buffer.take_next(), buffer.take_next()]

    first, second = _exercise_recviter_buffer(exercise)
    assert first is not None and first[0] == 0 and bytes(first[1]) == b"a"
    assert second is not None and second[0] == 1 and bytes(second[1]) == b"b"


def test_recviter_buffer_resume_waits_until_half_pool_is_free():
    resumed: list[bool] = []

    def resume() -> None:
        resumed.append(True)

    class _Pool:
        buffer_count = 4
        leased_count = 4

        def note_chunk_released(self) -> None:
            if self.leased_count:
                self.leased_count -= 1

    def exercise() -> list[bool]:
        pool = _Pool()
        buffer = proactor_module._RecvIterBuffer(buf_group=pool)
        buffer.on_result((0, memoryview(b"a")))
        buffer.on_result((1, memoryview(b"b")))
        buffer.on_result((RECV_MANY_BUFFER_PRESSURE, resume))
        _assert_recviter_pressure(buffer.take_next())
        buffer.consume_pressure_resume()
        assert resumed == []
        first = buffer.take_next()
        assert first is not None and first[0] == 0
        pool.note_chunk_released()
        assert resumed == []
        second = buffer.take_next()
        assert second is not None and second[0] == 1
        pool.note_chunk_released()
        buffer.on_result((2, memoryview(b"")))
        assert buffer.take_next() is None
        return resumed

    assert _exercise_recviter_buffer(exercise) == [True]


def test_recviter_buffer_ignores_late_callbacks_after_close():
    def exercise() -> tuple[int, bool]:
        buffer = proactor_module._RecvIterBuffer(buf_group=_recviter_test_pool())
        buffer.on_result((0, memoryview(b"a")))
        buffer.close()
        buffer.on_result((1, memoryview(b"b")))
        buffer.on_result((RECV_MANY_BUFFER_PRESSURE, _noop_recv_many_resume()))
        return len(buffer._ready), bool(buffer._reorder)

    ready_len, reorder_pending = _exercise_recviter_buffer(exercise)
    assert ready_len == 0
    assert not reorder_pending


def test_recviter_buffer_pressure_token_precedes_queued_views():
    def exercise() -> list[tuple[int, memoryview | None] | None]:
        buffer = proactor_module._RecvIterBuffer(buf_group=_recviter_test_pool())
        buffer.on_result((0, memoryview(b"a")))
        buffer.on_result((1, memoryview(b"b")))
        buffer.on_result((RECV_MANY_BUFFER_PRESSURE, _noop_recv_many_resume()))
        return [buffer.take_next(), buffer.take_next(), buffer.take_next()]

    token, first, second = _exercise_recviter_buffer(exercise)
    _assert_recviter_pressure(token)
    assert first is not None and first[0] == 0 and bytes(first[1]) == b"a"
    assert second is not None and second[0] == 1 and bytes(second[1]) == b"b"


def test_recviter_buffer_eof_stops_iteration():
    def exercise() -> list[tuple[int, memoryview | None] | None]:
        buffer = proactor_module._RecvIterBuffer(buf_group=_recviter_test_pool())
        buffer.on_result((0, memoryview(b"done")))
        buffer.on_result((1, memoryview(b"")))
        return [buffer.take_next(), buffer.take_next()]

    first, second = _exercise_recviter_buffer(exercise)
    assert first is not None and first[0] == 0 and bytes(first[1]) == b"done"
    assert second is None


def test_recviter_buffer_ordered_eof_wins_cancel_race():
    def exercise() -> list[tuple[int, memoryview | None] | None]:
        buffer = proactor_module._RecvIterBuffer(buf_group=_recviter_test_pool())
        buffer.on_result((0, memoryview(b"done")))
        with buffer._lock:
            buffer._stream_done = True
            buffer._stream_error = CancelledError()
        buffer.on_result((1, memoryview(b"")))
        return [buffer.take_next(), buffer.take_next()]

    first, second = _exercise_recviter_buffer(exercise)
    assert first is not None and first[0] == 0 and bytes(first[1]) == b"done"
    assert second is None


def test_recviter_buffer_delivers_buffered_chunks_before_stream_error():
    def exercise() -> list[object]:
        buffer = proactor_module._RecvIterBuffer(buf_group=_recviter_test_pool())
        buffer.on_result((0, memoryview(b"a")))
        buffer.on_result((1, memoryview(b"b")))
        with buffer._lock:
            buffer._stream_done = True
            buffer._stream_error = OSError("recv failed")
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
        buffer = proactor_module._RecvIterBuffer(buf_group=_recviter_test_pool())
        buffer.on_result((0, memoryview(b"a")))
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
        buffer = proactor_module._RecvIterBuffer(buf_group=_recviter_test_pool())
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
        assert type(chunk) is memoryview
        return index, chunk

    index, chunk = _exercise_recviter_buffer(exercise)
    assert index == 0
    assert bytes(chunk) == b"late"


def test_recviter_buffer_resumes_on_pressure_while_waiting(monkeypatch):
    """Regression: ENOBUFS while blocked must re-arm recv when no views remain."""

    resumed: list[bool] = []
    ready_to_wait = threading.Event()

    class _Pool:
        buffer_count = 4
        leased_count = 1

        def note_chunk_released(self) -> None:
            if self.leased_count:
                self.leased_count -= 1

    def exercise() -> tuple[tuple[int, memoryview], list[bool]]:
        pool = _Pool()
        buffer = proactor_module._RecvIterBuffer(buf_group=pool)

        def resume() -> None:
            resumed.append(True)
            buffer.on_result((1, memoryview(b"b")))

        buffer.on_result((0, memoryview(b"a")))
        first = buffer.take_next()
        assert first is not None and first[0] == 0 and bytes(first[1]) == b"a"

        real_swait = buffer._event.swait

        def swait_and_signal() -> bool:
            ready_to_wait.set()
            return real_swait()

        monkeypatch.setattr(buffer._event, "swait", swait_and_signal)

        def producer() -> None:
            assert ready_to_wait.wait(timeout=1.0)
            buffer.on_result((RECV_MANY_BUFFER_PRESSURE, resume))

        threading.Thread(target=producer, daemon=True).start()
        pressure = buffer.take_next()
        _assert_recviter_pressure(pressure)
        buffer.consume_pressure_resume()
        second = buffer.take_next()
        assert second is not None and second[0] == 1 and bytes(second[1]) == b"b"
        return second, resumed

    second, called = _exercise_recviter_buffer(exercise)
    assert second[0] == 1 and bytes(second[1]) == b"b"
    assert called == [True]


def test_recviter_buffer_single_slot_pool_requires_one_free_before_resume():
    resumed: list[bool] = []

    def resume() -> None:
        resumed.append(True)

    class _Pool:
        buffer_count = 1
        leased_count = 1

        def note_chunk_released(self) -> None:
            if self.leased_count:
                self.leased_count -= 1

    def exercise() -> list[bool]:
        pool = _Pool()
        buffer = proactor_module._RecvIterBuffer(buf_group=pool)
        buffer.on_result((0, memoryview(b"a")))
        buffer.on_result((RECV_MANY_BUFFER_PRESSURE, resume))
        first = buffer.take_next()
        _assert_recviter_pressure(first)
        second = buffer.take_next()
        assert second is not None and second[0] == 0
        pool.note_chunk_released()
        buffer.on_result((1, memoryview(b"")))
        assert buffer.take_next() is None
        return resumed

    assert _exercise_recviter_buffer(exercise) == [True]


def test_recviter_buffer_resumes_when_half_pool_is_free():
    resumed: list[bool] = []

    def resume() -> None:
        resumed.append(True)

    class _Pool:
        buffer_count = 4
        leased_count = 4

        def note_chunk_released(self) -> None:
            if self.leased_count:
                self.leased_count -= 1

    def exercise() -> list[bool]:
        pool = _Pool()
        buffer = proactor_module._RecvIterBuffer(buf_group=pool)
        buffer.on_result((0, memoryview(b"a")))
        buffer.on_result((1, memoryview(b"b")))
        buffer.on_result((RECV_MANY_BUFFER_PRESSURE, resume))
        token = buffer.take_next()
        _assert_recviter_pressure(token)
        first = buffer.take_next()
        assert first is not None and first[0] == 0
        pool.note_chunk_released()
        assert resumed == []
        second = buffer.take_next()
        assert second is not None and second[0] == 1
        pool.note_chunk_released()
        buffer.on_result((2, memoryview(b"")))
        assert buffer.take_next() is None
        return resumed

    assert _exercise_recviter_buffer(exercise) == [True]


def test_recviter_buffer_defers_resume_until_all_queued_chunks_yielded():
    resumed: list[bool] = []

    def resume() -> None:
        resumed.append(True)

    class _Pool:
        buffer_count = 4
        leased_count = 4

        def note_chunk_released(self) -> None:
            if self.leased_count:
                self.leased_count -= 1

    def exercise() -> tuple[list[tuple[int, memoryview]], list[bool]]:
        pool = _Pool()
        buffer = proactor_module._RecvIterBuffer(buf_group=pool)
        buffer.on_result((0, memoryview(b"a")))
        buffer.on_result((1, memoryview(b"b")))
        buffer.on_result((RECV_MANY_BUFFER_PRESSURE, resume))
        token = buffer.take_next()
        _assert_recviter_pressure(token)
        assert resumed == []
        first = buffer.take_next()
        assert first is not None and first[0] == 0 and bytes(first[1]) == b"a"
        pool.note_chunk_released()
        assert resumed == []
        second = buffer.take_next()
        assert second is not None and second[0] == 1 and bytes(second[1]) == b"b"
        pool.note_chunk_released()
        buffer.on_result((2, memoryview(b"")))
        eof = buffer.take_next()
        assert eof is None
        return [first, second], resumed

    chunks, called = _exercise_recviter_buffer(exercise)
    assert [(index, bytes(chunk)) for index, chunk in chunks] == [(0, b"a"), (1, b"b")]
    assert called == [True]


def test_recviter_buffer_defers_resume_until_next_take_after_yielding_chunk():
    resumed: list[bool] = []

    def resume() -> None:
        resumed.append(True)

    class _Pool:
        buffer_count = 2
        leased_count = 2

        def note_chunk_released(self) -> None:
            if self.leased_count:
                self.leased_count -= 1

    def exercise() -> tuple[tuple[int, memoryview | None] | None, list[bool]]:
        pool = _Pool()
        buffer = proactor_module._RecvIterBuffer(buf_group=pool)
        buffer.on_result((0, memoryview(b"a")))
        buffer.on_result((RECV_MANY_BUFFER_PRESSURE, resume))
        token = buffer.take_next()
        _assert_recviter_pressure(token)
        assert resumed == []
        first = buffer.take_next()
        assert first is not None and first[0] == 0 and bytes(first[1]) == b"a"
        pool.note_chunk_released()
        assert resumed == []
        buffer.on_result((1, memoryview(b"")))
        second = buffer.take_next()
        assert resumed == [True]
        return second, resumed

    eof, called = _exercise_recviter_buffer(exercise)
    assert eof is None
    assert called == [True]


def test_recviter_buffer_defers_resume_while_reorder_heap_has_gap():
    resumed: list[bool] = []

    def resume() -> None:
        resumed.append(True)

    class _Pool:
        buffer_count = 4
        leased_count = 0

    def exercise() -> list[bool]:
        pool = _Pool()
        buffer = proactor_module._RecvIterBuffer(buf_group=pool)
        buffer.on_result((1, memoryview(b"b")))
        buffer.on_result((2, memoryview(b"c")))
        buffer.on_result((RECV_MANY_BUFFER_PRESSURE, resume))
        _assert_recviter_pressure(buffer.take_next())
        assert resumed == []
        buffer.on_result((0, memoryview(b"a")))
        first = buffer.take_next()
        assert first is not None and first[0] == 0 and bytes(first[1]) == b"a"
        assert resumed == []
        second = buffer.take_next()
        assert second is not None and second[0] == 1 and bytes(second[1]) == b"b"
        assert resumed == []
        third = buffer.take_next()
        assert third is not None and third[0] == 2 and bytes(third[1]) == b"c"
        buffer.on_result((3, memoryview(b"")))
        assert buffer.take_next() is None
        return resumed

    assert _exercise_recviter_buffer(exercise) == [True]


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


def _pump_proactor(proactor: SelectorProactor | UringProactor, *operations: Operation[Any]) -> list[Operation[Any]]:
    if isinstance(proactor, UringProactor):
        if operations:
            _wait_for_uring(proactor, lambda: all(op.done() for op in operations))
        return list(operations)
    return _wait_until_done(proactor, *operations)


def _pump_until(proactor: SelectorProactor | UringProactor, predicate: Callable[[], bool], timeout: float = 1.0) -> None:
    if isinstance(proactor, UringProactor):
        _wait_for_uring(proactor, predicate, timeout)
        return
    deadline = proactor.get_time() + timeout
    while not predicate():
        if proactor.get_time() >= deadline:
            raise TimeoutError("timed out waiting for proactor condition")
        proactor.wait(min(deadline, proactor.get_time() + 0.05))


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
        operation._finish(result=42)
        operation.add_done_callback(lambda op: seen.append(op.result() + 1))

        assert seen == [42, 43]

    def test_proactor_cancel_completes_operation_with_cancelled_error(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            operation = proactor.recv(reader, 1)
            proactor.cancel(operation)
            assert operation.done() is True
            assert operation.cancelled() is True
            assert operation.exception()

            with pytest.raises(CancelledError):
                operation.result()
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_proactor_cancel_returns_teardown_operation(self) -> None:
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            target = proactor.recv(reader, 1)
            teardown = proactor.cancel(target)
            assert teardown.kind == "cancel"
            assert teardown.done() is True
            assert target.cancelled() is True
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_continuous_operation_emits_results_before_completion(self):
        seen: list[int] = []
        operation: ContinuousOperation[int] = ContinuousOperation(kind="test", result_callback=seen.append)
        operation._emit_result(1)
        operation._emit_result(2)
        operation._finish(result=None)

        assert seen == [1, 2]
        assert operation.done() is True
        assert operation.result() is None

    def test_continuous_operation_emit_result_skips_when_done(self):
        seen: list[int] = []
        operation: ContinuousOperation[int] = ContinuousOperation(kind="test", result_callback=seen.append)
        assert operation._emit_result(1) is True
        operation._finish(exception=CancelledError(), cancelled=True)
        assert operation._emit_result(2) is False
        assert seen == [1]

    def test_operation_deliver_ignored_after_cancel(self) -> None:
        operation = Operation(kind="test")
        operation._finish(exception=CancelledError(), cancelled=True)
        operation.deliver(object(), result=None)
        assert operation.cancelled()

    def test_continuous_operation_emit_result_false_after_cancel(self) -> None:
        seen: list[int] = []
        parent = ContinuousOperation(kind="test", result_callback=seen.append)
        parent._finish(exception=CancelledError(), cancelled=True)
        assert parent._emit_result(1) is False
        assert seen == []

    def test_marshal_to_scheduler_delivers_on_scheduler_thread(self):
        from tealetio.continuous_callbacks import marshal_to_scheduler

        scheduler = SyncProactorScheduler()
        delivery_threads: list[int] = []

        def exercise() -> None:
            owner = threading.get_ident()
            marshalled = marshal_to_scheduler(
                scheduler,
                lambda _result: delivery_threads.append(threading.get_ident()),
            )

            def invoke_from_worker() -> None:
                marshalled(7)

            worker = threading.Thread(target=invoke_from_worker)
            worker.start()
            worker.join()
            deadline = scheduler.time() + 1.0
            while len(delivery_threads) < 1 and scheduler.time() < deadline:
                scheduler.sleep(0)
            assert delivery_threads == [owner]

        set_scheduler(scheduler)
        try:
            scheduler.run_until_complete(scheduler.spawn(exercise))
        finally:
            scheduler.close()


def test_operation_deliver_completes_without_handler() -> None:
    operation = Operation[int](kind="test")
    operation.deliver(object(), result=7)
    assert operation.result() == 7


@pytest.mark.parametrize("proactor_factory", PROACTOR_CONTRACT_FACTORIES)
class TestProactorContract:
    def test_clock_can_be_replaced(self, proactor_factory: Callable[[], SelectorProactor | UringProactor]) -> None:
        proactor = proactor_factory()
        try:
            proactor.set_clock(lambda: 42.0)
            assert proactor.get_time() == 42.0
        finally:
            proactor.close()

    def test_recv_completes_after_wait(self, proactor_factory: Callable[[], SelectorProactor | UringProactor]) -> None:
        proactor = proactor_factory()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.recv(reader, 5)
            writer.sendall(b"hello")
            _pump_proactor(proactor, operation)
            assert operation.result() == b"hello"
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_recv_into_completes_buffer(self, proactor_factory: Callable[[], SelectorProactor | UringProactor]) -> None:
        proactor = proactor_factory()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            buf = bytearray(5)
            operation = proactor.recv_into(reader, buf)
            writer.sendall(b"hello")
            _pump_proactor(proactor, operation)
            assert operation.result() == 5
            assert bytes(buf) == b"hello"
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_send_can_complete_immediately(self, proactor_factory: Callable[[], SelectorProactor | UringProactor]) -> None:
        proactor = proactor_factory()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.send(writer, b"hello")
            if not operation.done():
                _pump_proactor(proactor, operation)
            assert operation.result() is None
            payload = b""
            deadline = proactor.get_time() + 1.0
            while len(payload) < 5 and proactor.get_time() < deadline:
                try:
                    payload += reader.recv(5 - len(payload))
                except BlockingIOError:
                    proactor.wait(min(deadline, proactor.get_time() + 0.05))
            assert payload == b"hello"
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_accept_and_connect_complete_after_pumping(
        self, proactor_factory: Callable[[], SelectorProactor | UringProactor]
    ) -> None:
        proactor = proactor_factory()
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
            completed = _pump_proactor(proactor, accept_operation, connect_operation)
            accepted = accept_operation.result()

            assert accept_operation in completed
            assert connect_operation in completed
            assert accepted.getpeername()[0] == "127.0.0.1"
            assert accepted.getblocking() is False
            assert os.get_inheritable(accepted.fileno()) is False
            assert connect_operation.result() is None
        finally:
            if accepted is not None:
                accepted.close()
            client.close()
            server.close()
            proactor.close()

    def test_datagram_helpers(self, proactor_factory: Callable[[], SelectorProactor | UringProactor]) -> None:
        proactor = proactor_factory()
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            receiver.setblocking(False)
            sender.setblocking(False)
            receiver.bind(("127.0.0.1", 0))
            buf = bytearray(5)

            receive_operation = proactor.recvfrom_into(receiver, buf)
            send_operation = proactor.sendto(sender, b"hello", receiver.getsockname())
            _pump_proactor(proactor, receive_operation, send_operation)

            count, address = receive_operation.result()
            assert count == 5
            assert bytes(buf) == b"hello"
            assert address[1] == sender.getsockname()[1]
            assert send_operation.result() == 5

            receive_bytes_operation = proactor.recvfrom(receiver, 5)
            sender.sendto(b"again", receiver.getsockname())
            _pump_until(proactor, receive_bytes_operation.done)
            data, address = receive_bytes_operation.result()
            assert data == b"again"
            assert address[1] == sender.getsockname()[1]
        finally:
            sender.close()
            receiver.close()
            proactor.close()

    def test_operations_reject_closed_proactor(self, proactor_factory: Callable[[], SelectorProactor | UringProactor]) -> None:
        proactor = proactor_factory()
        reader, writer = socket.socketpair()
        try:
            proactor.close()
            with pytest.raises(RuntimeError, match="closed"):
                proactor.recv(reader, 1)
            with pytest.raises(RuntimeError, match="closed"):
                proactor.send(writer, b"")
            with pytest.raises(RuntimeError, match="closed"):
                proactor.wait(0)
        finally:
            reader.close()
            writer.close()

class TestSelectorProactor:
    def test_stat_completes_immediately_with_blocking_fstat(self):
        proactor = SelectorProactor()
        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                path = tmp.name
                tmp.write(b"hello")
            try:
                fd = os.open(path, os.O_RDONLY)
                try:
                    operation = proactor.stat(fd=fd)
                    assert operation.done()
                    assert operation.result().st_size == 5
                finally:
                    os.close(fd)
            finally:
                os.unlink(path)
        finally:
            proactor.close()

    def test_stat_fdsize_completes_immediately_with_blocking_fstat(self):
        proactor = SelectorProactor()
        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                path = tmp.name
                tmp.write(b"hello")
            try:
                fd = os.open(path, os.O_RDONLY)
                try:
                    operation = proactor.stat_fdsize(fd)
                    assert operation.done()
                    assert operation.result() == 5
                finally:
                    os.close(fd)
            finally:
                os.unlink(path)
        finally:
            proactor.close()

    def test_file_operations_are_not_implemented(self):
        proactor = SelectorProactor()
        try:
            with pytest.raises(NotImplementedError):
                proactor.openat("/tmp/x", os.O_RDONLY)
            with pytest.raises(NotImplementedError):
                proactor.read(0, 1, 0)
            with pytest.raises(NotImplementedError):
                proactor.read_into(0, bytearray(1), 0)
            with pytest.raises(NotImplementedError):
                proactor.write(0, b"x", 0)
        finally:
            proactor.close()

    def test_close_fd_completes_immediately_with_blocking_os_close(self):
        proactor = SelectorProactor()
        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                path = tmp.name
            try:
                fd = os.open(path, os.O_RDONLY)
                try:
                    operation = proactor.close_fd(fd)
                    assert operation.done()
                    assert operation.result() is None
                    with pytest.raises(OSError):
                        os.fstat(fd)
                finally:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
            finally:
                os.unlink(path)
        finally:
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

    def test_accept_many_emulated_path_finishes_after_each_accept(self):
        proactor = SelectorProactor()
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clients: list[socket.socket] = []
        accepted: list[socket.socket] = []
        try:
            server.setblocking(False)
            server.bind(("127.0.0.1", 0))
            server.listen()

            for _index in range(2):
                operation = proactor.accept_many(server, accepted.append)
                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.setblocking(False)
                try:
                    client.connect(server.getsockname())
                except (BlockingIOError, InterruptedError):
                    pass
                clients.append(client)

                while len(accepted) <= _index:
                    proactor.wait(proactor.get_time() + 1.0)

                assert operation.done() is True

            assert [conn.getpeername()[0] for conn in accepted] == ["127.0.0.1", "127.0.0.1"]
            assert [conn.getblocking() for conn in accepted] == [False, False]
            assert [os.get_inheritable(conn.fileno()) for conn in accepted] == [False, False]
        finally:
            for conn in accepted:
                conn.close()
            for client in clients:
                client.close()
            server.close()
            proactor.close()

    def test_create_socket_returns_scheduler_socket_on_selector(self) -> None:
        proactor = SelectorProactor()
        try:
            operation = proactor.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            assert operation.done()
            sock = operation.result()
            _assert_scheduler_socket_fd(sock)
            sock.close()
        finally:
            proactor.close()

    def test_recv_many_emits_chunks_and_completes_on_eof(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        seen: list[tuple[int, memoryview]] = []
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.recv_many(reader, seen.append, buf_group=proactor.shared_recv_buffer_pool())

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

    @pytest.mark.skipif(
        not proactor_module._supports_release_buffer(), reason="leased selector chunks require Python 3.12+"
    )
    def test_recv_many_emits_pressure_when_pool_is_full(self):
        proactor = SelectorProactor()
        buf_group = proactor.create_buf_group(1024, 2)
        reader, writer = socket.socketpair()
        seen: list[_RecvManySeen] = []
        held: list[memoryview] = []
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            def on_result(result: _RecvManySeen) -> None:
                index, payload = result
                if index >= 0:
                    if payload:
                        view = cast(memoryview, payload)
                        held.append(view)
                        seen.append((index, bytes(view)))
                    else:
                        seen.append((index, b""))
                    return
                seen.append(result)
                if index == RECV_MANY_BUFFER_PRESSURE and callable(payload):
                    for view in held:
                        view.release()
                    held.clear()
                    cast(Callable[[], None], payload)()

            operation = proactor.recv_many(reader, on_result, buf_group=buf_group)
            writer.send(b"a")
            while len([item for item in seen if item[0] >= 0]) < 1:
                proactor.wait(proactor.get_time() + 1.0)
            writer.send(b"b")
            while len([item for item in seen if item[0] >= 0]) < 2:
                proactor.wait(proactor.get_time() + 1.0)
            writer.send(b"c")
            deadline = proactor.get_time() + 1.0
            while not any(index == RECV_MANY_BUFFER_PRESSURE for index, _payload in seen):
                if proactor.get_time() >= deadline:
                    break
                proactor.wait(proactor.get_time() + 0.05)
            assert any(index == RECV_MANY_BUFFER_PRESSURE and callable(payload) for index, payload in seen)
            while len([item for item in seen if item[0] >= 0]) < 3:
                proactor.wait(proactor.get_time() + 1.0)
            writer.shutdown(socket.SHUT_WR)
            while not operation.done():
                proactor.wait(proactor.get_time() + 1.0)
            data_seen = [(index, payload) for index, payload in seen if index >= 0]
            assert data_seen == [(0, b"a"), (1, b"b"), (2, b"c"), (3, b"")]
        finally:
            reader.close()
            writer.close()
            proactor.close()

    @pytest.mark.skipif(
        not proactor_module._supports_release_buffer(), reason="leased selector chunks require Python 3.12+"
    )
    def test_recv_many_reemits_pressure_after_premature_resume(self):
        proactor = SelectorProactor()
        buf_group = proactor.create_buf_group(1024, 2)
        reader, writer = socket.socketpair()
        seen: list[_RecvManySeen] = []
        held: list[memoryview] = []
        pressure_count = 0
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            def on_result(result: _RecvManySeen) -> None:
                nonlocal pressure_count
                index, payload = result
                if index >= 0:
                    if payload:
                        view = cast(memoryview, payload)
                        held.append(view)
                        seen.append((index, bytes(view)))
                    else:
                        seen.append((index, b""))
                    return
                seen.append(result)
                if index == RECV_MANY_BUFFER_PRESSURE and callable(payload):
                    pressure_count += 1
                    if pressure_count == 1:
                        cast(Callable[[], None], payload)()
                        return
                    for view in held:
                        view.release()
                    held.clear()
                    cast(Callable[[], None], payload)()

            operation = proactor.recv_many(reader, on_result, buf_group=buf_group)
            writer.send(b"a")
            while len([item for item in seen if item[0] >= 0]) < 1:
                proactor.wait(proactor.get_time() + 1.0)
            writer.send(b"b")
            while len([item for item in seen if item[0] >= 0]) < 2:
                proactor.wait(proactor.get_time() + 1.0)
            writer.send(b"c")
            deadline = proactor.get_time() + 1.0
            while pressure_count < 1:
                if proactor.get_time() >= deadline:
                    break
                proactor.wait(proactor.get_time() + 0.05)
            assert pressure_count == 1
            deadline = proactor.get_time() + 1.0
            while pressure_count < 2:
                if proactor.get_time() >= deadline:
                    break
                proactor.wait(proactor.get_time() + 0.05)
            assert pressure_count == 2
            while len([item for item in seen if item[0] >= 0]) < 3:
                proactor.wait(proactor.get_time() + 1.0)
            writer.shutdown(socket.SHUT_WR)
            while not operation.done():
                proactor.wait(proactor.get_time() + 1.0)
            data_seen = [(index, payload) for index, payload in seen if index >= 0]
            assert data_seen == [(0, b"a"), (1, b"b"), (2, b"c"), (3, b"")]
        finally:
            reader.close()
            writer.close()
            proactor.close()

    @pytest.mark.skipif(
        not proactor_module._supports_release_buffer(), reason="leased selector chunks require Python 3.12+"
    )
    def test_recviter_survives_selector_buffer_pressure(self):
        scheduler = SyncProactorScheduler()
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        state = {"got_pressure": False, "release": False}
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            def receive_chunks() -> tuple[bool, list[tuple[int, bytes]]]:
                got_memview = False
                saw_pressure = False
                seen: list[tuple[int, bytes]] = []
                held: list[memoryview] = []
                pool = scheduler.io.create_recv_buffer_pool(16 * 1024, 2)
                for index, chunk in _iter_recv_stream(scheduler.io.sock_recv_iter(reader, pool)):
                    if index == RECV_MANY_BUFFER_PRESSURE:
                        saw_pressure = True
                        state["got_pressure"] = True
                        for view in held:
                            view.release()
                        held.clear()
                        deadline = scheduler.proactor.get_time() + 1.0
                        while not state["release"] and scheduler.proactor.get_time() < deadline:
                            scheduler.sleep(0.02)
                        assert state["release"]
                        continue
                    if type(chunk) is memoryview:
                        got_memview = True
                        held.append(chunk)
                    seen.append((index, bytes(chunk)))
                return got_memview and saw_pressure, seen

            def deliver_chunks() -> None:
                scheduler.io.sock_sendall(writer, b"a").wait()
                scheduler.sleep(0.02)
                scheduler.io.sock_sendall(writer, b"b").wait()
                scheduler.sleep(0.02)
                scheduler.io.sock_sendall(writer, b"c").wait()
                deadline = scheduler.proactor.get_time() + 1.0
                while not state["got_pressure"] and scheduler.proactor.get_time() < deadline:
                    scheduler.sleep(0.02)
                assert state["got_pressure"]
                state["release"] = True
                scheduler.sleep(0.05)
                scheduler.io.sock_sendall(writer, b"d").wait()
                writer.shutdown(socket.SHUT_WR)

            task = scheduler.spawn(receive_chunks)
            scheduler.spawn(deliver_chunks)
            saw_memview_and_pressure, seen = scheduler.run_until_complete(task)
            assert saw_memview_and_pressure
            assert b"".join(payload for _, payload in seen) == b"abcd"
            assert [index for index, _payload in seen] == [0, 1, 2, 3]
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_continuous_fd_slot_stores_step_and_cancel_clears_registration(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            fd = reader.fileno()
            operation = proactor.poll_many(fd, select.POLLIN, lambda _mask: None)
            with proactor._lock:
                entry = proactor._fd_operations[fd]
                assert entry.reader is not None
                assert entry.reader.operation is operation
                assert entry.reader.step is not None
            proactor.cancel(operation)
            assert operation.cancelled() is True
            with proactor._lock:
                assert fd not in proactor._fd_operations
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_recv_many_repressure_finds_operation_via_fd_slot(self):
        proactor = SelectorProactor()
        buf_group = proactor.create_buf_group(1024, 2)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            fd = reader.fileno()
            operation = proactor.recv_many(reader, lambda _chunk: None, buf_group=buf_group)
            with proactor._lock:
                slot = proactor._fd_operations[fd].reader
                assert slot is not None
                assert slot.operation is operation
                assert slot.step is not None
            proactor._recv_many_repressure_pending.add(operation)
            completed = proactor._service_recv_many_repressure_pending()
            assert operation not in proactor._recv_many_repressure_pending
            assert completed == []
            with proactor._lock:
                assert proactor._fd_operations[fd].reader is slot
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_poll_rejects_empty_mask(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        try:
            with pytest.raises(ValueError, match="poll mask"):
                proactor.poll(reader.fileno(), 0)
            with pytest.raises(ValueError, match="poll mask"):
                proactor.poll_many(reader.fileno(), 0, lambda _mask: None)
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_poll_rejects_conflicting_recv_many_on_same_fd(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            recv_many = proactor.recv_many(reader, lambda _chunk: None, buf_group=proactor.shared_recv_buffer_pool())
            with pytest.raises(RuntimeError, match="already pending"):
                proactor.poll(reader.fileno(), select.POLLIN)
            proactor.cancel(recv_many)
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_poll_completes_once_with_bidirectional_mask(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            mask = select.POLLIN | select.POLLOUT
            operation = proactor.poll(reader.fileno(), mask)
            writer.send(b"a")
            _wait_until_done(proactor, operation)
            assert operation.done() is True
            assert operation.result() & mask
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_poll_many_emits_immediately_when_fd_already_ready(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        seen: list[int] = []
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            writer.send(b"a")
            operation = proactor.poll_many(reader.fileno(), select.POLLIN, seen.append)
            assert seen == [select.POLLIN]
            assert operation.done() is False
            proactor.cancel(operation)
        finally:
            reader.close()
            writer.close()
            proactor.close()

    @pytest.mark.skipif(not hasattr(select, "POLLRDHUP"), reason="POLLRDHUP is not defined on this platform")
    def test_poll_mask_accepts_pollrdhup(self):
        assert poll_helpers_module.poll_mask_to_selector_events(select.POLLRDHUP) == selectors.EVENT_READ

    def test_poll_detects_pollhup_after_peer_close(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.close()
            operation = proactor.poll(reader.fileno(), select.POLLHUP)
            _wait_until_done(proactor, operation)
            assert operation.result() & select.POLLHUP
        finally:
            reader.close()
            proactor.close()

    def test_poll_detects_pollin_and_pollhup_after_peer_close(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.close()
            mask = select.POLLIN | select.POLLHUP
            operation = proactor.poll(reader.fileno(), mask)
            _wait_until_done(proactor, operation)
            assert operation.result() & mask
        finally:
            reader.close()
            proactor.close()

    def test_poll_many_does_not_double_emit_when_mask_maps_to_both_directions(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        seen: list[int] = []
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            mask = select.POLLIN | select.POLLOUT
            operation = proactor.poll_many(reader.fileno(), mask, seen.append)
            writer.send(b"a")
            while not seen:
                proactor.wait(proactor.get_time() + 1.0)
            assert len(seen) == 1
            assert seen[0] & mask
            proactor.cancel(operation)
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_recvall_collects_chunks_and_reports_progress(self):
        scheduler = SyncProactorScheduler(SelectorProactor)
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        progress: list[bytes] = []
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            def receive() -> bytes:
                return scheduler.io.sock_recvall(reader, progress.append)

            def deliver() -> None:
                scheduler.io.sock_sendall(writer, b"hello").wait()
                scheduler.sleep(0.02)
                scheduler.io.sock_sendall(writer, b"world").wait()
                writer.shutdown(socket.SHUT_WR)

            task = scheduler.spawn(receive)
            scheduler.spawn(deliver)
            assert scheduler.run_until_complete(task) == b"helloworld"
            assert progress == [b"hello", b"world"]
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_sock_send_iter_sends_all_chunks(self):
        scheduler = SyncProactorScheduler(SelectorProactor)
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            def receive() -> bytes:
                return scheduler.io.sock_recvall(reader)

            def deliver() -> None:
                scheduler.io.sock_send_iter(writer, [b"hello", b"world"])
                writer.shutdown(socket.SHUT_WR)

            task = scheduler.spawn(receive)
            scheduler.spawn(deliver)
            assert scheduler.run_until_complete(task) == b"helloworld"
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_sock_send_iter_skips_empty_chunks(self):
        scheduler = SyncProactorScheduler(SelectorProactor)
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            def receive() -> bytes:
                return scheduler.io.sock_recvall(reader)

            def deliver() -> None:
                scheduler.io.sock_send_iter(writer, [b"hello", b"", memoryview(b""), b"world"])
                writer.shutdown(socket.SHUT_WR)

            task = scheduler.spawn(receive)
            scheduler.spawn(deliver)
            assert scheduler.run_until_complete(task) == b"helloworld"
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_sock_send_iter_accepts_bytearray_and_memoryview_chunks(self):
        scheduler = SyncProactorScheduler(SelectorProactor)
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            payload = bytearray(b"abc")

            def receive() -> bytes:
                return scheduler.io.sock_recvall(reader)

            def deliver() -> None:
                scheduler.io.sock_send_iter(writer, [payload, memoryview(b"def")])
                writer.shutdown(socket.SHUT_WR)

            task = scheduler.spawn(receive)
            scheduler.spawn(deliver)
            assert scheduler.run_until_complete(task) == b"abcdef"
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_sock_send_iter_pulls_chunks_lazily_from_generator(self):
        scheduler = SyncProactorScheduler(SelectorProactor)
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        pulls: list[int] = []

        def chunks() -> Any:
            pulls.append(0)
            yield b"hel"
            pulls.append(1)
            yield b"lo"

        try:
            reader.setblocking(False)
            writer.setblocking(False)

            def receive() -> bytes:
                return scheduler.io.sock_recvall(reader)

            def deliver() -> None:
                scheduler.io.sock_send_iter(writer, chunks())
                writer.shutdown(socket.SHUT_WR)

            task = scheduler.spawn(receive)
            scheduler.spawn(deliver)
            assert scheduler.run_until_complete(task) == b"hello"
            assert pulls == [0, 1]
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_sock_send_iter_propagates_send_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        scheduler = SyncProactorScheduler(SelectorProactor)
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        calls = 0
        real_sendall = scheduler.io.sock_sendall

        def boom(sock: socket.socket, data: Any, progress: Any = None) -> None:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("send failed")
            return real_sendall(sock, data, progress)

        monkeypatch.setattr(scheduler.io, "sock_sendall", boom)

        try:
            reader.setblocking(False)
            writer.setblocking(False)

            def deliver() -> None:
                scheduler.io.sock_send_iter(writer, [b"hello", b"world"])

            task = scheduler.spawn(deliver)
            with pytest.raises(OSError, match="send failed"):
                scheduler.run_until_complete(task)
            assert calls == 2
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_send_reports_progress(self):
        proactor = SelectorProactor()
        reader, writer = socket.socketpair()
        progress: list[int] = []
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            operation = proactor.send(writer, b"hello", progress.append)

            assert operation.result() is None
            assert progress == [5]
            assert reader.recv(5) == b"hello"
        finally:
            reader.close()
            writer.close()
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
            proactor.cancel(operation)
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

            proactor.cancel(operation)
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

            operation = proactor.send(writer, b"hello")

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

            teardown_holder: list[Operation[None] | None] = []

            def cancel_from_thread() -> None:
                teardown_holder.append(proactor.cancel(operation))

            thread = threading.Thread(target=cancel_from_thread)
            thread.start()
            thread.join(1.0)

            assert thread.is_alive() is False
            assert operation.cancelled() is True
            teardown = teardown_holder[0]
            assert teardown is not None
            assert teardown.kind == "cancel"
            assert teardown.done() is True
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
                    return scheduler.io.sock_recv(reader, 5).wait()

                task = scheduler.spawn(receive)
                await asyncio.sleep(0)
                writer.send(b"hello")

                return await scheduler.arun_until_complete(task)
            finally:
                reader.close()
                writer.close()
                scheduler.close()

        assert asyncio.run(run()) == b"hello"


class TestUringProactor:
    @pytest.fixture(autouse=True)
    def _default_multishot_probes(self, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> None:
        if "falls_back" in request.node.name:
            return
        _force_uring_multishot_probes(monkeypatch)

    def test_capabilities_cached_from_single_probe(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[tuple[int, int]] = []

        def tracking_capabilities(*args: object, **kwargs: object) -> dict[str, bool]:
            calls.append(
                (kwargs.get("entries", args[0] if args else 8), kwargs.get("flags", args[1] if len(args) > 1 else 0))
            )
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

    def test_openat_read_write_round_trip_from_ring_completion(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            open_operation = proactor.openat("/tmp/example.txt", os.O_RDWR | os.O_CREAT, 0o644)
            _wait_for_uring(proactor, lambda: open_operation.done())
            fd = open_operation.result()
            assert isinstance(proactor.ring, _FakeUringRing)
            assert proactor.ring.submitted_openat[0][:3] == ("/tmp/example.txt", os.O_RDWR | os.O_CREAT, 0o644)

            write_operation = proactor.write(fd, b"hello", 0)
            _wait_for_uring(proactor, lambda: write_operation.done())
            assert write_operation.result() == 5

            read_operation = proactor.read(fd, 5, 0)
            _wait_for_uring(proactor, lambda: read_operation.done())
            assert read_operation.result() == b"hello"

            buf = bytearray(5)
            read_into_operation = proactor.read_into(fd, buf, 0)
            _wait_for_uring(proactor, lambda: read_into_operation.done())
            assert read_into_operation.result() == 5
            assert bytes(buf) == b"hello"

            close_operation = proactor.close_fd(fd)
            _wait_for_uring(proactor, lambda: close_operation.done())
            assert close_operation.result() is None
            assert proactor.ring.submitted_close[-1][0] == fd
        finally:
            proactor.close()

    def test_stat_fd_uses_statx_when_capable(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            open_operation = proactor.openat("/tmp/stat.txt", os.O_RDWR | os.O_CREAT, 0o644)
            _wait_for_uring(proactor, lambda: open_operation.done())
            fd = open_operation.result()
            write_operation = proactor.write(fd, b"hello", 0)
            _wait_for_uring(proactor, lambda: write_operation.done())

            stat_operation = proactor.stat(fd=fd)
            _wait_for_uring(proactor, lambda: stat_operation.done())
            assert stat_operation.result().st_size == 5
            ring = cast(_FakeUringRing, proactor.ring)
            assert ring.submitted_statx[-1][:4] == (
                fd,
                "",
                uring_api.AT_EMPTY_PATH,
                uring_api.STATX_BASIC_STATS,
            )
        finally:
            proactor.close()

    def test_stat_falls_back_to_blocking_when_statx_unavailable(self, monkeypatch: pytest.MonkeyPatch):
        _patch_uring_capabilities(monkeypatch, IORING_OP_STATX=False)
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                path = tmp.name
                tmp.write(b"hello")
            try:
                fd = os.open(path, os.O_RDONLY)
                try:
                    stat_operation = proactor.stat(fd=fd)
                    assert stat_operation.done()
                    assert stat_operation.result().st_size == 5
                    assert cast(_FakeUringRing, proactor.ring).submitted_statx == []
                finally:
                    os.close(fd)
            finally:
                os.unlink(path)
        finally:
            proactor.close()

    def test_stat_fdsize_uses_statx_fdsize_when_capable(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            open_operation = proactor.openat("/tmp/stat-fdsize.txt", os.O_RDWR | os.O_CREAT, 0o644)
            _wait_for_uring(proactor, lambda: open_operation.done())
            fd = open_operation.result()
            write_operation = proactor.write(fd, b"hello", 0)
            _wait_for_uring(proactor, lambda: write_operation.done())

            stat_fdsize_operation = proactor.stat_fdsize(fd)
            _wait_for_uring(proactor, lambda: stat_fdsize_operation.done())
            assert stat_fdsize_operation.result() == 5
            ring = cast(_FakeUringRing, proactor.ring)
            assert ring.submitted_statx_fdsize[-1][0] == fd
            assert ring.submitted_statx == []
        finally:
            proactor.close()

    def test_stat_fdsize_falls_back_to_blocking_when_statx_unavailable(self, monkeypatch: pytest.MonkeyPatch):
        _patch_uring_capabilities(monkeypatch, IORING_OP_STATX=False)
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                path = tmp.name
                tmp.write(b"hello")
            try:
                fd = os.open(path, os.O_RDONLY)
                try:
                    stat_fdsize_operation = proactor.stat_fdsize(fd)
                    assert stat_fdsize_operation.done()
                    assert stat_fdsize_operation.result() == 5
                    assert cast(_FakeUringRing, proactor.ring).submitted_statx_fdsize == []
                finally:
                    os.close(fd)
            finally:
                os.unlink(path)
        finally:
            proactor.close()

    def test_stat_path_uses_statx_when_capable(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            stat_operation = proactor.stat(path="/tmp/stat-path.txt")
            _wait_for_uring(proactor, lambda: stat_operation.done())
            assert stat_operation.result().st_size == 0
            ring = cast(_FakeUringRing, proactor.ring)
            assert ring.submitted_statx[-1][:4] == (
                uring_api.AT_FDCWD,
                "/tmp/stat-path.txt",
                0,
                uring_api.STATX_BASIC_STATS,
            )
        finally:
            proactor.close()

    def test_stat_result_from_statx_maps_timestamps_and_devices(self):
        buf = bytearray(uring_api.STATX_BUFFER_SIZE)
        _pack_fake_statx_buffer(
            buf,
            size=42,
            mode=0o100600,
            ino=99,
            atime_sec=111,
            mtime_sec=222,
            ctime_sec=333,
            dev_major=1,
            dev_minor=2,
            rdev_major=3,
            rdev_minor=4,
        )
        result = proactor_module._stat_result_from_statx(buf)
        assert result.st_size == 42
        assert result.st_mode == 0o100600
        assert result.st_ino == 99
        assert result.st_atime == 111
        assert result.st_mtime == 222
        assert result.st_ctime == 333
        assert result.st_dev == os.makedev(1, 2)

    def test_uring_stat_stores_parse_errors_on_operation(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(
            proactor_module,
            "_stat_result_from_statx",
            lambda _buf: (_ for _ in ()).throw(ValueError("bad statx buffer")),
        )
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            open_operation = proactor.openat("/tmp/stat-parse.txt", os.O_RDWR | os.O_CREAT, 0o644)
            _wait_for_uring(proactor, lambda: open_operation.done())
            fd = open_operation.result()
            stat_operation = proactor.stat(fd=fd)
            _wait_for_uring(proactor, lambda: stat_operation.done())
            with pytest.raises(ValueError, match="bad statx buffer"):
                stat_operation.result()
        finally:
            proactor.close()

    @pytest.mark.skipif(not uring_api.is_available(), reason="io_uring is required")
    def test_native_openat_read_write_round_trip(self):
        proactor = UringProactor()
        fd: int | None = None
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "proactor-file.txt")
            try:
                open_operation = proactor.openat(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
                _wait_for_uring(proactor, lambda: open_operation.done())
                fd = open_operation.result()
                assert fd >= 0

                write_operation = proactor.write(fd, b"hello", 0)
                _wait_for_uring(proactor, lambda: write_operation.done())
                assert write_operation.result() == 5

                read_operation = proactor.read(fd, 5, 0)
                _wait_for_uring(proactor, lambda: read_operation.done())
                assert read_operation.result() == b"hello"

                buf = bytearray(5)
                read_into_operation = proactor.read_into(fd, buf, 0)
                _wait_for_uring(proactor, lambda: read_into_operation.done())
                assert read_into_operation.result() == 5
                assert bytes(buf) == b"hello"
            finally:
                if fd is not None:
                    os.close(fd)
                proactor.close()

    def test_submit_uring_entry_clears_pending_token_when_submit_raises(self):
        proactor = UringProactor(ring_factory=_FailingSubmitUringRing)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            ring = proactor.ring
            assert isinstance(ring, _FailingSubmitUringRing)
            ring.fail_next_submit = True
            with pytest.raises(RuntimeError, match="submit_recv failed"):
                proactor.recv(reader, 5)
            entry = ring.last_user_data
            assert entry is not None
            assert entry.active is False
            assert proactor.has_pending_operations() is False
            assert ring.submitted_recv == []
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

    def test_uring_entry_clears_completion_handle_after_delivery(self):
        proactor = UringProactor(ring_factory=_DeferredUringRing)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            operation = proactor.recv(reader, 5)
            _fd, _buf, entry = proactor.ring.submitted_recv[-1]
            assert entry.completion is not None

            proactor.ring.complete_recv()
            proactor.wait(proactor.get_time() + 1.0)

            assert operation.result() == b"hello"
            assert entry.completion is None

        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_multishot_recv_many_clears_completion_handle_when_done(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            operation = proactor.recv_many(reader, lambda _chunk: None, buf_group=proactor.shared_recv_buffer_pool())
            _fd, _group, entry = proactor.ring.submitted_recv_multishot[-1]
            assert entry.completion is not None

            proactor.ring.complete_recv_multishot(b"hello", more=False, sequence=0)
            proactor.ring.complete_recv_multishot(b"", more=False, sequence=1)
            _wait_for_uring(proactor, lambda: operation.done())

            assert entry.completion is None

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

            teardown = proactor.cancel(operation)
            assert teardown is not None
            assert teardown.kind == "cancel"
            assert teardown.done() is True
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

    def test_submission_stats_track_queue_full_backpressure(self):
        proactor = UringProactor(ring_factory=_BackpressuredUringRing)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            proactor.recv(reader, 5)
            assert proactor.submission_stats == UringSubmissionStats(
                submit_attempts=1,
                submit_queue_full=0,
                deferred_queue_peak=0,
            )

            proactor.ring.fail_next_recv = True
            proactor.recv(reader, 5)
            assert proactor.submission_stats == UringSubmissionStats(
                submit_attempts=2,
                submit_queue_full=1,
                deferred_queue_peak=1,
            )

            proactor.reset_submission_stats()
            assert proactor.submission_stats == UringSubmissionStats(0, 0, 0)
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
            proactor.cancel(second)

            assert second.cancelled() is True
            assert id(second) not in proactor._uring_operation_entries
            assert not any(
                submission.entry is not None and submission.entry.operation is second
                for submission in proactor._deferred_submissions
            )
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
            proactor.cancel(operation)

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

    def test_send_completes_from_ring_completion(self, monkeypatch):
        _patch_uring_capabilities(monkeypatch, IORING_OP_SEND_ZC=False)
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        try:
            writer.setblocking(False)
            payload = b"hello"
            operation = proactor.send(writer, payload)

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

    def test_send_uses_send_zc_when_probe_supports_it(self, monkeypatch):
        _patch_uring_capabilities(monkeypatch, IORING_OP_SEND_ZC=True)
        proactor = UringProactor(ring_factory=_FakeUringRing)
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        reader = None
        writer = None
        try:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(("127.0.0.1", 0))
            server.listen(1)
            writer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            writer.connect(server.getsockname())
            reader, _address = server.accept()
            writer.setblocking(False)
            payload = b"hello"
            operation = proactor.send(writer, payload)

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() is None
            assert len(proactor.ring.submitted_send_zc) == 1
            assert proactor.ring.submitted_send == []
            submitted = proactor.ring.submitted_send_zc[0][1]
            assert isinstance(submitted, memoryview)
            assert submitted.obj is payload
            assert bytes(submitted) == b"hello"
        finally:
            if reader is not None:
                reader.close()
            if writer is not None:
                writer.close()
            server.close()
            proactor.close()

    def test_send_uses_plain_send_for_unix_even_when_probe_supports_send_zc(self, monkeypatch):
        _patch_uring_capabilities(monkeypatch, IORING_OP_SEND_ZC=True)
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        try:
            writer.setblocking(False)
            operation = proactor.send(writer, b"hello")

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() is None
            assert len(proactor.ring.submitted_send) == 1
            assert proactor.ring.submitted_send_zc == []
            assert proactor._send_zc_supported is True
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_send_uses_plain_send_when_probe_lacks_send_zc(self, monkeypatch):
        _patch_uring_capabilities(monkeypatch, IORING_OP_SEND_ZC=False)
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        try:
            writer.setblocking(False)
            operation = proactor.send(writer, b"hello")

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() is None
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

    def test_sendto_completes_from_ring_completion(self, monkeypatch):
        _patch_uring_capabilities(monkeypatch, IORING_OP_SENDMSG_ZC=False)
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

    def test_sendto_uses_sendmsg_zc_when_probe_supports_it(self, monkeypatch):
        _patch_uring_capabilities(monkeypatch, IORING_OP_SENDMSG_ZC=True)
        proactor = UringProactor(ring_factory=_FakeUringRing)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sender.setblocking(False)
            payload = b"hello"
            address = ("127.0.0.1", 12345)
            operation = proactor.sendto(sender, payload, address)

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() == 5
            assert len(proactor.ring.submitted_sendmsg_zc) == 1
            assert proactor.ring.submitted_sendto == []
            submitted = proactor.ring.submitted_sendmsg_zc[0]
            assert submitted[0] == sender.fileno()
            assert isinstance(submitted[1], memoryview)
            assert submitted[1].obj is payload
            assert bytes(submitted[1]) == b"hello"
            assert submitted[2] == address
        finally:
            sender.close()
            proactor.close()

    def test_sendto_uses_plain_sendto_for_unix_even_when_probe_supports_sendmsg_zc(self, monkeypatch):
        _patch_uring_capabilities(monkeypatch, IORING_OP_SENDMSG_ZC=True)
        proactor = UringProactor(ring_factory=_FakeUringRing)
        sender = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            sender.setblocking(False)
            payload = b"hello"
            address = "/tmp/tealetio-sendto-test"
            operation = proactor.sendto(sender, payload, address)

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() == 5
            assert len(proactor.ring.submitted_sendto) == 1
            assert proactor.ring.submitted_sendmsg_zc == []
            assert proactor._sendmsg_zc_supported is True
        finally:
            sender.close()
            proactor.close()

    def test_sendto_uses_sendto_when_probe_lacks_sendmsg_zc(self, monkeypatch):
        _patch_uring_capabilities(monkeypatch, IORING_OP_SENDMSG_ZC=False)
        proactor = UringProactor(ring_factory=_FakeUringRing)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sender.setblocking(False)
            operation = proactor.sendto(sender, b"hello", ("127.0.0.1", 12345))

            proactor.wait(proactor.get_time() + 1.0)
            assert operation.result() == 5
            assert len(proactor.ring.submitted_sendto) == 1
            assert proactor.ring.submitted_sendmsg_zc == []
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
            conn = operation.result()
            assert conn.getpeername() == proactor.ring.accepted_peers[0].getsockname()
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
            proactor.cancel(operation)
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
            proactor.cancel(operation)
            assert operation.cancelled() is True
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_poll_many_oneshot_cancel_while_resubmit_deferred(self, monkeypatch):
        _patch_uring_capabilities(monkeypatch, IORING_POLL_MULTISHOT=False)
        proactor = UringProactor(ring_factory=_BackpressuredPollUringRing)
        reader, writer = socket.socketpair()
        seen: list[int] = []
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.poll_many(reader.fileno(), select.POLLIN, seen.append)
            assert len(proactor.ring.submitted_poll) == 1

            proactor.ring.complete_poll_oneshot(select.POLLIN)
            _wait_for_uring(proactor, lambda: seen == [select.POLLIN])
            assert len(proactor.ring.submitted_poll) == 1

            proactor.cancel(operation)
            assert operation.cancelled() is True
            proactor.wait(proactor.get_time() + 1.0)
            assert len(proactor.ring.submitted_poll) == 1
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
            teardown = proactor.cancel(operation)
            _wait_for_uring(proactor, lambda: pending in proactor.ring.submitted_cancel)
            assert proactor.ring.submitted_poll_remove == []
            assert operation.cancelled() is True
            assert teardown is not None
            assert teardown.kind == "cancel"
            assert teardown.done() is True
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

    @pytest.mark.skipif(not uring_api.is_available(), reason="io_uring is required")
    def test_native_poll_many_emits_and_cancels_on_multishot_path(self):
        if not uring_api.probe().get("IORING_POLL_MULTISHOT", False):
            pytest.skip("multishot poll is unavailable")
        proactor = UringProactor()
        reader, writer = socket.socketpair()
        seen: list[int] = []
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.poll_many(reader.fileno(), select.POLLIN, seen.append)
            writer.send(b"x")
            _wait_for_uring(proactor, lambda: len(seen) >= 1)
            assert seen[-1] & select.POLLIN
            proactor.cancel(operation)
            _wait_for_uring(proactor, lambda: not proactor.has_pending_operations())
            assert operation.cancelled() is True
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_accept_many_falls_back_to_oneshot_accept_and_finishes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_uring_capabilities(monkeypatch, IORING_ACCEPT_MULTISHOT=False)
        proactor = UringProactor(ring_factory=_FakeUringRing)
        server = socket.socket()
        accepted: list[socket.socket] = []
        try:
            server.setblocking(False)
            operation = proactor.accept_many(server, accepted.append)
            assert proactor.ring.submitted_accept_multishot == []
            assert len(proactor.ring.submitted_accept) == 1
            proactor.ring.complete_accept_oneshot()
            _wait_for_uring(proactor, lambda: len(accepted) == 1)
            assert operation.done() is True
            assert len(proactor.ring.submitted_accept) == 1

            pending = proactor.accept_many(server, accepted.append)
            assert len(proactor.ring.submitted_accept) == 2
            proactor.cancel(pending)
            assert pending.cancelled() is True
        finally:
            for conn in accepted:
                conn.close()
            server.close()
            proactor.close()

    def test_accept_many_uses_multishot_accept(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        server = socket.socket()
        accepted: list[socket.socket] = []
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

            assert accepted[0].getblocking() is False
            assert os.get_inheritable(accepted[0].fileno()) is False
        finally:
            for conn in accepted:
                conn.close()
            server.close()
            proactor.close()

    def test_handoff_accept_many_closes_socket_when_parent_done(self) -> None:
        parent: ContinuousOperation[Any] = ContinuousOperation(kind="accept_many", fileobj=object())
        parent._finish(exception=CancelledError(), cancelled=True)
        client, server = socket.socketpair()
        try:
            assert proactor_module._handoff_accept_many(parent, client) is False
            with pytest.raises(OSError):
                client.getsockname()
        finally:
            server.close()

    def test_accept_many_drops_connection_when_accept_completes_after_cancel(self) -> None:
        proactor = UringProactor(ring_factory=_FakeUringRing)
        server = socket.socket()
        accepted: list[socket.socket] = []
        try:
            server.setblocking(False)
            operation = proactor.accept_many(server, accepted.append)
            proactor.cancel(operation)
            assert operation.cancelled() is True
            proactor.ring.complete_accept_multishot("peer-1")
            proactor.wait(proactor.get_time() + 0.05)
            assert accepted == []
        finally:
            for conn in accepted:
                conn.close()
            server.close()
            proactor.close()

    def test_deferred_recv_many_resubmit_failure_fails_operation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_uring_capabilities(monkeypatch, IORING_RECV_MULTISHOT=False)
        proactor = UringProactor(ring_factory=_FailOnResubmitUringRing)
        reader, writer = socket.socketpair()
        seen: list[_RecvManySeen] = []
        try:
            reader.setblocking(False)
            operation = proactor.recv_many(
                reader, _recv_many_auto_resume_callback(seen), buf_group=proactor.shared_recv_buffer_pool()
            )
            proactor.ring.complete_recv_oneshot(b"hello")
            assert _recv_many_bytes(seen) == [(0, b"hello")]
            assert operation.done() is True
            assert isinstance(operation.exception(), RuntimeError)
            assert str(operation.exception()) == "deferred recv resubmit failed"
            assert proactor.has_pending_operations() is False
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_recv_many_falls_back_to_oneshot_recv_and_finishes_on_eof(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_uring_capabilities(monkeypatch, IORING_RECV_MULTISHOT=False)
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        seen: list[_RecvManySeen] = []
        try:
            reader.setblocking(False)
            operation = proactor.recv_many(
                reader, _recv_many_auto_resume_callback(seen), buf_group=proactor.shared_recv_buffer_pool()
            )
            assert proactor.ring.submitted_recv_multishot == []
            assert len(proactor.ring.submitted_recv) == 1
            proactor.ring.complete_recv_oneshot(b"hello")
            _wait_for_uring(proactor, lambda: _recv_many_bytes(seen) == [(0, b"hello")])
            _wait_for_uring(proactor, lambda: len(proactor.ring.submitted_recv) == 2)
            proactor.ring.complete_recv_oneshot(b"")
            _wait_for_uring(proactor, lambda: operation.done())
            assert _recv_many_bytes(seen) == [(0, b"hello"), (1, b"")]
        finally:
            reader.close()
            writer.close()
            proactor.close()

    @pytest.mark.skipif(not uring_api.is_available(), reason="io_uring is required for BufView recv_many completions")
    def test_recv_many_uses_multishot_recv_and_finishes_on_eof(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        seen: list[_RecvManySeen] = []
        try:
            reader.setblocking(False)
            operation = proactor.recv_many(
                reader, _recv_many_auto_resume_callback(seen), buf_group=proactor.shared_recv_buffer_pool()
            )
            assert isinstance(proactor.ring, _FakeUringRing)
            submitted = proactor.ring.submitted_recv_multishot[0]
            assert submitted[0] == reader.fileno()
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
        seen: list[_RecvManySeen] = []
        try:
            reader.setblocking(False)
            operation = proactor.recv_many(
                reader, _recv_many_auto_resume_callback(seen), buf_group=proactor.shared_recv_buffer_pool()
            )
            ring = proactor.ring
            ring.complete_recv_multishot(b"a", more=True, sequence=0)
            ring.complete_recv_multishot(b"b", more=True, sequence=1)
            ring.complete_recv_multishot_enobufs(sequence=2)
            assert seen[-1][0] == RECV_MANY_BUFFER_PRESSURE
            assert callable(seen[-1][1])
            assert len(ring.submitted_recv_multishot) == 2
            ring.complete_recv_multishot(b"c", more=True, sequence=0)
            ring.complete_recv_multishot(b"", more=False, sequence=1)
            assert _recv_many_bytes(seen) == [
                (0, b"a"),
                (1, b"b"),
                (2, b"c"),
                (3, b""),
            ]
            assert operation.done() is True
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_recv_many_pressure_resume_callable_defers_until_invoked(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        seen: list[_RecvManySeen] = []
        try:
            reader.setblocking(False)
            buf_group = proactor.create_buf_group(8, 4)
            operation = proactor.recv_many(reader, seen.append, buf_group=buf_group)
            ring = proactor.ring
            ring.complete_recv_multishot(b"a", more=True, sequence=0)
            ring.complete_recv_multishot_enobufs(sequence=1)
            assert seen[-1][0] == RECV_MANY_BUFFER_PRESSURE
            assert callable(seen[-1][1])
            assert len(ring.submitted_recv_multishot) == 1
            cast(Callable[[], None], seen[-1][1])()
            _wait_for_uring(proactor, lambda: len(ring.submitted_recv_multishot) == 2)
            ring.complete_recv_multishot(b"b", more=False, sequence=0)
            assert _recv_many_bytes(seen) == [(0, b"a"), (1, b"b")]
            assert operation.done() is True
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_recv_many_accumulates_stream_sequence_across_repeated_enobufs(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        seen: list[_RecvManySeen] = []
        try:
            reader.setblocking(False)
            operation = proactor.recv_many(
                reader, _recv_many_auto_resume_callback(seen), buf_group=proactor.shared_recv_buffer_pool()
            )
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
                (2, b"c"),
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
        seen: list[_RecvManySeen] = []
        try:
            reader.setblocking(False)
            operation = proactor.recv_many(
                reader, _recv_many_auto_resume_callback(seen), buf_group=proactor.shared_recv_buffer_pool()
            )
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
        seen: list[_RecvManySeen] = []
        try:
            reader.setblocking(False)
            operation = proactor.recv_many(
                reader, _recv_many_auto_resume_callback(seen), buf_group=proactor.shared_recv_buffer_pool()
            )
            ring = proactor.ring
            ring.complete_recv_multishot_enobufs(sequence=2)
            ring.complete_recv_multishot(b"a", more=True, sequence=0)
            ring.complete_recv_multishot(b"b", more=True, sequence=1)
            ring.complete_recv_multishot(b"c", more=False, sequence=0)
            assert _recv_many_bytes_sorted(seen) == [(0, b"a"), (1, b"b"), (2, b"c")]
            assert any(index == RECV_MANY_BUFFER_PRESSURE and callable(payload) for index, payload in seen)
            assert operation.done() is True
        finally:
            reader.close()
            writer.close()
            proactor.close()

    def test_recvall_defers_finalize_when_terminal_burst_arrives_out_of_order(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)

            def receive() -> bytes:
                return scheduler.io.sock_recvall(reader)

            def deliver() -> None:
                ring = scheduler.proactor.ring
                ring.complete_recv_multishot(b"", more=False, sequence=2)
                ring.complete_recv_multishot(b"hello", sequence=0)
                ring.complete_recv_multishot(b"world", sequence=1)

            task = scheduler.spawn(receive)
            scheduler.spawn(deliver)
            assert scheduler.run_until_complete(task) == b"helloworld"
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_recviter_reorders_out_of_order_multishot_chunks(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)

            def receive_chunks() -> list[tuple[int, bytes]]:
                return _recviter_bytes(
                    scheduler.io.sock_recv_iter(reader, scheduler.io.create_recv_buffer_pool(16 * 1024, 8))
                )

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

    def test_recviter_survives_buffer_pressure_and_continues_receive(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        state = {"got_pressure": False, "release": False}
        try:
            reader.setblocking(False)

            def receive_chunks() -> tuple[bool, list[tuple[int, bytes]]]:
                got_memview = False
                saw_pressure = False
                seen: list[tuple[int, bytes]] = []
                pool = scheduler.io.create_recv_buffer_pool(16 * 1024, 4)
                for index, chunk in _iter_recv_stream(scheduler.io.sock_recv_iter(reader, pool)):
                    if index == RECV_MANY_BUFFER_PRESSURE:
                        saw_pressure = True
                        state["got_pressure"] = True
                        deadline = scheduler.proactor.get_time() + 1.0
                        while not state["release"] and scheduler.proactor.get_time() < deadline:
                            scheduler.sleep(0.02)
                        assert state["release"]
                        continue
                    if type(chunk) is memoryview:
                        got_memview = True
                    seen.append((index, bytes(chunk)))
                    pool.note_chunk_released()
                return got_memview and saw_pressure, seen

            def deliver_chunks() -> None:
                ring = scheduler.proactor.ring
                ring.complete_recv_multishot(b"a", more=True, sequence=0)
                scheduler.sleep(0.05)
                ring.complete_recv_multishot(b"b", more=True, sequence=1)
                ring.complete_recv_multishot(b"c", more=True, sequence=2)
                ring.complete_recv_multishot_enobufs(sequence=3)
                deadline = scheduler.proactor.get_time() + 1.0
                while not state["got_pressure"] and scheduler.proactor.get_time() < deadline:
                    scheduler.sleep(0.02)
                assert state["got_pressure"]
                assert len(ring.submitted_recv_multishot) == 1
                state["release"] = True
                deadline = scheduler.proactor.get_time() + 1.0
                while len(ring.submitted_recv_multishot) < 2 and scheduler.proactor.get_time() < deadline:
                    scheduler.sleep(0.02)
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

    def test_recviter_uses_per_generator_buf_group(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)

            def receive_first_chunk() -> tuple[int, bytes]:
                pool = scheduler.io.create_recv_buffer_pool(4096, 4)
                stream = scheduler.io.sock_recv_iter(reader, pool)
                item = next(stream)
                assert item is not None
                index, chunk = item
                return index, bytes(chunk)

            def deliver_first_chunk() -> None:
                ring = scheduler.proactor.ring
                deadline = scheduler.proactor.get_time() + 1.0
                while len(ring.submitted_recv_multishot) < 1 and scheduler.proactor.get_time() < deadline:
                    scheduler.sleep(0.02)
                submitted = ring.submitted_recv_multishot[0]
                assert submitted[1].buffer_size == 4096
                assert submitted[1].buffer_count == 4
                assert submitted[1] is not scheduler.proactor.shared_recv_buffer_pool()
                ring.complete_recv_multishot(b"x", more=False, sequence=0)

            task = scheduler.spawn(receive_first_chunk)
            scheduler.spawn(deliver_first_chunk)
            assert scheduler.run_until_complete(task) == (0, b"x")
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_recviter_resumes_recv_only_after_consumer_takes_chunks(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        state = {"got_pressure": False, "release": False}
        try:
            reader.setblocking(False)

            def receive_chunks() -> list[tuple[int, bytes]]:
                seen: list[tuple[int, bytes]] = []
                pool = scheduler.io.create_recv_buffer_pool(16 * 1024, 4)
                for index, chunk in _iter_recv_stream(scheduler.io.sock_recv_iter(reader, pool)):
                    if index == RECV_MANY_BUFFER_PRESSURE:
                        state["got_pressure"] = True
                        deadline = scheduler.proactor.get_time() + 1.0
                        while not state["release"] and scheduler.proactor.get_time() < deadline:
                            scheduler.sleep(0.02)
                        assert state["release"]
                        continue
                    seen.append((index, bytes(chunk)))
                    pool.note_chunk_released()
                return seen

            def deliver_chunks() -> None:
                ring = scheduler.proactor.ring
                ring.complete_recv_multishot(b"a", more=True, sequence=0)
                ring.complete_recv_multishot(b"b", more=True, sequence=1)
                ring.complete_recv_multishot(b"c", more=True, sequence=2)
                ring.complete_recv_multishot_enobufs(sequence=3)
                assert len(ring.submitted_recv_multishot) == 1
                deadline = scheduler.proactor.get_time() + 1.0
                while not state["got_pressure"] and scheduler.proactor.get_time() < deadline:
                    scheduler.sleep(0.02)
                assert state["got_pressure"]
                assert len(ring.submitted_recv_multishot) == 1
                state["release"] = True
                deadline = scheduler.proactor.get_time() + 1.0
                while len(ring.submitted_recv_multishot) < 2 and scheduler.proactor.get_time() < deadline:
                    scheduler.sleep(0.02)
                assert len(ring.submitted_recv_multishot) == 2
                ring.complete_recv_multishot(b"d", more=False, sequence=0)

            task = scheduler.spawn(receive_chunks)
            scheduler.spawn(deliver_chunks)

            assert scheduler.run_until_complete(task) == [(0, b"a"), (1, b"b"), (2, b"c"), (3, b"d")]
            assert len(scheduler.proactor.ring.submitted_recv_multishot) == 2
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_recvall_survives_buffer_pressure_and_continues_receive(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)

            def receive() -> bytes:
                return scheduler.io.sock_recvall(reader)

            def deliver() -> None:
                scheduler.sleep(0.001)
                ring = scheduler.proactor.ring
                shared = scheduler.proactor.shared_recv_buffer_pool()
                ring.complete_recv_multishot(b"a", more=True, sequence=0)
                ring.complete_recv_multishot(b"b", more=True, sequence=1)
                ring.complete_recv_multishot(b"c", more=True, sequence=2)
                ring.complete_recv_multishot_enobufs(sequence=3)
                # fake-ring views do not decrement leased_count; sock_recvall drops each chunk after bytes()
                shared.leased_count = 0
                deadline = scheduler.proactor.get_time() + 1.0
                while len(ring.submitted_recv_multishot) < 2 and scheduler.proactor.get_time() < deadline:
                    scheduler.sleep(0.02)
                ring.complete_recv_multishot(b"d", more=False, sequence=0)

            task = scheduler.spawn(receive)
            scheduler.spawn(deliver)
            assert scheduler.run_until_complete(task) == b"abcd"
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    @pytest.mark.skipif(not uring_api.is_available(), reason="io_uring is required for BufView recv_many completions")
    def test_recvall_collects_many_out_of_order_chunks(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)

            def receive() -> bytes:
                return scheduler.io.sock_recvall(reader)

            def deliver() -> None:
                ring = scheduler.proactor.ring
                assert isinstance(ring, _FakeUringRing)
                for sequence, byte in enumerate(b"abcdefghijklmnop"):
                    ring.complete_recv_multishot(bytes((byte,)), sequence=sequence)
                    scheduler.sleep(0)
                ring.complete_recv_multishot(b"", more=False, sequence=16)

            task = scheduler.spawn(receive)
            scheduler.spawn(deliver)
            assert scheduler.run_until_complete(task) == b"abcdefghijklmnop"
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    @pytest.mark.skipif(not uring_api.is_available(), reason="io_uring is required for BufView recv_many completions")
    def test_recvall_collects_uring_multishot_receive_and_reports_progress(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        progress: list[bytes] = []
        try:
            reader.setblocking(False)

            def receive() -> bytes:
                return scheduler.io.sock_recvall(reader, progress.append)

            def deliver() -> None:
                ring = scheduler.proactor.ring
                assert isinstance(ring, _FakeUringRing)
                ring.complete_recv_multishot(b"world", sequence=1)
                scheduler.sleep(0)
                ring.complete_recv_multishot(b"hello", sequence=0)
                scheduler.sleep(0)
                ring.complete_recv_multishot(b"", more=False, sequence=2)

            task = scheduler.spawn(receive)
            scheduler.spawn(deliver)
            assert scheduler.run_until_complete(task) == b"helloworld"
            assert progress == [b"hello", b"world"]
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    @pytest.mark.requires_native_uring_recv_multishot
    def test_native_recv_many_cancel_after_data_before_sender_close(self):
        proactor = UringProactor()
        reader, writer = socket.socketpair()
        seen: list[_RecvManySeen] = []
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = proactor.recv_many(
                reader, _recv_many_auto_resume_callback(seen), buf_group=proactor.shared_recv_buffer_pool()
            )

            writer.send(b"hello")
            _wait_for_uring(proactor, lambda: _recv_many_bytes(seen) == [(0, b"hello")])

            proactor.cancel(operation)
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
        scheduler = SyncProactorScheduler(UringProactor)
        set_scheduler(scheduler)
        reader, writer = socket.socketpair()
        progress: list[bytes] = []
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            def receive() -> bytes:
                return scheduler.io.sock_recvall(reader, progress.append)

            def cancel_after_first_chunk() -> None:
                # Use a direct socket send so this recv-focused test does not
                # depend on native io_uring stream send support.
                writer.send(b"hello")
                deadline = scheduler.proactor.get_time() + 2.0
                while progress != [b"hello"] and scheduler.proactor.get_time() < deadline:
                    scheduler.sleep(0.01)
                task.cancel()

            task = scheduler.spawn(receive)
            scheduler.spawn(cancel_after_first_chunk)
            with pytest.raises(CancelledError):
                scheduler.run_until_complete(task)

            assert progress == [b"hello"]
            _wait_for_uring(scheduler.proactor, lambda: not scheduler.proactor.has_pending_operations())
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_send_reports_uring_progress(self):
        proactor = UringProactor(ring_factory=_FakeUringRing)
        reader, writer = socket.socketpair()
        progress: list[int] = []
        try:
            writer.setblocking(False)
            operation = proactor.send(writer, b"hello", progress.append)

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

    def test_sock_create_connects_without_initial_on_uring(self) -> None:
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            sock = _io_sock_create(
                proactor,
                socket.AF_INET,
                socket.SOCK_STREAM,
                connect_to=("127.0.0.1", 9),
            )
            _assert_scheduler_socket_fd(sock)
            sock.close()
        finally:
            proactor.close()

    def test_sock_create_connect_failure_does_not_leak_socket(self) -> None:
        proactor = UringProactor(ring_factory=_FailingConnectUringRing)
        try:
            scheduler = SyncProactorScheduler(lambda: proactor)
            set_scheduler(scheduler)
            try:
                with pytest.raises(OSError):
                    scheduler.run_until_complete(
                        scheduler.spawn(
                            lambda: scheduler.io.sock_create(
                                socket.AF_INET,
                                socket.SOCK_STREAM,
                                connect_to=("127.0.0.1", 9),
                            ).wait()
                        )
                    )
            finally:
                scheduler.close()
        finally:
            proactor.close()

    def test_create_socket_cancel_before_socket_completes(self) -> None:
        proactor = UringProactor(ring_factory=_DeferredSocketUringRing)
        try:
            operation = proactor.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            _wait_for_uring(proactor, lambda: len(proactor.ring.pending_socket) == 1)
            proactor.cancel(operation)
            assert operation.cancelled() is True
            assert len(proactor.ring.submitted_cancel) == 1
            proactor.ring.complete_socket()
            proactor.wait(proactor.get_time() + 0.05)
            assert operation.cancelled() is True
            assert proactor.ring.submitted_connect == []
            leaked_fd = proactor.ring.last_socket_fd
            assert leaked_fd is not None
            with pytest.raises(OSError):
                os.fstat(leaked_fd)
        finally:
            proactor.close()

@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
class TestProactorSchedulerIntegration:
    @pytest.fixture
    def scheduler(self, scheduler_factory: Callable[[], SyncProactorScheduler]) -> SyncProactorScheduler:
        sched = scheduler_factory()
        set_scheduler(sched)
        yield sched
        sched.close()

    def test_sock_create_uses_proactor_create_socket(self, scheduler: SyncProactorScheduler) -> None:
        def exercise() -> socket.socket:
            return scheduler.io.sock_create(socket.AF_INET, socket.SOCK_STREAM).wait()

        sock = scheduler.run_until_complete(scheduler.spawn(exercise))
        try:
            assert isinstance(sock, socket.socket)
            assert sock.family == socket.AF_INET
            assert sock.type == socket.SOCK_STREAM
            _assert_scheduler_socket_fd(sock)
        finally:
            sock.close()

    def test_scheduler_clock_drives_proactor_clock(self, scheduler: SyncProactorScheduler) -> None:
        scheduler._time = lambda: 24.0
        assert scheduler.proactor.get_time() == 24.0

    def test_socket_helpers(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            buf = bytearray(5)

            def exchange() -> tuple[int, bytes]:
                scheduler.io.sock_sendall(writer, b"world").wait()
                count = scheduler.io.sock_recv_into(reader, buf).wait()
                return count, bytes(buf)

            task = scheduler.spawn(exchange)
            assert scheduler.run_until_complete(task) == (5, b"world")
        finally:
            reader.close()
            writer.close()

    def test_create_recv_buffer_pool_delegates_to_proactor(self, scheduler: SyncProactorScheduler) -> None:
        pool = scheduler.io.create_recv_buffer_pool(8192, 4)
        assert pool.buffer_size == 8192
        assert pool.buffer_count == 4
        assert pool.leased_count == 0

    def test_sock_recv_iter_accepts_scheduler_buffer_pool(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            pool = scheduler.io.create_recv_buffer_pool(8192, 2)

            def receive_chunks() -> list[tuple[int, bytes]]:
                return _recviter_bytes(scheduler.io.sock_recv_iter(reader, pool))

            def send_chunks() -> None:
                scheduler.io.sock_sendall(writer, b"hello").wait()
                writer.shutdown(socket.SHUT_WR)

            task = scheduler.spawn(receive_chunks)
            scheduler.spawn(send_chunks)
            assert scheduler.run_until_complete(task) == [(0, b"hello")]
        finally:
            reader.close()
            writer.close()

    def test_sock_recv_iter_yields_chunks_in_stream_order(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            def receive_chunks() -> list[tuple[int, bytes]]:
                return _recviter_bytes(scheduler.io.sock_recv_iter(reader))

            def send_chunks() -> None:
                scheduler.io.sock_sendall(writer, b"hello").wait()
                scheduler.sleep(0.05)
                scheduler.io.sock_sendall(writer, b"world").wait()
                writer.shutdown(socket.SHUT_WR)

            task = scheduler.spawn(receive_chunks)
            scheduler.spawn(send_chunks)
            assert scheduler.run_until_complete(task) == [(0, b"hello"), (1, b"world")]
        finally:
            reader.close()
            writer.close()

    def test_poll_completes_when_fd_becomes_readable(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)

            def wait_for_read() -> int:
                return scheduler.io.poll(reader.fileno(), select.POLLIN).wait()

            def send() -> None:
                scheduler.sleep(0.001)
                writer.send(b"x")

            task = scheduler.spawn(wait_for_read)
            scheduler.spawn(send)
            assert scheduler.run_until_complete(task) & select.POLLIN
        finally:
            reader.close()
            writer.close()

    def test_poll_many_emits_until_cancelled(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        seen: list[int] = []
        waiter = None
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            waiter = scheduler.io.poll_many(reader.fileno(), select.POLLIN, seen.append)

            def send() -> None:
                scheduler.sleep(0.001)
                writer.send(b"x")

            def wait_for_event() -> None:
                while not seen:
                    scheduler.sleep(0.001)
                op = waiter.operation
                assert op is not None
                scheduler.proactor.cancel(op)

            scheduler.spawn(send)
            task = scheduler.spawn(wait_for_event)
            scheduler.run_until_complete(task)
            assert seen[0] & select.POLLIN
        finally:
            if waiter is not None:
                op = waiter.operation
                if op is not None and not op.done():
                    scheduler.proactor.cancel(op)
            reader.close()
            writer.close()

    def test_accept_and_connect(self, scheduler: SyncProactorScheduler) -> None:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server.setblocking(False)
            client.setblocking(False)
            server.bind(("127.0.0.1", 0))
            server.listen()

            def accept_and_read() -> bytes:
                conn, _initial = scheduler.io.sock_accept(server).wait()
                try:
                    return scheduler.io.sock_recv(conn, 4).wait()
                finally:
                    conn.close()

            def connect_and_send() -> None:
                scheduler.io.sock_connect(client, server.getsockname()).wait()
                scheduler.io.sock_sendall(client, b"ping").wait()

            task = scheduler.spawn(accept_and_read)
            scheduler.spawn(connect_and_send)
            assert scheduler.run_until_complete(task) == b"ping"
        finally:
            client.close()
            server.close()

    def test_datagram_helpers(self, scheduler: SyncProactorScheduler) -> None:
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            receiver.setblocking(False)
            sender.setblocking(False)
            receiver.bind(("127.0.0.1", 0))
            buf = bytearray(5)

            def receive() -> tuple[int, object]:
                return scheduler.io.sock_recvfrom_into(receiver, buf).wait()

            def send() -> int:
                scheduler.sleep(0.001)
                return scheduler.io.sock_sendto(sender, b"hello", receiver.getsockname()).wait()

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

    def test_io_waiter_timeout_cancels_operation(self, scheduler: SyncProactorScheduler) -> None:
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)
            writer.setblocking(False)
            operation = scheduler.proactor.recv(reader, 1)
            waiter = IOWaiter(scheduler.io, operation)

            def wait_with_timeout() -> bool:
                with pytest.raises(TimeoutError):
                    with timeout(0.001):
                        waiter.wait()
                deadline = scheduler.time() + 1.0
                while scheduler.time() < deadline and (
                    not operation.cancelled() or scheduler.proactor.has_pending_operations()
                ):
                    scheduler.proactor.wait(min(deadline, scheduler.time() + 0.01))
                return operation.cancelled() and not scheduler.proactor.has_pending_operations()

            task = scheduler.spawn(wait_with_timeout)
            assert scheduler.run_until_complete(task) is True
        finally:
            reader.close()
            writer.close()


class TestProactorScheduler:
    @pytest.fixture(autouse=True)
    def _patch_uring_probe_capabilities(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_uring_capabilities(monkeypatch)

    def test_proactor_scheduler_is_abstract(self):
        with pytest.raises(TypeError, match="abstract"):
            ProactorScheduler()

    def test_default_proactor_factory_uses_uring_when_available(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(uring_api, "is_available", lambda: True)
        monkeypatch.setattr(proactor_module, "_default_uring_ring_factory", _FakeUringRing)
        scheduler = SyncProactorScheduler()
        try:
            assert isinstance(scheduler.proactor, UringProactor)
        finally:
            scheduler.close()

    def test_default_proactor_factory_uses_selector_when_uring_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(uring_api, "is_available", lambda: False)
        scheduler = SyncProactorScheduler()
        try:
            assert isinstance(scheduler.proactor, SelectorProactor)
        finally:
            scheduler.close()

    def test_create_socket_uses_uring_submit_when_available(self) -> None:
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            operation = proactor.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            _wait_for_uring(proactor, operation.done)
            sock = operation.result()
            try:
                assert len(proactor.ring.submitted_socket) == 1
                _domain, submit_type, _proto, submit_flags, _user_data = proactor.ring.submitted_socket[0]
                expected_type = socket.SOCK_STREAM | getattr(socket, "SOCK_NONBLOCK", 0) | getattr(
                    socket, "SOCK_CLOEXEC", 0
                )
                assert submit_type == expected_type
                assert submit_flags == 0
                _assert_scheduler_socket_fd(sock)
            finally:
                sock.close()
        finally:
            proactor.close()

    @pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="AF_UNIX is not supported")
    def test_create_socket_uses_uring_submit_for_unix(self) -> None:
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            operation = proactor.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
            _wait_for_uring(proactor, operation.done)
            sock = operation.result()
            try:
                assert len(proactor.ring.submitted_socket) == 1
                domain, submit_type, _proto, submit_flags, _user_data = proactor.ring.submitted_socket[0]
                assert domain == socket.AF_UNIX
                expected_type = socket.SOCK_STREAM | getattr(socket, "SOCK_NONBLOCK", 0) | getattr(
                    socket, "SOCK_CLOEXEC", 0
                )
                assert submit_type == expected_type
                assert submit_flags == 0
                assert sock.family == socket.AF_UNIX
                _assert_scheduler_socket_fd(sock)
            finally:
                sock.close()
        finally:
            proactor.close()

    @pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="AF_UNIX is not supported")
    def test_sock_create_unix_connect_to_uses_sync_connect_not_uring(self) -> None:
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                path = f"{temp_dir}/sock"
                server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                try:
                    server.bind(path)
                    server.listen()
                    sock = _io_sock_create(
                        proactor,
                        socket.AF_UNIX,
                        socket.SOCK_STREAM,
                        connect_to=path,
                    )
                    try:
                        assert len(proactor.ring.submitted_socket) == 1
                        assert proactor.ring.submitted_connect == []
                        assert sock.family == socket.AF_UNIX
                        server.accept()
                    finally:
                        sock.close()
                finally:
                    server.close()
        finally:
            proactor.close()

    def test_sock_create_connect_uses_uring_socket_submit(self) -> None:
        proactor = UringProactor(ring_factory=_FakeUringRing)
        try:
            sock = _io_sock_create(
                proactor,
                socket.AF_INET,
                socket.SOCK_STREAM,
                connect_to=("127.0.0.1", 9),
            )
            try:
                assert len(proactor.ring.submitted_socket) == 1
                assert len(proactor.ring.submitted_connect) == 1
            finally:
                sock.close()
        finally:
            proactor.close()

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
                return scheduler.io.sock_recv(reader, 5).wait()

            def send() -> None:
                scheduler.sleep(0.001)
                scheduler.io.sock_sendall(writer, b"hello").wait()

            task = scheduler.spawn(receive)
            scheduler.spawn(send)

            assert len(created) == 1
            assert scheduler.proactor is created[0]
            assert scheduler.run_until_complete(task) == b"hello"
        finally:
            reader.close()
            writer.close()
            scheduler.close()

    def test_set_shared_recv_buffer_pool_replaces_default(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        reader, _writer = socket.socketpair()
        try:
            reader.setblocking(False)
            custom = scheduler.io.create_recv_buffer_pool(4096, 16)
            scheduler.io.set_shared_recv_buffer_pool(custom)

            def receive_first_chunk() -> tuple[int, bytes]:
                stream = scheduler.io.sock_recv_iter(reader)
                item = next(stream)
                assert item is not None
                index, chunk = item
                return index, bytes(chunk)

            task = scheduler.spawn(receive_first_chunk)
            scheduler.spawn(lambda: scheduler.proactor.ring.complete_recv_multishot(b"x", more=False, sequence=0))

            assert scheduler.run_until_complete(task) == (0, b"x")
            submitted = scheduler.proactor.ring.submitted_recv_multishot[0][1]
            assert submitted is custom
            assert scheduler.io.shared_recv_buffer_pool() is custom
            assert scheduler.proactor.shared_recv_buffer_pool() is custom
        finally:
            reader.close()
            scheduler.close()

    def test_io_waiter_wakes_event_on_scheduler_thread_from_uring_callback(self, monkeypatch):
        import tealetio.io_waiter as io_waiter_module

        event_set_threads: list[int] = []
        original_event = io_waiter_module.ThreadsafeEvent

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

        monkeypatch.setattr(io_waiter_module, "ThreadsafeEvent", TrackingEvent)
        scheduler = SyncProactorScheduler(proactor_factory)
        set_scheduler(scheduler)
        scheduler_thread = threading.get_ident()
        reader, writer = socket.socketpair()
        try:
            reader.setblocking(False)

            def receive() -> bytes:
                return scheduler.io.sock_recv(reader, 5).wait()

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
                    return scheduler.io.sock_recv(reader, 5).wait()

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

    def test_open_returns_raw_io_file_from_fake_ring(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        try:

            def exercise() -> tuple[bytes, bytes]:
                with scheduler.io.open("/tmp/example.txt", "w+b").wait() as handle:
                    assert isinstance(handle, io.RawIOBase)
                    assert handle.name == "/tmp/example.txt"
                    assert handle.write(b"hello") == 5
                    handle.seek(0)
                    payload = handle.read()
                    assert handle.tell() == 5
                    handle.seek(0)
                    prefix = handle.read(3)
                return payload, prefix

            payload, prefix = scheduler.run_until_complete(scheduler.spawn(exercise))
            assert payload == b"hello"
            assert prefix == b"hel"
        finally:
            scheduler.close()

    def test_readinto_uses_proactor_read_into(self, monkeypatch):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        read_into_calls: list[tuple[int, int]] = []
        original_read_into = UringProactor.read_into

        def tracking_read_into(self, fd: int, buf: Any, offset: int):
            read_into_calls.append((fd, offset))
            return original_read_into(self, fd, buf, offset)

        monkeypatch.setattr(UringProactor, "read_into", tracking_read_into)
        try:

            def exercise() -> tuple[int, bytes]:
                with scheduler.io.open("/tmp/buffered.txt", "w+b").wait() as handle:
                    handle.write(b"hello")
                    handle.seek(0)
                    buf = bytearray(5)
                    nbytes = handle.readinto(buf)
                    return nbytes, bytes(buf)

            nbytes, payload = scheduler.run_until_complete(scheduler.spawn(exercise))
            assert nbytes == 5
            assert payload == b"hello"
            assert read_into_calls == [(read_into_calls[0][0], 0)]
        finally:
            scheduler.close()

    def test_buffered_reader_stacks_on_proactor_file(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        try:

            def exercise() -> bytes:
                with scheduler.io.open("/tmp/stacked.txt", "w+b").wait() as handle:
                    handle.write(b"hello")
                    handle.seek(0)
                    return io.BufferedReader(handle, buffer_size=2).read()

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"hello"
        finally:
            scheduler.close()

    def test_open_requires_proactor_with_openat_support(self):
        scheduler = SyncProactorScheduler(SelectorProactor)
        set_scheduler(scheduler)
        try:
            with pytest.raises(NotImplementedError, match="openat support"):
                scheduler.io.open("/tmp/x", "rb").wait()
        finally:
            scheduler.close()

    @pytest.mark.skipif(not uring_api.is_available(), reason="io_uring is required")
    def test_native_open_read_write_and_buffered_stack(self):
        scheduler = SyncProactorScheduler(UringProactor)
        set_scheduler(scheduler)
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "scheduler-file.txt")
            try:

                def exercise() -> bytes:
                    with scheduler.io.open(path, "wb").wait() as handle:
                        assert handle.write(b"hello") == 5

                    with scheduler.io.open(path, "rb").wait() as handle:
                        buffered = io.BufferedReader(handle)
                        return buffered.read()

                assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"hello"
            finally:
                scheduler.close()

    @pytest.mark.skipif(not uring_api.is_available(), reason="io_uring is required")
    def test_native_readinto_after_seek(self):
        scheduler = SyncProactorScheduler(UringProactor)
        set_scheduler(scheduler)
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "readinto-seek.txt")
            try:

                def exercise() -> tuple[int, bytes, int]:
                    with scheduler.io.open(path, "w+b").wait() as handle:
                        handle.write(b"hello")
                        handle.seek(1)
                        buf = bytearray(3)
                        nbytes = handle.readinto(buf)
                        return nbytes, bytes(buf), handle.tell()

                nbytes, payload, pos = scheduler.run_until_complete(scheduler.spawn(exercise))
                assert nbytes == 3
                assert payload == b"ell"
                assert pos == 4
            finally:
                scheduler.close()

    def test_seek_cur_updates_logical_position(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        try:

            def exercise() -> int:
                with scheduler.io.open("/tmp/seek-cur.txt", "w+b").wait() as handle:
                    handle.write(b"hello")
                    handle.seek(2, os.SEEK_CUR)
                    return handle.tell()

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == 7
        finally:
            scheduler.close()

    def test_seek_end_and_positioned_read_use_file_size(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        ring = cast(_FakeUringRing, scheduler._proactor.ring)
        try:

            def exercise() -> tuple[int, bytes]:
                with scheduler.io.open("/tmp/seek-end.txt", "w+b").wait() as handle:
                    handle.write(b"hello")
                    handle.seek(0, os.SEEK_END)
                    end_pos = handle.tell()
                    handle.seek(-2, os.SEEK_END)
                    return end_pos, handle.read(2)

            end_pos, suffix = scheduler.run_until_complete(scheduler.spawn(exercise))
            assert end_pos == 5
            assert suffix == b"lo"
            assert ring.submitted_read[-1][2] == 3
            assert ring.submitted_statx_fdsize
            assert ring.submitted_statx == []
        finally:
            scheduler.close()

    def test_append_write_ignores_prior_seek(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        ring = cast(_FakeUringRing, scheduler._proactor.ring)
        try:

            def exercise() -> bytes:
                with scheduler.io.open("/tmp/append.txt", "a+b").wait() as handle:
                    handle.write(b"hello")
                    handle.seek(0)
                    handle.write(b"!")
                    handle.seek(0)
                    return handle.read()

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"hello!"
            assert ring.submitted_write[-1][2] == 5
            assert len(ring.submitted_statx_fdsize) == 2
            assert ring.submitted_statx == []
        finally:
            scheduler.close()

    def test_append_sequential_writes_stat_once_at_open(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        ring = cast(_FakeUringRing, scheduler._proactor.ring)
        try:

            def exercise() -> None:
                with scheduler.io.open("/tmp/append-seq.txt", "ab").wait() as handle:
                    handle.write(b"hello")
                    handle.write(b"world")

            scheduler.run_until_complete(scheduler.spawn(exercise))
            assert len(ring.submitted_statx_fdsize) == 1
            assert ring.submitted_write[0][2] == 0
            assert ring.submitted_write[1][2] == 5
        finally:
            scheduler.close()

    def test_open_closes_fd_when_append_initial_size_lookup_fails(self, monkeypatch: pytest.MonkeyPatch):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        closed_fds: list[int] = []
        original_close = os.close

        def tracking_close(fd: int) -> None:
            closed_fds.append(fd)
            original_close(fd)

        monkeypatch.setattr(os, "close", tracking_close)

        def failing_stat_fdsize(self: UringProactor, fd: int) -> Operation[int]:
            operation = Operation[int](kind="stat_fdsize", fileobj=fd)
            operation._finish(exception=OSError(errno.EIO, "stat failed"))
            return operation

        monkeypatch.setattr(UringProactor, "stat_fdsize", failing_stat_fdsize)
        try:

            def exercise() -> None:
                scheduler.io.open("/tmp/leak.txt", "ab").wait()

            with pytest.raises(OSError, match="stat failed"):
                scheduler.run_until_complete(scheduler.spawn(exercise))
            assert len(closed_fds) == 1
        finally:
            scheduler.close()

    def test_append_seek_end_zero_is_noop_when_already_at_eof(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        ring = cast(_FakeUringRing, scheduler._proactor.ring)
        try:

            def exercise() -> int:
                with scheduler.io.open("/tmp/append-seek-end.txt", "ab").wait() as handle:
                    handle.write(b"hello")
                    handle.seek(0, os.SEEK_END)
                    return handle.tell()

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == 5
            assert len(ring.submitted_statx_fdsize) == 1
        finally:
            scheduler.close()

    def test_writeonly_handle_rejects_read(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        try:

            def exercise() -> None:
                with scheduler.io.open("/tmp/write-only.txt", "wb").wait() as handle:
                    with pytest.raises(OSError) as excinfo:
                        handle.read(1)
                    assert excinfo.value.errno == errno.EBADF

            scheduler.run_until_complete(scheduler.spawn(exercise))
        finally:
            scheduler.close()

    def test_readonly_handle_rejects_write(self):
        scheduler = SyncProactorScheduler(lambda: UringProactor(ring_factory=_FakeUringRing))
        set_scheduler(scheduler)
        try:

            def exercise() -> None:
                with scheduler.io.open("/tmp/read-only.txt", "rb").wait() as handle:
                    with pytest.raises(OSError) as excinfo:
                        handle.write(b"y")
                    assert excinfo.value.errno == errno.EBADF

            scheduler.run_until_complete(scheduler.spawn(exercise))
        finally:
            scheduler.close()

    @pytest.mark.skipif(not uring_api.is_available(), reason="io_uring is required")
    def test_native_append_mode_appends_after_seek(self):
        scheduler = SyncProactorScheduler(UringProactor)
        set_scheduler(scheduler)
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "append-native.txt")
            try:

                def exercise() -> bytes:
                    with scheduler.io.open(path, "wb").wait() as handle:
                        handle.write(b"hello")
                    with scheduler.io.open(path, "ab").wait() as handle:
                        handle.seek(0)
                        handle.write(b"!")
                    with scheduler.io.open(path, "rb").wait() as handle:
                        return handle.read()

                assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"hello!"
            finally:
                scheduler.close()
