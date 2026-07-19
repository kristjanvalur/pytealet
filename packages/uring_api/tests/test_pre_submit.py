"""Ring.pre_submit hook ordering."""

from __future__ import annotations

import socket

import pytest

import uring_api

from helpers import wait_one
from conftest import require_uring


def test_pre_submit_default_is_none():
    require_uring()
    with uring_api.Ring() as ring:
        assert ring.pre_submit is None
        ring.pre_submit = None
        assert ring.pre_submit is None


def test_pre_submit_rejects_non_callable():
    require_uring()
    with uring_api.Ring() as ring:
        with pytest.raises(TypeError, match="pre_submit"):
            ring.pre_submit = 1  # type: ignore[assignment]


def test_pre_submit_arms_before_return_with_user_data_set():
    require_uring()

    token = object()
    events: list[object] = []
    linked: dict[str, object | None] = {"completion": None}

    def pre_submit(completion: object) -> None:
        events.append(completion)
        assert completion.user_data is token  # type: ignore[union-attr]
        linked["completion"] = completion

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        buf = bytearray(5)
        with uring_api.Ring() as ring:
            ring.pre_submit = pre_submit
            pending = ring.submit_recv(reader.fileno(), buf, token)
            # reverse link must exist before wait (and before any CQE delivery)
            assert linked["completion"] is pending
            assert events == [pending]
            writer.send(b"hello")
            done = wait_one(ring, 1.0)
        assert done is pending
        assert bytes(buf) == b"hello"
        # successful path: hook once only (no retract)
        assert events == [pending]
    finally:
        reader.close()
        writer.close()


def test_pre_submit_raise_skips_submit():
    require_uring()

    token = object()
    calls = 0

    def pre_submit(completion: object) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("arm failed")

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        buf = bytearray(4)
        with uring_api.Ring() as ring:
            ring.pre_submit = pre_submit
            with pytest.raises(RuntimeError, match="arm failed"):
                ring.submit_recv(reader.fileno(), buf, token)
            # hook once; no second retract call
            assert calls == 1
            # nothing should complete from the failed submit
            assert ring.wait(0) == []
    finally:
        reader.close()
        writer.close()


def test_pre_submit_sees_cancel_target_as_user_data():
    require_uring()

    token = object()
    seen_user_data: list[object] = []

    def pre_submit(completion: object) -> None:
        seen_user_data.append(completion.user_data)  # type: ignore[union-attr]

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        buf = bytearray(8)
        with uring_api.Ring() as ring:
            ring.pre_submit = pre_submit
            pending = ring.submit_recv(reader.fileno(), buf, token)
            ring.submit_cancel(pending)
        assert token in seen_user_data
        assert pending in seen_user_data  # cancel default user_data is the target
    finally:
        reader.close()
        writer.close()


def test_c_pre_submit_runs_before_python_and_before_return():
    require_uring()
    from helpers import build_c_api_client

    client = build_c_api_client()
    token = object()
    c_seen: list[object] = []
    py_seen: list[object] = []

    def pre_submit(completion: object) -> None:
        # C hook already ran when Python is invoked
        assert c_seen == [completion]
        py_seen.append(completion)

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        buf = bytearray(5)
        with uring_api.Ring() as ring:
            client.set_c_pre_submit(ring, c_seen)
            ring.pre_submit = pre_submit
            pending = ring.submit_recv(reader.fileno(), buf, token)
            assert c_seen == [pending]
            assert py_seen == [pending]
            writer.send(b"hello")
            done = wait_one(ring, 1.0)
            client.clear_c_pre_submit(ring)
        assert done is pending
        assert bytes(buf) == b"hello"
    finally:
        reader.close()
        writer.close()
