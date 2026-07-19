"""Ring.pre_submit hook ordering and retract semantics."""

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


def test_pre_submit_arms_before_completion_and_keeps_link():
    require_uring()

    token = object()
    events: list[tuple[object, object | None]] = []
    linked: dict[str, object | None] = {"completion": None}

    def pre_submit(user_data: object, completion: object | None) -> None:
        events.append((user_data, completion))
        if completion is not None:
            linked["completion"] = completion
            assert completion.user_data is token  # type: ignore[union-attr]
        else:
            linked["completion"] = None

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
            assert events == [(token, pending)]
            writer.send(b"hello")
            done = wait_one(ring, 1.0)
        assert done is pending
        assert bytes(buf) == b"hello"
        # successful submit: no retract call
        assert events == [(token, pending)]
        assert linked["completion"] is pending
    finally:
        reader.close()
        writer.close()


def test_pre_submit_retracts_when_arm_hook_raises():
    require_uring()

    token = object()
    events: list[tuple[object, bool]] = []

    def pre_submit(user_data: object, completion: object | None) -> None:
        events.append((user_data, completion is not None))
        if completion is not None:
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
        assert events == [(token, True), (token, False)]
    finally:
        reader.close()
        writer.close()


def test_pre_submit_called_for_cancel_user_data():
    require_uring()

    token = object()
    seen: list[object] = []

    def pre_submit(user_data: object, completion: object | None) -> None:
        if completion is not None:
            seen.append(user_data)

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        buf = bytearray(8)
        with uring_api.Ring() as ring:
            ring.pre_submit = pre_submit
            pending = ring.submit_recv(reader.fileno(), buf, token)
            ring.submit_cancel(pending)
        assert token in seen
        assert pending in seen  # cancel default user_data is the target completion
    finally:
        reader.close()
        writer.close()
