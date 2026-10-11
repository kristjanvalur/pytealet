"""Readiness-table poll, select, and epoll, plus the select module patch."""

from __future__ import annotations

import select
import selectors
import socket
import threading
import time

import pytest

from tealetio import set_scheduler
from tealetio.proactor import SelectorProactor
from tealetio.readiness import Epoll, Poll, readiness_table
from tealetio.readiness import select as tealet_select
from tealetio.scheduler import sleep
from uring_fakes import SCHEDULER_INTEGRATION_FACTORIES, run_scheduler_task


def _run(scheduler, func):
    set_scheduler(scheduler)
    return run_scheduler_task(scheduler, func)


def _pair() -> tuple[socket.socket, socket.socket]:
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    return left, right


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
def test_poll_reports_stored_readiness_then_clears(scheduler_factory) -> None:
    scheduler = scheduler_factory()

    def body() -> None:
        left, right = _pair()
        poller = Poll()
        try:
            poller.register(left, select.POLLIN)
            assert poller.poll(0) == []
            right.send(b"x")
            sleep(0.05)
            if not isinstance(scheduler.io.proactor, SelectorProactor):
                watch = readiness_table(scheduler.io).watches[left.fileno()]
                assert watch.stored & select.POLLIN
            events = poller.poll(500)
            assert events
            assert events[0][0] == left.fileno()
            assert events[0][1] & select.POLLIN
            assert left.recv(1) == b"x"
            assert poller.poll(0) == []
        finally:
            poller.close()
            left.close()
            right.close()

    _run(scheduler, body)


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
def test_poll_then_recv_does_not_collide(scheduler_factory) -> None:
    scheduler = scheduler_factory()

    def body() -> None:
        left, right = _pair()
        poller = Poll()
        try:
            poller.register(left, select.POLLIN)
            right.send(b"q")
            assert poller.poll(500)
            assert scheduler.io.sock_recv(left, 1).wait() == b"q"
        finally:
            poller.close()
            left.close()
            right.close()

    _run(scheduler, body)


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
def test_epoll_edge_reports_once_until_new_data(scheduler_factory) -> None:
    scheduler = scheduler_factory()

    def body() -> None:
        left, right = _pair()
        ep = Epoll()
        try:
            ep.register(left, select.EPOLLIN | select.EPOLLET)
            right.send(b"a")
            first = ep.poll(0.5)
            assert first and first[0][0] == left.fileno()
            assert ep.poll(0) == []
            assert ep.poll(0.05) == []
            assert left.recv(1) == b"a"

            def later() -> None:
                time.sleep(0.05)
                try:
                    right.send(b"b")
                except OSError:
                    return

            threading.Thread(target=later, daemon=True).start()
            second = ep.poll(1.0)
            assert second and second[0][0] == left.fileno()
        finally:
            ep.close()
            left.close()
            right.close()

    _run(scheduler, body)


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
def test_patched_poll_select_and_epoll(scheduler_factory) -> None:
    import tealetio.ext.poll as ext_poll

    scheduler = scheduler_factory()
    ext_poll.patch()

    def body() -> None:
        left, right = _pair()
        poller = select.poll()
        try:
            poller.register(left, select.POLLIN)
            right.send(b"z")
            events = poller.poll(500)
            assert events and events[0][0] == left.fileno()
            assert left.recv(1) == b"z"
            assert poller.poll(0) == []

            right.send(b"s")
            readable, writable, errors = select.select([left], [], [], 0.5)
            assert readable == [left]
            assert writable == []
            assert errors == []
            assert left.recv(1) == b"s"

            ep = select.epoll()
            try:
                ep.register(left, select.EPOLLIN)
                right.send(b"e")
                found = ep.poll(0.5)
                assert found and found[0][0] == left.fileno()
            finally:
                ep.close()
        finally:
            poller.close()
            left.close()
            right.close()

    try:
        _run(scheduler, body)
    finally:
        ext_poll.unpatch()


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
def test_poll_selector_follows_patch(scheduler_factory) -> None:
    import tealetio.ext.poll as ext_poll

    scheduler = scheduler_factory()
    ext_poll.patch()

    def body() -> None:
        left, right = _pair()
        chosen = selectors.PollSelector()
        try:
            chosen.register(left, selectors.EVENT_READ)
            right.send(b"p")
            found = chosen.select(0.5)
            assert found
            assert found[0][0].fd == left.fileno()
        finally:
            chosen.close()
            left.close()
            right.close()

    try:
        _run(scheduler, body)
    finally:
        ext_poll.unpatch()


def test_kqueue_and_selector_classes_roundtrip() -> None:
    import tealetio.ext.poll as ext_poll
    from tealetio.readiness import poll as poll_factory

    kqueue = getattr(select, "kqueue", None)
    poll_cls = selectors.PollSelector._selector_cls
    epoll_selector = getattr(selectors, "EpollSelector", None)
    epoll_cls = None if epoll_selector is None else epoll_selector._selector_cls
    ext_poll.patch()
    try:
        assert getattr(select, "kqueue", None) is kqueue
        assert select.poll is poll_factory
        assert select.select is tealet_select
        assert selectors.PollSelector._selector_cls is select.poll
        if epoll_selector is not None:
            assert select.epoll is Epoll
            assert epoll_selector._selector_cls is select.epoll
        ext_poll.patch()
        assert getattr(select, "_real_select", None) is not None
    finally:
        ext_poll.unpatch()
    assert selectors.PollSelector._selector_cls is poll_cls
    if epoll_selector is not None:
        assert epoll_selector._selector_cls is epoll_cls
    assert not hasattr(select, "_real_select")


def test_select_rejects_negative_timeout() -> None:
    with pytest.raises(ValueError):
        tealet_select([], [], [], -1)
