"""Readiness-table poll, select, and epoll, plus the select module patch."""

from __future__ import annotations

import select
import selectors
import socket
import sys
import threading
import time

import pytest

from tealetio import set_scheduler
from tealetio.proactor import SelectorProactor
from tealetio.readiness import Epoll, Poll, readiness_table
from tealetio.readiness import select as tealet_select
from tealetio.scheduler import sleep
from uring_fakes import SCHEDULER_INTEGRATION_FACTORIES, run_scheduler_task


def test_epoll_rejects_non_positive_maxevents() -> None:
    epoll = Epoll()
    try:
        for maxevents in (0, -2):
            with pytest.raises(ValueError, match="maxevents"):
                epoll.poll(0, maxevents)
    finally:
        epoll.close()


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
def test_poll_stays_armed_while_another_wait_is_parked(scheduler_factory) -> None:
    from tealetio import Event

    scheduler = scheduler_factory()

    def body() -> None:
        left, right = _pair()
        parked = Poll()
        quick = Poll()
        entered = Event()
        try:
            parked.register(left, select.POLLIN)
            quick.register(left, select.POLLIN)

            def wait_for_byte() -> None:
                entered.set()
                events = parked.poll(1000)
                assert events and events[0][0] == left.fileno()
                assert left.recv(1) == b"x"

            task = scheduler.spawn(wait_for_byte)
            entered.swait()
            sleep(0)
            assert quick.poll(0) == []
            right.send(b"x")
            task.wait()
            if isinstance(scheduler.io.proactor, SelectorProactor):
                watch = readiness_table(scheduler.io).watches.get(left.fileno())
                assert watch is not None and watch.handle is None
        finally:
            parked.close()
            quick.close()
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
def test_edge_commit_leaves_level_readiness(scheduler_factory) -> None:
    scheduler = scheduler_factory()

    def body() -> None:
        left, right = _pair()
        level = Poll()
        edge = Epoll()
        try:
            level.register(left, select.POLLIN)
            edge.register(left, select.EPOLLIN | select.EPOLLET)
            right.send(b"x")
            first = edge.poll(0.5)
            assert first and first[0][0] == left.fileno()
            watch = readiness_table(scheduler.io).watches[left.fileno()]
            assert watch.stored & select.POLLIN
            second = level.poll(0)
            assert second and second[0][0] == left.fileno()
            assert second[0][1] & select.POLLIN
            assert left.recv(1) == b"x"
        finally:
            level.close()
            edge.close()
            left.close()
            right.close()

    _run(scheduler, body)


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
def test_patched_poll_select_and_epoll(scheduler_factory) -> None:
    import tealetio.ext.poll as ext_poll

    scheduler = scheduler_factory()
    ext_poll.patch()

    def body() -> None:
        installed = sys.modules["select"]
        left, right = _pair()
        poller = installed.poll()
        try:
            poller.register(left, installed.POLLIN)
            right.send(b"z")
            events = poller.poll(500)
            assert events and events[0][0] == left.fileno()
            assert left.recv(1) == b"z"
            assert poller.poll(0) == []

            right.send(b"s")
            readable, writable, errors = installed.select([left], [], [], 0.5)
            assert readable == [left]
            assert writable == []
            assert errors == []
            assert left.recv(1) == b"s"

            ep = installed.epoll()
            try:
                ep.register(left, installed.EPOLLIN)
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


def test_patch_replaces_sys_modules_select() -> None:
    import importlib

    import tealetio.ext.poll as ext_poll
    from tealetio.poll_helpers import real_select_module
    from tealetio.readiness import poll as poll_factory

    # This module imported select before patch, so its binding stays.
    original_module = select
    original_poll = select.poll
    kqueue = getattr(select, "kqueue", None)
    poll_cls = selectors.PollSelector._selector_cls
    epoll_selector = getattr(selectors, "EpollSelector", None)
    epoll_cls = None if epoll_selector is None else epoll_selector._selector_cls
    ext_poll.patch()
    try:
        installed = sys.modules["select"]
        assert installed is ext_poll
        assert installed is not original_module
        assert importlib.import_module("select") is installed
        assert select is original_module
        assert select.poll is original_poll
        assert not hasattr(original_module, "_real_select")
        assert installed.poll is poll_factory
        assert installed.select is tealet_select
        assert installed.POLLIN == original_module.POLLIN
        assert "POLLIN" in dir(installed)
        assert installed.real_select is original_module
        assert real_select_module() is original_module
        assert real_select_module().poll is original_poll
        assert getattr(installed, "kqueue", None) is kqueue
        # selectors was imported before patch, so its class attributes stay.
        assert selectors.PollSelector._selector_cls is poll_cls
        if epoll_selector is not None:
            assert installed.epoll is Epoll
            assert epoll_selector._selector_cls is epoll_cls
        ext_poll.patch()
        assert sys.modules["select"] is installed
    finally:
        ext_poll.unpatch()
    assert sys.modules["select"] is original_module
    assert importlib.import_module("select") is original_module
    assert select.poll is original_poll
    assert selectors.PollSelector._selector_cls is poll_cls
    if epoll_selector is not None:
        assert epoll_selector._selector_cls is epoll_cls
    ext_poll.unpatch()


def test_select_rejects_negative_timeout() -> None:
    with pytest.raises(ValueError):
        tealet_select([], [], [], -1)
