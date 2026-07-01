from __future__ import annotations

import socket

import pytest

from tealetio import ensure_resolved, getaddrinfo, set_scheduler
from tealetio.dns import ipaddr_info
from tealetio.proactor import SyncProactorScheduler


class TestDnsResolution:
    def test_ipaddr_info_returns_literal_ipv4_without_lookup(self):
        info = ipaddr_info("127.0.0.1", 8080, socket.AF_UNSPEC, socket.SOCK_STREAM, 0)
        assert info is not None
        assert info[0] == socket.AF_INET
        assert info[4] == ("127.0.0.1", 8080)

    def test_ensure_resolved_skips_executor_for_literal_ipv4(self, monkeypatch):
        scheduler = SyncProactorScheduler()
        set_scheduler(scheduler)
        calls: list[object] = []
        real_run = scheduler.run_in_executor

        def tracking_run(executor, func, *args):
            calls.append(func)
            return real_run(executor, func, *args)

        monkeypatch.setattr(scheduler, "run_in_executor", tracking_run)
        try:
            infos = scheduler.ensure_resolved(("127.0.0.1", 80), type=socket.SOCK_STREAM)
            assert infos == [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("127.0.0.1", 80))]
            assert calls == []
        finally:
            scheduler.close()

    def test_getaddrinfo_uses_executor(self, monkeypatch):
        scheduler = SyncProactorScheduler()
        set_scheduler(scheduler)
        calls: list[object] = []
        real_run = scheduler.run_in_executor

        def tracking_run(executor, func, *args):
            calls.append(func)
            return real_run(executor, func, *args)

        monkeypatch.setattr(scheduler, "run_in_executor", tracking_run)

        def exercise() -> list[tuple[int, int, int, str, tuple[object, ...]]]:
            return getaddrinfo("localhost", 0, type=socket.SOCK_STREAM)

        try:
            task = scheduler.spawn(exercise)
            infos = scheduler.run_until_complete(task)
            assert calls == [socket.getaddrinfo]
            assert infos
        finally:
            scheduler.close()

    def test_ensure_resolved_module_helper_delegates_to_scheduler(self):
        scheduler = SyncProactorScheduler()
        set_scheduler(scheduler)
        try:

            def exercise() -> str:
                infos = ensure_resolved(("127.0.0.1", 0), type=socket.SOCK_STREAM)
                return infos[0][4][0]

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == "127.0.0.1"
        finally:
            scheduler.close()

    def test_getaddrinfo_propagates_resolution_errors(self):
        scheduler = SyncProactorScheduler()
        set_scheduler(scheduler)
        try:

            def exercise() -> None:
                with pytest.raises(OSError):
                    getaddrinfo("this-host-should-not-exist.invalid.", 0, type=socket.SOCK_STREAM)

            scheduler.run_until_complete(scheduler.spawn(exercise))
        finally:
            scheduler.close()