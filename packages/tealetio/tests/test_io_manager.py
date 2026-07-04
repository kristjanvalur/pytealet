from __future__ import annotations

import socket

import pytest

from tealetio import set_scheduler
from tealetio.io_manager import FileIO, PollIO, ProactorIOManager, SocketIO
from tealetio.proactor import SyncProactorScheduler


class TestProactorIOManager:
    def test_scheduler_exposes_io_facade(self):
        scheduler = SyncProactorScheduler()
        io = scheduler.io
        assert isinstance(io, ProactorIOManager)
        assert isinstance(io, SocketIO)
        assert isinstance(io, PollIO)
        assert isinstance(io, FileIO)
        assert io.proactor is scheduler.proactor

    def test_scheduler_io_forwards_sock_recv(self):
        scheduler = SyncProactorScheduler()
        set_scheduler(scheduler)
        client, server = socket.socketpair()
        try:
            client.setblocking(False)
            server.setblocking(False)
            client.sendall(b"ping")

            def exercise() -> bytes:
                return scheduler.io.sock_recv(server, 4)

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"ping"
            assert scheduler.io.sock_recv(server, 0) == b""
        finally:
            client.close()
            server.close()
            scheduler.close()

    def test_basic_scheduler_io_raises(self):
        from tealetio.scheduler import BasicScheduler

        scheduler = BasicScheduler()
        with pytest.raises(RuntimeError, match="scheduler with IO support"):
            scheduler.io