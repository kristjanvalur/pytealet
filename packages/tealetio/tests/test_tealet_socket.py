"""TealetSocket parking methods and the socket module patch."""

from __future__ import annotations

import socket
import threading
import types

import pytest

from tealetio import set_scheduler
from tealetio.io_manager import _create_scheduler_socket
from tealetio.sockets import TealetSocket, create_connection, socketpair
from uring_fakes import SCHEDULER_INTEGRATION_FACTORIES, run_scheduler_task


def _run(scheduler, func):
    set_scheduler(scheduler)
    return run_scheduler_task(scheduler, func)


def _refused_socket() -> tuple[socket.socket, int]:
    """Bound TCP socket that is not listening. Connects are refused."""

    held = socket.socket()
    held.bind(("127.0.0.1", 0))
    return held, held.getsockname()[1]


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
def test_socketpair_echo(scheduler_factory) -> None:
    scheduler = scheduler_factory()

    def body() -> None:
        left, right = socketpair()
        try:
            assert right.send(b"hi") == 2
            assert left.recv(2) == b"hi"
        finally:
            left.close()
            right.close()

    _run(scheduler, body)


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
def test_accept_connect_from_peer_thread(scheduler_factory) -> None:
    scheduler = scheduler_factory()

    def body() -> None:
        listener = TealetSocket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = listener.getsockname()[1]

        def client() -> None:
            peer = socket.socket()
            try:
                peer.connect(("127.0.0.1", port))
                peer.sendall(b"ping")
            finally:
                peer.close()

        thread = threading.Thread(target=client, daemon=True)
        thread.start()
        try:
            conn, _addr = listener.accept()
            try:
                assert conn.recv(4) == b"ping"
            finally:
                conn.close()
        finally:
            listener.close()
            thread.join(timeout=2)

    _run(scheduler, body)


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
def test_shutdown_unconnected_raises(scheduler_factory) -> None:
    scheduler = scheduler_factory()

    def body() -> None:
        sock = TealetSocket()
        try:
            with pytest.raises(OSError):
                sock.shutdown(socket.SHUT_RDWR)
        finally:
            sock.close()

    _run(scheduler, body)


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
def test_close_releases_fd(scheduler_factory) -> None:
    scheduler = scheduler_factory()

    def body() -> None:
        sock = TealetSocket()
        sock.close()
        assert sock.fileno() == -1
        sock.close()

    _run(scheduler, body)


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
def test_recv_timeout_and_nonblocking(scheduler_factory) -> None:
    scheduler = scheduler_factory()

    def body() -> None:
        left, right = socketpair()
        try:
            left.settimeout(0.1)
            with pytest.raises(socket.timeout):
                left.recv(1)
            left.settimeout(0)
            with pytest.raises(BlockingIOError):
                left.recv(1)
            with pytest.raises(ValueError):
                left.recv(1, socket.MSG_PEEK)
        finally:
            left.close()
            right.close()

    _run(scheduler, body)


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
def test_makefile_reads_a_line(scheduler_factory) -> None:
    scheduler = scheduler_factory()

    def body() -> None:
        left, right = socketpair()
        try:
            right.sendall(b"hello\n")
            stream = left.makefile()
            assert stream.readline() == "hello\n"
            left.close()
            assert left.fileno() != -1
            stream.close()
            assert left.fileno() == -1
        finally:
            right.close()
            left.close()

    _run(scheduler, body)


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
def test_create_connection_skips_refused_address(scheduler_factory) -> None:
    scheduler = scheduler_factory()
    refused, refused_port = _refused_socket()
    live = socket.socket()
    live.bind(("127.0.0.1", 0))
    live.listen(1)
    live_port = live.getsockname()[1]
    accepted: list[socket.socket] = []

    def accept_one() -> None:
        try:
            conn, _addr = live.accept()
        except OSError:
            return
        accepted.append(conn)

    thread = threading.Thread(target=accept_one, daemon=True)
    thread.start()

    def fake_ensure_resolved(address, *, family=0, type=0, proto=0, flags=0):
        del address, family, type, proto, flags
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", refused_port)),
            (socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", live_port)),
        ]

    scheduler.ensure_resolved = fake_ensure_resolved

    def body() -> None:
        sock = create_connection(("ignored.example", 1), timeout=2)
        try:
            assert sock.getpeername()[1] == live_port
        finally:
            sock.close()

    try:
        _run(scheduler, body)
    finally:
        live.close()
        refused.close()
        thread.join(timeout=2)
        for conn in accepted:
            conn.close()


def test_patch_installs_real_socket_snapshot() -> None:
    import tealetio.ext.socket as ext_socket

    original = socket.socket
    ext_socket.patch()
    try:
        assert socket.socket is TealetSocket
        assert isinstance(socket._real_socket, types.ModuleType)
        assert socket._real_socket.socket is original
        if hasattr(socket, "SocketType"):
            assert socket.SocketType is TealetSocket
        ext_socket.patch()
        assert socket._real_socket.socket is original
        raw = _create_scheduler_socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            assert isinstance(raw, original)
            assert not isinstance(raw, TealetSocket)
        finally:
            raw.close()
    finally:
        ext_socket.unpatch()
    assert socket.socket is original
    assert not hasattr(socket, "_real_socket")
    ext_socket.unpatch()


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
def test_patched_getaddrinfo(scheduler_factory) -> None:
    import tealetio.ext.socket as ext_socket

    scheduler = scheduler_factory()
    ext_socket.patch()

    def body() -> None:
        infos = socket.getaddrinfo("localhost", 0, socket.AF_INET, socket.SOCK_STREAM)
        assert infos
        assert infos[0][0] == socket.AF_INET

    try:
        _run(scheduler, body)
    finally:
        ext_socket.unpatch()
