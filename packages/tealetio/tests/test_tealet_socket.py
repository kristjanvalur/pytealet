"""TealetSocket parking methods and the socket module patch."""

from __future__ import annotations

import socket
import sys
import threading

import pytest

from tealetio import set_scheduler
from tealetio.io_manager import _create_scheduler_socket
from tealetio.sockets import TealetSocket, connect_tcp, create_connection, socketpair
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
def test_send_returns_one_count_and_send_all_drains(scheduler_factory) -> None:
    scheduler = scheduler_factory()
    real_send = scheduler.io.sock_send
    real_sendall = scheduler.io.sock_sendall
    calls: list[tuple[str, bytes]] = []

    def short_send(sock, data):
        payload = memoryview(data)[:1]
        calls.append(("send", bytes(payload)))
        return real_send(sock, payload)

    def track_sendall(sock, data, progress=None):
        calls.append(("sendall", bytes(data)))
        return real_sendall(sock, data, progress)

    scheduler.io.sock_send = short_send
    scheduler.io.sock_sendall = track_sendall

    def body() -> None:
        left, right = socketpair()
        try:
            assert right.send(b"hello") == 1
            assert left.recv(1) == b"h"
            right.send_all(b"yo")
            right.sendall(b"z")
            assert left.recv(3) == b"yoz"
            assert calls == [("send", b"h"), ("sendall", b"yo"), ("sendall", b"z")]
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


def test_connect_tcp_forwards_source_address(monkeypatch: pytest.MonkeyPatch) -> None:
    from tealetio import happy_eyeballs

    captured: dict[str, object] = {}

    def fake(addr: tuple[str, int], **kwargs: object) -> socket.socket:
        captured["addr"] = addr
        captured["source_address"] = kwargs.get("source_address")
        left, right = socket.socketpair()
        right.close()
        return left

    monkeypatch.setattr(happy_eyeballs, "connect_tcp", fake)
    sock = connect_tcp(("example.com", 80), source_address=("127.0.0.1", 0))
    try:
        assert isinstance(sock, TealetSocket)
        assert captured == {"addr": ("example.com", 80), "source_address": ("127.0.0.1", 0)}
    finally:
        sock.close()


def test_connect_tcp_rejects_a_bad_timeout() -> None:
    with pytest.raises(TypeError):
        connect_tcp(("127.0.0.1", 1), timeout=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        connect_tcp(("127.0.0.1", 1), -1)


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
def test_connect_tcp_wraps_the_winner(scheduler_factory) -> None:
    scheduler = scheduler_factory()
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

    def body() -> None:
        sock = connect_tcp(("127.0.0.1", live_port), timeout=2)
        try:
            assert isinstance(sock, TealetSocket)
            assert sock.getpeername()[1] == live_port
            assert sock.gettimeout() == socket.getdefaulttimeout()
        finally:
            sock.close()

    try:
        _run(scheduler, body)
    finally:
        live.close()
        thread.join(timeout=2)
        for conn in accepted:
            conn.close()


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
def test_connect_tcp_timeout_limits_the_race(scheduler_factory, monkeypatch: pytest.MonkeyPatch) -> None:
    from tealetio.locks import Event

    scheduler = scheduler_factory()
    started = threading.Event()

    def hang(*_args: object, **_kwargs: object) -> object:
        started.set()

        class _Hang:
            def wait(self) -> None:
                Event().swait()

        return _Hang()

    monkeypatch.setattr(scheduler.io, "sock_create", hang)

    def body() -> None:
        with pytest.raises(socket.timeout) as raised:
            connect_tcp(("127.0.0.1", 1), timeout=0.1)
        assert str(raised.value) == "timed out"

    try:
        _run(scheduler, body)
    finally:
        assert started.is_set()


def test_patch_replaces_sys_modules_socket() -> None:
    import importlib

    import tealetio.ext.socket as ext_socket
    from tealetio.socket_helpers import real_socket_module

    # This module imported socket before patch, so its binding stays.
    original_module = socket
    original_class = socket.socket
    ext_socket.patch()
    try:
        installed = sys.modules["socket"]
        assert installed is ext_socket
        assert installed is not original_module
        assert importlib.import_module("socket") is installed
        assert installed.real_socket is original_module
        assert socket is original_module
        assert socket.socket is original_class
        assert installed.socket is TealetSocket
        assert installed.SocketType is TealetSocket
        assert installed.connect_tcp is connect_tcp
        assert installed.AF_INET == original_module.AF_INET
        assert "AF_INET" in dir(installed)
        assert real_socket_module() is original_module
        assert real_socket_module().socket is original_class
        assert original_module.socket is original_class
        assert not hasattr(original_module, "_real_socket")
        ext_socket.patch()
        assert sys.modules["socket"] is installed
        left, right = installed.socketpair()
        try:
            assert isinstance(left, TealetSocket)
            assert isinstance(left._sock, original_class)
        finally:
            left.close()
            right.close()
        raw = _create_scheduler_socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            assert isinstance(raw, original_class)
            assert not isinstance(raw, TealetSocket)
        finally:
            raw.close()
        fresh_left, fresh_right = original_module.socketpair()
        try:
            assert type(fresh_left) is original_class
            assert type(fresh_right) is original_class
        finally:
            fresh_left.close()
            fresh_right.close()
    finally:
        ext_socket.unpatch()
    assert sys.modules["socket"] is original_module
    assert importlib.import_module("socket") is original_module
    assert original_module.socket is original_class
    ext_socket.unpatch()


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
def test_patched_getaddrinfo(scheduler_factory) -> None:
    import tealetio.ext.socket as ext_socket

    scheduler = scheduler_factory()
    ext_socket.patch()

    def body() -> None:
        installed = sys.modules["socket"]
        infos = installed.getaddrinfo("localhost", 0, installed.AF_INET, installed.SOCK_STREAM)
        assert infos
        assert infos[0][0] == socket.AF_INET

    try:
        _run(scheduler, body)
    finally:
        ext_socket.unpatch()
