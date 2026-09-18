from __future__ import annotations

import asyncio
import importlib.util
import shutil
import socket
import ssl
import subprocess
from pathlib import Path

import pytest

from tealetio import Event, run, set_scheduler
from tealetio.asyncio import TealetProactorEventLoop
from tealetio.proactor import SyncProactorScheduler
from tealetio.streams import open_connection, open_streams, ssl_server_context, start_server
from tealetio.streams.ssl import wrap_ssl
from uring_fakes import SCHEDULER_INTEGRATION_FACTORIES

pytestmark = pytest.mark.skipif(importlib.util.find_spec("ssl") is None, reason="ssl module is not available")


def _require_openssl() -> str:
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("openssl is not available")
    return openssl


@pytest.fixture
def tls_cert(tmp_path: Path) -> tuple[Path, Path]:
    openssl = _require_openssl()
    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    result = subprocess.run(
        [
            openssl,
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
            "-keyout",
            str(key),
            "-out",
            str(cert),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        # OpenSSL 1.0 has no -addext; CN=localhost is enough for hostname checks there
        result = subprocess.run(
            [
                openssl,
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-keyout",
                str(key),
                "-out",
                str(cert),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            pytest.fail(f"openssl failed to mint a test certificate: {result.stderr}")
    return cert, key


def _server_context(cert: Path, key: Path) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert, key)
    return ctx


def _client_context(cert: Path) -> ssl.SSLContext:
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.check_hostname = True
    ctx.load_verify_locations(cert)
    return ctx


@pytest.mark.parametrize("scheduler_factory", SCHEDULER_INTEGRATION_FACTORIES)
class TestNativeSslWrap:
    @pytest.fixture
    def scheduler(self, scheduler_factory) -> SyncProactorScheduler:
        sched = scheduler_factory()
        set_scheduler(sched)
        yield sched
        sched.close()

    def test_socketpair_echo(self, scheduler: SyncProactorScheduler, tls_cert: tuple[Path, Path]) -> None:
        cert, key = tls_cert
        server_ctx = _server_context(cert, key)
        client_ctx = _client_context(cert)

        server_sock, client_sock = socket.socketpair()
        try:
            server_sock.setblocking(False)
            client_sock.setblocking(False)

            def exercise() -> bytes:
                server_reader, server_writer = open_streams(server_sock)
                client_reader, client_writer = open_streams(client_sock)
                ssl_server, ssl_server_w = wrap_ssl(
                    server_reader, server_writer, server_ctx, server_side=True
                )
                ssl_client, ssl_client_w = wrap_ssl(
                    client_reader,
                    client_writer,
                    client_ctx,
                    server_side=False,
                    server_hostname="localhost",
                )
                assert ssl_server is ssl_server_w
                assert ssl_client is ssl_client_w
                assert not ssl_client.can_write_eof()
                with pytest.raises(NotImplementedError, match="half-close"):
                    ssl_client.write_eof()

                def server_side() -> None:
                    ssl_server.handshake()
                    payload = ssl_server.readline()
                    assert payload == b"ping\n"
                    ssl_server.write(b"PONG\n")
                    ssl_server.drain()

                server_task = scheduler.spawn(server_side)
                ssl_client.handshake()
                assert ssl_client.get_extra_info("sslcontext") is client_ctx
                assert ssl_client.get_extra_info("peercert")
                assert ssl_client.get_extra_info("cipher")
                assert ssl_client.get_extra_info("socket") is client_sock
                ssl_client.write(b"ping\n")
                ssl_client.drain()
                reply = ssl_client.readexactly(5)
                server_task.wait()
                ssl_client.close()
                ssl_client.wait_closed()
                ssl_server.close()
                ssl_server.wait_closed()
                return reply

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"PONG\n"
        finally:
            server_sock.close()
            client_sock.close()

    def test_start_tls_upgrades_plaintext_pair(
        self, scheduler: SyncProactorScheduler, tls_cert: tuple[Path, Path]
    ) -> None:
        cert, key = tls_cert
        server_ctx = _server_context(cert, key)
        client_ctx = _client_context(cert)

        server_sock, client_sock = socket.socketpair()
        try:
            server_sock.setblocking(False)
            client_sock.setblocking(False)

            def exercise() -> bytes:
                server_reader, server_writer = open_streams(server_sock)
                client_reader, client_writer = open_streams(client_sock)

                def server_side() -> None:
                    assert server_reader.readline() == b"STARTTLS\n"
                    server_writer.write(b"220\n")
                    server_writer.drain()
                    tls_reader, tls_writer = server_writer.start_tls(
                        server_ctx, server_reader, server_side=True
                    )
                    assert tls_reader is tls_writer
                    line = tls_reader.readline()
                    tls_writer.write(line.upper())
                    tls_writer.drain()

                server_task = scheduler.spawn(server_side)
                client_writer.write(b"STARTTLS\n")
                client_writer.drain()
                assert client_reader.readline() == b"220\n"
                client_reader, client_writer = client_writer.start_tls(
                    client_ctx, client_reader, server_hostname="localhost"
                )
                assert client_reader is client_writer
                client_writer.write(b"ping\n")
                client_writer.drain()
                reply = client_reader.readline()
                server_task.wait()
                client_writer.close()
                client_writer.wait_closed()
                return reply

            assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"PING\n"
        finally:
            server_sock.close()
            client_sock.close()

    def test_ssl_kwargs_open_connection_and_start_server(
        self, scheduler: SyncProactorScheduler, tls_cert: tuple[Path, Path]
    ) -> None:
        cert, key = tls_cert
        server_ctx = ssl_server_context(cert, key)
        client_ctx = _client_context(cert)
        handled = Event()

        def handler(reader, writer) -> None:
            line = reader.readline()
            writer.write(line.upper())
            writer.drain()
            handled.set()

        def exercise() -> bytes:
            server = start_server(
                handler,
                addr=("127.0.0.1", 0),
                ssl=server_ctx,
                scheduler=scheduler,
            )
            try:
                port = server.sockets[0].getsockname()[1]
                reader, writer = open_connection(
                    addr=("127.0.0.1", port),
                    ssl=client_ctx,
                    server_hostname="localhost",
                )
                assert reader is writer
                writer.write(b"ping\n")
                writer.drain()
                reply = reader.readline()
                handled.swait()
                writer.close()
                writer.wait_closed()
                return reply
            finally:
                server.close()
                server.wait_closed()

        assert scheduler.run_until_complete(scheduler.spawn(exercise)) == b"PING\n"

    def test_ssl_kwarg_validation(self, scheduler: SyncProactorScheduler) -> None:
        with pytest.raises(ValueError, match="server_hostname is only meaningful with ssl"):
            open_connection(addr=("127.0.0.1", 1), server_hostname="localhost", scheduler=scheduler)
        with pytest.raises(TypeError, match="SSLContext or None"):
            start_server(lambda r, w: None, addr=("127.0.0.1", 0), ssl=True, scheduler=scheduler)
        with pytest.raises(TypeError, match="async_=True"):
            open_connection(addr=("127.0.0.1", 1), ssl=True, async_=True, scheduler=scheduler)


def test_hosted_asyncio_ssl(tls_cert: tuple[Path, Path]) -> None:
    cert, key = tls_cert
    server_ctx = _server_context(cert, key)
    client_ctx = _client_context(cert)

    def exercise() -> None:
        loop = TealetProactorEventLoop()
        try:
            asyncio.set_event_loop(loop)

            async def _handle_client(
                reader: asyncio.StreamReader,
                writer: asyncio.StreamWriter,
            ) -> None:
                line = await reader.readline()
                writer.write(line.upper())
                await writer.drain()
                writer.close()
                await writer.wait_closed()

            async def _serve_once() -> bytes:
                server = await asyncio.start_server(_handle_client, "127.0.0.1", 0, ssl=server_ctx)
                port = server.sockets[0].getsockname()[1]
                async with server:
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(
                            "127.0.0.1",
                            port,
                            ssl=client_ctx,
                            server_hostname="localhost",
                        ),
                        timeout=5.0,
                    )
                    writer.write(b"ping\n")
                    await writer.drain()
                    data = await reader.readline()
                    writer.close()
                    await writer.wait_closed()
                    return data

            assert loop.run_until_complete(_serve_once()) == b"PING\n"
        finally:
            asyncio.set_event_loop(None)
            loop.close()

    run(exercise, scheduler_factory=SyncProactorScheduler)
