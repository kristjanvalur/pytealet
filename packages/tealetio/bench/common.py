"""Shared helpers for tealetio HTTP benchmark servers."""

from __future__ import annotations

import argparse
import socket
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tealetio.streams import AsyncStreamReader, StreamReader

HTML_BODY = b"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>tealetio bench</title>
</head>
<body>
  <h1>tealetio HTTP benchmark</h1>
  <p>Static HTML payload for wrk load tests.</p>
  <p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed non risus.
  Suspendisse lectus tortor, dignissim sit amet, adipiscing nec, ultricies sed,
  dolor. Cras elementum ultrices diam. Maecenas ligula massa, varius a, semper
  congue, euismod non, mi. Proin porttitor, orci nec nonummy molestie, enim est
  eleifend mi, non fermentum diam nisl sit amet erat.</p>
</body>
</html>
"""

RESPONSE = (
    b"HTTP/1.1 200 OK\r\n"
    b"Content-Type: text/html; charset=utf-8\r\n"
    b"Content-Length: " + str(len(HTML_BODY)).encode() + b"\r\n"
    b"Connection: close\r\n"
    b"\r\n" + HTML_BODY
)


def add_server_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--host", default="127.0.0.1", help="bind address")
    parser.add_argument("--port", type=int, default=8080, help="listen port")
    parser.add_argument("--backlog", type=int, default=256, help="listen backlog")


def add_tealetio_args(parser: argparse.ArgumentParser) -> None:
    add_server_args(parser)
    parser.add_argument(
        "--proactor",
        choices=("default", "selector", "uring", "uring-sync"),
        default="default",
        help=(
            "proactor backend for tealetio servers "
            "(default: uring when available; uring-sync is single-threaded ring.wait)"
        ),
    )
    parser.add_argument(
        "--reuse-address",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="SO_REUSEADDR when binding (default: asyncio POSIX behaviour)",
    )
    parser.add_argument(
        "--reuse-port",
        action="store_true",
        help="enable SO_REUSEPORT when binding (default off, like asyncio)",
    )


def drain_request(reader: StreamReader) -> None:
    """Discard HTTP request headers (through the blank line)."""

    while True:
        line = reader.readline()
        if not line or line in (b"\r\n", b"\n"):
            break


async def drain_request_async(reader: AsyncStreamReader) -> None:
    while True:
        line = await reader.readline()
        if not line or line in (b"\r\n", b"\n"):
            break


def wait_for_listen(host: str, port: int, timeout: float = 10.0) -> None:
    """Poll until ``host:port`` accepts TCP connections."""

    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"server did not start on {host}:{port} within {timeout}s")
