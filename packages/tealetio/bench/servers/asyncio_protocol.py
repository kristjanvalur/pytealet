#!/usr/bin/env python3
"""Minimal HTTP server on stdlib asyncio using Protocol (no streams, no Task)."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

_BENCH_DIR = Path(__file__).resolve().parent.parent
if str(_BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_BENCH_DIR))

from common import RESPONSE, add_server_args, http_headers_complete  # noqa: E402


class HttpProtocol(asyncio.Protocol):
    def __init__(self) -> None:
        self._transport: asyncio.Transport | None = None
        self._buf = bytearray()
        self._done = False

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = transport  # type: ignore[assignment]

    def data_received(self, data: bytes) -> None:
        if self._done:
            return
        self._buf.extend(data)
        if not http_headers_complete(self._buf):
            return
        self._done = True
        transport = self._transport
        assert transport is not None
        transport.write(RESPONSE)
        transport.close()


async def _serve(host: str, port: int, backlog: int) -> None:
    server = await asyncio.get_running_loop().create_server(
        HttpProtocol,
        host,
        port,
        backlog=backlog,
    )
    async with server:
        await server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_server_args(parser)
    args = parser.parse_args()
    try:
        asyncio.run(_serve(args.host, args.port, args.backlog))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
