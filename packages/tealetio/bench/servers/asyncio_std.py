#!/usr/bin/env python3
"""Minimal HTTP server on the stdlib asyncio event loop (baseline)."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

_BENCH_DIR = Path(__file__).resolve().parent.parent
if str(_BENCH_DIR) not in sys.path:
    sys.path.insert(0, str(_BENCH_DIR))

from common import RESPONSE, add_server_args, drain_request_async  # noqa: E402


async def _handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    await drain_request_async(reader)
    writer.write(RESPONSE)
    await writer.drain()
    writer.close()


async def _serve(host: str, port: int, backlog: int) -> None:
    server = await asyncio.start_server(_handle_client, host, port, backlog=backlog)
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
