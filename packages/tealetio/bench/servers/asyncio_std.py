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

_profile_seq = 0


def _next_req_num(profile: bool) -> int | None:
    global _profile_seq
    if not profile:
        return None
    _profile_seq += 1
    if _profile_seq == 1:
        return None
    return _profile_seq - 1


async def _handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    profile: bool = False,
) -> None:
    timer = None
    if profile:
        from profile_timing import PhaseTimer, drain_request_async_profile

        req_num = _next_req_num(profile)
        if req_num is not None:
            timer = PhaseTimer("asyncio", req_num)
            timer.mark("handler_start")
    if timer is not None:
        await drain_request_async_profile(reader, timer)
        timer.mark("drain")
        writer.write(RESPONSE)
        timer.mark("write")
        await writer.drain()
        timer.mark("drain_out")
        writer.close()
        timer.mark("close")
        timer.finish()
    else:
        await drain_request_async(reader)
        writer.write(RESPONSE)
        await writer.drain()
        writer.close()


async def _serve(host: str, port: int, backlog: int, *, profile: bool) -> None:
    async def client_handler(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        await _handle_client(reader, writer, profile=profile)

    server = await asyncio.start_server(client_handler, host, port, backlog=backlog)
    async with server:
        await server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    add_server_args(parser)
    parser.add_argument("--profile", action="store_true", help="emit per-request PROFILE lines to stderr")
    args = parser.parse_args()
    try:
        asyncio.run(_serve(args.host, args.port, args.backlog, profile=args.profile))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
