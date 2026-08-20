#!/usr/bin/env python3
"""Compare uring IO-manager eager send vs proactor-only send.

Accept and recv always go to the proactor (no manager-side first try).
``TEALETIO_EAGER_SEND`` / ``TEALETIO_EAGER_IO`` still control the send try.

``SyncUringProactor`` is the target. Env knobs are applied at IO-manager
construction. High-volume cases use many small ops; large-payload cases move
256 KiB (recv/send) or a fat accept backlog.

Usage::

    uv run --active --package tealetio python packages/tealetio/bench/micro_eager_compare.py
"""

from __future__ import annotations

import argparse
import os
import socket
import statistics
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from tealetio.operations import is_io_cancellation
from tealetio.proactor import SyncProactorScheduler, SyncUringProactor
from tealetio.scheduler import set_scheduler

_EAGER_KEYS = (
    "TEALETIO_EAGER_IO",
    "TEALETIO_EAGER_SEND",
)


def _ns() -> int:
    return time.perf_counter_ns()


@contextmanager
def _eager_send(enabled: bool) -> Iterator[None]:
    saved = {key: os.environ.get(key) for key in _EAGER_KEYS}
    os.environ.pop("TEALETIO_EAGER_IO", None)
    os.environ["TEALETIO_EAGER_SEND"] = "1" if enabled else "0"
    try:
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _make_scheduler(*, entries: int) -> SyncProactorScheduler:
    return SyncProactorScheduler(lambda: SyncUringProactor(entries=entries))


def _cancel_waiter(io: Any, waiter: Any) -> None:
    operation = getattr(waiter, "operation", None)
    if operation is not None and not operation.done():
        io.proactor.cancel(operation)


def _wait_ignore_cancel(waiter: Any) -> None:
    try:
        waiter.wait()
    except OSError as exc:
        if not is_io_cancellation(exc):
            raise


def _mean_us(samples: list[int]) -> float:
    return statistics.fmean(samples) / 1000.0


def _drain(sock: socket.socket) -> None:
    while True:
        try:
            chunk = sock.recv(65536)
        except BlockingIOError:
            return
        if not chunk:
            return


def _bench_accept(*, iterations: int, backlog: int, entries: int) -> list[int]:
    samples: list[int] = []
    scheduler = _make_scheduler(entries=entries)
    set_scheduler(scheduler)
    try:

        def exercise() -> None:
            nonlocal samples
            io = scheduler.io
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 0))
            listener.listen(max(backlog * 2, 128))
            listener.setblocking(False)
            addr = listener.getsockname()
            try:
                for _ in range(iterations):
                    clients = []
                    for _i in range(backlog):
                        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        client.connect(addr)
                        clients.append(client)
                    try:
                        got = 0

                        def on_accept(delivery: object) -> None:
                            nonlocal got
                            conn, _initial = delivery  # type: ignore[misc]
                            conn.close()
                            got += 1

                        t0 = _ns()
                        waiter = None
                        while got < backlog:
                            waiter = io.accept_many(listener, on_accept)
                            if got >= backlog:
                                break
                            if not waiter.poll():
                                while got < backlog and not waiter.poll():
                                    scheduler.proactor.wait(0.0)
                                if got < backlog and not waiter.poll():
                                    _wait_ignore_cancel(waiter)
                        if waiter is not None and not waiter.poll():
                            _cancel_waiter(io, waiter)
                            _wait_ignore_cancel(waiter)
                        samples.append(_ns() - t0)
                    finally:
                        for client in clients:
                            client.close()
            finally:
                listener.close()

        scheduler.run_until_complete(scheduler.spawn(exercise))
    finally:
        scheduler.close()
        set_scheduler(None)
    return samples


def _bench_recv(*, iterations: int, payload_bytes: int, chunk_size: int, entries: int) -> list[int]:
    samples: list[int] = []
    payload = b"x" * payload_bytes
    scheduler = _make_scheduler(entries=entries)
    set_scheduler(scheduler)
    try:

        def exercise() -> None:
            nonlocal samples
            io = scheduler.io
            need = max(8, payload_bytes // chunk_size + 4)
            buffer_count = 1
            while buffer_count < need:
                buffer_count *= 2
            pool = io.create_recv_buffer_pool(chunk_size, buffer_count)
            for _ in range(iterations):
                reader, writer = socket.socketpair()
                reader.setblocking(False)
                try:
                    writer.sendall(payload)
                    writer.shutdown(socket.SHUT_WR)
                    t0 = _ns()
                    data = io.sock_recvall(reader, buffer_pool=pool)
                    samples.append(_ns() - t0)
                    assert data == payload
                finally:
                    reader.close()
                    writer.close()

        scheduler.run_until_complete(scheduler.spawn(exercise))
    finally:
        scheduler.close()
        set_scheduler(None)
    return samples


def _bench_send_small(*, iterations: int, payload_bytes: int, entries: int) -> list[int]:
    samples: list[int] = []
    payload = b"x" * payload_bytes
    scheduler = _make_scheduler(entries=entries)
    set_scheduler(scheduler)
    try:

        def exercise() -> None:
            nonlocal samples
            io = scheduler.io
            reader, writer = socket.socketpair()
            reader.setblocking(False)
            writer.setblocking(False)
            try:
                for _ in range(iterations):
                    t0 = _ns()
                    io.sock_sendall(writer, payload).wait()
                    samples.append(_ns() - t0)
                    _drain(reader)
            finally:
                reader.close()
                writer.close()

        scheduler.run_until_complete(scheduler.spawn(exercise))
    finally:
        scheduler.close()
        set_scheduler(None)
    return samples


def _bench_send_large(*, iterations: int, payload_bytes: int, entries: int) -> list[int]:
    samples: list[int] = []
    payload = b"x" * payload_bytes
    scheduler = _make_scheduler(entries=entries)
    set_scheduler(scheduler)
    try:

        def exercise() -> None:
            nonlocal samples
            io = scheduler.io
            for _ in range(iterations):
                reader, writer = socket.socketpair()
                writer.setblocking(False)
                stop = threading.Event()

                def drain() -> None:
                    reader.setblocking(True)
                    while not stop.is_set():
                        try:
                            chunk = reader.recv(65536)
                        except OSError:
                            return
                        if not chunk:
                            return

                thread = threading.Thread(target=drain, daemon=True)
                thread.start()
                try:
                    t0 = _ns()
                    io.sock_sendall(writer, payload).wait()
                    samples.append(_ns() - t0)
                finally:
                    stop.set()
                    try:
                        writer.shutdown(socket.SHUT_WR)
                    except OSError:
                        pass
                    thread.join(timeout=2.0)
                    reader.close()
                    writer.close()

        scheduler.run_until_complete(scheduler.spawn(exercise))
    finally:
        scheduler.close()
        set_scheduler(None)
    return samples


def _run_once(
    name: str,
    measure,
    *,
    warmup: int,
    iterations: int,
    kwargs: dict[str, Any],
) -> float:
    print(f"\n{name}", flush=True)
    measure(iterations=warmup, **kwargs)
    mean = _mean_us(measure(iterations=iterations, **kwargs))
    print(f"  mean={mean:8.2f} us", flush=True)
    return mean


def _run_send_case(
    name: str,
    measure,
    *,
    warmup: int,
    iterations: int,
    kwargs: dict[str, Any],
) -> dict[str, float]:
    means: dict[str, float] = {}
    print(f"\n{name}", flush=True)
    baseline: float | None = None
    for label, enabled in (("send-on", True), ("send-off", False)):
        with _eager_send(enabled):
            measure(iterations=warmup, **kwargs)
            samples = measure(iterations=iterations, **kwargs)
        mean = _mean_us(samples)
        means[label] = mean
        if baseline is None:
            baseline = mean
            delta = "baseline"
        else:
            pct = 100.0 * (mean - baseline) / baseline
            delta = f"{pct:+.1f}% vs send-on"
        print(f"  {label:10s}  mean={mean:8.2f} us  ({delta})", flush=True)
    return means


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--entries", type=int, default=512)
    parser.add_argument("--warmup", type=int, default=8)
    parser.add_argument("--volume-n", type=int, default=80)
    parser.add_argument("--large-n", type=int, default=20)
    parser.add_argument("--large-payload", type=int, default=256 * 1024)
    args = parser.parse_args()
    print(
        f"SyncUringProactor entries={args.entries} "
        f"volume_n={args.volume_n} large_n={args.large_n} "
        f"large_payload={args.large_payload}",
        flush=True,
    )
    _run_once(
        "accept volume (backlog=32, proactor only)",
        _bench_accept,
        warmup=max(3, args.warmup // 2),
        iterations=args.volume_n,
        kwargs={"backlog": 32, "entries": args.entries},
    )
    _run_once(
        "recv volume (64 B recvall, proactor only)",
        _bench_recv,
        warmup=args.warmup,
        iterations=args.volume_n,
        kwargs={"payload_bytes": 64, "chunk_size": 4096, "entries": args.entries},
    )
    _run_once(
        f"recv large ({args.large_payload} B recvall, proactor only)",
        _bench_recv,
        warmup=max(3, args.warmup // 2),
        iterations=args.large_n,
        kwargs={"payload_bytes": args.large_payload, "chunk_size": 16384, "entries": args.entries},
    )
    _run_send_case(
        "send volume (64 B sendall)",
        _bench_send_small,
        warmup=args.warmup,
        iterations=args.volume_n,
        kwargs={"payload_bytes": 64, "entries": args.entries},
    )
    _run_send_case(
        f"send large ({args.large_payload} B sendall)",
        _bench_send_large,
        warmup=max(3, args.warmup // 2),
        iterations=args.large_n,
        kwargs={"payload_bytes": args.large_payload, "entries": args.entries},
    )


if __name__ == "__main__":
    main()
