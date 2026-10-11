"""Stdlib ``HTTPServer`` on one tealet scheduler.

Patches ``socket`` and ``poll``, then accepts with ``selectors.PollSelector``.
Each connection is finished on its own tealet. A client tealet on the same
scheduler performs a few requests. Stock ``serve_forever`` handles one
connection at a time; ``process_request`` is where a thread or a tealet is
spawned.

Run from a checkout::

    uv run --active --package tealetio python packages/tealetio/examples/stdlib_http.py
"""

from __future__ import annotations

import http.client
import selectors
from http.server import BaseHTTPRequestHandler, HTTPServer

import tealetio.ext.poll
import tealetio.ext.socket
from tealetio import Event, run, spawn

# Patch before http.server builds a selector. PollSelector captures
# select.poll in the class body; ext.poll.patch retargets that attribute.
tealetio.ext.socket.patch()
tealetio.ext.poll.patch()

REQUESTS = 3


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def do_GET(self) -> None:
        body = f"hello {self.path}\n".encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        # address_string() calls getfqdn and would block the OS thread.
        print(f"server {self.client_address[0]} {format % args}")


class Server(HTTPServer):
    """Accept ``count`` connections and finish each on a spawned tealet."""

    def __init__(self, server_address: tuple[str, int], handler: type[BaseHTTPRequestHandler]) -> None:
        super().__init__(server_address, handler)
        self._left = 0
        self._done = Event()
        self._error: BaseException | None = None

    def process_request(self, request: object, client_address: object) -> None:
        self._left += 1
        spawn(
            lambda request=request, client_address=client_address: self._run_request(
                request,
                client_address,
            )
        )

    def _run_request(self, request: object, client_address: object) -> None:
        try:
            self.finish_request(request, client_address)
        except Exception as exc:
            if self._error is None:
                self._error = exc
            self.handle_error(request, client_address)
        finally:
            self.shutdown_request(request)
            self._left -= 1
            if self._left == 0:
                self._done.set()

    def serve_n(self, count: int) -> None:
        """Wait for ``count`` connections. Uses the patched ``select.poll``."""

        with selectors.PollSelector() as selector:
            selector.register(self, selectors.EVENT_READ)
            accepted = 0
            while accepted < count:
                if not selector.select(timeout=2):
                    raise TimeoutError(f"timed out waiting for connection {accepted + 1}")
                before = self._left
                self._handle_request_noblock()
                if self._left == before:
                    raise RuntimeError("readable listener produced no request")
                accepted += 1
        self._done.swait()
        if self._error is not None:
            raise self._error


def client(port: int, seen: list[bytes]) -> None:
    for i in range(REQUESTS):
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        try:
            connection.request("GET", f"/{i}")
            response = connection.getresponse()
            body = response.read()
            print(f"client {response.status} {body!r}")
            if response.status != 200:
                raise RuntimeError(f"status {response.status}")
            seen.append(body)
        finally:
            connection.close()


def main() -> None:
    server = Server(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    seen: list[bytes] = []
    client_task = spawn(lambda: client(port, seen))
    try:
        server.serve_n(REQUESTS)
        client_task.wait()
    finally:
        server.server_close()
    expected = [f"hello /{i}\n".encode() for i in range(REQUESTS)]
    if seen != expected:
        raise RuntimeError(f"unexpected responses: {seen!r}")
    print(f"ok {REQUESTS} requests")


if __name__ == "__main__":
    run(main)
