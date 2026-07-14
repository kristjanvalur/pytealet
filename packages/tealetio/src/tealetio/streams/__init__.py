"""Tealet-native stream helpers with optional asyncio-compatible facades."""

from .common import run_coro
from .connect import open_connection, open_streams
from .factories import (
    AsyncStreamFactory,
    StreamFactory,
    default_async_stream_factory,
    default_stream_factory,
    open_recv_buffer,
    open_send_buffer,
    open_streams as open_streams_internal,
    pooled_default_stream_factory,
)
from .reader import AsyncStreamReader, StreamReader
from .server import StreamServer, bind_tcp_socket, default_reuse_address, start_server
from .writer import AsyncStreamWriter, StreamWriter, shutdown_stream_writer

# Backward-compatible private aliases used by tests and ``io_manager``.
_open_streams = open_streams_internal
_open_recv_buffer = open_recv_buffer
_open_send_buffer = open_send_buffer
_default_reuse_address = default_reuse_address
_shutdown_stream_writer = shutdown_stream_writer
_bind_tcp_socket = bind_tcp_socket

__all__ = [
    "StreamReader",
    "StreamWriter",
    "AsyncStreamReader",
    "AsyncStreamWriter",
    "StreamFactory",
    "AsyncStreamFactory",
    "StreamServer",
    "default_stream_factory",
    "default_async_stream_factory",
    "pooled_default_stream_factory",
    "open_connection",
    "open_streams",
    "start_server",
    "run_coro",
]
