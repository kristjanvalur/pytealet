"""Backward-compatible re-exports from ``streams.open``."""

from .open import (
    AsyncClientHandler,
    AsyncStreamFactory,
    AsyncStreamPair,
    ClientHandler,
    NativeClientHandler,
    NativeStreamPair,
    StreamFactory,
    StreamFactoryArg,
    default_async_stream_factory,
    default_server_stream_factory,
    default_stream_factory,
    open_recv_buffer,
    open_send_buffer,
    open_streams,
    pooled_default_stream_factory,
)

__all__ = [
    "AsyncClientHandler",
    "AsyncStreamFactory",
    "AsyncStreamPair",
    "ClientHandler",
    "NativeClientHandler",
    "NativeStreamPair",
    "StreamFactory",
    "StreamFactoryArg",
    "default_async_stream_factory",
    "default_server_stream_factory",
    "default_stream_factory",
    "open_recv_buffer",
    "open_send_buffer",
    "open_streams",
    "pooled_default_stream_factory",
]
