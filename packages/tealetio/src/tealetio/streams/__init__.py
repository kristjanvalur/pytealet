"""Tealet-native stream helpers with optional asyncio-compatible facades."""

from __future__ import annotations

from typing import Any

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

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "run_coro": (".util", "run_coro"),
    "open_connection": (".connect", "open_connection"),
    "open_streams": (".connect", "open_streams"),
    "StreamReader": (".reader", "StreamReader"),
    "StreamWriter": (".writer", "StreamWriter"),
    "AsyncStreamReader": (".reader", "AsyncStreamReader"),
    "AsyncStreamWriter": (".writer", "AsyncStreamWriter"),
    "StreamFactory": (".open", "StreamFactory"),
    "AsyncStreamFactory": (".open", "AsyncStreamFactory"),
    "default_stream_factory": (".open", "default_stream_factory"),
    "default_async_stream_factory": (".open", "default_async_stream_factory"),
    "pooled_default_stream_factory": (".open", "pooled_default_stream_factory"),
    "StreamServer": (".server", "StreamServer"),
    "start_server": (".server", "start_server"),
}

_LAZY_PRIVATE_EXPORTS: dict[str, tuple[str, str]] = {
    "_open_streams": (".open", "open_streams"),
    "_open_recv_buffer": (".open", "open_recv_buffer"),
    "_open_send_buffer": (".open", "open_send_buffer"),
    "_default_reuse_address": (".server", "default_reuse_address"),
    "_shutdown_stream_writer": (".writer", "shutdown_stream_writer"),
    "_bind_tcp_socket": (".server", "bind_tcp_socket"),
}


def __getattr__(name: str) -> Any:
    spec = _LAZY_EXPORTS.get(name) or _LAZY_PRIVATE_EXPORTS.get(name)
    if spec is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = spec
    from importlib import import_module

    module = import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted([*__all__, *_LAZY_PRIVATE_EXPORTS])
