"""Shared type aliases for tealetio."""

from __future__ import annotations

from typing import TypeAlias

SocketSendBuffer: TypeAlias = bytes | bytearray | memoryview

__all__ = ["SocketSendBuffer"]