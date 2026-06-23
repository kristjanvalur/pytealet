"""Compatibility shim for :mod:`tealetio.asyncio`."""

import sys

from tealetio import asyncio as _asyncio

sys.modules[__name__] = _asyncio
