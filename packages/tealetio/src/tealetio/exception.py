"""Split, join, and re-raise an exception's ``__context__`` chain.

``raise`` while another exception is being handled replaces ``__context__``
with that exception, even when a link is already set. ``raise_as_is`` puts
the saved chain back.
"""

from __future__ import annotations

import itertools
from collections.abc import Sequence
from typing import NoReturn

__all__ = [
    "exception_join_context",
    "exception_split_context",
    "raise_as_is",
]


def exception_split_context(exc: BaseException) -> list[BaseException]:
    """Return ``exc`` and each ``__context__``, starting at ``exc``.

    The walk stops at ``None``, or when an exception is already in the list.
    """

    chain: list[BaseException] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None:
        ident = id(current)
        if ident in seen:
            break
        seen.add(ident)
        chain.append(current)
        current = current.__context__
    return chain


def exception_join_context(excs: Sequence[BaseException]) -> BaseException:
    """Link ``excs`` through ``__context__`` and return ``excs[0]``.

    ``excs[0]`` is the exception to raise. Each later item becomes the
    previous item's ``__context__``. The last item keeps the context it
    already has, so a chain hanging off the tail stays there.
    """

    if not excs:
        raise ValueError("exception chain must not be empty")
    for current, context in itertools.pairwise(excs):
        current.__context__ = context
    return excs[0]


def raise_as_is(exc: BaseException) -> NoReturn:
    """Raise ``exc`` and keep the ``__context__`` it had before the raise.

    ``raise`` replaces ``__context__`` with the exception being handled,
    even when a link is already set. The saved chain is put back in a
    ``finally``, and ``__suppress_context__`` is restored with it.
    """

    saved = exc.__context__
    suppress = exc.__suppress_context__
    try:
        raise exc
    finally:
        exc.__context__ = saved
        exc.__suppress_context__ = suppress
