from __future__ import annotations

import pytest

from tealetio.exception import exception_join_context, exception_split_context, raise_as_is


def test_split_walks_context_nearest_first() -> None:
    tail = ValueError("tail")
    mid = RuntimeError("mid")
    head = TimeoutError("head")
    mid.__context__ = tail
    head.__context__ = mid
    assert exception_split_context(head) == [head, mid, tail]


def test_join_links_nearest_first_and_keeps_the_tail() -> None:
    older = ValueError("older")
    first = RuntimeError("first")
    first.__context__ = older
    second = RuntimeError("second")
    assert exception_join_context([second, first]) is second
    assert second.__context__ is first
    assert first.__context__ is older


def test_split_join_round_trip() -> None:
    tail = ValueError("tail")
    mid = RuntimeError("mid")
    head = TimeoutError("head")
    mid.__context__ = tail
    head.__context__ = mid
    chain = exception_split_context(head)
    head.__context__ = None
    mid.__context__ = None
    assert exception_join_context(chain) is head
    assert exception_split_context(head) == chain


def test_split_stops_on_a_cycle() -> None:
    first = RuntimeError("first")
    second = RuntimeError("second")
    first.__context__ = second
    second.__context__ = first
    assert exception_split_context(first) == [first, second]


def test_join_rejects_an_empty_chain() -> None:
    with pytest.raises(ValueError, match="empty"):
        exception_join_context([])


def test_raise_as_is_keeps_context_and_suppress() -> None:
    inner = ValueError("inner")
    outer = RuntimeError("outer")
    outer.__context__ = inner
    outer.__suppress_context__ = True
    try:
        raise KeyError("handled")
    except KeyError:
        with pytest.raises(RuntimeError) as exc_info:
            raise_as_is(outer)
    assert exc_info.value is outer
    assert outer.__context__ is inner
    assert outer.__suppress_context__ is True
