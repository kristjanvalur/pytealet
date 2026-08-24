"""Keyword-argument paths for methods gated to PyArg_ParseArrayAndKeywords on 3.15+."""

import pytest

import _uring_api
import uring_api

from conftest import require_uring


def test_probe_accepts_keywords():
    result = _uring_api.probe(entries=4, flags=0)
    assert isinstance(result, dict)
    assert "available" in result


def test_probe_rejects_unknown_keyword():
    with pytest.raises(TypeError):
        _uring_api.probe(bogus=1)


def test_ring_construct_read_accepts_keywords():
    require_uring()

    buf = bytearray(16)
    with uring_api.Ring() as ring:
        completion = ring.construct_read(fd=0, buf=buf, offset=0, user_data=None)
        assert completion is not None
        ring.prepare(completion)


def test_ring_wait_idle_accepts_timeout_keyword():
    require_uring()

    with uring_api.Ring() as ring:
        assert ring.wait_idle(timeout=0) is False


def test_ring_constructor_still_uses_tuple_keywords():
    """tp_init cannot use METH_FASTCALL; keyword construction must keep working."""
    require_uring()

    with uring_api.Ring(entries=8, flags=0, auto_submit=True, experimental_send_all_submit_next=False) as ring:
        assert ring.closed is False
        assert ring.experimental_send_all_submit_next is False
