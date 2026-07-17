import os

import pytest

import uring_api

from helpers import kernel_version_at_least
from conftest import require_uring

_VERSION_GATED_CAPABILITIES = {
    "IORING_OP_STATX": (5, 6),
    "IORING_POLL_MULTISHOT": (5, 13),
    "IORING_ACCEPT_MULTISHOT": (5, 19),
    "IORING_OP_SOCKET": (5, 19),
    "IORING_BUF_RING": (5, 19),
    "IORING_RECV_MULTISHOT": (6, 0),
    "IORING_OP_SEND_ZC": (6, 0),
    "IORING_OP_SENDMSG_ZC": (6, 0),
}


def test_probe_returns_structured_result():
    probe = uring_api.probe()

    assert set(probe) == {
        "available",
        "IORING_ACCEPT_MULTISHOT",
        "IORING_POLL_MULTISHOT",
        "IORING_BUF_RING",
        "IORING_RECV_MULTISHOT",
        "IORING_OP_SEND_ZC",
        "IORING_OP_SENDMSG_ZC",
        "IORING_OP_SOCKET",
        "IORING_OP_STATX",
    }
    assert probe["available"] is True
    assert isinstance(probe["IORING_ACCEPT_MULTISHOT"], bool)
    assert isinstance(probe["IORING_POLL_MULTISHOT"], bool)
    assert isinstance(probe["IORING_BUF_RING"], bool)
    assert isinstance(probe["IORING_RECV_MULTISHOT"], bool)
    assert isinstance(probe["IORING_OP_SEND_ZC"], bool)
    assert isinstance(probe["IORING_OP_SENDMSG_ZC"], bool)
    assert isinstance(probe["IORING_OP_SOCKET"], bool)
    assert isinstance(probe["IORING_OP_STATX"], bool)
    # multishot recv requires provided-buffer rings (man: 6.0 vs 5.19)
    if probe["IORING_RECV_MULTISHOT"]:
        assert probe["IORING_BUF_RING"] is True


def test_kernel_version_at_least_handles_release_candidate_suffixes():
    assert kernel_version_at_least("5.6.0-rc1", 5, 6)
    assert not kernel_version_at_least("5.5.99-rc7", 5, 6)
    assert kernel_version_at_least("6.6.12-1-WSL2", 5, 6)


def test_probe_capabilities_match_kernel_version_gates():
    require_uring()

    probe = uring_api.probe()
    release = os.uname().release
    for name, (major, minor) in _VERSION_GATED_CAPABILITIES.items():
        expected = kernel_version_at_least(release, major, minor)
        assert probe[name] is expected, name


def test_probe_capabilities_are_stable_across_calls():
    require_uring()

    first = uring_api.probe()
    second = uring_api.probe()

    assert first == second


def test_probe_reports_requested_setup_flags():
    flags = uring_api.IORING_SETUP_SINGLE_ISSUER
    probe = uring_api.probe(flags=flags)

    if probe:
        assert probe["available"] is True


def test_ring_accepts_setup_flags_when_probe_accepts_them():
    require_uring()
    flags = uring_api.IORING_SETUP_SINGLE_ISSUER
    probe = uring_api.probe(flags=flags)
    if not probe:
        pytest.skip("setup flags are not accepted")

    with uring_api.Ring(entries=2, flags=flags) as ring:
        assert ring.sq_entries > 0
        assert ring.cq_entries > 0


def test_probe_rejects_invalid_entries():
    with pytest.raises(ValueError):
        uring_api.probe(0)