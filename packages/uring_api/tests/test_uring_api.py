import errno

import pytest

import uring_api


def test_probe_returns_structured_result():
    probe = uring_api.probe()

    assert isinstance(probe.available, bool)
    assert isinstance(probe.features, int)
    assert isinstance(probe.sq_entries, int)
    assert isinstance(probe.cq_entries, int)
    assert probe.liburing_version
    if probe.available:
        assert probe.errno is None
        assert probe.message is None
        assert probe.sq_entries > 0
        assert probe.cq_entries > 0
    else:
        assert probe.errno is not None
        assert probe.message


def test_ring_lifecycle_when_available():
    probe = uring_api.probe()
    if not probe.available:
        pytest.skip(f"io_uring is not available: errno={probe.errno} message={probe.message}")

    with uring_api.Ring() as ring:
        assert ring.fd >= 0
        assert ring.sq_entries > 0
        assert ring.cq_entries > 0
        assert not ring.closed

    assert ring.fd == -1
    assert ring.closed


def test_ring_rejects_invalid_entries():
    with pytest.raises(ValueError):
        uring_api.Ring(0)


def test_probe_rejects_invalid_entries():
    with pytest.raises(ValueError):
        uring_api.probe(0)


def test_ring_raises_oserror_or_initializes():
    try:
        ring = uring_api.Ring(2)
    except OSError as exc:
        assert exc.errno in {errno.ENOSYS, errno.EPERM, errno.EOPNOTSUPP, errno.ENOMEM, errno.EMFILE, errno.ENFILE}
    else:
        ring.close()