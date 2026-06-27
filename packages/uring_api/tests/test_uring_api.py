import errno
from importlib import resources
import socket
import threading

import pytest

import uring_api


def test_package_is_marked_as_typed():
    assert resources.files("uring_api").joinpath("py.typed").is_file()


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


def test_ring_recv_completion_when_available():
    probe = uring_api.probe()
    if not probe.available:
        pytest.skip(f"io_uring is not available: errno={probe.errno} message={probe.message}")

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            ring.submit_recv(reader.fileno(), 5, 123)
            writer.send(b"hello")

            completion = ring.wait(1.0)

        assert completion is not None
        assert completion["user_data"] == 123
        assert completion["res"] == 5
        assert completion["result"] == b"hello"
    finally:
        reader.close()
        writer.close()


def test_ring_send_completion_when_available():
    probe = uring_api.probe()
    if not probe.available:
        pytest.skip(f"io_uring is not available: errno={probe.errno} message={probe.message}")

    reader, writer = socket.socketpair()
    try:
        reader.setblocking(False)
        writer.setblocking(False)
        with uring_api.Ring() as ring:
            ring.submit_send(writer.fileno(), b"hello", 124)

            completion = ring.wait(1.0)

        assert completion is not None
        assert completion["user_data"] == 124
        assert completion["res"] == 5
        assert completion["result"] == 5
        assert reader.recv(5) == b"hello"
    finally:
        reader.close()
        writer.close()


def test_ring_break_wait_interrupts_wait_when_available():
    probe = uring_api.probe()
    if not probe.available:
        pytest.skip(f"io_uring is not available: errno={probe.errno} message={probe.message}")

    with uring_api.Ring() as ring:
        results: list[object] = []
        thread = threading.Thread(target=lambda: results.append(ring.wait(10.0)))
        thread.start()
        ring.break_wait()
        thread.join(1.0)
        if thread.is_alive():
            ring.break_wait()
            thread.join(1.0)

    assert thread.is_alive() is False
    assert results == [None]
