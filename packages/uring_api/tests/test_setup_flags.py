import errno
import fcntl
import gc
import importlib.util
import mmap
import os
import select
import shlex
import shutil
import socket
import subprocess
import sys
import sysconfig
import tempfile
import threading
import time
import weakref
from importlib import resources
from pathlib import Path

import pytest

import _uring_api
import uring_api

from helpers import (
    assert_fd_nonblocking_cloexec,
    build_c_api_client,
    collect_until_stable,
    connect_to_listener,
    connected_tcp_pair,
    kernel_version_at_least,
    oversized_file_buffer,
    require_setup_flags,
    wait_until_running,
)
from conftest import require_uring, require_uring_capability

def test_single_issuer_allows_submit_and_wait_from_one_thread():
    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=uring_api.IORING_SETUP_SINGLE_ISSUER) as ring:
            recv_buf = bytearray(8)
            ring.submit_recv(reader.fileno(), recv_buf)
            writer.send(b"x")
            completion = ring.wait(1.0)
            assert completion is not None
            assert completion.res == 1
            assert bytes(recv_buf[:1]) == b"x"
    finally:
        reader.close()
        writer.close()

def test_single_issuer_rejects_cross_thread_submit():
    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=uring_api.IORING_SETUP_SINGLE_ISSUER) as ring:
            ring.submit_recv(reader.fileno(), bytearray(8))
            errors: list[RuntimeError] = []

            def submit_from_other_thread():
                try:
                    ring.submit_recv(reader.fileno(), bytearray(8))
                except RuntimeError as exc:
                    errors.append(exc)

            thread = threading.Thread(target=submit_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert len(errors) == 1
            assert "IORING_SETUP_SINGLE_ISSUER" in str(errors[0])
    finally:
        reader.close()
        writer.close()

def test_defer_taskrun_allows_wait_from_one_thread():
    flags = uring_api.IORING_SETUP_SINGLE_ISSUER | uring_api.IORING_SETUP_DEFER_TASKRUN
    require_setup_flags(flags)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=flags) as ring:
            recv_buf = bytearray(8)
            ring.submit_recv(reader.fileno(), recv_buf)
            writer.send(b"x")
            completion = ring.wait(1.0)
            assert completion is not None
            assert completion.res == 1
            assert bytes(recv_buf[:1]) == b"x"
    finally:
        reader.close()
        writer.close()

def test_defer_taskrun_rejects_cross_thread_wait():
    flags = uring_api.IORING_SETUP_SINGLE_ISSUER | uring_api.IORING_SETUP_DEFER_TASKRUN
    require_setup_flags(flags)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=flags) as ring:
            ring.submit_recv(reader.fileno(), bytearray(8))
            assert ring.wait(0) is None
            errors: list[RuntimeError] = []

            def wait_from_other_thread():
                try:
                    ring.wait(0)
                except RuntimeError as exc:
                    errors.append(exc)

            thread = threading.Thread(target=wait_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert len(errors) == 1
            assert "IORING_SETUP_DEFER_TASKRUN" in str(errors[0])
    finally:
        reader.close()
        writer.close()

def test_defer_taskrun_rejects_cross_thread_submit():
    flags = uring_api.IORING_SETUP_SINGLE_ISSUER | uring_api.IORING_SETUP_DEFER_TASKRUN
    require_setup_flags(flags)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=flags) as ring:
            ring.submit_recv(reader.fileno(), bytearray(8))
            errors: list[RuntimeError] = []

            def submit_from_other_thread():
                try:
                    ring.submit_recv(reader.fileno(), bytearray(8))
                except RuntimeError as exc:
                    errors.append(exc)

            thread = threading.Thread(target=submit_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert len(errors) == 1
            assert "IORING_SETUP_DEFER_TASKRUN" in str(errors[0])
    finally:
        reader.close()
        writer.close()

def test_single_issuer_allows_cross_thread_wait():
    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=uring_api.IORING_SETUP_SINGLE_ISSUER) as ring:
            ring.submit_recv(reader.fileno(), bytearray(8))
            writer.send(b"x")
            results: list[object] = []

            def wait_from_other_thread():
                results.append(ring.wait(1.0))

            thread = threading.Thread(target=wait_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert len(results) == 1
            completion = results[0]
            assert completion is not None
            assert completion.res == 1
    finally:
        reader.close()
        writer.close()

def test_single_issuer_allows_break_wait_from_owner_thread():
    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=uring_api.IORING_SETUP_SINGLE_ISSUER) as ring:
            ring.submit_recv(reader.fileno(), bytearray(8))
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
    finally:
        reader.close()
        writer.close()

def test_single_issuer_rejects_cross_thread_break_wait():
    require_setup_flags(uring_api.IORING_SETUP_SINGLE_ISSUER)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=uring_api.IORING_SETUP_SINGLE_ISSUER) as ring:
            ring.submit_recv(reader.fileno(), bytearray(8))
            errors: list[RuntimeError] = []

            def break_from_other_thread():
                try:
                    ring.break_wait()
                except RuntimeError as exc:
                    errors.append(exc)

            thread = threading.Thread(target=break_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert len(errors) == 1
            assert "IORING_SETUP_SINGLE_ISSUER" in str(errors[0])
    finally:
        reader.close()
        writer.close()

def test_defer_taskrun_rejects_cross_thread_serve_completions():
    flags = uring_api.IORING_SETUP_SINGLE_ISSUER | uring_api.IORING_SETUP_DEFER_TASKRUN
    require_setup_flags(flags)
    reader, writer = connected_tcp_pair()
    try:
        with uring_api.Ring(entries=4, flags=flags) as ring:
            ring.callback = lambda completion: None
            ring.submit_recv(reader.fileno(), bytearray(8))
            errors: list[RuntimeError] = []

            def serve_from_other_thread():
                try:
                    ring.serve_completions()
                except RuntimeError as exc:
                    errors.append(exc)

            thread = threading.Thread(target=serve_from_other_thread)
            thread.start()
            thread.join(1.0)
            assert thread.is_alive() is False
            assert len(errors) == 1
            assert "IORING_SETUP_DEFER_TASKRUN" in str(errors[0])
    finally:
        reader.close()
        writer.close()

