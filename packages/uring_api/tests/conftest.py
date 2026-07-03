import pytest

import uring_api

# Mirror packages/uring_api/setup.py EXTENSION_C_COMPILE_ARGS.
EXTENSION_C_COMPILE_ARGS = [
    "-std=c17",
    "-pedantic-errors",
    "-Wall",
    "-Wno-unused-function",
]


def require_uring():
    probe = uring_api.probe()
    if not probe:
        pytest.skip("io_uring is not available")


def require_uring_capability(name: str) -> None:
    probe = uring_api.probe()
    if not probe:
        pytest.skip("io_uring is not available")
    if not probe.get(name, False):
        pytest.skip(f"{name} is not supported")
