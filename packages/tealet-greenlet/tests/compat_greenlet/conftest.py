import os
from pathlib import Path

import pytest


def _env_flag(name):
    value = os.environ.get(name)
    if value is None:
        return None
    return value.strip().lower() in {"1", "true", "yes", "on"}


RUNNING_ON_CI = bool(
    os.environ.get("GITHUB_ACTIONS")
    or os.environ.get("TRAVIS")
    or os.environ.get("APPVEYOR")
    or os.environ.get("CI")
)

RUN_UPSTREAM = os.environ.get("PYTEALET_RUN_UPSTREAM_GREENLET_TESTS") == "1"

_skip_long_greenlet_tests = _env_flag("PYTEALET_SKIP_LONG_GREENLET_TESTS")
if _skip_long_greenlet_tests is None:
    _skip_long_greenlet_tests = RUNNING_ON_CI

LONG_RUNNING_TESTS = {
    "test_leaks.py::TestLeaks::test_untracked_memory_doesnt_increase_unfinished_thread_dealloc_in_main": (
        "Skipped long-running compat leak test in CI. "
        "Set PYTEALET_SKIP_LONG_GREENLET_TESTS=0 to run it."
    ),
}

SKIP_FILES = {
    "test_cpp.py": "Requires the upstream C++ test extension (_test_extension_cpp).",
    "test_extension_interface.py": "Requires the upstream C test extension (_test_extension).",
    "test_greenlet_trash.py": "Relies on CPython trashcan internals not implemented in pytealet.",
    "test_interpreter_shutdown.py": "Relies on greenlet shutdown semantics and subprocess coverage not yet supported.",
}


def _deps_available() -> bool:
    try:
        import psutil  # noqa: F401
        import objgraph  # noqa: F401
    except Exception:
        return False
    return True


def pytest_ignore_collect(collection_path, config):
    path = Path(collection_path)
    if "compat_greenlet" not in path.parts:
        return False
    if not RUN_UPSTREAM:
        return True
    if path.name in SKIP_FILES:
        return True
    if not _deps_available():
        return True
    return False


def pytest_collection_modifyitems(config, items):
    if not RUN_UPSTREAM:
        return
    skip_reasons = SKIP_FILES
    for item in items:
        name = Path(item.fspath).name
        reason = skip_reasons.get(name)
        if reason:
            item.add_marker(pytest.mark.skip(reason=reason))
        if _skip_long_greenlet_tests:
            for nodeid_suffix, long_reason in LONG_RUNNING_TESTS.items():
                if item.nodeid.endswith(nodeid_suffix):
                    item.add_marker(pytest.mark.skip(reason=long_reason))
                    break


@pytest.fixture(autouse=True)
def _sweep_tealet_threads_between_compat_tests():
    """Best-effort stale-thread cleanup around compat tests.

    This keeps background thread lineages from leaking between tests,
    especially for refcount-sensitive compat scenarios.
    """
    try:
        import _tealet
    except Exception:
        yield
        return

    _tealet.thread_sweep()
    yield
    _tealet.thread_sweep()
