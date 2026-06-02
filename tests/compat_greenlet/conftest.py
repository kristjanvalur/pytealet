import os
from pathlib import Path

import pytest

RUN_UPSTREAM = os.environ.get("PYTEALET_RUN_UPSTREAM_GREENLET_TESTS") == "1"

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
