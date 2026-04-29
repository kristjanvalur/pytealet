import os
import subprocess

import pytest


os.environ.setdefault("PYTEALET_CHECK_STACK", "1")


def pytest_sessionstart(session):
	root = os.path.dirname(os.path.dirname(__file__))
	env = os.environ.copy()
	env.setdefault("BUILD_LIBTEALET_FROM_SOURCE", "1")
	env.setdefault("LIBTEALET_DEBUG", "1")
	env.setdefault("PYTEALET_EXT_DEBUG", "1")
	env.setdefault("CFLAGS", "-g -O0 -UNDEBUG")
	subprocess.run(
		["uv", "sync", "--active", "--reinstall-package", "tealet"],
		cwd=root,
		env=env,
		check=True,
	)


def pytest_configure(config):
	config.addinivalue_line("markers", "stub: tests that exercise currently-disabled stub functionality")
	config.addinivalue_line("markers", "greenlet_compat: tests that require greenlet compatibility layer")


def pytest_ignore_collect(collection_path, config):
	if os.environ.get("PYTEALET_ENABLE_GREENLET_TESTS") == "1":
		return False
	return collection_path.name == "test_greenlet.py"


def pytest_collection_modifyitems(config, items):
	if os.environ.get("PYTEALET_ENABLE_STUB_TESTS") == "1":
		enable_stub = True
	else:
		enable_stub = False
	enable_greenlet = os.environ.get("PYTEALET_ENABLE_GREENLET_TESTS") == "1"
	skip_stub = pytest.mark.skip(reason="temporary: stub functionality is disabled in current build")
	skip_greenlet = pytest.mark.skip(reason="temporary: greenlet emulation layer is disabled in current build")
	for item in items:
		if not enable_stub and "stub" in item.keywords:
			item.add_marker(skip_stub)
		if not enable_greenlet and item.nodeid.startswith("tests/test_greenlet.py"):
			item.add_marker(skip_greenlet)