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


def pytest_collection_modifyitems(config, items):
	if os.environ.get("PYTEALET_ENABLE_STUB_TESTS") == "1":
		return
	skip_stub = pytest.mark.skip(reason="temporary: stub functionality is disabled in current build")
	for item in items:
		if "stub" in item.keywords:
			item.add_marker(skip_stub)