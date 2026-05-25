import os
import subprocess

import pytest


os.environ.setdefault("PYTEALET_CHECK_STACK", "1")


def pytest_addoption(parser):
	parser.addoption(
		"--rebuild-ext",
		action="store_true",
		default=False,
		help="Rebuild extension via uv sync --reinstall-package tealet at session start.",
	)
	parser.addoption(
		"--skip-ext-rebuild",
		action="store_true",
		default=False,
		help="Deprecated alias. Rebuild is skipped by default.",
	)


def pytest_sessionstart(session):
	if session.config.getoption("--skip-ext-rebuild") or os.environ.get("PYTEALET_SKIP_EXT_REBUILD") == "1":
		return

	if not (
		session.config.getoption("--rebuild-ext")
		or os.environ.get("PYTEALET_REBUILD_EXT") == "1"
	):
		return

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
	config.addinivalue_line("markers", "stub: tests that exercise stub functionality")
	config.addinivalue_line("markers", "greenlet_compat: tests that require greenlet compatibility layer")