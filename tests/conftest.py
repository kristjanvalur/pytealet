import os
import subprocess

import pytest


os.environ.setdefault("PYTEALET_CHECK_STACK", "1")


def _env_flag(name):
	value = os.environ.get(name)
	if value is None:
		return None
	return value.strip().lower() in {"1", "true", "yes", "on"}


def _configure_greenlet_stub_from_env():
	use_stub = _env_flag("PYTEALET_GREENLET_USE_STUB")
	if use_stub is None:
		return

	import tealet.greenlet as greenlet_shim
	greenlet_shim.set_stub(use_stub)


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
	skip_rebuild = (
		session.config.getoption("--skip-ext-rebuild")
		or os.environ.get("PYTEALET_SKIP_EXT_REBUILD") == "1"
	)
	do_rebuild = (
		session.config.getoption("--rebuild-ext")
		or os.environ.get("PYTEALET_REBUILD_EXT") == "1"
	)

	if not skip_rebuild and do_rebuild:
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

	# Optional shim behavior toggle: when enabled, initialize a thread-local
	# stub template on the main test thread and keep it for the session.
	_configure_greenlet_stub_from_env()


def pytest_configure(config):
	config.addinivalue_line("markers", "stub: tests that exercise stub functionality")
	config.addinivalue_line("markers", "greenlet_compat: tests that require greenlet compatibility layer")