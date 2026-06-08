# GitHub Copilot Instructions for pytealet

## Project Overview

`pytealet` is a Python wrapper for
[libtealet](https://github.com/kristjanvalur/libtealet), providing efficient
stack-slicing coroutines to Python through a C extension.

Core intent:
- Keep `_tealet` runtime semantics correct and memory-safe.
- Provide a practical greenlet-compatible shim in `tealet.greenlet`.
- Prioritize deterministic runtime behavior over broad parity claims.

## Current Project Structure

```
pytealet/
├── src/
│   ├── tealet/
│   │   ├── __init__.py
│   │   └── greenlet/
│   │       ├── __init__.py
│   │       └── _greenlet.py
│   ├── _tealet/
│   │   ├── pytealet.c
│   │   ├── pytealet_module.c
│   │   ├── tstate_state.c
│   │   ├── frame_info.c
│   │   ├── pytealet_common.h
│   │   ├── pytealet.h
│   │   ├── pytealet_module.h
│   │   └── libtealet/    # vendored release archive (currently 0.7.5)
│   └── tealet_examples.py
├── tests/
│   ├── test_tealet.py
│   ├── test_greenlet_legacy.py
│   ├── test_examples.py
│   └── compat_greenlet/
├── docs/
│   ├── ARCHITECTURE.md
│   ├── ISSUES.md
│   └── GREENLET_COMPATIBILITY_PLAN.md
├── scripts/
│   └── fast_build.sh
├── pyproject.toml
├── setup.py
└── README.md
```

## Development Environment

Preferred tooling is `uv`.

### Standard dev setup

```bash
uv venv --python 3.13
source .venv/bin/activate
uv sync --active --dev
```

### Debug CPython setup

When using a debug interpreter (`python3.xd`), prefer explicit `uv pip`:

```bash
uv venv --python /path/to/cpython-debug/python .venv-cpython313-debug
uv pip install --python .venv-cpython313-debug/bin/python -e .[dev]
```

When the debug venv is activated, prefer `uv --active` forms.

## Build Workflow

Recommended rapid rebuild:

```bash
./scripts/fast_build.sh
./scripts/fast_build.sh debug
```

Manual rebuild if needed:

```bash
rm -rf build/ src/_tealet*.so
uv sync --active --reinstall-package tealet
```

## Test Workflow

Core suite:

```bash
uv run --active python -m pytest tests/
```

Targeted examples:

```bash
uv run --active python -m pytest tests/test_tealet.py -v
uv run --active python -m pytest tests/test_greenlet_legacy.py -v
```

Upstream compat tests are opt-in and can be expensive:

```bash
PYTEALET_RUN_UPSTREAM_GREENLET_TESTS=1 \
  uv run --active python -m pytest tests/compat_greenlet -v
```

CI defaults to `PYTEALET_RUN_UPSTREAM_GREENLET_TESTS=0`.

## Current Status Snapshot

- Core in-repo test baseline is healthy.
- `_tealet` has active hardening around GC traversal/clear semantics.
- Greenlet compatibility is functional for in-repo coverage, but upstream
  parity is still an ongoing effort.

## Coding Guidelines

### Runtime safety first

- Treat segfaults, aborts, and debug-assert crashes as highest priority.
- Do not hide runtime crashes by skipping tests unless explicitly requested.
- Prefer root-cause fixes over behavioral workarounds.

### C extension practices

- Preserve ownership and refcount invariants.
- Keep changes narrow and localized.
- Add tests for behavior changes when practical.
- Use C89-style comments in C code.

### Vendored libtealet policy

- Do not modify `src/_tealet/libtealet/` unless explicitly asked.
- Treat vendored contents as external dependency snapshots.

## Greenlet Compatibility Guidance

- Keep `tealet.greenlet` behavior aligned with tests in this repo first.
- Use `tests/compat_greenlet/` as opt-in parity tracking.
- Maintain explicit skip reasons for unsupported upstream scenarios.

## Documentation and Change Hygiene

- Update `docs/ARCHITECTURE.md` when design or API contracts change.
- Update `docs/ISSUES.md` for major resolved issues or active hardening work.
- Keep `README.md` focused on accurate setup/run guidance.

## References

- libtealet: https://github.com/kristjanvalur/libtealet
- greenlet: https://github.com/python-greenlet/greenlet
- stackman: https://github.com/stackless-dev/stackman
