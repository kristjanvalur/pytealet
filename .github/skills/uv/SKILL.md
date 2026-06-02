---
name: uv
user-invocable: true
description: "Use when: setting up or using uv venvs, running uv commands with active environments, or installing with uv pip for debug Python builds. Covers uv venv, uv run --active, and uv pip --python."
---

# uv Workflow Basics

## Scope

This skill captures common uv venv operations used in this repo, especially when
working with debug CPython builds.

## Core Commands

### Create a venv

```bash
uv venv --python 3.10
```

With a specific interpreter path (for example, a debug build):

```bash
uv venv --python /path/to/cpython-debug/python .venv-cpython310-debug
```

### Install project deps into a debug venv

Prefer uv pip with an explicit interpreter path for debug builds:

```bash
uv pip install --python .venv-cpython310-debug/bin/python -e .[dev]
```

### Use uv with an activated venv

When the venv is activated, use `--active` to avoid environment mismatch:

```bash
source .venv-cpython310-debug/bin/activate
uv run --active python -m pytest tests/
```

### Working with multiple venvs

If a project keeps multiple venvs (for example, `.venv312` and `.venv313`),
always be explicit about which environment to target:

- Prefer `--python` with the venv interpreter for installs.
- Prefer `uv run --active` when the intended venv is activated.

Examples:

```bash
uv pip install --python .venv313/bin/python -e .[dev]

source .venv312/bin/activate
uv pip install test-dependency  # Installs into active .venv312
uv run --active python -m pytest tests/
```

Note: `uv pip install` uses the active venv if one is activated; otherwise it
selects a venv from the current or parent directories. When multiple venvs
exist, this selection is ambiguous, so use `--python`.

## Notes

- Debug builds can fail `uv sync` because of ABI checks. When that happens,
  prefer `uv pip` with an explicit interpreter path, for example:

```bash
uv pip install --python .venv-cpython310-debug/bin/python -e .[dev]
```
- `fast_build.sh` works with debug venvs once the venv is active.
