---
name: uv
user-invocable: true
description: >-
  Use only for debug CPython builds or multiple venvs: uv venv --python, uv pip
  --python, ABI-check failures of uv sync, and choosing among .venv312/.venv313.
  Do not load for ordinary uv sync --active, uv run --active, or pytest — those
  commands are in root AGENTS.md.
---

# uv debug and multi-venv

Everyday `uv sync --active` / `uv run --active` / package pytest commands live
in root `AGENTS.md`. This skill is the extra recipe for debug interpreters and
more than one venv.

## Debug CPython

Debug builds can fail `uv sync` because of ABI checks. Prefer `uv pip` with an
explicit interpreter path:

```bash
uv venv --python /path/to/cpython-debug/python .venv-cpython313-debug
uv pip install --python .venv-cpython313-debug/bin/python -e . --group dev
source .venv-cpython313-debug/bin/activate
uv run --active python -m pytest tests/
```

`fast_build.sh` and `make rebuild-ext` work with debug venvs once the venv is
active. Prefer `uv --active` forms while it is.

## Multiple venvs

If the tree has more than one venv (for example `.venv312` and `.venv313`),
selection is ambiguous. Always be explicit:

- `--python` with the venv interpreter for installs
- `uv run --active` only when the intended venv is already activated

```bash
uv pip install --python .venv313/bin/python -e . --group dev

source .venv312/bin/activate
uv pip install test-dependency  # installs into the active .venv312
uv run --active python -m pytest tests/
```

`uv pip install` uses the active venv if one is activated; otherwise it picks
a venv from the current or parent directories.
