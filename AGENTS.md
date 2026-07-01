# Agent Instructions for pytealet

## Project Overview

`pytealet` is a `uv` workspace publishing the core `tealet` package: a Python
wrapper for [libtealet](https://github.com/kristjanvalur/libtealet) that provides
efficient stack-slicing coroutines through a C extension.

Higher-level workspace packages build on core `tealet`:

- **`tealet`** (`src/`): low-level stack-switching primitives (`switch`, `run`,
  `throw`, thread ownership, runtime state). Intentionally not a scheduler or
  event loop.
- **`tealetio`** (`packages/tealetio/`): synchronous, asyncio-like runtime
  (schedulers, tasks, futures, locks, queues, selector helpers, asyncio
  coexistence).
- **`tealet-greenlet`** (`packages/tealet-greenlet/`): experimental greenlet
  emulation via tealet, including upstream-style compatibility tests.
- **`uring-api`** (`packages/uring_api/`): Linux `io_uring` experiment (ring
  probing, socket send/recv submission, completion waiting, callback-thread
  delivery).

Core intent:

- Keep `_tealet` runtime semantics correct and memory-safe.
- Keep `tealet` dependency-light; put schedulers and compatibility layers in
  workspace packages.
- Prioritise deterministic runtime behaviour over broad parity claims.

## Workspace Layout

```
pytealet/
├── AGENTS.md
├── Makefile
├── pyproject.toml          # workspace root; core tealet package metadata
├── uv.lock
├── src/
│   ├── tealet/             # core Python package
│   │   ├── __init__.py
│   │   ├── simple_scheduler.py
│   │   ├── include/pytealet_capi.h
│   │   └── greenlet/__init__.py   # transition wrapper -> tealet-greenlet
│   ├── _tealet/            # C extension sources
│   │   ├── pytealet.c, pytealet_module.c, pytealet_lineage.c, ...
│   │   └── libtealet/      # vendored release archive (currently 0.7.6)
│   ├── capi_client/        # C API client test helper
│   └── tealet_examples.py
├── tests/                  # core tealet tests only
├── docs/
│   ├── ARCHITECTURE.md
│   ├── C_API.md
│   ├── ISSUES.md
│   └── PYTHON_API.md
├── scripts/
│   └── fast_build.sh
└── packages/
    ├── tealetio/
    ├── tealet-greenlet/
    └── uring_api/
```

Greenlet compatibility tests live in
`packages/tealet-greenlet/tests/compat_greenlet/`, not under top-level
`tests/`. `src/tealet/greenlet/` is only a transition import wrapper.

## Development Environment

Preferred tooling is `uv`. This is a workspace: sync from the repo root.

### Standard dev setup

```bash
uv venv --python 3.13
source .venv/bin/activate
uv sync --active --dev
```

### Package-scoped sync

When working on a workspace package:

```bash
uv sync --active --locked --dev --package tealetio
uv sync --active --locked --dev --package tealet-greenlet
uv sync --active --locked --dev --package uring-api
```

### Debug CPython setup

When using a debug interpreter (`python3.xd`), prefer explicit `uv pip`:

```bash
uv venv --python /path/to/cpython-debug/python .venv-cpython313-debug
uv pip install --python .venv-cpython313-debug/bin/python -e . --group dev
```

When the debug venv is activated, prefer `uv --active` forms.

## Build Workflow

Recommended rapid rebuild of the core extension:

```bash
./scripts/fast_build.sh
./scripts/fast_build.sh debug
```

Or via Makefile:

```bash
make rebuild-ext
```

Manual rebuild if needed:

```bash
rm -rf build/ src/_tealet*.so
uv sync --active --reinstall-package tealet
```

Compile-only C extension checks:

```bash
make cext-cc
make cext-cc-debug
make cext-cc-ci
```

Format C sources (core `_tealet` and `uring_api`):

```bash
make format-c
make format-c-check
```

## Test Workflow

### Core `tealet`

```bash
uv run --active python -m pytest tests/
# or
make test
```

Targeted core suites (see `tests/README.md` for the split layout):

```bash
uv run --active python -m pytest tests/test_tealet_runtime.py -v
uv run --active python -m pytest tests/test_tealet_switching.py -v
```

### Workspace packages

```bash
uv run --active --package tealetio python -m pytest packages/tealetio/tests/ -v
uv run --active --package tealet-greenlet python -m pytest packages/tealet-greenlet/tests/ -v
uv run --active --package uring-api python -m pytest packages/uring_api/tests/ -v
```

`uring-api` requires Linux and `liburing-dev` (`sudo apt-get install -y
liburing-dev` on Ubuntu).

### Upstream greenlet compat (opt-in, expensive)

```bash
PYTEALET_RUN_UPSTREAM_GREENLET_TESTS=1 \
  uv run --active --package tealet-greenlet python -m pytest \
  packages/tealet-greenlet/tests/compat_greenlet -v
```

CI defaults: `PYTEALET_RUN_UPSTREAM_GREENLET_TESTS=0`,
`PYTEALET_SKIP_LONG_GREENLET_TESTS=1`, `PYTEALET_EXT_DEBUG=1`.

## Code Quality Workflow

Run from the workspace root:

```bash
make check    # ruff format --check, ruff check, ty check
make fix      # apply ruff format and autofixes
```

Direct equivalents:

```bash
uvx ruff format --check .
uvx ruff check .
uv run --all-packages --with ty ty check
```

Ruff uses 120-character lines. Test directories are excluded from Ruff; keep
installable source trees clean under `ruff check`. `ty` checks `src/` and
`packages/*/src/`, not test folders.

Before release tags, run `make check` and `uv lock --check`.

## CI Overview

- `.github/workflows/ci.yml`: core `tealet` quality (`make check`) and tests
  across Python 3.10–3.15 (including free-threaded `3.14t` / `3.15t`).
- `.github/workflows/tealetio-ci.yml`: `packages/tealetio/`
- `.github/workflows/tealet-greenlet-ci.yml`: `packages/tealet-greenlet/`
- `.github/workflows/uring-api-ci.yml`: `packages/uring_api/` (Linux + liburing)

Release tags trigger per-package publish workflows:

- `tealet`: `tealet-vX.Y.Z` or `vX.Y.Z`
- `tealetio`: `tealetio-vX.Y.Z`
- `tealet-greenlet`: `tealet-greenlet-vX.Y.Z`
- `uring-api`: `uring-api-vX.Y.Z`

Sibling packages should declare `tealet` compatibility ranges (for the current
`0.1` line: `tealet>=0.1.0rc2,<0.2`), not exact pins.

## Coding Guidelines

### Internal contracts in production code

Trust invariants your own code establishes. Do not litter production paths with
defensive checks whose only job is to confirm that internal state still matches
an invariant you control.

**User-facing validation is different.** Check arguments, feature combinations,
and resource state that callers can actually get wrong, and raise clear errors
for those cases.

**Do not add internal sanity checks** such as:

- `isinstance` solely to verify an object you constructed or stored yourself
- `if field is None: raise RuntimeError("... missing its ...")` for fields your
  setup code is responsible for populating
- Broad `try`/`except` wrappers whose main purpose is to re-raise a tidier
  message for logic bugs

Let internal contract violations surface as ordinary logic failures —
`AttributeError`, `TypeError`, `KeyError`, and similar — so bugs stay loud and
locate the broken assumption quickly. Tests may still use `isinstance`,
`assert`, or explicit guard checks to document API contracts.

Use `isinstance` when polymorphism is genuinely expected — for example,
branching across several concrete types that callers may pass in.

Debug-only `assert` statements are fine when they document invariants rather
than acting as user-facing validation.

### Runtime safety first

- Treat segfaults, aborts, and debug-assert crashes as highest priority.
- Do not hide runtime crashes by skipping tests unless explicitly requested.
- Prefer root-cause fixes over behavioural workarounds.

### C extension practices

- Preserve ownership and refcount invariants.
- Keep changes narrow and localised.
- Add tests for behaviour changes when practical.
- Use C89-style comments in C code.

### Vendored libtealet policy

- Do not modify `src/_tealet/libtealet/` unless explicitly asked.
- Treat vendored contents as external dependency snapshots.
- See `src/_tealet/README.md` for refresh instructions.

### Greenlet compatibility

- Implement greenlet behaviour in `packages/tealet-greenlet/`, not core `tealet`.
- Keep `tealet_greenlet` aligned with in-repo tests first.
- Use `packages/tealet-greenlet/tests/compat_greenlet/` for opt-in upstream
  parity tracking.
- Maintain explicit skip reasons for unsupported upstream scenarios.

### Package boundaries

- Keep core `tealet` free of scheduler/IO/asyncio dependencies.
- Put asyncio coexistence, selectors, and proactor experiments in `tealetio` or
  `uring-api` as appropriate.
- Do not expand scope into unrelated packages when fixing a single component.

## Documentation and Change Hygiene

- Update `docs/ARCHITECTURE.md` when core design or API contracts change.
- Update package docs under `packages/*/docs/` when package APIs change.
- Update `docs/ISSUES.md` for major resolved issues or active hardening work.
- Update package `CHANGELOG.md` and version bounds before release tags.
- Keep `README.md` focused on accurate setup and run guidance.

## Prose Style Guidelines

When writing documentation, changelog entries, docstrings, or comments, follow
this style:

### General Principles

- **Clear and technical**: write in a clear, direct style that assumes technical
  competence
- **Conversational yet precise**: friendly tone with technical accuracy
- **British English spelling**: prefer British spelling in documentation and
  user-facing prose, unless quoting APIs, filenames, third-party terms, or
  existing text
- **Explain motivation**: say why something is useful, not only what it does
- **Use examples liberally**: concrete code examples clarify abstract concepts
- **Emphasise with formatting**: use **bold** for emphasis, _italics_ for terms,
  and `backticks` for code

### Documentation Style (README, Guides)

- Lead with the problem or use case
- Use rhetorical questions where they help orientation
- Show before-and-after when introducing improvements
- Let code examples carry the narrative with brief explanatory text

### Changelog Style

- Structured, scannable section headers
- Lead with user-facing impact, then technical detail
- Name exact tools, versions, and what changed
- Explain reasoning and quantify impact when relevant

### Code Comments Style

- Lowercase, conversational comments that explain reasoning and tradeoffs
- Note alternative approaches when useful

### Docstrings Style

- Start with what the function or class does
- Brief first line; add detail only when behaviour is subtle
- Do not over-document obvious behaviour

### What to Avoid

- Passive voice and overly formal wording
- Redundant qualifiers
- Apologetic tone; be confident about design decisions

## References

- libtealet: https://github.com/kristjanvalur/libtealet
- greenlet: https://github.com/python-greenlet/greenlet
- stackman: https://github.com/stackless-dev/stackman