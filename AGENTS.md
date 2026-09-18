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

Greenlet compatibility tests live in
`packages/tealet-greenlet/tests/compat_greenlet/`, not under top-level
`tests/`. `src/tealet/greenlet/` is only a transition import wrapper.

## Package instructions (not auto-injected)

Root `AGENTS.md` is injected every turn. Package `AGENTS.md` files are **not**.
When editing a workspace package, **read that package's `AGENTS.md` first**:

- `packages/tealetio/AGENTS.md` — quality checks, large-file map, tealetio
  conventions
- `packages/uring_api/AGENTS.md` — build constraints, ring invariants, test
  policy

Do not dump `packages/tealetio/src/tealetio/proactor.py`,
`scheduler.py`, or `packages/tealetio/tests/test_proactor.py` in one read.
Grep, then `read_file` with `offset`/`limit`. The tealetio file map lives in
that package's `AGENTS.md`.

## Quality checks and pull requests

**Before opening or updating a PR**, from the workspace root:

```bash
make check    # ruff format --check, ruff check, ty check
# or fix first:
make fix
make check
```

Package tests can pass while `ruff`/`ty` still fail. Root CI runs `make check`
on every PR.

Ruff uses 120-character lines. Test directories are excluded from Ruff; keep
installable source trees clean under `ruff check`. `ty` checks `src/` and
`packages/*/src/`, not test folders. Typing is **gradual**: public APIs must
stay annotated for library users; private helpers may omit annotations (see
`docs/TYPING.md`). ty does not require every def to be fully typed.

Before release tags, run `make check` and `uv lock --check`.

### Pull-request workflow (do not load giant skills)

After `make check` is green:

- Create or update a PR with `gh pr create` / `gh pr edit` / ordinary git.
- Address review comments with `gh api` / `gh pr view --comments` and then
  edit the code. Do **not** invoke `/review` or read the bundled `review`
  skill unless the user asked for a review.
- Do **not** read `pr-babysit` or `long-running-background-tasks` unless the
  user asked to watch CI, poll a PR, or babysit a long job.

## Development Environment

Preferred tooling is `uv`. This is a workspace: sync from the repo root with
the intended venv activated.

```bash
source .venv/bin/activate
uv sync --active --dev
```

Package-scoped sync and tests:

```bash
uv sync --active --locked --dev --package tealetio   # or tealet-greenlet, uring-api
uv run --active --package tealetio python -m pytest packages/tealetio/tests/ -v
```

Everyday `uv sync --active` / `uv run --active` / pytest commands are listed
here. Load the **uv** skill only for **debug CPython builds** or **multiple
venvs** (ABI-check failures, `--python` vs `--active`).

## Build Workflow

Rapid rebuild of the core extension:

```bash
./scripts/fast_build.sh
./scripts/fast_build.sh debug
# or
make rebuild-ext
```

Compile-only C extension checks: `make cext-cc`, `make cext-cc-debug`,
`make cext-cc-ci`. Format C sources (core `_tealet` and `uring_api`):
`make format-c` / `make format-c-check`.

## Test Workflow

```bash
uv run --active python -m pytest tests/          # core tealet; or `make test`
uv run --active --package tealetio python -m pytest packages/tealetio/tests/ -v
uv run --active --package tealet-greenlet python -m pytest packages/tealet-greenlet/tests/ -v
uv run --active --package uring-api python -m pytest packages/uring_api/tests/ -v
```

`uring-api` requires Linux and `liburing-dev`. Upstream greenlet compat is
opt-in (`PYTEALET_RUN_UPSTREAM_GREENLET_TESTS=1`); CI leaves it off. See
`packages/tealet-greenlet/` and `tests/README.md` for the rest.

## CI

- `.github/workflows/ci.yml`: core `tealet` quality (`make check`) and tests
  across Python 3.10–3.15 (including free-threaded `3.14t` / `3.15t`).
- Package workflows: `tealetio-ci.yml`, `tealet-greenlet-ci.yml`,
  `uring-api-ci.yml` (Linux + liburing).
- Release tags: `tealet-vX.Y.Z` or `vX.Y.Z`, `tealetio-vX.Y.Z`,
  `tealet-greenlet-vX.Y.Z`, `uring-api-vX.Y.Z`.
- Sibling packages declare `tealet>=0.1.0rc2,<0.2`, not exact pins.

## Coding Guidelines

### Typing (public boundary vs internals)

Full policy: **`docs/TYPING.md`**.

- **Public API** (documented surface, no leading `_`, `__all__` exports): keep
  accurate type annotations so callers and editors see real types (`py.typed`).
- **Private / hot-path internals** (`_name`, ring SQ/CQ helpers, freelist
  plumbing): annotations are optional. Prefer simple untyped or lightly typed
  code over `typing.cast` gymnastics.
- **Do not** enable or assume mypy-style “all defs must be annotated” strictness;
  CI uses **ty**, which allows gradual typing by default.
- On hot paths, avoid runtime-expensive `cast(Complex[T] | Other[U], x)`. Use
  correct types, omit internal annotations, or `assert isinstance` for trust
  boundaries (see internal contracts below). Do not add cast-shaped helpers
  whose body is only `return x  # type: ignore`.

### Internal contracts in production code

Trust invariants your own code establishes. Do not litter production paths with
defensive checks whose only job is to confirm that internal state still matches
an invariant you control.

**Keep public calls light.** API contracts are not aggressively enforced at
every entry. Do not wrap methods with the same state guard (`if self.closed()`,
`_check_open()`, and similar) when a deeper layer already fails on misuse — a
closed `io_uring` ring, kernel `EBADF`, or an ordinary `TypeError` from the
real call. Use-after-close is not a recovery path; a tidy
`RuntimeError("… is closed")` at every API is not required.

Validate at the boundary that owns the resource, and only when the real call
would otherwise succeed silently or corrupt state. Feature combinations and
arguments with no deeper failure (unsupported flags, mutually exclusive
options) still deserve a clear error at the API that accepted them.

**Do not add internal sanity checks** such as:

- `isinstance` solely to verify an object you constructed or stored yourself
- `if field is None: raise RuntimeError("... missing its ...")` for fields your
  setup code is responsible for populating
- Broad `try`/`except` wrappers whose main purpose is to re-raise a tidier
  message for logic bugs

In Python, let internal contract violations surface as ordinary logic failures —
`AttributeError`, `TypeError`, `KeyError`, and similar — so bugs stay loud and
locate the broken assumption quickly. Tests may still use `isinstance`,
`assert`, or explicit guard checks to document API contracts.

Use `isinstance` when polymorphism is genuinely expected — for example,
branching across several concrete types that callers may pass in.

**Do not paper over internal bugs with user-facing exceptions.** Paths that
cannot fail unless our own code broke an invariant (for example a CQE missing
the `Completion` pointer we stored at submit time) are not caller errors. Do not
convert them into `SystemError`, `RuntimeError`, or similar — that suggests
recovery or external misuse when the real problem is an internal logic bug.

**`assert` is the right contract check** for structural invariants that cannot
fail unless our own code broke an assumption — in Python as well as C extension
code. It documents the logical structure of the program and fails loudly when
that structure is violated. Stripping under `python -O` is intentional: these
are not user-recoverable error paths.

Do not replace such asserts with `RuntimeError`, `PyErr_SetString`, or similar
guards just to survive optimised builds with a tidier exception. Review comments
that warn “assert does not work in production” miss the point; ignore or correct
those suggestions rather than papering over internal bugs with user-facing
exceptions.

### Python boolean style

Avoid `bool(object)` in contexts that already apply implicit truthiness — `if`,
`while`, `and`/`or`, and ternary conditions.

Prefer direct checks: `if items:` not `if bool(items):`. Redundant `bool()` adds
noise without changing behaviour.

Be careful with `and`/`or`: they return the deciding operand, not necessarily
`True`/`False`. An expression like `self._heap and self._heap[0][0] == index` can
return the heap list instead of a boolean. Use an explicit `if` when you need a
real boolean result:

```python
# prefer
if self._heap:
    return self._heap[0][0] == self._next_index
return False

# not
return bool(self._heap) and self._heap[0][0] == self._next_index
```

Reserve explicit `bool()` for APIs that require a `bool` return value (for example
`__bool__`) or when converting for storage or passing to a typed parameter.

### Runtime safety first

- Treat segfaults, aborts, and debug-assert crashes as highest priority.
- Do not hide runtime crashes by skipping tests unless explicitly requested.
- Prefer root-cause fixes over behavioural workarounds.

### C extension practices

- Preserve ownership and refcount invariants.
- Keep changes narrow and localised.
- Add tests for behaviour changes when practical.
- Use C89-style comments in C code.
- Use `assert` for internal invariants that cannot fail unless our own submit,
  drain, or refcount logic is wrong. Reserve `PyErr_SetString` for argument,
  feature, and resource errors callers can actually trigger.

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
- Update `docs/TYPING.md` when the public-vs-internal typing policy changes.
- Update package docs under `packages/*/docs/` when package APIs change.
- Update `docs/ISSUES.md` for major resolved issues or active hardening work.
- Update package `CHANGELOG.md` and version bounds before release tags.
- Keep `README.md` focused on accurate setup and run guidance.

Comments: lowercase, explain reasoning and tradeoffs. Docstrings: what the
function or class does; first line brief; do not over-document the obvious.
British English in user-facing strings (except APIs, filenames, third-party
terms).

When writing README, `PYTHON_API.md`, guides, `CHANGELOG.md`, `ROADMAP.md`, or
internal design docs, load the **prose** skill. Do not load it for ordinary
code comments, docstrings, `AGENTS.md`, or code-only PRs.

## References

- libtealet: https://github.com/kristjanvalur/libtealet
- greenlet: https://github.com/python-greenlet/greenlet
- stackman: https://github.com/stackless-dev/stackman
