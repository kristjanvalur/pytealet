# Greenlet Compatibility Plan

## Goal

Re-establish and verify the `tealet.greenlet` compatibility layer in two stages:

1. Make legacy compatibility tests run on modern Python.
2. Move toward parity with modern upstream greenlet behavior.

## Intent And Rationale

The intended architecture is intentionally layered:

- `pytealet` itself stays minimal and focused on core tealet mechanics.
- A greenlet-like interface should be buildable on top as a compatibility layer.

This enables a practical "greenlet monkeypatch" proof-of-concept:

- Provide a greenlet-compatible module surface that can be substituted for
  libraries expecting greenlet semantics.
- Validate whether tealet can serve as a drop-in backend for those libraries,
  either as an experiment or as a potential performance-oriented variant.

In short: keep core primitives small and robust, and make greenlet
compatibility an explicit higher-level adapter.

## Current State

- Compatibility shim exists in `src/tealet/greenlet.py` but contains Python 2 syntax.
- Legacy greenlet tests exist in `tests/test_greenlet.py`.
- Greenlet tests are disabled by default via `PYTEALET_ENABLE_GREENLET_TESTS` in `tests/conftest.py`.
- Existing tests import `greenlet` directly, so they need to target the tealet shim explicitly for compatibility-layer validation.

## Phase 1: Make Legacy Tests Work On Modern Python

### 1. Port shim to Python 3 syntax

- Replace Python 2 exception raising forms.
- Add `__bool__` (keep `__nonzero__` alias for intent).
- Keep behavior unchanged except for syntax/runtime compatibility.

### 2. Point legacy tests at the tealet shim

- In `tests/test_greenlet.py`, import from `tealet.greenlet` instead of external `greenlet` package.
- Keep test semantics the same.

### 3. Establish a baseline

- Run `PYTEALET_ENABLE_GREENLET_TESTS=1 pytest tests/test_greenlet.py`.
- Record failures by category:
  - API mismatch
  - exception propagation
  - parent/dead semantics
  - frame semantics
  - cross-thread behavior

## Phase 2: Incremental Behavior Fixes

Prioritized order:

1. `switch`/`throw` exception triplet handling and return-value normalization.
2. Parent chain and dead/live truthiness semantics.
3. `gr_frame` behavior.
4. Cross-thread safety behavior and error typing.
5. Exception state preservation.

For each mismatch:

- Fix shim logic.
- Add or adjust tests.
- Avoid broad refactors unless necessary.

## Phase 3: Modern Upstream Greenlet Test Adoption

After legacy suite is stable:

1. Import a curated subset of modern upstream greenlet tests into a dedicated area (for example `tests/compat_greenlet/`).
2. Keep explicit skip/xfail reasons for known differences.
3. Expand subset incrementally as parity improves.

## CI Plan

1. Add a dedicated greenlet-compat job (separate from core tealet tests).
2. Enable with `PYTEALET_ENABLE_GREENLET_TESTS=1`.
3. Start with one Python version (3.14 or 3.15), then expand.
4. Optionally add free-threaded matrix entries once baseline behavior is stable.

## Immediate Next Steps

1. Complete Python 3 shim port.
2. Rewire legacy tests to use `tealet.greenlet`.
3. Run legacy suite and categorize first failure set.