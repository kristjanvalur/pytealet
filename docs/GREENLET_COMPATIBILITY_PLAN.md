# Greenlet Compatibility Status And Plan

## Goal

Keep `tealet.greenlet` useful for in-repo compatibility coverage while
incrementally improving parity with upstream greenlet behavior.

## Current State (June 2026)

- The shim lives in `src/tealet/greenlet/__init__.py`.
- Legacy compatibility tests live in `tests/test_greenlet_legacy.py`.
- Upstream-ported compatibility tests live in `tests/compat_greenlet/`.
- Upstream compat collection is opt-in and controlled by
  `PYTEALET_RUN_UPSTREAM_GREENLET_TESTS=1` in `tests/compat_greenlet/conftest.py`.
- CI keeps upstream compat disabled by default via
  `PYTEALET_RUN_UPSTREAM_GREENLET_TESTS=0` to keep runtime predictable.

## Working Policy

1. Keep core `_tealet` runtime correctness first.
2. Treat greenlet parity as an explicit compatibility layer concern.
3. Keep compat expectations explicit with targeted skips where behavior is
   intentionally different or currently unsupported.

## Known Gap Areas

- `greenlet._greenlet` surface is only partially emulated.
- `gr_context` behavior is incomplete compared to upstream.
- Generator/Genlet edge semantics differ in some destruction/error paths.
- Parent/dead state transitions still have mismatch cases.
- Leak-sensitive tests in compat suites remain a hardening target.
- C/C++ helper extension tests are skipped when helpers are unavailable.

## How To Run Compatibility Suites

Core legacy-style compat:

```bash
python -m pytest tests/test_greenlet_legacy.py -v
```

Upstream-ported compat subset (opt-in):

```bash
PYTEALET_RUN_UPSTREAM_GREENLET_TESTS=1 python -m pytest tests/compat_greenlet -v
```

## Near-Term Plan

1. Continue stabilizing `switch`/`throw` parity paths and parent/dead behavior.
2. Improve `gr_context` semantics where feasible without regressing core runtime.
3. Reduce leakcheck deltas with focused dealloc/refcount hardening.
4. Expand upstream compat coverage only when runtime and CI cost stay acceptable.
