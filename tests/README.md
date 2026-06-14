# Test Suite Organization

This directory contains two broad test scopes:

- Pure pytealet tests (default, fast): files directly under `tests/`.
- Upstream greenlet compatibility tests (opt-in): `tests/compat_greenlet/`.

## Pure pytealet layout

- `test_tealet_runtime.py`: module-level behavior, lifecycle state, prepare/run basics, subclassing, and traversal APIs.
- `test_tealet_threading.py`: thread ownership, cross-thread restrictions, and lineage cleanup semantics.
- `test_tealet_context.py`: `contextvars` integration and cross-thread context access rules.
- `test_tealet_switching.py`: switch/throw/set_pending_exception semantics and panic/remote error handling.
- `test_tealet_frames_random.py`: frame introspection behavior and randomized stress flows.
- `_tealet_test_helpers.py`: shared helper constructors and utilities used by the split tealet tests.

Related pure-suite files remain scoped by feature:

- `test_tealet_capi_client.py`: C API client contract checks.
- `test_public_capi_headers.py`: public header exposure/install checks.
- `test_examples.py`: examples behavior checks.
- `test_greenlet_legacy.py`: legacy shim behavior checks.

## Running pure tests only

```bash
uv run --active python -m pytest \
  tests/test_tealet_runtime.py \
  tests/test_tealet_threading.py \
  tests/test_tealet_context.py \
  tests/test_tealet_switching.py \
  tests/test_tealet_frames_random.py \
  tests/test_tealet_capi_client.py \
  tests/test_public_capi_headers.py \
  tests/test_examples.py \
  tests/test_greenlet_legacy.py
```