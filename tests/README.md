# Test Suite Organization

This directory contains the core pytealet tests. Greenlet compatibility tests now live in `packages/tealet-greenlet/tests/`.

## Pure pytealet layout

- `test_tealet_runtime.py`: module-level behavior, lifecycle state, prime/run basics, subclassing, and traversal APIs.
- `test_tealet_threading.py`: thread ownership, cross-thread restrictions, and lineage cleanup semantics.
- `test_tealet_context.py`: `contextvars` integration and cross-thread context access rules.
- `test_tealet_switching.py`: switch/throw/set_pending_exception semantics and panic/remote error handling.
- `test_tealet_tracing.py`: `_tealet.settrace()` / `gettrace()` switch and throw callbacks.
- `test_tealet_profile.py`: `tealet.profile.Profile` per-tealet stacks.
- `test_tealet_cprofile.py`: `tealet.cprofile.Profile` (Python 3.12+).
- `test_tealet_frames_random.py`: frame introspection behavior and randomized stress flows.
- `test_tealet_stub_workload.py`: seeded recursion/create/switch/exit workload comparing in-place creation vs cloning from a fixed stub. See [Stub vs in-place workload](#stub-vs-in-place-workload).
- `test_tealet_vs_greenlet_workload.py`: the same seeded workload comparing core tealet with PyPI greenlet. See [Tealet vs greenlet workload](#tealet-vs-greenlet-workload).
- `_tealet_test_helpers.py`: shared helper constructors and utilities used by the split tealet tests.

Related pure-suite files remain scoped by feature:

- `test_tealet_capi_client.py`: C API client contract checks.
- `test_public_capi_headers.py`: public header exposure/install checks.
- `test_examples.py`: examples behavior checks.

## Running pure tests only

```bash
uv run --active python -m pytest \
  tests/test_tealet_runtime.py \
  tests/test_tealet_threading.py \
  tests/test_tealet_context.py \
  tests/test_tealet_switching.py \
  tests/test_tealet_tracing.py \
  tests/test_tealet_profile.py \
  tests/test_tealet_cprofile.py \
  tests/test_tealet_frames_random.py \
  tests/test_tealet_stub_workload.py \
  tests/test_tealet_vs_greenlet_workload.py \
  tests/test_tealet_capi_client.py \
  tests/test_public_capi_headers.py \
  tests/test_examples.py
```

## Stub vs in-place workload

`test_tealet_stub_workload.py` is a seeded create/recurse/switch/exit
workload, modelled on libtealet's `tests/test_stochastic.c --compare`.
Pytest runs a short correctness check (both modes complete; the same seed
produces the same event counts). Timing and RSS comparison is opt-in and
runs each mode in a child process:

```bash
python tests/test_tealet_stub_workload.py --compare -n 20000
python tests/test_tealet_stub_workload.py --compare -n 20000 --stub-depth 20
```

Cloning from a stub typically uses less memory than creating each tealet in
place, because children share the template's stack base and their saved
stacks overlap as they recurse. Switch time in Python is usually dominated
by interpreter overhead; results depend on the workload.

## Tealet vs greenlet workload

`test_tealet_vs_greenlet_workload.py` runs the same create/recurse/switch/exit
loop against core tealet and against the PyPI [greenlet](https://pypi.org/project/greenlet/)
package. Pytest is a short correctness check (both backends complete; the same
seed produces the same event counts). Greenlet tests are skipped unless the
optional `greenlet` dependency group is installed and `import greenlet` is the
C extension, not the `tealet-greenlet` shim.

```bash
uv sync --active --dev --group greenlet
python tests/test_tealet_vs_greenlet_workload.py --compare -n 20000
```

If `tealet-greenlet` is also installed in the environment, its `greenlet`
package shadows PyPI greenlet. Use an isolated run that only has core tealet
and the optional group:

```bash
uv run --isolated --group greenlet python tests/test_tealet_vs_greenlet_workload.py --compare -n 20000
```

This is a comparison of stack-slicing implementations, not of the
`tealet-greenlet` compatibility layer. Extra RSS is typically lower for
tealet than for greenlet on this mixed workload. Wall time is close;
switch time in Python is usually dominated by interpreter overhead.
