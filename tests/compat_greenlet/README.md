# Upstream greenlet tests (vendored)

This folder contains a copy of the upstream greenlet test suite from
https://github.com/python-greenlet/greenlet/tree/master/src/greenlet/tests.

## Why these are here

We keep these tests to track compatibility gaps and to make it easy to run
and compare behavior when we intentionally match greenlet semantics.

## Default behavior

These tests are not collected by pytest unless you opt in:

```
PYTEALET_RUN_UPSTREAM_GREENLET_TESTS=1
```

If you enable them, you must also have `psutil` and `objgraph` installed or
collection will be skipped.

## Known skips

Even when enabled, a small set of files are skipped because they depend on
features or build steps that are not currently supported by pytealet:

- test_cpp.py: requires the upstream C++ test extension (_test_extension_cpp)
- test_extension_interface.py: requires the upstream C test extension (_test_extension)
- test_greenlet_trash.py: depends on CPython trashcan internals not implemented here
- test_interpreter_shutdown.py: relies on greenlet shutdown semantics and subprocess coverage not yet supported

## Notes

- The fail_*.py scripts are helpers used by some tests; they are kept
  verbatim to mirror upstream behavior.
- When compatibility gaps are closed, remove entries from the skip list and
  update this README accordingly.
