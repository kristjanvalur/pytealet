# tealetio Split Feasibility

## Summary

Splitting the scheduler and asyncio compatibility layer into a separate package is feasible.

The experiment on `experiment/tealetio-workspace-split` introduces a uv workspace with:

- root package: `tealet`, retaining the `_tealet` C extension, greenlet shim, headers, and low-level runtime API.
- member package: `tealetio`, owning scheduler, task/future, locks, runner, selector, asyncio coexistence, and compatibility helpers.

The old `tealet.scheduler`, `tealet.tasks`, `tealet.locks`, `tealet.runner`, `tealet.selector`, `tealet.asyncio`, and `tealet.compat` modules remain as compatibility shims that alias the corresponding `tealetio` modules through `sys.modules`.

## uv Workspace Shape

uv workspaces are configured from the root `pyproject.toml`:

```toml
[tool.uv.workspace]
members = ["packages/*"]

[tool.uv.sources]
tealetio = { workspace = true }
```

Each workspace member needs its own `pyproject.toml`. Inter-package dependencies are declared normally in `[project.dependencies]`, then resolved locally with `[tool.uv.sources]`:

```toml
[project]
name = "tealetio"
dependencies = [
    "asynkit>=0.17.7",
    "tealet==0.1.0rc1",
]

[tool.uv.sources]
tealet = { workspace = true }
```

Important uv constraints:

- The workspace shares one lockfile and one dependency resolution.
- `uv run --package tealetio ...` works from the workspace root.
- Workspace members share a compatible `requires-python` intersection.
- uv cannot enforce runtime import isolation between members, so tests/package metadata still matter.

## Package Name Check

PyPI checks performed on 2026-06-23:

- `syncio`: taken, latest `0.0.4`.
- `tealetio`: available on PyPI, 404.
- `tealet-io`: available on PyPI, 404.
- `tealetio-runtime`: available on PyPI, 404.
- `tealet-scheduler`: available on PyPI, 404.
- `pytealet-io`: available on PyPI, 404.
- `pytealet-scheduler`: available on PyPI, 404.
- `tealet-asyncio`: available on PyPI, 404.
- `tealio`: available on PyPI, 404.
- `tealet-syncio`: available on PyPI, 404.

Recommendation: use `tealetio`.

Rationale:

- `syncio` is unavailable and too generic.
- `tealetio` clearly ties the package to `tealet` while signalling I/O/event-loop scope.
- The import name and distribution name can match (`import tealetio`).
- It leaves room for scheduler, selector, and asyncio features without overfitting to only asyncio.

## Prototype Result

Validated successfully:

```text
uv workspace list
# tealet
# tealetio

uv run --active python -m pytest tests/test_scheduler.py tests/test_runtime_runner.py tests/test_scheduler_crash_repros.py tests/test_examples.py -q
# 330 passed

uv run --active --package tealetio python -c "from tealetio.runner import run; print(run(lambda: 'ok'))"
# ok

uv build --package tealetio
# Successfully built dist/tealetio-0.1.0rc1.tar.gz
# Successfully built dist/tealetio-0.1.0rc1-py3-none-any.whl
```

The build emits setuptools deprecation warnings for the license classifier, matching current project style; this is not specific to the split.

## Boundary Assessment

The dependency direction is clean:

- `tealetio` imports `tealet` / `_tealet` for core tealet objects and native runtime behavior.
- Core `tealet` does not need scheduler internals except for backwards-compatible import shims.
- `asynkit` can move from `tealet` extras/dev dependencies into `tealetio` proper.

The moved implementation size is approximately:

- runtime modules: about 3.5k lines.
- scheduler/runtime tests: about 5k lines.

## Compatibility Notes

Compatibility shims should alias modules, not copy globals:

```python
import sys
from tealetio import scheduler as _scheduler
sys.modules[__name__] = _scheduler
```

A first `globals().update(...)` shim allowed reads but failed tests that monkeypatch private module globals such as `_CoroStart`, because writes landed on the shim rather than the implementation module.

## Suggested Migration Plan

1. Publish/reserve `tealetio` if this split is accepted.
2. Keep `tealet[asyncio]` depending on `tealetio` for one transition cycle.
3. Keep the old `tealet.*` scheduler module paths as module-alias compatibility shims.
4. Move scheduler docs and tests toward `tealetio.*` imports while leaving explicit compatibility tests for old imports.
5. Decide whether examples should live in root `tealet`, in `tealetio`, or in a workspace-level examples package.
6. Split CI packaging jobs so `tealet` still builds native wheels and `tealetio` builds pure-Python wheels/sdists.

## Open Questions

- Should `tealet` have no runtime dependency on `tealetio` by default, with only `tealet[asyncio]` pulling it in?
- Should `tealetio` version-lock to the same release as `tealet`, or use a compatible range such as `tealet>=0.1.0rc1,<0.2`?
- Should compatibility shims warn, or remain silent until after the experimental period?
- Should `tealetio` be pure Python forever, or eventually own any optional C helpers for scheduler performance?
