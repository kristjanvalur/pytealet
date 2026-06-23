# tealetio

Experimental scheduler, synchronization primitives, selector integration, and asyncio coexistence helpers for `tealet`.

This package is a feasibility spike for splitting the scheduler layer out of the core `tealet` package. It depends on `tealet` for the native `_tealet` runtime and keeps development in the same repository through a uv workspace.

```python
from tealetio import Scheduler, run
```

Most scheduler, task/future, lock, selector, runner, and asyncio bridge classes
and helpers are re-exported from the top-level `tealetio` package, similar to
`asyncio`. The submodules remain available as the implementation homes for more
specific imports.

## Documentation

- [docs/PYTHON_API.md](docs/PYTHON_API.md) describes the `tealetio` Python API.
- [docs/ASYNCIO_COEXISTENCE.md](docs/ASYNCIO_COEXISTENCE.md) documents asyncio coexistence design.
- [docs/SCHEDULER_RUNTIME_API_SPEC.md](docs/SCHEDULER_RUNTIME_API_SPEC.md) tracks the scheduler runtime API design.
- [docs/TEALETIO_SPLIT_FEASIBILITY.md](docs/TEALETIO_SPLIT_FEASIBILITY.md) records the workspace split assessment.
