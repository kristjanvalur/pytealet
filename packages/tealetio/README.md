# tealetio

Experimental scheduler, synchronization primitives, selector integration, and asyncio coexistence helpers for `tealet`.

This package is a feasibility spike for splitting the scheduler layer out of the core `tealet` package. It depends on `tealet` for the native `_tealet` runtime and keeps development in the same repository through a uv workspace.

```python
from tealetio.runner import run
from tealetio.scheduler import Scheduler
```

During a transition, the core `tealet.scheduler`, `tealet.tasks`, `tealet.locks`, `tealet.runner`, `tealet.selector`, `tealet.asyncio`, and `tealet.compat` modules can remain as compatibility shims that re-export `tealetio`.
