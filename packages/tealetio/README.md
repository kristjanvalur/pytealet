# tealetio

Did you ever wish `tealet` had the familiar scheduling tools you reach for in
`asyncio`, but for stack-slicing tealet code? `tealetio` provides the scheduler,
task, synchronization, selector, runner, and asyncio coexistence APIs for that
job.

The top-level package is meant to feel direct: import the common classes and
helpers from `tealetio`, just as you would from `asyncio`. Submodules remain
available when you want to name the implementation home explicitly.

```python
from tealetio import Event, Scheduler, gather, run, wait_for
```

## Installation

For the usual scheduler and synchronization APIs, install the base package:

```console
python -m pip install tealetio
```

Need the optional asyncio bridge optimizations? Install the `asyncio` extra to
bring in `asynkit`:

```console
python -m pip install 'tealetio[asyncio]'
```

## Quick Start

Need a scheduler for synchronous tealet code? Use `run()` and ask for the
running scheduler inside your entry point:

```python
from tealetio import Event, get_running_scheduler, run


def main() -> str:
    scheduler = get_running_scheduler()
    event = Event()
    seen: list[str] = []

    def worker() -> None:
        seen.append("waiting")
        event.swait()
        seen.append("done")

    task = scheduler.spawn(worker)
    scheduler.call_soon(event.set)
    task.wait()
    return ", ".join(seen)


assert run(main) == "waiting, done"
```

Already inside an asyncio program? Use `AsyncRunner` to host tealet work without
leaving the asyncio world:

```python
import asyncio

from tealetio import AsyncRunner, Event, get_running_scheduler


async def main() -> list[str]:
    async with AsyncRunner() as runner:
        def entry() -> list[str]:
            scheduler = get_running_scheduler()
            event = Event()
            seen: list[str] = []

            def worker() -> None:
                seen.append("waiting")
                event.swait()
                seen.append("done")

            task = scheduler.spawn(worker)
            scheduler.call_soon(event.set)
            task.wait()
            return seen

        return await runner.run(entry)


assert asyncio.run(main()) == ["waiting", "done"]
```

## Public API

The common API is available directly from `tealetio`:

- schedulers and runners: `Scheduler`, `SelectorScheduler`, `AsyncScheduler`, `Runner`, `AsyncRunner`, `run`, `run_async`
- tasks and futures: `Future`, `TealetTask`, `CancelledError`, `shield`
- wait helpers: `gather`, `wait`, `wait_for`, `as_completed`, `ensure_future`, `to_thread`
- synchronization primitives: `Event`, `Lock`, `Semaphore`, `Condition`, `Barrier`, `Queue`
- rendezvous communication: `Channel`
- asyncio coexistence helpers: `run_in_asyncio`, `run_asyncio_in_tealet`, `TealetSelectorEventLoop`

If you prefer explicit homes, submodules such as `tealetio.scheduler`,
`tealetio.tasks`, `tealetio.locks`, `tealetio.runner`, `tealetio.selector`, and
`tealetio.asyncio` define the same objects.

## Asyncio Model

The design intentionally follows `asyncio` where the mapping is useful. In
effect, you already know much of the shape: the learning curve stays small, and
interop with asyncio-hosted programs stays straightforward. Some names differ to
match tealet execution: `Scheduler` fills the role normally held by an event
loop, and `scheduler.spawn(...)` is the tealet-facing equivalent of
`create_task(...)`.

Synchronization primitives are asyncio-compatible where practical and add
`s`-prefixed methods for tealet-blocking operations, such as `Event.swait()`,
`Lock.sacquire()`, and `Queue.sget()`. `tealetio` also reuses asyncio
exceptions where that preserves familiar behavior and compatibility.

`Channel` is inspired by Stackless Python channels. It provides rendezvous-style
communication between tasks, with selectable sender/receiver preference models,
and can also be used for inter-thread communication through scheduler-safe
wakeup paths.

Notice one important runtime difference: exception delivery. Exceptions such as
`CancelledError` are delivered immediately by switching to the target tealet,
instead of being kept as pending exceptions. This avoids races where multiple
pending exceptions can accumulate, and removes the need to track pending
cancellations separately. These deviations are intentional, but the exact API
shape remains subject to change before a stable release.

## Status

`tealetio` is pre-1.0 software. APIs are usable for experimentation and in-repo
testing, but may change before a stable release.

## Documentation

- [Python API reference](https://github.com/kristjanvalur/pytealet/blob/main/packages/tealetio/docs/PYTHON_API.md)
- [Asyncio coexistence](https://github.com/kristjanvalur/pytealet/blob/main/packages/tealetio/docs/ASYNCIO_COEXISTENCE.md)
- [Scheduler runtime API spec](https://github.com/kristjanvalur/pytealet/blob/main/packages/tealetio/docs/SCHEDULER_RUNTIME_API_SPEC.md)
