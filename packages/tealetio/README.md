# tealetio

`tealetio` provides scheduler, task, synchronization, selector, runner, and
asyncio coexistence APIs for programs built on `tealet`.

The package is intentionally shaped like a Python module API: common classes and
helpers are available from the top-level `tealetio` namespace, while submodules
remain available for code that wants a more specific import home.

```python
from tealetio import Event, Scheduler, gather, run, wait_for
```

## Installation

```console
python -m pip install tealetio
```

The base package depends on `tealet`. Optional asyncio bridge optimizations use
`asynkit` when installed:

```console
python -m pip install 'tealetio[asyncio]'
```

## Quick Start

Use `run()` to create and drive a scheduler for synchronous tealet code:

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

For asyncio-hosted programs, use the async runner APIs:

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

The top-level package re-exports the common public API, including:

- schedulers and runners: `Scheduler`, `SelectorScheduler`, `AsyncScheduler`, `Runner`, `AsyncRunner`, `run`, `run_async`
- tasks and futures: `Future`, `TealetTask`, `CancelledError`, `shield`
- wait helpers: `gather`, `wait`, `wait_for`, `as_completed`, `ensure_future`, `to_thread`
- synchronization primitives: `Event`, `Lock`, `Semaphore`, `Condition`, `Barrier`, `Queue`
- rendezvous communication: `Channel`
- asyncio coexistence helpers: `run_in_asyncio`, `run_asyncio_in_tealet`, `TealetSelectorEventLoop`

Submodules such as `tealetio.scheduler`, `tealetio.tasks`, `tealetio.locks`,
`tealetio.runner`, `tealetio.selector`, and `tealetio.asyncio` define the same
objects at their implementation homes.

## Asyncio Model

The design intentionally follows `asyncio` where the mapping is useful. This
keeps the learning curve small and makes interop with asyncio-hosted programs
straightforward. Some names differ to match tealet execution: `Scheduler` fills
the role normally held by an event loop, and `scheduler.spawn(...)` is the
tealet-facing equivalent of `create_task(...)`.

Synchronization primitives are asyncio-compatible where practical and add
`s`-prefixed methods for tealet-blocking operations, such as `Event.swait()`,
`Lock.sacquire()`, and `Queue.sget()`. `tealetio` also reuses asyncio
exceptions where that preserves familiar behavior and compatibility.

`Channel` is inspired by Stackless Python channels. It provides rendezvous-style
communication between tasks, with selectable sender/receiver preference models,
and can also be used for inter-thread communication through scheduler-safe
wakeup paths.

One notable runtime difference is exception delivery. Exceptions such as
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
