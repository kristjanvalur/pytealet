# tealetio

**tealetio: async without the async.**

Where `tealet` gives you stack-slicing primitives, `tealetio` turns them into a
runtime framework with a familiar shape. It adds scheduler, task,
synchronisation, selector, runner, and asyncio coexistence APIs for ordinary
tealet code.

In effect, async operations can work without the `async` keyword: tealet-powered
stack slicing lets ordinary-looking functions block, resume, and compose through
the scheduler.

The top-level package is meant to feel direct: import the common classes and
helpers from `tealetio`, just as you would from `asyncio`. Submodules remain
available when you want to name the implementation home explicitly.

```python
from tealetio import Event, Scheduler, gather, run, wait_for
```

## Installation

For the usual scheduler and synchronisation APIs, install the base package:

```console
python -m pip install tealetio
```

`tealetio` relies on [asynkit](https://github.com/kristjanvalur/asynkit) for
some advanced async trickery, including efficient coroutine await-protocol
driving in the asyncio bridge.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for package-specific release notes.

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

- schedulers and runners: `Scheduler`, `ProactorScheduler`, `SyncProactorScheduler`, `AsyncProactorScheduler`, `SelectorScheduler`, `SyncSelectorScheduler`, `AsyncSelectorScheduler`, `BasicScheduler`, `AsyncScheduler`, `Runner`, `AsyncRunner`, `run`, `run_async`
- tasks and futures: `Future`, `Task`, `spawn`, `create_task`, `get_current`, `CancelledError`, `shield`
- wait helpers: `gather`, `wait`, `wait_for`, `as_completed`, `ensure_future`, `to_thread`
- synchronisation primitives: `Event`, `Lock`, `Semaphore`, `Condition`, `Barrier`, `Queue`
- runnable scheduling policies: `FifoRunnableQueue`, `PrescheduledRunnableQueue`, `PriorityRunnableQueue`
- rendezvous communication: `Channel`
- asyncio coexistence helpers: `asyncio_get_current`, `run_in_asyncio`, `run_asyncio_in_tealet`, `ForwardingSelector`, `ForwardingProactor`, `TealetSelectorEventLoop`, `TealetProactorEventLoop`

If you prefer explicit homes, submodules such as `tealetio.scheduler`,
`tealetio.tasks`, `tealetio.locks`, `tealetio.runner`, `tealetio.selector`, and
`tealetio.asyncio` define the same objects.

`Scheduler` is the normal synchronous scheduler alias and uses a proactor backend.
`ProactorScheduler` is the abstract shared proactor core, with
`SyncProactorScheduler` and `AsyncProactorScheduler` providing concrete driving
facades.
`SelectorScheduler` follows the same pattern for selector readiness, with
`SyncSelectorScheduler` and `AsyncSelectorScheduler` as concrete variants.
`run_asyncio_in_tealet(...)` uses `SyncSelectorScheduler` with
`TealetSelectorEventLoop`'s `ForwardingSelector` to host asyncio inside tealet.
For explicit proactor experiments, pass `SyncProactorScheduler` and
`TealetProactorEventLoop` so asyncio's proactor-shaped wait delegates to the
host tealet proactor through `ForwardingProactor`.
`BasicScheduler` remains available for tests and pure scheduling experiments
that intentionally avoid IO support. Internally, tealetio keeps cooperative
scheduling mechanics separate from the sync and async driving facades, so custom
schedulers can share task behaviour while choosing their own wait point.

## Asyncio Model

The design intentionally follows `asyncio` where the mapping is useful. In
effect, you already know much of the shape: the learning curve stays small, and
interop with asyncio-hosted programs stays straightforward. Some names differ to
match tealet execution: `Scheduler` fills the role normally held by an event
loop, and `spawn(...)` is the native tealet-facing equivalent of
`create_task(...)`. The package root also exports `create_task(...)` as a
familiar alias.

Synchronisation primitives are asyncio-compatible where practical and add
`s`-prefixed methods for tealet-blocking operations, such as `Event.swait()`,
`Lock.sacquire()`, and `Queue.sget()`. `tealetio` also reuses asyncio
exceptions where that preserves familiar behaviour and compatibility.

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
