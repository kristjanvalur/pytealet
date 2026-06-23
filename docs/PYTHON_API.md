# Python API Reference

This document describes the Python-facing API for tealet.

Status note:
- The project is pre-1.0 and APIs may evolve.
- Runtime semantics and safety are prioritized over strict compatibility.

## Import Surfaces

## tealet package

The `tealet` package re-exports `_tealet` symbols and provides helper utilities.

- `tealet.get_include() -> str`
  - Returns the installed include directory containing `pytealet_capi.h`.

Common constants/types re-exported from `_tealet` include:
- `tealet.tealet` (core type)
- `tealet.STATE_NEW`, `tealet.STATE_STUB`, `tealet.STATE_RUN`, `tealet.STATE_EXIT`
- `tealet.TealetError`, `tealet.DefunctError`, `tealet.PanicError`, `tealet.InvalidError`, `tealet.ThreadMismatchError`, `tealet.StateError`, `tealet.TealetExit`

## _tealet module

Module-level functions:
- `_tealet.current() -> _tealet.tealet`
- `_tealet.main() -> _tealet.tealet`
- `_tealet.previous() -> _tealet.tealet | None`
- `_tealet.thread_reap(cleanup_passes: int = 3, kill_exc = None) -> list[_tealet.tealet]`
- `_tealet.thread_sweep() -> list[_tealet.tealet]`
- `_tealet.thread_active() -> list[_tealet.tealet]`
- `_tealet.thread_kill(cleanup_passes: int = 3, kill_exc = None) -> list[_tealet.tealet]`
- `_tealet.error_was_remote() -> bool`
- `_tealet.hide_frame(callable, args=(), kwargs={...}) -> object` (when provided, `kwargs` must be a `dict`)
- `_tealet.frame_introspection() -> bool`
- `_tealet.frame_introspection(enabled) -> bool`

Notable module attributes:
- `_tealet.C_API_ABI_VERSION` (int)
- `_tealet.PYTEALET_WITH_PENDING_FRAME_INTROSPECTION` (int, compile-time capability)
- `_tealet.__version__` (str)
- `_tealet._C_API` (PyCapsule for C clients)

## _tealet.tealet type

Constructor:
- `_tealet.tealet()`

Methods:
- `stub() -> _tealet.tealet`
- `set_stub(source, duplicate=True) -> _tealet.tealet`
- `duplicate() -> _tealet.tealet`
- `current() -> _tealet.tealet`
- `previous() -> _tealet.tealet | None`
- `main() -> _tealet.tealet`
- `is_foreign() -> bool`
- `resolve_target(result, exc, exc_target) -> tuple[_tealet.tealet, object] | tuple[_tealet.tealet, object, bool]`
- `prepare(function) -> _tealet.tealet`
- `run(function, arg=None) -> object`
- `switch(arg=None, panic=False) -> object`
- `set_pending_exception(exception, fallback=None) -> None`
- `throw(exception, *, return_target=current) -> object`

`resolve_target` is a class-level override hook for frameworks that need custom
exit-target routing or exception disposition from the worker callback.
Custom overrides receive the raw worker return value, worker exception
(if any), and `exc_target`.
`exc_target` is `None` unless the worker exception matches the current
in-flight injected exception token and that token has a valid fallback target.
When populated, it is the redirect fallback target for that uncaught exception.
Overrides must return `(target, arg)` or `(target, arg, suppress)`.
`target` must be an active tealet in the same lineage. A tealet returned by
`prepare()` is already active and may be used as an exit target. If `suppress`
is truthy, any captured worker exception is suppressed before
uncaught-exception handling.
The default implementation maps successful worker return values from
`target` or `(target, arg)` into `(target, arg, suppress=False)`. When the worker
raises `_tealet.TealetExit`, the default implementation routes to `exc_target`
or main and suppresses the exception. When the worker raises `SystemExit` or
`KeyboardInterrupt`, the default implementation queues that exception on main,
routes to main, and suppresses the original worker exception. Other worker
exceptions route to `exc_target` or main with `suppress=False`; any exception left
unsuppressed after the resolver returns is reported via `sys.unraisablehook`.
If the hook raises or returns an invalid value (including `None`), the runtime
reports it via `sys.unraisablehook` and falls back to `(main, None)`; any
original worker exception left unsuppressed by that fallback is also unraisable.

Properties:
- `state: int`
- `frame: frame | None`
- `context: contextvars.Context | None` (get/set)
- `thread_id: int`

## Exceptions

The runtime exposes these exception classes:
- `TealetError`
- `DefunctError`
- `PanicError`
- `InvalidError`
- `ThreadMismatchError`
- `StateError`
- `TealetExit`

`PanicError` also exposes:
- `result()`
- `exception()`

## Greenlet Compatibility Shim

The `tealet.greenlet` module is a compatibility-oriented layer built on top of tealet primitives.

Important scope note:
- It is best viewed as a proof-of-concept and compatibility shim, not as a statement that tealet itself is a full greenlet runtime replacement for all workloads.

Primary public names include:
- `greenlet`
- `getcurrent()`
- `settrace(func)`, `gettrace()`
- `GreenletExit`
- `error`

## Minimal Scheduler Example

`tealet.simple_scheduler.SimpleScheduler` is an installed example of a small
cooperative scheduler built directly on core tealet primitives.

It intentionally supports only a runnable queue, `spawn(...)`, cooperative
`yield_()`, `run()`, and `run_until_complete(...)`. It does not provide IO
facilities, timers, futures, cancellation, thread-safe callbacks, or asyncio
interoperability.

## tealetio Package

The richer scheduler, task/future, lock, selector, runner, and asyncio
coexistence APIs live in the separate `tealetio` workspace package. See
`packages/tealetio/docs/PYTHON_API.md` for that package's API reference.
