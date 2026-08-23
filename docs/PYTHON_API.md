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
- `tealet.STATE_NEW`, `tealet.STATE_STUB`, `tealet.STATE_RUN`, `tealet.STATE_EXIT`, `tealet.STATE_PRIMED`
- `tealet.TealetError`, `tealet.DefunctError`, `tealet.PanicError`, `tealet.InvalidError`, `tealet.ThreadMismatchError`, `tealet.StateError`, `tealet.TealetExit`

## _tealet module

Module-level functions:
- `_tealet.current() -> _tealet.tealet`
- `_tealet.main() -> _tealet.tealet`
- `_tealet.get_tealet_factory() -> Callable[[], _tealet.tealet]`
- `_tealet.set_tealet_factory(factory | None) -> _tealet.tealet`
- `_tealet.previous() -> _tealet.tealet | None`
- `_tealet.thread_reap(cleanup_passes: int = 3, kill_exc = None) -> list[_tealet.tealet]`
- `_tealet.thread_sweep() -> list[_tealet.tealet]`
- `_tealet.thread_active() -> list[_tealet.tealet]`
- `_tealet.thread_kill(cleanup_passes: int = 3, kill_exc = None) -> list[_tealet.tealet]`
- `_tealet.error_was_remote() -> bool`
- `_tealet.hide_frame(callable, args=(), kwargs={...}) -> object` (when provided, `kwargs` must be a `dict`)
- `_tealet.frame_introspection() -> bool`
- `_tealet.frame_introspection(enabled) -> bool`
- `_tealet.gettrace() -> Callable | None`
- `_tealet.settrace(callback) -> Callable | None`

`settrace` installs a single switch/throw hook (last setter wins; same shape as
`greenlet.settrace`). The callback is `callback(event, (origin, target))` where
`event` is `"switch"` or `"throw"` and both values are `_tealet.tealet`
wrappers. It runs **after** the transfer on the resumed tealet. When a tealet
exits, `origin` is that finishing wrapper (it may already be in `STATE_EXIT`).
A callback exception clears the hook. Passing `None` clears it. A C debugger
can install the same slot via `set_trace` on the capsule C API without a
Python callable.

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
- `is_main() -> bool`
- `resolve_target(result, exc, exc_target) -> tuple[_tealet.tealet, object] | tuple[_tealet.tealet, object, bool]`
- `prime(function) -> _tealet.tealet`
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
A tealet in `STATE_PRIMED` may be used as an exit target. If `suppress`
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

Equality and hashing use normal Python wrapper identity. Use `is` and `is not`
when comparing tealet wrappers directly.

`_tealet.set_tealet_factory(factory)` configures the callable used for
internally created tealet wrappers. The factory is called with no arguments and
must return a new, unlinked `_tealet.tealet` instance. Passing `None` resets the
factory to the base `_tealet.tealet` constructor. If the current thread already
has a main wrapper from an older factory generation, the runtime creates a
replacement wrapper around the same underlying main tealet and returns it.
Existing references to the older main wrapper become detached old wrappers, and
future `_tealet.main()` calls may return a different wrapper instance. Use
`is_main()` to test whether a live tealet wrapper is the current main wrapper
for its lineage.
Direct `_tealet.tealet()` construction still constructs exactly
`_tealet.tealet()`. Duplicating a base-wrapper tealet uses the configured
factory; duplicating an explicit subclass preserves that subclass.

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

## Related Workspace Packages

Core `tealet` stays focused on low-level stack-slicing primitives. Higher-level APIs live in sibling workspace packages:

- `tealetio`: scheduler, task/future, lock, selector, runner, and asyncio APIs. See `packages/tealetio/docs/PYTHON_API.md`.
- `tealet-greenlet`: experimental greenlet emulation via tealet. See `packages/tealet-greenlet/docs/PYTHON_API.md`.

## tealet.profile

`tealet.profile.Profile` is a `profile.Profile` subclass. Each **stack** (a
tealet, or a thread default until the first switch) has its own parallel call
context and timings, so recursion bookkeeping stays true. Stacks that share a
**root function** — `(co_filename, co_firstlineno, co_name)` of the first real
call, including the same lambda line or the same `Thread(target=f)` — form a
**stack family**.

```python
import pstats
import tealet.profile

prof = tealet.profile.Profile()
prof.runcall(driver)
prof.print_stats()  # combined total
pstats.Stats(*prof.stack_families()).print_stats()
for family in prof.stack_families():
    print(family.family, family.nstacks)
```

- `stacks()` — one `StackStats` per retained stack
- `stack_families()` — one `StackStats` per root function (`nstacks` is how
  many stacks were folded)
- `combined()` — everything; `create_stats` / `print_stats` use this

``fold_on_exit=True`` (the default) merges a tealet into its family when it
reaches ``STATE_EXIT`` and drops the individual from :meth:`stacks`. Pass
``fold_on_exit=False`` to keep every stack for a post-mortem of individuals.

`StackStats` is `pstats`-compatible (`create_stats` / `.stats`). It also
carries `family`, `nstacks`, `thread_id` (set when exactly one thread
contributed) and `thread_ids` (the set of native thread ids in the snapshot).
`enable()` / `disable()` start and stop tracing. `sys.setprofile` stays
per-thread; `_tealet.settrace` is interpreter-wide, so a trampoline looks up
this thread's `Profile` in TLS. `run()` / `runctx()` match `profile`. This is
not `cProfile`: the stdlib pure-Python profiler exposes a swappable parallel
stack.

The default timer is `time.thread_time` (this thread's CPU), not stdlib
`profile`'s process-wide `time.process_time`. Each stack is one thread of
control, so another thread's CPU is not billed to a parked function. Pass
`timer=time.process_time` or `timer=time.perf_counter` to opt into those
clocks. A tealet `switch` still stops the origin stack.

`enable()` is this thread only; `disable()` matches it, so one thread
stopping does not stop others that called `enable()` on the same
instance. `enable_all_threads()` also hooks `threading` threads (and, on
3.12+, threads already running). Each thread gets its own `Profile`
(same timer and fold policy). `thread_profiles()` returns those instances
so you can inspect or merge them yourself; `stacks()` /
`stack_families()` / `combined()` collate them. `disable()` on that
coordinator stops every instance started this way. On 3.10/3.11 a worker
that is still running restores `sys.setprofile` on its next profile
event. Threads that never used `threading` are not included unless they
call `enable()` themselves.

## tealet.cprofile

`tealet.cprofile.Profile` is the 3.12+ C implementation (`_tealet_profile`).
It uses `sys.monitoring` for call/return and `_tealet.settrace` (capsule
`set_trace`) to swap stacks. The public views match `tealet.profile`: `stacks()`,
`stack_families()`, `combined()`, plus `runcall` / `enable` / `disable`.
`enable(builtins=True)` (the default) also records C calls such as `len`
via `CALL` / `C_RETURN` / `C_RAISE`, matching `cProfile`. ``fold_on_exit``
matches `tealet.profile`. `enable()` occupies `PROFILER_ID` for the whole
interpreter, so every thread is sampled; stacks stay per-thread via TLS.
``timer="wall"`` (default) uses monotonic wall time, with GIL slicing when
the GIL is enabled; ``timer="thread"`` uses this thread's CPU. Import
raises `ImportError` on Python older than 3.12.

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
