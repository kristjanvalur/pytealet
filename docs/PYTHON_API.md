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
- `_tealet.hide_frame(callable, args=(), kwargs=None) -> object`
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
- `duplicate() -> _tealet.tealet`
- `current() -> _tealet.tealet`
- `previous() -> _tealet.tealet | None`
- `main() -> _tealet.tealet`
- `is_foreign() -> bool`
- `prepare(function) -> _tealet.tealet`
- `run(function, arg=None) -> object`
- `switch(arg=None, panic=False) -> object`
- `set_exception(exception, fallback=None) -> None`
- `throw(exception) -> object`

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
