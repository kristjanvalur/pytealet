# tealet-greenlet Architecture

`tealet-greenlet` layers a greenlet-compatible object model over the lower-level
tealet stack-slicing primitives.

The package exists separately from `tealet` because greenlet compatibility has a
large surface area: parent/child switching, deallocation behaviour, tracing,
contextvars, thread ownership errors, compatibility probes, and upstream-style
tests. Keeping this work in its own workspace package lets the core `tealet`
wheel stay small and focused.

## Package Shape

- `tealet_greenlet`: canonical implementation package.
- `greenlet`: top-level wrapper package for upstream-style imports.
- `greenlet._greenlet`: wrapper for implementation-specific compatibility APIs.
- `greenlet_legacy`: legacy emulation kept as a package-local development and test helper.
- `tests/compat_greenlet`: vendored upstream-style compatibility tests.

`tealet.greenlet` remains in the core package only as a transition wrapper that
imports from `tealet_greenlet` when this package is installed.

## Runtime Model

The implementation wraps `_tealet.tealet` objects with greenlet-compatible Python
objects. Each wrapper tracks its parent, main greenlet, current run callable,
thread ownership, context, and cleanup state.

Important behaviours include:

- switching through `greenlet.switch(...)` maps onto tealet stack transfers;
- `greenlet.throw(...)` delivers exceptions through the target tealet;
- main greenlets are represented by wrappers around each thread's main tealet;
- cross-thread parent or switch errors are translated into greenlet-style `error` messages;
- tracing hooks package switch/throw metadata so callbacks observe greenlet-like events;
- deallocation cleanup uses per-main pending cleanup queues for cases that cannot safely switch immediately.

## Comparison With Upstream greenlet

**Similarities:**

- cooperative coroutines without `async`/`await` keywords;
- stack switching and preservation;
- familiar `greenlet`, `getcurrent()`, `switch()`, `throw()`, `settrace()`, and `GreenletExit` APIs.

**Differences:**

- built on libtealet and `_tealet` instead of upstream greenlet's native implementation;
- implemented mostly in Python, so tracing/profiling can observe shim helper frames;
- compatibility behaviour is tracked pragmatically through tests rather than promised as full parity;
- optional implementation probes under `greenlet._greenlet` are compatibility placeholders where tealet has no matching runtime concept.

## Tests

The default package test suite covers the local legacy shim and package wiring.
Vendored upstream-style compatibility tests live under `tests/compat_greenlet`
and are opt-in through `PYTEALET_RUN_UPSTREAM_GREENLET_TESTS=1`.
