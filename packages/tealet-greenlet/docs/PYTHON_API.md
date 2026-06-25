# tealet-greenlet Python API

`tealet-greenlet` provides a greenlet-compatible interface built on top of
tealet primitives. It is a compatibility-oriented package, not part of the core
`tealet` runtime.

## Imports

The canonical import package is `tealet_greenlet`:

```python
from tealet_greenlet import GreenletExit, error, getcurrent, greenlet
```

For code that expects upstream-style imports, `tealet-greenlet` also provides a
`greenlet` package wrapper:

```python
from greenlet import GreenletExit, getcurrent, greenlet
```

For transition from the old in-core location, `tealet.greenlet` remains as a thin
wrapper when `tealet-greenlet` is installed.

## Scope

`tealet-greenlet` is best viewed as a proof-of-concept and compatibility shim. It
tracks useful greenlet semantics where practical, but it does not claim full
upstream greenlet parity for all workloads.

## Public Names

Primary public names include:

- `greenlet`
- `getcurrent()`
- `settrace(func)` and `gettrace()`
- `GreenletExit`
- `error`
- `install(force=True)`

## `greenlet`

```python
class greenlet(run=None, parent=None)
```

Creates a greenlet-compatible wrapper around a tealet.

### Methods

- `switch(*args, **kwds)`: switch to this greenlet.
- `throw(type=None, value=None, traceback=None)`: throw an exception in this greenlet.

### Properties

- `gr_frame`: current or suspended frame, when available.
- `gr_context`: contextvars context associated with the greenlet.
- `dead`: `True` if the greenlet has exited.
- `parent`: parent greenlet used when the greenlet returns or exits.

## Exceptions

- `error`: greenlet-style operational error.
- `GreenletExit`: raised to exit a greenlet.

## Compatibility Installation

```python
import tealet_greenlet

tealet_greenlet.install()
```

`install()` exposes the shim as importable top-level `greenlet` and
`greenlet._greenlet` modules in `sys.modules`. This is useful for tests or
applications that want to exercise code written against upstream greenlet without
installing upstream greenlet itself.
