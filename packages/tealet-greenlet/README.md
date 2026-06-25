# tealet-greenlet

**tealet-greenlet: greenlet compatibility on top of tealet.**

`tealet-greenlet` packages the greenlet-compatible layer separately from the
core `tealet` runtime. The core package provides stack-slicing primitives;
this package carries the larger greenlet compatibility surface, including
parent/child switching, tracing hooks, context handling, and upstream-style
compatibility tests.

## Installation

```console
python -m pip install tealet-greenlet
```

The canonical import is `tealet_greenlet`:

```python
from tealet_greenlet import greenlet, getcurrent
```

Need drop-in `import greenlet` compatibility? Install the shim into
`sys.modules` before importing code that expects upstream greenlet:

```python
import tealet_greenlet

tealet_greenlet.install()

import greenlet
```

For transition from the old in-core location, `tealet.greenlet` remains as a
thin wrapper when this package is installed.

## Status

`tealet-greenlet` is pre-1.0 software. It is useful for experimentation and for
tracking compatibility, but it does not claim full upstream greenlet parity yet.

## Tests

Run the package-local tests from the repository root:

```console
uv run --active python -m pytest packages/tealet-greenlet/tests -q
```

The upstream compatibility tests are opt-in:

```console
PYTEALET_RUN_UPSTREAM_GREENLET_TESTS=1 \
  uv run --active python -m pytest packages/tealet-greenlet/tests/compat_greenlet -q
```
