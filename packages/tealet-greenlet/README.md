# tealet-greenlet

**tealet-greenlet: greenlet compatibility on top of tealet.**

`tealet-greenlet` packages the greenlet-compatible layer separately from the
core `tealet` runtime. The core package provides stack-slicing primitives;
this package carries the larger greenlet compatibility surface, including
parent/child switching, tracing hooks, context handling, and upstream-style
compatibility tests.

## Rationale

`tealet-greenlet` is an experimental replacement library for greenlet based on
the tealet engine. It is currently intended as a proof of concept: how much of
greenlet's behaviour can we express by keeping the low-level stack engine small,
portable, and focused, while putting the greenlet business logic in a separate
layer?

That split is the point. `tealet` benefits from a simple core API and the
portability work already done in libtealet and Stackman. `tealet-greenlet` then
adds the compatibility policy on top: parent selection, error translation,
context handling, tracing hooks, cleanup behaviour, and test-facing
implementation probes.

Future development may keep improving the Python implementation, or move hot
paths and tricky compatibility edges into a C-accelerated core if that turns out
to be worthwhile. For now, the package gives us a concrete compatibility target
without making the core `tealet` wheel carry all of greenlet's semantic cargo.

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

`tealet-greenlet` is pre-1.0 software. It is useful for experimentation,
tracking compatibility, and testing the architecture, but it does not claim full
upstream greenlet parity yet.

## Documentation

- [Python API](docs/PYTHON_API.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Upstream compatibility tests](tests/compat_greenlet/README.md)

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
