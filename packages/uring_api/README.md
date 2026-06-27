# uring-api

`uring-api` is a small Python wrapper around Linux `io_uring`.

The first goal is deliberately modest: expose enough of the native ring lifecycle
to probe availability and build higher-level completion abstractions in Python.
It does not implement an event loop, scheduler, or asyncio compatibility layer.

## Quick Check

```python
import uring_api

print(uring_api.probe())

with uring_api.Ring() as ring:
    print(ring.fd)
```

## Build Requirements

`uring-api` links against system `liburing`:

```bash
sudo apt install liburing-dev
```

The extension uses multi-phase module initialisation and declares itself safe to
import without enabling the GIL on free-threaded CPython builds.