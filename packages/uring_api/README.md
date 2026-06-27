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

## Checking Availability

`io_uring` availability depends on more than the Python package importing
successfully. The kernel, container sandbox, seccomp profile, and process limits
can all affect whether a ring can actually be created.

Use `probe()` when you want the reason as well as the answer:

```python
import uring_api

probe = uring_api.probe()

if probe.available:
    print("io_uring is available")
    print("liburing:", probe.liburing_version)
    print("features:", probe.features)
    print("sq entries:", probe.sq_entries)
    print("cq entries:", probe.cq_entries)
else:
    print("io_uring is not available")
    print("errno:", probe.errno)
    print("message:", probe.message)
```

Use `is_available()` when you only need a boolean:

```python
import uring_api

if not uring_api.is_available():
    raise RuntimeError("io_uring is not available in this environment")
```

`probe()` creates a tiny temporary ring and closes it right away. It is useful for
startup diagnostics, but production code should still handle `OSError` when it
creates the real ring because limits or sandbox policy may differ for larger
settings.

## Initialising a Ring

The current wrapper exposes the native ring lifecycle. A ring is a file
descriptor plus shared submission/completion queues owned by the process.

```python
import uring_api

with uring_api.Ring(entries=8) as ring:
    print("fd:", ring.fd)
    print("kernel features:", ring.features)
    print("submission entries:", ring.sq_entries)
    print("completion entries:", ring.cq_entries)
```

`entries` is the requested submission queue depth. The kernel may round or size
the actual submission and completion queues, so inspect `sq_entries` and
`cq_entries` after initialisation if the exact capacity matters.

If initialisation fails, the constructor raises `OSError`:

```python
import errno
import uring_api

try:
    ring = uring_api.Ring(entries=256)
except OSError as exc:
    if exc.errno == errno.EPERM:
        raise RuntimeError("io_uring is blocked by seccomp or policy") from exc
    if exc.errno == errno.ENOMEM:
        raise RuntimeError("io_uring could not allocate or pin the requested resources") from exc
    raise
else:
    try:
        print(ring.fd)
    finally:
        ring.close()
```

## Choosing Ring Sizes

Ring sizing is about queue depth, not payload buffer size. A modest application
can start with a small number of in-flight operations; a server usually wants
enough entries to cover its expected concurrent I/O without constantly draining
and refilling the ring.

Typical starting points:

| Use case | Suggested entries | Notes |
| --- | ---: | --- |
| Availability probe | 2 | Enough to prove the kernel will create a ring. |
| Modest local I/O | 8-32 | Good for simple tools and initial experiments. |
| Concurrent client work | 64-256 | Enough room for batches without large memory pressure. |
| Server-style I/O | 512-4096 | Needs deliberate resource-limit checks and backpressure. |

For now, `uring-api` does not register fixed buffers or provide operation
submission helpers. When those are added, ring entries and registered buffers
should be configured separately:

- ring entries control how many operations can be submitted or completed at
  once;
- registered buffers control how much memory the kernel pins for direct I/O or
  zero-copy style operation;
- large registered buffer pools can exceed `RLIMIT_MEMLOCK` even when ring
  creation itself succeeds.

That distinction matters. During probing, a 64 MiB fixed-buffer pool exceeded a
default 64 MiB memlock limit because the limit must cover the pinned payload
memory plus kernel/accounting overhead.

You can inspect the process limit before choosing future buffer-pool sizes:

```python
import resource

soft, hard = resource.getrlimit(resource.RLIMIT_MEMLOCK)

print("memlock soft limit:", soft)
print("memlock hard limit:", hard)
```

For a future registered-buffer API, size the pool explicitly rather than
assuming the largest useful value is safe:

```python
buffer_size = 16 * 1024
buffer_count = 256
pool_bytes = buffer_size * buffer_count

print("planned pinned buffer pool:", pool_bytes)
```

Good default profiles for that future layer would look something like:

| Profile | Ring entries | Buffer size | Buffer count | Pinned bytes |
| --- | ---: | ---: | ---: | ---: |
| modest | 32 | 16 KiB | 64 | 1 MiB |
| interactive | 128 | 16 KiB | 256 | 4 MiB |
| server | 1024 | 64 KiB | 1024 | 64 MiB |

The server profile is intentionally near the common default memlock limit on
some systems. In practice, leave headroom or raise the limit before registering
that much memory.

## Containers and Limits

Containers may block `io_uring_setup()` even when the host kernel supports it.
For example, Docker's default seccomp profile commonly rejects ring creation
with `EPERM`. A less restricted profile may be required for development.

Large future registered-buffer pools may also require raising `RLIMIT_MEMLOCK`.
Prefer smaller buffers while developing the operation model, then make server
profiles opt-in and explicit.

## Build Requirements

`uring-api` links against system `liburing`:

```bash
sudo apt install liburing-dev
```

The extension uses multi-phase module initialisation and declares itself safe to
import without enabling the GIL on free-threaded CPython builds.