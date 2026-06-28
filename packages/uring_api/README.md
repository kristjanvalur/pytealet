# uring-api

`uring-api` is a small Python wrapper around Linux `io_uring`.

The goal is deliberately modest: expose enough of the native ring lifecycle,
socket send/recv submission, completion waiting, and callback delivery to build
higher-level completion abstractions in Python. It does not implement an event
loop, scheduler, or asyncio compatibility layer.

## Quick Check

```python
import uring_api

print(uring_api.probe())

with uring_api.Ring() as ring:
    print(ring.fd)
```

## Socket I/O

`Ring` currently exposes `submit_recv()`, `submit_send()`, and `wait()` for
minimal socket-oriented experiments. Each submitted operation carries a
Python `user_data` object which comes back with its completion.

```python
import socket
import uring_api

reader, writer = socket.socketpair()
try:
    reader.setblocking(False)
    writer.setblocking(False)

    with uring_api.Ring() as ring:
        token = {"operation": "greeting"}
        ring.submit_recv(reader.fileno(), 5, token)
        writer.send(b"hello")

        completion = ring.wait(1.0)

    assert completion is not None
    assert completion.user_data is token
    print(completion.res, completion.result)
finally:
    reader.close()
    writer.close()
```

For sends, `uring-api` keeps the exported buffer alive until the kernel reports
the completion. That avoids copying the outgoing payload into an internal bytes
object just to keep memory valid.

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
    print("compiled liburing:", probe.compiled_liburing_version)
    print("compiled liburing version info:", probe.compiled_liburing_version_info)
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

The compiled liburing version fields report the header version used to build the
binary extension. This is useful in CI because Linux distribution images can
compile the same Python package against different liburing development packages
while still running on the hosted runner's kernel.

If the native extension cannot be imported after installation, importing
`uring_api` still succeeds and `probe()` reports `available=False` with a
message describing the import problem. Source builds with unsupported native
dependencies warn and install the pure Python wrapper without `_uring_api`.

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

## Threading Model

`Ring` deliberately stays close to liburing's shared-ring model, but the Python
object adds native locking around the parts that matter for normal use.

The intended baseline is simple:

- one thread may reap completions with `wait()`;
- other threads may call submit-side methods such as `submit_recv()`,
    `submit_send()`, and `break_wait()`;
- `break_wait()` is safe to call while another thread is blocked in `wait()`;
- multiple concurrent `wait()` calls are serialised by the `Ring` object;
- alternatively, `Ring.callback` plus `start()` can run a native delivery thread
  that waits for completions and calls the callback directly.

`break_wait()` prepares and submits an internal NOP. When the reaper consumes that
completion, `wait()` returns `None` rather than a user completion.

The delivery thread uses the same receive side as `wait()`, so public `wait()`
calls raise `RuntimeError` while it is running. `stop()` asks the thread to exit,
wakes it with `break_wait()`, and waits until it has stopped. `close()` does the
same before closing the ring. If the callback raises, the exception is reported
as unraisable and the delivery thread exits.

Native C clients can register a worker-thread callback through the C API. When a
C callback is present, the delivery thread calls it instead of `Ring.callback`;
otherwise it falls back to the Python callback property.

```python
import uring_api


def delivered(completion):
    print(completion.user_data, completion.res, completion.result)


with uring_api.Ring() as ring:
    ring.callback = delivered
    ring.start()
    try:
        ring.submit_recv(fd, 4096, 200)
    finally:
        ring.stop()
```

`close()` is still an owner-coordinated shutdown operation for submissions. Do
not close a ring while another thread may submit new user operations.

## C API

Native clients can include `uring_api_capi.h` and import `_uring_api._C_API` with
`PyCapsule_Import()`. Use `uring_api.get_include()` to find the installed header
directory when compiling an extension module.

The capsule currently exposes:

- `abi_version`, `struct_size`, and `feature_flags` for compatibility checks;
- `compiled_liburing_major` and `compiled_liburing_minor` for build-time header
    visibility;
- `probe(entries, flags)`, which returns a new reference to the same structured
    dictionary as `_uring_api.probe()`;
- `ring_new()`, lifecycle helpers, metadata helpers, `ring_submit_recv()`,
    `ring_submit_send()`, `ring_break_wait()`, and `ring_wait()`;
- `ring_set_callback()`, `ring_set_c_callback()`, `ring_start()`, and
    `ring_stop()` for delivery-thread control;
- `completion_check()`, `completion_user_data()`, `completion_res()`,
    `completion_flags()`, and `completion_result()` for native completion
    inspection.

Check `URING_API_CAPI_FEATURE_PROBE`, `URING_API_CAPI_FEATURE_RING`, and
`URING_API_CAPI_FEATURE_C_CALLBACK` before calling those groups of functions.
Check `URING_API_CAPI_FEATURE_COMPLETION` before using completion accessors. A C
completion callback receives the ring object, the completion object, and the
supplied `user_data`. Return `0` for success; return a negative value with a
Python exception set to report an unraisable error and stop the delivery thread.

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

For now, `uring-api` does not register fixed buffers. When those are added, ring
entries and registered buffers should be configured separately:

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

The native extension requires `liburing >= 2.4`. Older headers do not expose the
version macros we use for build-time validation, and they also predate the data
and ring entry helpers used by the extension. On Ubuntu, that means
`ubuntu-23.10` or newer from distro packages; `ubuntu-22.04` needs a newer
liburing installed from another source to build `_uring_api`.

The extension uses multi-phase module initialisation and declares itself safe to
import without enabling the GIL on free-threaded CPython builds.
