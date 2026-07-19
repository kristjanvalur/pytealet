# uring-api

`uring-api` is a small Python wrapper around Linux `io_uring`.

The goal is deliberately modest: expose enough of the native ring lifecycle,
socket send/recv submission, completion waiting, and callback delivery to build
higher-level completion abstractions in Python. It does not implement an event
loop, scheduler, or asyncio compatibility layer.

Future work is tracked in [ROADMAP.md](ROADMAP.md), including queue resizing
and specialised kernel tuning. Caller-owned provided-buffer receive with leased
`BufView` delivery is already part of the Python surface.

## Quick Check

```python
import uring_api

print(uring_api.probe())

with uring_api.Ring() as ring:
    print(ring.fd)
```

## Socket I/O

Need to drive socket work through a ring without building a full event loop?
`Ring` exposes direct submit wrappers for the common Python-oriented cases:

- stream I/O: `submit_recv()`, and provided-buffer `submit_recv_buf()` /
  `submit_recv_multishot()` via `create_buf_group()`;
- message I/O: `submit_recvmsg()`, `submit_sendto()`, `submit_sendmsg()`, and
  zero-copy `submit_sendmsg_zc()`;
- listeners and setup: `submit_accept()`, `submit_accept_multishot()`,
  `submit_connect()`, and `submit_socket()`;
- lifecycle: `submit_shutdown()`, `submit_close()`, send helpers `submit_send()`
  / `submit_send_zc()`, and `wait()` for completion reaping.

Each submitted operation carries a Python `user_data` object which comes back
with its completion. Inspect the semantic operation with `completion.kind`
(`CompletionKind` enum, or the matching `COMPLETION_KIND_*` constants) when
callbacks need to branch on completion type rather than inferring from
`result` alone.

Optional `Ring.pre_submit` runs after the SQE is prepared and before
`io_uring_submit`, as `hook(user_data, completion)`. If that submit fails, the
same hook is called again as `hook(user_data, None)` so reverse links can be
cleared. The hook must not re-enter ring submit/wait/serve APIs.

```python
import socket
import uring_api

reader, writer = socket.socketpair()
try:
    reader.setblocking(False)
    writer.setblocking(False)

    with uring_api.Ring() as ring:
        token = {"operation": "greeting"}
        buf = bytearray(5)
        ring.submit_recv(reader.fileno(), buf, token)
        writer.send(b"hello")

        batch = ring.wait(1.0)

    assert len(batch) == 1
    completion = batch[0]
    assert completion.user_data is token
    assert bytes(buf) == b"hello"
    print(completion.res, completion.result)
finally:
    reader.close()
    writer.close()
```

For sends, `uring-api` keeps the exported buffer alive until the kernel reports
the completion. That avoids copying the outgoing payload into an internal bytes
object just to keep memory valid. `submit_send_zc()` uses
`IORING_OP_SEND_ZC`, while `submit_sendmsg_zc()` uses `IORING_OP_SENDMSG_ZC` for
the `sendmsg` shape. Their ordinary operation CQE is delivered as the submitted
`Completion`; the later `IORING_CQE_F_NOTIF` buffer-lifetime CQE is consumed
internally and releases the retained buffer.

`submit_shutdown()` is a socket operation and mirrors `shutdown(fd, how)`.
`submit_accept()` and `submit_accept_multishot()` accept optional accept flags;
pass `socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC` when accepted sockets should
be ready for proactor ownership without a follow-up `fcntl()` call.
`submit_accept_multishot()` also accepts optional `base_sequence` (default 0),
matching `submit_recv_multishot`: the first successful accept leg uses that
index in `completion.sequence`, then increments. Use it when continuing a
stream after eager accepts already delivered earlier indices.
`submit_accept()` and `submit_accept_multishot()` deliver the accepted fd in
`completion.res` and `completion.result`. Call `getpeername()` on the fd when
you need the peer address.
`submit_close()` is lower-level: pass only a raw fd whose ownership has already
been transferred away from Python objects such as `socket.socket`, for example
with `detach()`. Otherwise, Python and the kernel may both believe they own the
same descriptor.

## File metadata and positioned I/O

`Ring` also exposes positioned file helpers for caller-owned fds:

- `submit_openat(path, flags, mode=0, *, dfd=AT_FDCWD)` opens a path and
  returns the new fd in the completion result;
- `submit_read(fd, buf, offset)` and `submit_write(fd, data, offset)` perform
  explicit-offset I/O into caller buffers;
- `submit_statx_fdsize(fd)` is the common fast path for open-file metadata: it
  runs fd-only statx internally and puts the byte length in `completion.result`
  on success (`completion.kind == CompletionKind.STATX_FDSIZE`);
- `submit_statx(dfd, path, flags, mask, buf)` fills a caller-provided 256-byte
  statx buffer asynchronously when you need a custom mask or path lookup.

The usual positioned-file case (append EOF, `SEEK_END`, sendfile bounds) is an
open fd whose size you already own:

```python
handle = ring.submit_statx_fdsize(fd)
[completion] = ring.wait()
if completion.res == 0:
    size = completion.result
```

No caller buffer is required for `submit_statx_fdsize()`. Use `submit_statx()`
when you need path-based metadata or fields beyond `stx_size`.

Successful `submit_statx()` completions always leave `completion.result` as
`None`; read fields from the caller-owned submit buffer (for example via
`statx_st_size(buf)` when you requested `STATX_SIZE`). Only
`submit_statx_fdsize()` puts the byte length in `completion.result`. If the
internal buffer lacks size fields, `completion.result` is `None` and the
completion is still delivered.

**Behaviour change (since PR #34):** successful `submit_statx()` no longer sets
`completion.result` to `0`; it stays `None`.

Provided-buffer receive uses a caller-owned ring created with
`create_buf_group()`. Submit one-shot receives with `submit_recv_buf()` or
stream receives with `submit_recv_multishot(fd, buf_group, ...)`. Both paths
return read-only `BufView` objects rather than copying into `bytes`. Export the
payload with `memoryview(view)` and drop the export (or call
`memoryview.release()`) before the kernel buffer is recycled:

```python
buf_group = ring.create_buf_group(buffer_size=16384, buffer_count=256)
pending = ring.submit_recv_buf(reader.fileno(), buf_group, token)
[completion] = ring.wait(1.0)

view = memoryview(completion.result)
try:
    process(view)
finally:
    del view
```

Multishot receive reuses the same `BufGroup` contract. Each CQE delivers a
leased `BufView`, sets `completion.sequence` for out-of-order callback
reconstruction, and uses `IORING_CQE_F_MORE` until EOF, cancellation, or
`-ENOBUFS` when the buffer ring is empty. After `-ENOBUFS`, return buffers to
the ring and submit a fresh `submit_recv_multishot()`; stream consumers should
continue ordinal indexing from the terminal completion's `sequence`.

```python
handle = ring.submit_recv_multishot(reader.fileno(), buf_group, token)
[completion] = ring.wait(1.0)
view = memoryview(completion.result)
try:
    process(view)
finally:
    del view
```

`BufView` tracks active exported memoryviews and recycles the selected buffer
back to the ring when the last export is released. Provided-buffer completions
always return `BufView`, including EOF (`completion.res == 0`), where the view
has `length == 0` and is falsy. Detect stream end from `completion.res`, not
from the result type. `BufGroup` and `BufView` cannot be constructed directly;
use `Ring.create_buf_group()` and let receive completions create the views.

`completion.kind` uses `RECV_MULTISHOT` (13) for multishot provided-buffer
receive and `RECV_BUF` (16) for one-shot `submit_recv_buf()`.

`CompletionKind` values are stable across releases and mirror the C API
constants in `uring_api_completion_kinds.h`. Prefer the enum in Python code;
native clients include the same header and call `completion_kind()` on the
completion object.

The local liburing headers expose more socket-adjacent operations than this
wrapper publishes, but those are intentionally outside the core Python-oriented
surface. Readiness polling is optional for a completion proactor, fixed-buffer
send variants still need a different ownership contract than leased `BufView`
receive, and socket command or NAPI controls are specialised tuning hooks. Those
items are tracked in [ROADMAP.md](ROADMAP.md) rather than implied by `probe()`,
which remains a compact runtime availability check.

If the submission queue cannot provide another entry after flushing already
prepared work to the kernel, submit methods raise `SubmissionQueueFull`. Treat
that as backpressure rather than as a permanent ring failure: wait for
completions, then retry or let a higher-level proactor defer the submission.

## Checking Availability

`io_uring` availability depends on more than the Python package importing
successfully. The kernel, container sandbox, seccomp profile, and process limits
can all affect whether a ring can actually be created.

Use `probe()` when you want a compact availability and capability dictionary:

```python
import uring_api

probe = uring_api.probe()

if probe:
    print("io_uring is available")
    print("capabilities:", probe)
else:
    print("io_uring is not available")
```

Use `is_available()` when you only need a boolean:

```python
import uring_api

if not uring_api.is_available():
    raise RuntimeError("io_uring is not available in this environment")
```

`probe()` creates a tiny temporary ring and closes it right away to test ring
creation with the requested `entries` and `flags`. Targeted capability probes run
once per process and are cached in static variables; later `probe()` calls reuse
those results. If ring creation fails,
it returns an empty dictionary. If it succeeds, the dictionary contains
`"available": True` plus named optional capabilities such as
`"IORING_ACCEPT_MULTISHOT"`, `"IORING_POLL_MULTISHOT"`, `"IORING_RECV_MULTISHOT"`, and
`"IORING_OP_SEND_ZC"` and `"IORING_OP_SENDMSG_ZC"` (version-gated at kernel
6.0 per `io_uring_enter(2)`), and `"IORING_OP_STATX"` (version-gated at 5.6).
Production code should
still handle `OSError` when it creates the real ring because limits or sandbox
policy may differ for larger settings.

Pass setup flags to `probe(flags=...)` to check whether this build and kernel
combination accepts a ring mode before using it for the real ring:

```python
import uring_api

flags = uring_api.IORING_SETUP_SINGLE_ISSUER
probe = uring_api.probe(flags=flags)

if probe:
    print("setup flags accepted")
else:
    print("setup flags rejected")
```

Some flags also impose application-level contracts. For example,
`IORING_SETUP_SINGLE_ISSUER` means callers must submit SQEs from a single owning
thread even on kernels that accept the flag. `IORING_SETUP_DEFER_TASKRUN`
requires that same owning thread to reap completions too: `wait()` and
`serve_completions()` must run there, not on a worker pool. Kernels expect
`IORING_SETUP_DEFER_TASKRUN` together with `IORING_SETUP_SINGLE_ISSUER`.

The compiled liburing version fields report the header version used to build the
binary extension. This is useful in CI because Linux distribution images can
compile the same Python package against different liburing development packages
while still running on the hosted runner's kernel.

`submit_send_zc()` and `submit_sendmsg_zc()` are best gated with
`probe()["IORING_OP_SEND_ZC"]` and `probe()["IORING_OP_SENDMSG_ZC"]`. Both
entries use the documented kernel 6.0 floor via `uname(2)`. Zerocopy can still
complete with `ENOTSUP` or `EOPNOTSUPP` for some protocols (for example
`AF_UNIX` on WSL); higher layers should route those sockets through copying
send paths. If your CI image is expected to support zerocopy on inet sockets,
make that expectation explicit:

```bash
uv run --active python - <<'PY'
import uring_api

probe = uring_api.probe()
print(probe)
raise SystemExit(0 if probe.get("IORING_OP_SEND_ZC") and probe.get("IORING_OP_SENDMSG_ZC") else 1)
PY
```

If the native extension cannot be imported after installation, importing
`uring_api` still succeeds and `probe()` returns `{}`. Source builds with
unsupported native dependencies warn and install the pure Python wrapper without
`_uring_api`.

The `IORING_ACCEPT_MULTISHOT` capability uses a runtime operation probe rather
than a kernel version check. It creates a private temporary ring and loopback
listener, submits one multishot accept request, connects a local client, and
checks whether the first accept completion keeps the request armed. If the build
headers do not expose the helper flag, the capability simply reports `False`.

The `IORING_POLL_MULTISHOT` capability uses a runtime operation probe. It creates
a private socket pair, submits one multishot poll for `POLLIN`, writes one byte
to the peer, and reports `True` only if the first completion reports readiness
and keeps the request armed with `IORING_CQE_F_MORE`. Gate
`submit_poll_multishot()` on this entry; one-shot `submit_poll()` and
`submit_poll_remove()` are treated as baseline poll surface.

The `IORING_RECV_MULTISHOT` capability is also checked with a runtime operation
probe because it requires newer kernel support than multishot accept. It creates
a private socket pair and provided-buffer ring, submits one multishot receive,
sends one byte, and reports `True` only if the first completion selects a buffer
and keeps the request armed with `IORING_CQE_F_MORE`.

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

Pass `flags=` to request setup modes that were accepted by `probe(flags=...)`:

```python
import uring_api

flags = uring_api.IORING_SETUP_SINGLE_ISSUER

if uring_api.probe(flags=flags):
    with uring_api.Ring(entries=8, flags=flags) as ring:
        ...
```

The constructor passes these flags to `io_uring_queue_init_params()` for the
real ring. The application is still responsible for the contracts implied by
each flag; for example, `IORING_SETUP_SINGLE_ISSUER` requires all submissions to
come from the owning thread.

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
    `create_buf_group()`, `submit_recv_buf()`, `submit_recv_multishot()`,
    `submit_send()`, `submit_send_zc()`,
    `submit_recvmsg()`, `submit_sendto()`, `submit_sendmsg_zc()`,
    `submit_accept()`, `submit_accept_multishot()`, `submit_connect()`, and
    `break_wait()`;
- `break_wait()` is safe to call while another thread is blocked in `wait()`;
- multiple concurrent `wait()` calls are serialised by the `Ring` object;
- alternatively, callers may start their own Python threads and have each one
    call `serve_completions()` to wait for completions and call the callback
    directly.

Rings created with `IORING_SETUP_DEFER_TASKRUN` do not follow that worker-pool
model. Submit, `wait()`, `serve_completions()`, and `break_wait()` must all run
on the owning thread established by the first gated call.

`break_wait()` is the single ring wakeup entry point. It always opens the
host-side `wait_idle()` park **immediately**. When completion service is not
active, it also best-effort submits **one** internal NOP (not a user completion)
so a caller blocked in `wait()` on an **empty CQ** can return. While
`serve_completions()` workers own CQ reaping, the NOP is skipped — only the idle
park is needed. `stop_serving()` still forces a NOP so workers blocked in the
kernel wait can observe stop.

`wait_idle` is a **multi-signaller, single-waiter** park: many threads may call
`break_wait()`, but only one host may park at a time (the proactor driver).
Concurrent `wait_idle` waiters are not supported.

If the submission queue is full, the NOP may be omitted and `break_wait()` still
succeeds: a full SQ means outstanding work, so a real CQE will arrive soon
enough. The idle park does not wait on the NOP path.

The NOP is not a broadcast to worker threads. Serve workers that drain a wake CQE
treat it as an empty/internal batch and continue.

Serving workers use the same receive side as `wait()`, so public `wait()` calls
raise `RuntimeError` while they are running. Each worker calls
`serve_completions()`, then loops until `stop_serving()` asks the service to
exit. Workers compete for an internal wait lock, so only one worker is inside
`io_uring_wait_cqe()` at a time, while another worker can dispatch a completion
callback.

`stop_serving()` sets the stop flag and uses `break_wait()` so a worker blocked
in the kernel wait can observe stop and exit. The caller owns the threads, so the
caller must join them before closing the ring; `close()` and `__exit__()` raise
while completion service is still active. `reset_serving()` clears the stop flag
so a fresh set of workers can enter `serve_completions()` again. If a delivery
callback raises, the ring invokes `exception_handler` when one is set. The handler
receives a context dict with `message`, `exception`, `ring`, and `completions`
(the batch being delivered). When the handler returns normally, that worker
continues serving. When no handler is set, or the handler itself raises,
`serve_completions()` exits with the exception; only that worker stops — other
serving workers keep running until `stop_serving()`.

Native C clients can register a worker-thread callback through the C API. When a
C callback is present, the serving worker calls it instead of `Ring.callback`;
otherwise it falls back to the Python callback property.

```python
import uring_api
import threading


def delivered(batch):
    for completion in batch:
        print(completion.user_data, completion.res, completion.result)


with uring_api.Ring() as ring:
    ring.callback = delivered
    threads = [threading.Thread(target=ring.serve_completions) for _ in range(2)]
    for thread in threads:
        thread.start()
    try:
        ring.submit_recv(fd, bytearray(4096), 200)
    finally:
        ring.stop_serving()
        for thread in threads:
            thread.join()
```

`close()` is still an owner-coordinated shutdown operation for submissions. Do
not close a ring while another thread may submit new user operations.

## C API

Native clients can include `uring_api_capi.h` and import `_uring_api._C_API` with
`PyCapsule_Import()`. Use `uring_api.get_include()` to find the installed header
directory when compiling an extension module.

The capsule currently exposes:

- `abi_version`, `struct_size`, and `feature_flags` for compatibility checks.
  While the package remains pre-release, `abi_version` stays at **1** but the
  function table may be reordered or extended; clients should compare
  `struct_size` and null-check pointers they rely on;
- `compiled_liburing_major` and `compiled_liburing_minor` for build-time header
    visibility;
- `probe(entries, flags)`, which returns a new reference to the same flat
    availability and capability dictionary as `_uring_api.probe()`;
- `ring_new()`, lifecycle helpers, metadata helpers, `ring_submit_recv()`,
    `ring_submit_recv_multishot()`, `ring_submit_send()`,
    `ring_submit_send_zc()`, `ring_submit_recvmsg()`, `ring_submit_sendto()`,
    `ring_submit_sendmsg()`, `ring_submit_sendmsg_zc()`, `ring_submit_accept()`,
    `ring_submit_accept_multishot()`, `ring_submit_connect()`,
    `ring_submit_shutdown()`, `ring_submit_close()`, `ring_submit_read()`,
    `ring_submit_write()`, `ring_submit_openat()`, `ring_submit_statx()`,
    `ring_submit_statx_fdsize()`, `statx_st_size()`, `ring_submit_socket()`,
    `ring_submit_poll()`,
    `ring_submit_poll_multishot()`,
    `ring_submit_poll_remove()`,
    `ring_break_wait()`, and `ring_wait()`;
- `ring_set_callback()`, `ring_set_exception_handler()`, `ring_set_c_callback()`,
    `ring_serve_completions()`,
    `ring_stop_serving()`, and `ring_reset_serving()` for completion-service
    control;
- `completion_check()`, `completion_user_data()`, `completion_res()`,
    `completion_flags()`, `completion_sequence()`, `completion_result()`, and
    `completion_kind()` for native completion inspection. Kind values match
    `URING_API_COMPLETION_KIND_*` in `uring_api_completion_kinds.h` and
    `CompletionKind` in Python.

Check `URING_API_CAPI_FEATURE_CORE` before calling the function table. The flag
describes the capsule API surface, not runtime kernel support for individual
operations. Use `probe()` to check whether this process can create a ring and to
read runtime support for optional operation helpers from the returned flat
dictionary. A C completion callback receives the ring object, a list of
completions for one kernel drain batch, and the supplied `user_data`. Return
`0` for success; return a negative value with a Python exception set so the
current `serve_completions()` call exits with that error (other workers are not
stopped). Callback pointers must
not be changed while `serve_completions()` workers are active.

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

Ring entries and provided-buffer pools should be configured separately:

- ring entries control how many operations can be submitted or completed at
  once;
- `create_buf_group()` registers a provided-buffer ring whose storage stays
  pinned for receive operations that select buffers from that group;
- large provided-buffer pools can exceed `RLIMIT_MEMLOCK` even when ring
  creation itself succeeds.

`uring-api` does not yet expose fixed-buffer registration for send-side fixed
zero-copy variants. When that is added, treat it as a separate pool from
caller-owned `BufGroup` rings.

That distinction matters. During probing, a 64 MiB fixed-buffer pool exceeded a
default 64 MiB memlock limit because the limit must cover the pinned payload
memory plus kernel/accounting overhead.

You can inspect the process limit before choosing `BufGroup` sizes:

```python
import resource

soft, hard = resource.getrlimit(resource.RLIMIT_MEMLOCK)

print("memlock soft limit:", soft)
print("memlock hard limit:", hard)
```

Size provided-buffer pools explicitly rather than assuming the largest useful
value is safe:

```python
buffer_size = 16 * 1024
buffer_count = 256
pool_bytes = buffer_size * buffer_count

print("planned pinned buffer pool:", pool_bytes)
```

Good default `create_buf_group()` profiles would look something like:

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

Large `BufGroup` pools may also require raising `RLIMIT_MEMLOCK`. Prefer smaller
buffers while developing the operation model, then make server profiles opt-in
and explicit.

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
