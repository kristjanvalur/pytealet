# uring-api Roadmap

`uring-api` currently targets a deliberately small `liburing >= 2.4` surface:
ring creation, probe/version reporting, Python-oriented socket operations,
completion waiting, and caller-owned completion service workers. The basic
socket I/O surface is now complete for the intended proactor baseline. This
roadmap tracks newer liburing and kernel features that remain optional,
specialised, or only worth exposing if they can keep a clear Python ownership
model.

The bias here is practical server I/O: features that improve socket throughput,
reduce completion overhead, or let applications adapt queue pressure should come
before broad opcode coverage. Remaining socket-adjacent work should not be read
as a gap in the baseline API unless it supports a Python-friendly ownership
contract.

## Current Socket Surface

The wrapper currently exposes these socket-oriented operations:

- `prepare_recv()` / `IORING_OP_RECV`
- `create_buf_group()` / caller-owned provided-buffer rings (`BufGroup`)
- `prepare_recv_buf()` / `IORING_OP_RECV` with caller-owned `BufGroup`, delivering
  leased read-only `BufView` results
- `prepare_recv_multishot()` / `IORING_OP_RECV` with `IORING_RECV_MULTISHOT`
  and caller-owned `BufGroup`, delivering leased `BufView` results per CQE
- `prepare_recvmsg()` / `IORING_OP_RECVMSG`
- `prepare_send()` / `IORING_OP_SEND`
- `prepare_send_zc()` / `IORING_OP_SEND_ZC`, retaining the submitted buffer
  until the internal `IORING_CQE_F_NOTIF` notification CQE arrives
- `prepare_sendto()` / `IORING_OP_SEND`
- `prepare_sendmsg()` / `IORING_OP_SENDMSG`
- `prepare_sendmsg_zc()` / `IORING_OP_SENDMSG_ZC`, retaining the submitted
  `msghdr` buffer until the internal `IORING_CQE_F_NOTIF` notification CQE
  arrives
- `prepare_accept()` / `IORING_OP_ACCEPT`
- `prepare_accept_multishot()` / `IORING_OP_ACCEPT` with `IORING_ACCEPT_MULTISHOT`
- `prepare_connect()` / `IORING_OP_CONNECT`
- `prepare_shutdown()` / `IORING_OP_SHUTDOWN`
- `prepare_close()` / `IORING_OP_CLOSE`, for caller-owned raw fds
- `prepare_socket()` / `IORING_OP_SOCKET`, returning a caller-owned raw fd
- `prepare_cancel()` / `IORING_OP_ASYNC_CANCEL`, for pending request handles
- `prepare_poll()` / `IORING_OP_POLL_ADD`
- `prepare_poll_multishot()` / `IORING_OP_POLL_ADD` with `IORING_POLL_MULTISHOT`
- `prepare_poll_remove()` / `IORING_OP_POLL_REMOVE`, for multishot poll handles

Basic regular-file I/O is also exposed:

- `prepare_read()` / `IORING_OP_READ`, with an explicit file offset
- `prepare_write()` / `IORING_OP_WRITE`, with an explicit file offset
- `prepare_openat()` / `IORING_OP_OPENAT`, returning a caller-owned fd; path
  strings are copied into completion-owned heap state for the submission lifetime

This is the complete basic Python-oriented socket surface for the low-level
package. It covers ordinary byte I/O, message I/O, zero-copy send lifetimes,
listener accept, multishot accept, caller-owned provided-buffer receive with
leased `BufView` delivery (one-shot and multishot), connection setup,
shutdown, fd creation/close, handle-based cancellation, readiness polling
for any pollable file descriptor, positional file read/write, and async
`openat` for caller-owned fds.

The local liburing headers also expose helpers that are not part of this
baseline. `io_uring_prep_openat2()` covers extended open resolve flags.
`prepare_statx()` now covers async metadata via `io_uring_prep_statx()`.
Fixed-buffer zero-copy
sends still require a different ownership contract than caller-owned `BufGroup`
rings and leased `BufView` results.

## Kernel Support Notes

Most currently exposed methods are baseline one-shot io_uring operations in the
installed liburing documentation. The man pages do not call out special kernel
version gates for the simple forms of `recv`, `send`, `sendto`, `recvmsg`,
`sendmsg`, `accept`, `connect`, `shutdown`, `close`, `NOP` wakeups, completion
waiting, or basic user-data cancellation.

The caveats are attached to optional variants, specialised optimisations, or
operations that intentionally stay outside the basic Python surface:

- `prepare_cancel()` uses basic user-data cancellation. The documented extended
  cancel flags are newer: `IORING_ASYNC_CANCEL_ALL`, `IORING_ASYNC_CANCEL_FD`,
  and `IORING_ASYNC_CANCEL_ANY` are available since kernel 5.19;
  `IORING_ASYNC_CANCEL_FD_FIXED` is available since kernel 6.0.
- `prepare_accept()` exposes one-shot accept. `prepare_accept_multishot()` exposes
  multishot accept, available since kernel 5.19. Both accept methods expose
  accept flags, so proactor users can request `SOCK_NONBLOCK | SOCK_CLOEXEC` for
  accepted sockets. Direct-descriptor accept still needs registered files and is
  not exposed because normal Python sockets use ordinary process fds.
- `prepare_recv()` and `prepare_recvmsg()` expose one-shot receive.
  `create_buf_group()`, `prepare_recv_buf()`, and `prepare_recv_multishot()`
  expose caller-owned provided-buffer rings and leased `BufView` delivery.
  Multishot receive is available since kernel 6.0.
  `probe()` includes a targeted `"IORING_RECV_MULTISHOT"` runtime capability
  entry because it is newer than multishot accept and depends on provided-buffer
  support. Receive/send polling hints such as
  `IORING_RECVSEND_POLL_FIRST` and `IORING_CQE_F_SOCK_NONEMPTY` are available
  since kernel 5.19, but they are optimisation hints rather than required
  baseline behaviour.
- `prepare_send_zc()` exposes basic `io_uring_prep_send_zc()`. The normal
  operation CQE is delivered to Python; the separate `IORING_CQE_F_NOTIF`
  notification CQE is consumed internally because it only closes the retained
  buffer-lifetime window. The fixed-buffer variant does not fit the current
  caller-owned buffer model.
- `prepare_sendmsg_zc()` exposes `io_uring_prep_sendmsg_zc()` with the same
  operation-CQE plus notification-CQE lifetime split. `probe()` reports
  `"IORING_OP_SEND_ZC"` and `"IORING_OP_SENDMSG_ZC"` via the documented kernel
  6.0 floor; protocol-specific `-EOPNOTSUPP` (for example `AF_UNIX`) is handled
  in higher layers rather than by a runtime zerocopy submission probe.
- `prepare_socket()` uses `IORING_OP_SOCKET`, which is a newer socket opcode even
  though the installed man page does not give a precise kernel version. `probe()`
  now includes a targeted `"IORING_OP_SOCKET"` runtime capability entry by
  submitting a private socket creation request and closing the returned fd if it
  succeeds.
- `prepare_poll()` exposes one-shot `io_uring_prep_poll_add()`. One-shot poll and
  `prepare_poll_remove()` are treated as baseline poll surface and are not probed
  separately.
- `prepare_poll_multishot()` exposes `io_uring_prep_poll_multishot()`. `probe()`
  includes a targeted `"IORING_POLL_MULTISHOT"` runtime capability entry because
  multishot poll is newer than one-shot poll. Successful poll completions expose
  the event mask as `result == res`.

Keep `probe()` focused on runtime capabilities that higher layers may need to
branch on. Ring creation is the broad availability check; targeted entries are
appropriate for operations that are either optional or known to vary across
kernels and liburing builds.

## Priorities

### 1. Queue resizing

`io_uring_resize_rings()` landed in liburing 2.9 and is available with kernel
6.13. It allows resizing SQ and CQ rings after creation, which is useful when a
server discovers that its initial completion queue was too small for real load.

A first low-level API could look like:

```python
ring.resize(sq_entries=None, cq_entries=None, clamp=True)
```

The first version should expose the primitive, not automatic policy. Callers can
then decide whether to grow the CQ after observing queue pressure.

Important constraints:

- kernel 6.13 or newer is required;
- the ring must be created with `IORING_SETUP_DEFER_TASKRUN`;
- resizing a CQ ring that is already in overflow is not legal;
- liburing currently does not support resizing rings created with
  `IORING_SETUP_NO_MMAP`, even though the kernel can;
- recent liburing releases include resize cleanup fixes, so CI should exercise
  the exact liburing versions we claim to support.

Open design questions:

- Should `Ring` expose `resize()` only, or also an `is_resize_supported()` helper?
- Should `probe()` report resize capability by trying a tiny resizable ring?
- Should `UringProactor` default to a resize-friendly setup flag once resize is
  exposed?

### 2. Provided-buffer receive status

Caller-owned provided-buffer receive is implemented. `Ring.create_buf_group()`
registers a provided-buffer ring. `prepare_recv_buf()` and
`prepare_recv_multishot()` both require that `BufGroup` and deliver read-only
`BufView` completion results. Non-empty views keep the selected buffer alive
until the last exported `memoryview` is released, then recycle the buffer back
to the ring. EOF (`res == 0`) also returns an empty `BufView` with
`length == 0` rather than `bytes`.

**TODO — C API BufGroup surface:** the capsule can submit with a borrowed
`PyObject *buf_group`, but it does **not** create, close, or own `BufGroup`
lifetimes. Python has `create_buf_group()`, `BufGroup.close()`, and optional
`release_callback` for pool reuse (e.g. tealetio size cache). When pure-C
owners need the same contract, append vtable entries such as
`ring_create_buf_group`, `buf_group_close`, Python and/or C release hooks, and
the usual getters (`buffer_size`, `leased_count`, …). Check `struct_size` /
null pointers; no need to bump pre-release `abi_version` solely for appends.

```python
buf_group = ring.create_buf_group(buffer_size=16384, buffer_count=256)
pending = ring.prepare_recv_buf(fd, buf_group, user_data=token)
completion = ring.wait()
view = memoryview(completion.result)
try:
    process(view)
finally:
    del view
```

Use `prepare_recv_multishot()` when one submission should produce many leased
views until EOF, cancellation, or `-ENOBUFS`:

```python
handle = ring.prepare_recv_multishot(fd, buf_group, user_data=token)
completion = ring.wait()
view = memoryview(completion.result)
try:
    process(view)
finally:
    del view
ring.prepare_cancel(handle)
```

When the buffer ring is exhausted, multishot receive terminates with
`-ENOBUFS`. Return leased buffers to the ring and submit a new multishot
receive; higher layers such as `tealetio.UringProactor` resubmit automatically
and surface buffer pressure to consumers.

### 3. Multishot receive status

`io_uring_prep_recv_multishot()` is available since kernel 6.0 and pairs with
provided buffers. One submission can produce many completions until the kernel
clears `IORING_CQE_F_MORE`, the stream ends, the operation is cancelled, or the
buffer ring runs dry.

The Python API is implemented on the leased `BufView` model only. Delivered
multishot completions carry `sequence` numbers because worker-thread delivery
can be out of order even though CQEs are reaped in order. Recv multishot uses
that value to reconstruct stream order across resubmits after `-ENOBUFS`; accept
multishot carries the same ordinal for symmetry even though accept ordering has
less semantic weight.

Callers must drop exported memoryviews (or call `memoryview.release()`) before
the kernel buffer can be recycled.

The completion object still needs enough flag helpers to decode:

- whether `IORING_CQE_F_MORE` is set;
- the selected buffer ID;
- socket non-empty hints;
- receive bundle metadata when supported.

Kernel gates worth tracking:

- multishot receive: kernel 6.0;
- `IORING_RECVSEND_POLL_FIRST` and `IORING_CQE_F_SOCK_NONEMPTY`: kernel 5.19;
- recv bundles: kernel 6.10.

### 4. Multishot accept status

`io_uring_prep_multishot_accept()` is available since kernel 5.19 and is a clear
server-performance enhancer. One accept request can produce accepted sockets as
connections arrive, again using `IORING_CQE_F_MORE` to indicate whether the
request remains active.

The kernel documents that a shared `addr`/`addrlen` buffer is unsafe across
multiple multishot legs: a later accept can overwrite the peer sockaddr before
earlier CQEs are processed. `prepare_accept_multishot()` therefore passes
`NULL, NULL` (the liburing-recommended shape). Delivered completions expose the accepted fd in `completion.res` and
`completion.result`; callers that need the peer address should call
`getpeername()` on that fd. One-shot and multishot accept share this shape.

The low-level API is implemented and uses `prepare_cancel()` for explicit
teardown:

```python
# positional: fd, user_data, flags, base_sequence (METH_FASTCALL; no kwargs)
handle = ring.prepare_accept_multishot(fd, user_data, socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC, 0)
ring.prepare_cancel(handle)
```

`base_sequence` seeds `completion.sequence` for the first accept leg (same idea
as `prepare_recv_multishot`), so a continuous arm can continue after eager
accepts already used earlier indices.

At the `tealetio` layer, this likely wants a higher-level accept stream or a
server helper, not just `Operation[tuple[socket.socket, address]]`, because one
submitted request produces multiple accepted sockets.

### 5. Poll and readiness status

`io_uring_prep_poll_add()` and `io_uring_prep_poll_remove()` expose readiness
notifications through the ring. They are not socket-specific: any pollable file
descriptor can use them, so they may matter for sockets, pipes, terminals, and
some file-I/O coordination patterns.

For sockets, poll requests can support readiness-style APIs, backpressure, and
integration points where a higher layer wants to wait for `POLLIN`, `POLLOUT`,
or error/hangup readiness rather than submit an immediate recv/send operation.
They remain optional for the planned proactor because the core API submits real
I/O operations and receives completions directly.
`io_uring_prep_poll_multishot()` is especially relevant for servers because one
registration can produce repeated CQEs, using `IORING_CQE_F_MORE` to show that
the request remains active.

The low-level API is implemented and keeps the surface close to `poll(2)`:

```python
handle = ring.prepare_poll(fd, mask, user_data)
completion = ring.wait()
assert completion.result == completion.res  # event mask

handle = ring.prepare_poll_multishot(fd, mask, user_data)
completion = ring.wait()
ring.prepare_poll_remove(handle)
```

One-shot poll returns the pending handle as the delivered completion. Multishot
poll follows the same shell / terminal contract as multishot accept and recv:

- **MORE** legs deliver a shell `Completion` (copied `user_data`, leg
  `sequence`); the submitted armed handle stays pending (shells do not re-arm
  reverse links).
- **Terminal** `!MORE` (including after `prepare_poll_remove()`) delivers the
  **armed handle itself**.

`prepare_poll_remove()` is distinct from `prepare_cancel()` because poll removal is
the kernel-supported teardown path for multishot poll handles.

The native C API (`uring_api_capi.h`) exposes `ring_construct_poll()`,
`ring_construct_poll_multishot()`, and `ring_construct_poll_remove()`. C clients
prepare with `ring_prepare()` and flush with `ring_submit()`.

Remaining design questions:

- Should poll completions grow named helpers for common readiness bits, or is the
  raw event mask enough for the low-level package?
- Should higher layers (`tealetio`, proactor helpers) wrap poll behind a
  readiness stream API similar to multishot accept?

### 6. Socket command operations

`io_uring_prep_cmd_sock()` arrived in liburing 2.5. It can expose async socket
commands such as `SIOCINQ`, `SIOCOUTQ`, `getsockopt()`, and `setsockopt()`.
These are useful for richer socket management, but they are not core proactor
I/O. Common setup such as `bind()`, `listen()`, and most `setsockopt()` calls is
normally done synchronously when creating the listener socket.

Kernel support is command-specific. For example, the current man page documents
`SIOCINQ` and `SETSOCKOPT` availability from kernel 6.7, while newer socket
commands appear in later kernels.

### 7. NAPI busy-poll tuning

`io_uring_register_napi()` can reduce network round-trip latency for specialised
network workloads. It should be an explicit low-level tuning API, not a default
`UringProactor` policy.

Possible shape:

```python
ring.register_napi(timeout_us=50, prefer_busy_poll=True)
ring.unregister_napi()
```

### 8. Registered waits and wait regions

Registered waits reduce repeated timeout/signal-mask copying for wait calls.
This could matter once completion workers use more advanced wait strategies, but
it is less urgent while `serve_completions()` blocks in a simple completion wait.

### 9. Application-owned ring memory and memory sizing helpers

liburing 2.5 added application-allocated ring memory through
`io_uring_queue_init_mem()`, and later releases added helpers for sizing the
required memory. This may be useful for hugepage placement or embedding, but it
should wait until the socket performance surface is clearer.

### 10. Multi-buffer / vector stream send (for tealetio `SendBuffer`)

`prepare_sendmsg()` already exists for message-oriented I/O with a single
payload buffer. Stream writers in `tealetio` currently coalesce line-sized
`write()` calls into one `bytearray` and issue a single `send` per drain
cycle (asyncio proactor style).

A useful next step is a **scatter-gather stream send** that keeps several
caller-owned buffers without joining them:

- either multi-iovec `sendmsg` with a retained buffer list until CQE, or
- a small dedicated API if liburing/kernel makes fixed multi-buffer send
  clearer than hand-built `msghdr` iovecs.

Higher layers would then queue a list of chunks and submit one vector op
instead of copy-joining. Priority is medium: coalescing already covers the
common line-at-a-time case; vector send matters when large mixed writes make
the join cost visible.

### 11. Breadth opcodes

Futex, waitid, pipe operations, fixed file installation, bind/listen helpers,
and similar additions are useful, but they are not the first server-socket
performance bottleneck. Add them when a concrete higher-level use case appears.

## Capability Reporting

New operations should be capability-gated in two layers:

- compile-time: whether the built liburing headers expose the helper and needed
  constants;
- runtime: whether the current kernel accepts the operation or setup flags.

`probe()` already reports compiled liburing version and basic ring creation
success. Future additions should either extend `probe()` with targeted capability
checks or let methods fail with precise `OSError` values and document the common
kernel gates.

For high-level code, avoid assuming that a new liburing header implies runtime
support. Containers, seccomp, process limits, and older kernels can all reject
specific setup flags or opcodes.

## Queue Pressure Notes

There are two different queue-pressure failure modes:

- no submission queue entry is currently available;
- the completion queue is too small and completions can overflow.

The current native prepare path (`get_sqe`) handles the first case according
to `Ring.auto_submit` (default true): flush already-prepared SQEs when the SQ
is full, then retry. With `IORING_SETUP_SQPOLL`, if a slot is still unavailable
it waits for the poller (with a timeout). If a slot still cannot be obtained
after that, it raises `RuntimeError` — a stuck queue / dead poller. When
`auto_submit` is false, a full SQ raises `SubmissionQueueFull` so the caller
can `submit()` and retry.

Queue resizing can still help if the application needs more concurrent
in-flight prepares, but `tealetio` does **not** defer failed prepares onto a
FIFO and retry after CQEs. Normal producer flow is already limited by waitables
parked on incomplete operations; a hard “max deferred SQEs” guard is not part
of the current design.

### `UringProactor` submission threading and `IORING_SETUP_SINGLE_ISSUER`

`tealetio.UringProactor` may prepare SQEs from completion-service threads for
next-leg work (oneshot continuous fallbacks, partial send legs, ENOBUFS stream
re-arm at higher layers). There is no deferred SQ-full queue for workers to
drain. Application code may also prepare through the shared `uring_api.Ring`
from other threads; the extension serialises ring access with a critical
section, but the kernel still sees the calling OS thread.

`IORING_SETUP_SINGLE_ISSUER` is exposed as `uring_api.IORING_SETUP_SINGLE_ISSUER`
and may be passed through `UringProactor(flags=...)` after `probe(flags=...)`
accepts it. **`UringProactor` does not enable this flag by default.** The
kernel enforces that every SQE comes from one owning thread; violating that
returns `-EEXIST`.

We considered routing all prepares through a single issuer thread so the flag
could be enabled safely. That model is **not** the current plan:

- the kernel optimisation is only a hint and is hard to quantify for this stack;
- marshaling every prepare through Python thread hand-off adds latency for
  next-leg arms and ENOBUFS stream re-arm on completion paths;
- worker-thread and callback-thread prepare is intentional for completion
  workers, continuous-operation callbacks, and future threaded backends.

Callers that want `IORING_SETUP_SINGLE_ISSUER` must guarantee one kernel-visible
issuer themselves. A dedicated issuer thread that only drains a queue is a
possible future experiment, not the default `UringProactor` shape.

CQ resizing is different. It helps when completions accumulate faster than the
application can reap them, especially in server workloads with bursts or
multishot operations. The API should expose enough diagnostics to distinguish SQ
slot exhaustion from CQ pressure before adding automatic resize policy.
