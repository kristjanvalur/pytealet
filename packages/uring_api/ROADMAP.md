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

- `submit_recv()` / `IORING_OP_RECV`
- `submit_recv_multishot()` / `IORING_OP_RECV` with `IORING_RECV_MULTISHOT`,
  copying selected provided buffers into Python `bytes`
- `submit_recvmsg()` / `IORING_OP_RECVMSG`
- `submit_send()` / `IORING_OP_SEND`
- `submit_send_zc()` / `IORING_OP_SEND_ZC`, retaining the submitted buffer
  until the internal `IORING_CQE_F_NOTIF` notification CQE arrives
- `submit_sendto()` / `IORING_OP_SEND`
- `submit_sendmsg()` / `IORING_OP_SENDMSG`
- `submit_sendmsg_zc()` / `IORING_OP_SENDMSG_ZC`, retaining the submitted
  `msghdr` buffer until the internal `IORING_CQE_F_NOTIF` notification CQE
  arrives
- `submit_accept()` / `IORING_OP_ACCEPT`
- `submit_accept_multishot()` / `IORING_OP_ACCEPT` with `IORING_ACCEPT_MULTISHOT`
- `submit_connect()` / `IORING_OP_CONNECT`
- `submit_shutdown()` / `IORING_OP_SHUTDOWN`
- `submit_close()` / `IORING_OP_CLOSE`, for caller-owned raw fds
- `submit_socket()` / `IORING_OP_SOCKET`, returning a caller-owned raw fd
- `submit_cancel()` / `IORING_OP_ASYNC_CANCEL`, for pending request handles

This is the complete basic Python-oriented socket surface for the low-level
package. It covers ordinary byte I/O, message I/O, zero-copy send lifetimes,
listener accept, multishot accept, copied multishot receive, connection setup,
shutdown, fd creation/close, and handle-based cancellation.

The local liburing headers also expose socket-adjacent helpers that are not part
of this baseline. `io_uring_prep_poll_add()` / `io_uring_prep_poll_remove()` are
readiness APIs rather than core completion proactor operations. Fixed-buffer
zero-copy sends and public provided-buffer rings require ownership contracts
that do not map cleanly onto ordinary Python buffers. The remaining plausible
receive-side extension is a zero-copy multishot receive API with explicit leased
buffers, but that should be treated as a separate design rather than a missing
piece of the baseline.

## Kernel Support Notes

Most currently exposed methods are baseline one-shot io_uring operations in the
installed liburing documentation. The man pages do not call out special kernel
version gates for the simple forms of `recv`, `send`, `sendto`, `recvmsg`,
`sendmsg`, `accept`, `connect`, `shutdown`, `close`, `NOP` wakeups, completion
waiting, or basic user-data cancellation.

The caveats are attached to optional variants, specialised optimisations, or
operations that intentionally stay outside the basic Python surface:

- `submit_cancel()` uses basic user-data cancellation. The documented extended
  cancel flags are newer: `IORING_ASYNC_CANCEL_ALL`, `IORING_ASYNC_CANCEL_FD`,
  and `IORING_ASYNC_CANCEL_ANY` are available since kernel 5.19;
  `IORING_ASYNC_CANCEL_FD_FIXED` is available since kernel 6.0.
- `submit_accept()` exposes one-shot accept. `submit_accept_multishot()` exposes
  multishot accept, available since kernel 5.19. Both accept methods expose
  accept flags, so proactor users can request `SOCK_NONBLOCK | SOCK_CLOEXEC` for
  accepted sockets. Direct-descriptor accept still needs registered files and is
  not exposed because normal Python sockets use ordinary process fds.
- `submit_recv()` and `submit_recvmsg()` expose one-shot receive.
  `submit_recv_multishot()` exposes multishot receive with internal buffer-ring
  ownership and `bytes` delivery. Multishot receive is available since kernel
  6.0. `probe()` includes a targeted `"IORING_RECV_MULTISHOT"` runtime
  capability entry because it is newer than multishot accept and depends on
  provided-buffer support. A future zero-copy multishot receive API would need a
  separate leased-buffer model. Receive/send polling hints such as
  `IORING_RECVSEND_POLL_FIRST` and `IORING_CQE_F_SOCK_NONEMPTY` are available
  since kernel 5.19, but they are optimisation hints rather than required
  baseline behaviour.
- `submit_send_zc()` exposes basic `io_uring_prep_send_zc()`. The normal
  operation CQE is delivered to Python; the separate `IORING_CQE_F_NOTIF`
  notification CQE is consumed internally because it only closes the retained
  buffer-lifetime window. The fixed-buffer variant does not fit the current
  caller-owned buffer model.
- `submit_sendmsg_zc()` exposes `io_uring_prep_sendmsg_zc()` with the same
  operation-CQE plus notification-CQE lifetime split. `probe()` includes a
  targeted `"IORING_OP_SEND_ZC"` and `"IORING_OP_SENDMSG_ZC"` runtime capability
  entry based on a UDP loopback `sendmsg_zc` send to a bound local receiver, so
  the probe avoids TCP connection setup while still testing a real destination.
- `submit_socket()` uses `IORING_OP_SOCKET`, which is a newer socket opcode even
  though the installed man page does not give a precise kernel version. `probe()`
  now includes a targeted `"IORING_OP_SOCKET"` runtime capability entry by
  submitting a private socket creation request and closing the returned fd if it
  succeeds.

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

### 2. Optional zero-copy multishot receive

Copied `submit_recv_multishot()` is already part of the baseline. It uses an
internal provided-buffer ring, copies selected buffers into Python `bytes`, and
recycles the kernel buffers right away. That keeps ownership simple and works
well for a Python proactor.

A later zero-copy receive API would need explicit leased-buffer ownership rather
than exposing raw provided-buffer management as a general low-level primitive:

```python
handle = ring.submit_recv_multishot_buf(fd, buf_group, user_data=token)
with completion.result as view:
  process(memoryview(view))
```

The result object would need to keep the selected buffer alive until the caller
returns it. It also needs to account for active exported memoryviews before
recycling the buffer back to the kernel. That is why public provided-buffer
management is not part of the baseline API.

### 3. Multishot receive status

`io_uring_prep_recv_multishot()` is available since kernel 6.0 and pairs with
provided buffers. One submission can produce many completions until the kernel
clears `IORING_CQE_F_MORE`.

This is a major performance feature, and the first Python API is implemented. It
keeps lifetime simple by letting `_uring_api` own the provided-buffer ring
internally, copy each selected buffer into a Python `bytes` object, and recycle
the kernel buffer right away:

```python
handle = ring.submit_recv_multishot(fd, buffer_size=16384, buffer_count=256, user_data=token)
ring.submit_cancel(handle)
```

Delivered multishot completions carry `sequence` numbers because worker thread
delivery can be out of order even though CQEs are reaped in order. Recv
multishot uses that value to reconstruct stream order; accept multishot carries
the same ordinal for symmetry even though accept ordering has less semantic
weight.

A later zero-copy API can expose leased buffer objects, but those objects must
return buffers to the original operation before the kernel can reuse them. That
is a separate ownership contract from the current copied-`bytes` model.

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

The low-level API is implemented and uses `submit_cancel()` for explicit
teardown:

```python
handle = ring.submit_accept_multishot(fd, user_data, flags=socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC)
ring.submit_cancel(handle)
```

At the `tealetio` layer, this likely wants a higher-level accept stream or a
server helper, not just `Operation[tuple[socket.socket, address]]`, because one
submitted request produces multiple accepted sockets.

### 5. Poll and readiness operations

`io_uring_prep_poll_add()` and `io_uring_prep_poll_remove()` expose readiness
notifications through the ring. They are not socket-specific: any pollable file
descriptor can use them, so they may matter for sockets, pipes, terminals, and
some file-I/O coordination patterns.

For sockets, poll requests can support readiness-style APIs, backpressure, and
integration points where a higher layer wants to wait for `POLLIN`, `POLLOUT`,
or error/hangup readiness rather than submit an immediate recv/send operation.
They are optional for the planned proactor because the core API submits real
I/O operations and receives completions directly.
`io_uring_prep_poll_multishot()` is especially relevant for servers because one
registration can produce repeated CQEs, using `IORING_CQE_F_MORE` to show that
the request remains active.

A first low-level API could keep this close to `poll(2)`:

```python
handle = ring.submit_poll(fd, mask, user_data)
handle = ring.submit_poll_multishot(fd, mask, user_data)
ring.submit_poll_remove(handle)
```

Open design questions:

- Should poll completions expose the raw event mask only, or named helpers for
  common readiness bits?
- Should `submit_poll_remove()` be distinct from `submit_cancel()`, or should
  the cancellation API grow typed helpers for poll requests?
- Should multishot poll share the same pending-completion lifetime rules as
  future multishot accept/recv APIs?

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

### 10. Breadth opcodes

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

The current native submission path handles the first case by trying
`io_uring_get_sqe()`, submitting already-prepared SQEs if no slot is available,
then trying `io_uring_get_sqe()` again. If the second attempt still fails, it
raises `RuntimeError("no submission queue entries available")`.

That is an SQ pressure signal. Queue resizing can help if the application really
needs more in-flight submissions, but immediate backpressure may still be the
right answer.

For `tealetio`, the likely policy is to treat SQ exhaustion as deferrable
submission work. `_uring_api` should raise a dedicated low-level exception, and
`UringProactor` can keep the `Operation` pending while retrying submission after
future completions. That does not require a hard backlog limit by default: the
tealet that requested the operation is suspended while the `Operation` is
pending, which already throttles normal producer flow. A maximum deferred
submission count can still be an opt-in overload guard for embedders or unusual
producer patterns.

CQ resizing is different. It helps when completions accumulate faster than the
application can reap them, especially in server workloads with bursts or
multishot operations. The API should expose enough diagnostics to distinguish SQ
slot exhaustion from CQ pressure before adding automatic resize policy.
