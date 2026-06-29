# uring-api Roadmap

`uring-api` currently targets a deliberately small `liburing >= 2.4` surface:
ring creation, probe/version reporting, one-shot socket operations, completion
waiting, and caller-owned completion service workers. This roadmap tracks newer
liburing and kernel features that look worth exposing once the baseline API has
settled.

The bias here is practical server I/O: features that improve socket throughput,
reduce completion overhead, or let applications adapt queue pressure should come
before broad opcode coverage.

## Current Socket Surface

The wrapper currently exposes these socket-oriented operations:

- `submit_recv()` / `IORING_OP_RECV`
- `submit_recvmsg()` / `IORING_OP_RECVMSG`
- `submit_send()` / `IORING_OP_SEND`
- `submit_sendto()` / `IORING_OP_SEND`
- `submit_accept()` / `IORING_OP_ACCEPT`
- `submit_connect()` / `IORING_OP_CONNECT`
- `submit_shutdown()` / `IORING_OP_SHUTDOWN`
- `submit_close()` / `IORING_OP_CLOSE`, for caller-owned raw fds
- `submit_cancel()` / `IORING_OP_ASYNC_CANCEL`, for pending request handles

The local liburing headers also expose socket-relevant helpers that are not yet
wrapped: `io_uring_prep_sendmsg()`, `io_uring_prep_recv_multishot()`,
`io_uring_prep_socket()`, `io_uring_prep_poll_add()` /
`io_uring_prep_poll_remove()`, zero-copy send helpers, and provided-buffer
management. `poll_*` and provided buffers are not socket-only, but they matter
for high-throughput socket designs.

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

### 2. `sendmsg` and socket creation

`io_uring_prep_sendmsg()` is the obvious companion to the existing `recvmsg`
support. The first wrapper can target the common datagram/vector send case and
return `None` for the result on success or the sent byte count if that proves
more useful for parity with `send`.

`io_uring_prep_socket()` can create sockets asynchronously and returns a new fd.
Like accept and close, this must keep ownership explicit: the low-level API
should return the fd, and higher-level wrappers can decide whether to wrap it in
a `socket.socket` object.

### 3. Provided buffers and buffer rings

Provided buffers are the foundation for efficient receive-heavy networking.
They let the kernel select a buffer from a registered group and report the chosen
buffer ID in the completion flags. Newer liburing releases improve buffer-ring
management, including partial consumption support.

A low-level API should probably introduce an explicit buffer-pool object rather
than hiding pinned memory behind socket methods:

```python
pool = ring.register_buffer_ring(buffer_size=16384, buffer_count=256)
ring.submit_recv_buffer_select(fd, pool, user_data)
```

This should stay separate from ring sizing. Ring entries control queue depth;
registered buffers control pinned payload memory and can hit `RLIMIT_MEMLOCK`.

### 4. Multishot receive

`io_uring_prep_recv_multishot()` is available since kernel 6.0 and pairs with
provided buffers. One submission can produce many completions until the kernel
clears `IORING_CQE_F_MORE`.

This is a major performance feature, but it changes the current one-submission,
one-completion model. The Python API should make the lifetime explicit:

```python
handle = ring.submit_recv_multishot(fd, buffer_group, user_data)
handle.cancel()
```

The completion object will need to expose enough flag helpers to decode:

- whether `IORING_CQE_F_MORE` is set;
- the selected buffer ID;
- socket non-empty hints;
- receive bundle metadata when supported.

Kernel gates worth tracking:

- multishot receive: kernel 6.0;
- `IORING_RECVSEND_POLL_FIRST` and `IORING_CQE_F_SOCK_NONEMPTY`: kernel 5.19;
- recv bundles: kernel 6.10.

### 5. Multishot accept

`io_uring_prep_multishot_accept()` is available since kernel 5.19 and is a clear
server-performance enhancer. One accept request can produce accepted sockets as
connections arrive, again using `IORING_CQE_F_MORE` to indicate whether the
request remains active.

A low-level API could mirror multishot recv:

```python
handle = ring.submit_accept_multishot(fd, user_data)
handle.cancel()
```

At the `tealetio` layer, this likely wants a higher-level accept stream or a
server helper, not just `Operation[tuple[socket.socket, address]]`, because one
submitted request produces multiple accepted sockets.

### 6. Socket command operations

`io_uring_prep_cmd_sock()` arrived in liburing 2.5. It can expose async socket
commands such as `SIOCINQ`, `SIOCOUTQ`, `getsockopt()`, and `setsockopt()`.
These are useful for richer socket management, but less central than multishot
I/O.

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
