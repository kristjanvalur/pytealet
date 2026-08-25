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
`Ring` exposes direct prepare wrappers for the common Python-oriented cases:

- stream I/O: `prepare_recv()`, and provided-buffer `prepare_recv_buf()` /
  `prepare_recv_multishot()` via `create_buf_group()`;
- message I/O: `prepare_recvmsg()`, `prepare_sendto()`, `prepare_sendmsg()`, and
  zero-copy `prepare_sendmsg_zc()`;
- listeners and setup: `prepare_accept()`, `prepare_accept_multishot()`,
  `prepare_connect()`, and `prepare_socket()`;
- lifecycle: `prepare_shutdown()`, `prepare_close()`, nowait
  `prepare_*_nowait()` for close/shutdown/cancel/poll_remove, send helpers
  `prepare_send()` / `prepare_send_zc()`, and `wait()` for completion reaping.

Each submitted operation carries a Python `user_data` object which comes back
with its completion. `completion.user_data` is **settable** (assign `None` or
`del` to clear); use that after delivery to drop cycles with waitables. Kernel
SQE identity remains the `Completion` pointer, not `user_data`. Inspect the
semantic operation with `completion.kind` (`CompletionKind` enum, or the
matching `COMPLETION_KIND_*` constants) when callbacks need to branch on
completion type rather than inferring from `result` alone.

### Multishot delivery contract

Multishot ops (`prepare_*_multishot`) keep one **armed** `Completion` handle —
the object returned from prepare, also the cancel / poll_remove target. Kernel
SQE `user_data` always points at that handle.

- **Intermediate legs** (`IORING_CQE_F_MORE`): delivery is a fresh **shell**
  `Completion` that copies `user_data` (and leg `sequence`) from the armed
  handle. `clear_user_data()` on the armed handle defers while more CQEs are
  staged, so a concurrent `!MORE` delivery cannot clear the slot first. The
  armed object is left untouched; shells do not re-arm reverse links.
- **Terminal leg** (`!MORE`, including cancel / poll_remove / `-ENOBUFS` /
  stream end): delivery **is** the armed handle itself. Call
  `completion.clear_user_data()` (or assign `None`) to drop the cycle with
  any waitable that stored the reverse link.

Clients that reverse-link waitable → `Completion` should call
`clear_user_data()` on every delivered object (shell or terminal); only the
terminal clear hits the armed handle, and that clear waits until every staged
leg has been packaged. Do not assume every multishot CQE is a distinct object
— only MORE legs are.

**In-flight handle.** Prepare holds an extra reference on the armed
`Completion` because the kernel SQE stores only a pointer. Oneshot drops that
ref when its CQE is packaged. Multishot and zero-copy send share the pointer
across several CQEs, so the extra ref stays until every **already staged** CQE
for that handle has been turned into a Python object (MORE shells first, then
the parent on `!MORE`; for `send_zc`, until the internal `NOTIF` CQE). A
per-handle counter (`aux_refcount`) plus a sticky “terminal was staged” flag
(`AUX_DECREF`) make it safe to package `!MORE` before an earlier MORE row in
the same batch — the parent is not `DECREF`’d while a shell still needs it.

**Safe clear.** `clear_user_data()` on a shell or idle handle writes `None`
immediately. On an armed handle that still has staged CQEs it only sets
`USER_DATA_CLEAR` and leaves the live slot so a not-yet-built MORE shell can
copy the waitable. The real clear runs after the last staged leg is packaged
(same window as the in-flight ref). The flag and the pointer are updated
together under the ring’s refcount mutex; the old waitable is released after
the lock is dropped.

Calling `clear_user_data()` or assigning `user_data` after the `Ring` object
has been deallocated is **undefined**. `ring.close()` is fine (the mutex
still belongs to the live `Ring`); dropping the last reference so the ring
is collected while a handle remains is not a supported use.

**Send-all:** `prepare_send_all(fd, data)` (or `construct_send_all` then
`prepare`) is a synthetic drain: the kernel still sees ordinary send SQEs, but
Python gets one `Completion` when the buffer is exhausted. Partial CQEs re-arm
the remainder internally (`POLL_FIRST` on later legs when probed). Success
`res` is the total byte count, clamped to `INT_MAX`; `result` is the full
unsigned count. Zero-byte send on a non-empty remainder fails
with `-EAGAIN`. `nowait` keeps the handle off `wait()`; failures use
`nowait_error_handler`. Unlike other nowait helpers, nowait `send_all` still
holds the prepare in-flight ref and is included in `pending_count()` until the
drain terminals. `prepare_cancel` of the handle abandons further legs: a parked
continuation completes `-ECANCELED` instead of flushing another send.
Next-leg SQEs stay in the SQ until `wait()` / `submit()` / SQ-full, like other
prepares. With `auto_submit` off, `wait()` does not publish them — call
`submit()` as with any other prepared SQE (including after an empty wait batch
while `pending_count()` is still non-zero). The next user `prepare` fills parked
next-legs first (they take the SQ slot ahead of the new op; `auto_submit` makes
room if the SQ is full). `submit()` never raises `SubmissionQueueFull`: a full
SQ is submitted first, then parked legs are filled. The returned count is
every SQE that enter submitted, including those flushed to make room.
Set
`Ring(..., experimental_send_all_submit_next=True)` (or the property) to
`io_uring_submit` each next-leg immediately — experimental, for comparing
delayed vs eager enter cost. While a send-all is busy on an fd,
`prepare` of send/close/shutdown/another send-all on that fd parks on a
per-fd conflict FIFO (`prepared` stays false until drain copies it into the
SQ). Recv is full-duplex and still fills an SQE. `prepare([send_all, close])`
therefore serialises in one batch. Cancel of the active drain still fills an
SQE; cancel of a **queued** op stays behind it; cancel of an already-prepared
(SQ / in-kernel) send on that fd fills an SQE now. SQ-full still raises
`SubmissionQueueFull` from `prepare` when `auto_submit` is off — it does not
spill onto the FIFO. Once an fd has used send-all, later send/shutdown/close
on it should go through the ring until that fd is idle (libc `close()` while a
drain is live stales the table).

**Lazy submit:** `prepare_*` / nowait helpers (including cancel and poll_remove)
only fill SQEs. Work becomes kernel-visible when you call `ring.submit()`,
when **`auto_submit` is on (the default) and `wait()` / serve flush pending
SQEs at entry** (if this thread may submit), when prepare hits a full SQ, or
after delivery batches. Set `Ring(..., auto_submit=False)` or
`ring.auto_submit = False` to raise `SubmissionQueueFull` instead of flushing
from prepare, and to make wait/serve leave prepared SQEs unsubmitted until
you call `submit()`. `Ring.prepare(...)` returns the number of entries
successfully prepared. With `auto_submit` on, do not call `submit()` before
every `wait()` — wait does that. With completion workers parked only on
`wait_idle`, the issuer still flushes before that park (workers never call
`wait()`).

**Pending count:** `ring.pending_count()` is the number of waitable
`Completion`s that still hold the prepare in-flight ref. It goes up at
successful waitable `prepare`, and down when that ref is dropped (oneshot
CQE packaged, or multishot / `send_zc` / `send_all` after the terminal CQE).
Construct without prepare, ordinary nowait helpers, and MORE shells do not
change it. Nowait `send_all` is the exception: it keeps the in-flight ref
until the drain terminals.

**Construct then prepare:** every waitable op has `construct_*` (bind cargo,
no SQE) and `prepare_*` (construct + prepare of one handle). Cargo lives on
the matching sidecar; `completion.prepared` is false until an SQE is filled.
Arm a reverse link on the constructed object, then `ring.prepare(completion)`
or `ring.prepare([c1, c2, ...])`. `prepare` returns the number filled and does
not submit; `wait()` / `submit()` flush as usual (or a full SQ if
`auto_submit` is on). On prepare error, earlier entries in the list may
already have SQEs.

```python
pending = []
for chunk in outgoing:
    completion = ring.construct_send(fd, chunk, 0, token)
    # arm reverse here — nothing can complete yet
    pending.append(completion)
ring.prepare(pending)
batch = ring.wait(1.0)
```

Internal `break_wait` NOPs use tagged `user_data` (wake `…01`). Nowait prepare
stamps a tagged nowait token (`…11` with kind/fd payload), not a `Completion*`;
a constructed nowait handle is only a temporary hold for `prepare` and is never
delivered to the client.

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
        ring.prepare_recv(reader.fileno(), buf, 0, token)
        # optional explicit flush; wait() also flushes first
        ring.submit()
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
object just to keep memory valid. `prepare_send_zc()` uses
`IORING_OP_SEND_ZC`, while `prepare_sendmsg_zc()` uses `IORING_OP_SENDMSG_ZC` for
the `sendmsg` shape. Their ordinary operation CQE is delivered as the submitted
`Completion`; the later `IORING_CQE_F_NOTIF` buffer-lifetime CQE is consumed
internally and releases the retained buffer.

`prepare_shutdown()` is a socket operation and mirrors `shutdown(fd, how)`.
`prepare_*` / `construct_*` take SQE cargo first and `user_data` last.
METH_FASTCALL helpers (send, accept, poll, close, …) are positional-only: a
three-arg `prepare_send(fd, data, x)` is flags, not a token. `openat` is
`prepare_openat(dfd, path, flags, mode=0, user_data=None)`.
`prepare_recv` / `prepare_recvmsg` take `flags` like send. Include
`IORING_RECVSEND_POLL_FIRST` to poll before the first recv/send; the
extension puts that bit in SQE `ioprio` and leaves other `MSG_*` in
`msg_flags`. **`POLL_FIRST` is `1 << 0`, the same bit as `MSG_OOB`:**
that value is treated as poll-first, not out-of-band. Do not pass
`socket.MSG_OOB` in this word. A separate ioprio argument can wait
until we need more `IORING_RECVSEND_*` bits.
`probe()["IORING_RECVSEND_POLL_FIRST"]` is the 5.19 floor. Do **not**
combine `POLL_FIRST` with `prepare_recv_multishot` — liburing does not
test that pairing, and the kernel can leave a `MORE` handle with no
terminal CQE. The bit is ignored on multishot prepare.
`IORING_CQE_F_SOCK_NONEMPTY` may appear on `completion.flags` after a recv
when more data is already queued.
`prepare_accept()` and `prepare_accept_multishot()` accept optional accept flags;
pass `socket.SOCK_NONBLOCK | socket.SOCK_CLOEXEC` when accepted sockets should
be ready for proactor ownership without a follow-up `fcntl()` call.
Multishot first-leg numbering is `completion.sequence`, not a construct
argument: set it after `construct_*` / `prepare_*` when continuing a stream
after eager accepts or `ENOBUFS` re-arm. `prepare_recv_multishot` is the same
shape (`fd, buf_group, flags=0, user_data=None`).
`prepare_accept()` and `prepare_accept_multishot()` deliver the accepted fd in
`completion.res` and `completion.result`. Call `getpeername()` on the fd when
you need the peer address.
`prepare_close()` is lower-level: pass only a raw fd whose ownership has already
been transferred away from Python objects such as `socket.socket`, for example
with `detach()`. Otherwise, Python and the kernel may both believe they own the
same descriptor. When you do not need a result or waitable handle, nowait
helpers construct a temporary `Completion`, prepare a tagged nowait SQE, and
drop the handle: `prepare_close_nowait(fd)`,
`prepare_shutdown_nowait(fd, how)`, `prepare_cancel_nowait(completion)`, and
`prepare_poll_remove_nowait(completion)`. They return `None`, and never deliver via `wait()` or callbacks.
To batch with waitable ops, use `construct_close_nowait(fd)` (or set
`completion.nowait = True` on a constructed close/shutdown/cancel/poll_remove)
and pass it to `prepare`. On kernels with
`IORING_FEAT_CQE_SKIP`, successful nowait ops post no CQE
(`IOSQE_CQE_SKIP_SUCCESS`). Failed nowait CQEs (`res < 0`) invoke optional
`Ring.nowait_error_handler` (successful CQEs, when posted without
`CQE_SKIP_SUCCESS`, are dropped silently) with a context dict (`message`,
`ring`, `res`, `flags`, `kind` as `COMPLETION_KIND_*`, and advisory `fd` or
`None`). Nowait cancel acks of `-ENOENT` / `-EALREADY` (target already gone
or already completing) are dropped silently; waitable cancel still reports
those as `res < 0`. If that hook raises, `exception_handler` is used; the
CQ drain always continues.

## File metadata and positioned I/O

`Ring` also exposes positioned file helpers for caller-owned fds:

- `prepare_openat(dfd, path, flags, mode=0, user_data=None)` opens a path and
  returns the new fd in the completion result (`dfd=AT_FDCWD` for a cwd-relative
  path);
- `prepare_read(fd, buf, offset)` and `prepare_write(fd, data, offset)` perform
  explicit-offset I/O into caller buffers;
- `prepare_statx_fdsize(fd)` is the common fast path for open-file metadata: it
  runs fd-only statx internally and puts the byte length in `completion.result`
  on success (`completion.kind == CompletionKind.STATX_FDSIZE`);
- `prepare_statx(dfd, path, flags, mask, buf)` fills a caller-provided 256-byte
  statx buffer asynchronously when you need a custom mask or path lookup.

The usual positioned-file case (append EOF, `SEEK_END`, sendfile bounds) is an
open fd whose size you already own:

```python
handle = ring.prepare_statx_fdsize(fd)
[completion] = ring.wait()
if completion.res == 0:
    size = completion.result
```

No caller buffer is required for `prepare_statx_fdsize()`. Use `prepare_statx()`
when you need path-based metadata or fields beyond `stx_size`.

Successful `prepare_statx()` completions always leave `completion.result` as
`None`; read fields from the caller-owned submit buffer (for example via
`statx_st_size(buf)` when you requested `STATX_SIZE`). Only
`prepare_statx_fdsize()` puts the byte length in `completion.result`. If the
internal buffer lacks size fields, `completion.result` is `None` and the
completion is still delivered.

**Behaviour change (since PR #34):** successful `prepare_statx()` no longer sets
`completion.result` to `0`; it stays `None`.

Provided-buffer receive uses a caller-owned ring created with
`create_buf_group()`. Submit one-shot receives with `prepare_recv_buf()` or
stream receives with `prepare_recv_multishot(fd, buf_group, ...)`. Both paths
return read-only `BufView` objects rather than copying into `bytes`. Export the
payload with `memoryview(view)` and drop the export (or call
`memoryview.release()`) before the kernel buffer is recycled:

```python
buf_group = ring.create_buf_group(buffer_size=16384, buffer_count=256)
pending = ring.prepare_recv_buf(reader.fileno(), buf_group, 0, token)
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
`-ENOBUFS` when the buffer ring is empty. MORE legs are shell Completions;
the terminal `!MORE` is the armed submit handle (see multishot delivery
contract above). After `-ENOBUFS`, return buffers to the ring and submit a
fresh `prepare_recv_multishot()`; stream consumers should continue ordinal
indexing from the terminal completion's `sequence`.

```python
handle = ring.prepare_recv_multishot(reader.fileno(), buf_group, token)
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

`BufGroup` supports an optional **owner hook** for reusing pools without
wrappers:

- Set `buf_group.release_callback = callable` (or `None`).
- `buf_group.close()` calls `release_callback(buf_group)` when set and **does
  not** free the provided-buffer ring — the owner (for example a size-keyed
  cache) keeps the group alive for the next checkout.
- With no callback, `close()` frees the kernel buf ring immediately.
- Finalization still frees the group if nothing called `close()`; dealloc does
  **not** call `release_callback` (abandoned groups are not returned to a
  cache). Clear the callback before a real dispose so `close()` destroys the
  group rather than re-entering the owner.

```python
free: list[uring_api.BufGroup] = []


def return_to_cache(group: uring_api.BufGroup) -> None:
    free.append(group)


group = ring.create_buf_group(16384, 4)
group.release_callback = return_to_cache
group.close()  # returns to free list; ring buffers stay registered
assert free == [group]

group.release_callback = None
group.close()  # frees the provided-buffer ring
```

`completion.kind` uses `RECV_MULTISHOT` (13) for multishot provided-buffer
receive and `RECV_BUF` (16) for one-shot `prepare_recv_buf()`.

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

When the SQ is full, prepare paths flush pending entries and retry. With
`IORING_SETUP_SQPOLL`, after a second flush without a free slot they wait for
the kernel poller to free space and retry (not a CQE wait). Non-SQPOLL rings
must free a slot after one successful flush. If a slot still cannot be obtained
(or SQPOLL wait times out), prepare raises `RuntimeError` — a stuck queue or
dead poller, not ordinary backpressure.

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

`IORING_SETUP_SQPOLL` enables a kernel submission-queue poller. Pass it in
`Ring(..., flags=...)` when you want that mode; ring construction may raise
`OSError` if the environment rejects it (privileges, container policy). There
is no dedicated capability key — handle failure at create time (or try
`probe(flags=IORING_SETUP_SQPOLL)` first if you prefer). Liburing's submit path
wakes a sleeping poller automatically when needed. When the SQ is full and the
poller has not yet freed a slot, prepare waits with the GIL released but still
under the ring critical section (up to a few seconds); prefer
`IORING_SETUP_SINGLE_ISSUER` (or a single submitter) with SQPOLL.

The compiled liburing version fields report the header version used to build the
binary extension. This is useful in CI because Linux distribution images can
compile the same Python package against different liburing development packages
while still running on the hosted runner's kernel.

`prepare_send_zc()` and `prepare_sendmsg_zc()` are best gated with
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
`prepare_poll_multishot()` on this entry; one-shot `prepare_poll()` and
`prepare_poll_remove()` are treated as baseline poll surface.

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
- other threads may call `construct_*` / `prepare_*`, `create_buf_group()`,
    and `break_wait()`;
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
        ring.prepare_recv(fd, bytearray(4096), 0, 200)
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
  `struct_size` and null-check pointers they rely on. **Break vs earlier v1
  drafts:** `ring_set_pre_submit` / `ring_set_c_pre_submit` were removed;
  all `ring_submit_*` / `ring_submit_*_nowait` op slots were dropped — C
  clients construct then `ring_prepare()`; `ring_construct_*_multishot` no
  longer takes `base_sequence` (set `completion.sequence` after construct).
  Appended: `completion_set_sequence`, `completion_clear_user_data`,
  `ring_wait_idle`. Python `Ring.prepare_*` is construct+prepare sugar with
  cargo then `user_data`. Rebuild any out-of-tree C client that cached
  `offsetof` values;
- `compiled_liburing_major` and `compiled_liburing_minor` for build-time header
    visibility;
- `probe(entries, flags)`, which returns a new reference to the same flat
    availability and capability dictionary as `_uring_api.probe()`;
- `ring_new()`, lifecycle helpers, metadata helpers, `ring_construct_*()` for
    every waitable op, `statx_st_size()`, `ring_prepare()`,
    `completion_prepared()`, `completion_nowait()`, `completion_set_nowait()`,
    `ring_break_wait()`, and `ring_wait()`;
- **not yet:** `BufGroup` lifecycle over the C API (`create_buf_group`,
    `close` / `release_callback`, C release hook). Provided-buffer constructs take
    a Python `BufGroup` object; manage groups from Python until that surface is
    added (see `ROADMAP.md`);
- `ring_set_callback()`, `ring_set_exception_handler()`, `ring_set_c_callback()`,
    `ring_serve_completions()`, `ring_stop_serving()`, and `ring_reset_serving()`
    for completion-service control;
- `completion_check()`, `completion_user_data()`, `completion_set_user_data()`,
    `completion_clear_user_data()`, `completion_res()`, `completion_flags()`,
    `completion_sequence()`, `completion_set_sequence()`,
    `completion_result()`, and `completion_kind()` for native completion
    inspection. Kind values match `URING_API_COMPLETION_KIND_*` in
    `uring_api_completion_kinds.h` and `CompletionKind` in Python;
    `ring_wait_idle()` parks until `break_wait`;
- `ring_set_nowait_error_handler()` and `ring_submit()` (flush prepared SQEs).
    Nowait is `completion_set_nowait` then `ring_prepare` (no dedicated C nowait
    slots). `ring_auto_submit` / `ring_set_auto_submit` match `Ring.auto_submit`
    (default on; off raises `SubmissionQueueFull` instead of flushing a full SQ,
    and wait/serve do not auto-submit).

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

`UringProactor` still defaults to `entries=8` (modest / test size). Production
rings should pick from this table and may want `IORING_SETUP_CQSIZE` so a
multishot burst does not overflow the CQ. Create-time depth, `CQSIZE`,
`COOP_TASKRUN`, and recv/send hints (`POLL_FIRST`, `SOCK_NONEMPTY`) are
tracked in [ROADMAP.md](ROADMAP.md) under **Setup flags and SQ/CQ sizing**.

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
