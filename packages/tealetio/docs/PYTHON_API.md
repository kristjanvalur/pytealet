# tealetio Python API Reference

This document describes the Python-facing API for `tealetio`, the scheduler,
synchronisation, selector, runner, and asyncio coexistence package built on top
of `tealet`.

Status note:
- The project is pre-1.0 and APIs may evolve.
- `tealetio` depends on `tealet`; `tealet` does not depend on `tealetio`.

## Import Surface

The top-level `tealetio` package re-exports the common scheduler, task/future,
lock, selector, runner, and asyncio bridge APIs. This follows the `asyncio`
pattern: submodules define explicit public names with `__all__`, and
`tealetio.__init__` imports and combines those surfaces.

Common imports can use the package root:

```python
from tealetio import Event, Future, Scheduler, gather, run, sleep, spawn, wait_for
from tealetio import AsyncRunner, AsyncScheduler, SyncSelectorScheduler
```

Submodule imports remain supported when code wants to make the implementation
home explicit:

```python
from tealetio.scheduler import Scheduler
from tealetio.runner import run
```

`Scheduler` is an alias for the default synchronous scheduler and is backed by a proactor.
When no custom factory is supplied, proactor-backed schedulers choose
`UringProactor` if `uring_api.is_available()` is true and otherwise
`SelectorProactor`. Use `SyncProactorScheduler` directly when you want to provide
a custom proactor factory for synchronous driving, and use `AsyncProactorScheduler`
for the same proactor-backed IO model under an async driving facade. `ProactorScheduler` is
the shared abstract proactor core. Likewise, `SelectorScheduler` is the shared
abstract selector core, with `SyncSelectorScheduler` and `AsyncSelectorScheduler`
as concrete driving variants. `run_asyncio_in_tealet(...)` chooses the hosted
asyncio loop from the scheduler it creates: proactor schedulers use
`TealetProactorEventLoop` with `ForwardingProactor`, while selector schedulers
use `TealetSelectorEventLoop` with `ForwardingSelector`. Use `BasicScheduler`
when you deliberately want the small no-IO driver that only waits for timers and
explicit scheduler wakeups. The shared task, timer, future, and callback
behaviour lives in the cooperative scheduling core; the blocking and
asyncio-hosted run loops are separate driving facades.

Proactors expose both `wait(deadline=None)` and `await wait_async(deadline=None)`.
The synchronous form blocks the current thread; the async form waits through the
running asyncio loop so future asyncio-hosted schedulers can pump tealetio-owned
IO completions without blocking asyncio itself. Deadlines use the proactor clock:
`None` waits forever, and `0` always means poll without blocking.

An operation may also complete before it ever reaches the backend wait queue.
In that case the proactor returns an already-done `Operation`, and callers can
read its result directly without switching or waiting. Selector-backed proactors
use this fast path for socket operations that succeed right away.

Separately, **`scheduler.io` tries non-blocking socket syscalls first** for
common stream work (accept, recv, send, and related continuous drains) and only
submits to the proactor when that would block. Ready work completes as
`IOWaiterSync` without a submit/CQE round-trip, which is a large win under
`UringProactor` when backlog or peer data is already available. Connect still
always goes through the proactor; shutdown/close run as direct stdlib calls.
See `IO_MANAGER_DESIGN.md` (**Eager non-blocking first**) for the full policy.

`UringProactor` also exposes positioned file I/O through io_uring:
`openat(path, flags, mode=0, *, dfd=AT_FDCWD)` returns a caller-owned fd,
`read(fd, n, offset)` and `read_into(fd, buf, offset)` read at an explicit
offset, and `write(fd, data, offset)` writes at an explicit offset. The `dfd`
argument is forwarded to `uring_api.submit_openat()` for directory-relative
opens. Selector-backed proactors do not implement these operations yet. Path,
flags, mode, offsets, and fds are forwarded unchanged to `uring_api`; kernel
and CQE errors surface as operation failures. `uring_api` may still raise
`ValueError` synchronously at submit time for some invalid offsets or buffers.

Long-lived socket operations use `ContinuousOperation`.
`scheduler.io.accept_many(sock, callback, *, recv_size=None)` first drains ready
connections with non-blocking `accept()` on the calling thread, then arms
`proactor.accept_many(..., base_sequence=N)` when the listen socket would block
(`N` is the number of eager accepts). Eager and continuous legs share one
index sequence for reorder buffers. Each delivery is
`(conn, initial_data)` (recv failures are handled before the user callback).
The continuous leg remains active until cancelled or the backend reports a
terminal error. Call `conn.getpeername()` when the peer address is needed.

Internal `ProactorIOManager._recv_many` is a thin wrap over `proactor.recv_many`
(returns a `ContinuousOperation` like the proactor): it drains ready bytes with
non-blocking `recv()`, then arms the proactor with the same `callback` and a
continued `base_sequence`. No extra marshal or reorder. Intermediate eager
chunks may arrive with `operation=None`; pure-eager EOF or hard error finishes
a synthetic done operation; when the call falls through to the proactor, the
return value is always a real continuous operation. Eager startup does not apply
provided-buffer pool backpressure: data already sitting in the socket receive
buffer is copied into user memory (it may as well live there as in the kernel)
until the consumer drains it; continuous legs still observe pool / ENOBUFS
limits. `sock_recv_iter` / `RecvIterBuffer` start legs through this helper and
cancel unfinished ops on the proactor as usual.

`initial_data` holds accept-time pre-read bytes when `recv_size` is set;
otherwise it is `None`. An empty `initial_data` (`b""`) means the peer closed
the write side before sending data (EOF). One-shot `sock_accept()` also tries a
direct accept first (returning `IOWaiterSync` when ready) and otherwise uses
`proactor.accept`; without `recv_size`, `initial_data` is `None`. When
`recv_size` is set, preread uses the same eager `recv` try as `sock_recv`
before `proactor.recv`.
`recv_size` must be positive when provided; values
above 64 KiB (`2**16`) are silently capped. Leave `recv_size` at the default
for server-speaks-first protocols.
`poll(fd, mask)` waits for fd readiness and returns a one-shot `Operation[int]`.
The result is the event bitmask currently set on the fd (`select.POLL*` bits
among those requested in `mask`). `poll_many(fd, mask, callback)` emits that
bitmask on each readiness event and remains active until cancelled or the
backend reports a terminal error. Poll works on any file descriptor, not only
sockets.

`Proactor.create_socket(family, type, proto=0, *, flags=0)` creates a
scheduler-contract socket (non-blocking, close-on-exec). ``UringProactor`` uses
``uring_api.Ring.submit_socket()`` when ``IORING_OP_SOCKET`` is available; extra
``flags`` are ORed with non-blocking and close-on-exec on that path.
``SelectorProactor`` creates via stdlib ``socket.socket()`` and
``configure_scheduler_socket()``.

``scheduler.io.sock_create(family, type, proto=0, *, flags=0, connect_to=None,
initial_data=None)`` creates sockets **directly** (stdlib + scheduler contract)
rather than through ``Proactor.create_socket`` — blocking creation is faster than
the uring path for this hot entry point. Create-only returns an
``IOWaiterSync`` holding the socket (or create error); optional ``connect_to``
and ``initial_data`` are composed via ``IOWaitGroup`` (connect → optional
``sock_sendall``). ``initial_data`` without ``connect_to`` raises ``ValueError``.
The call either returns the socket or raises. When ``connect_to`` is set the
returned socket is already connected (and any ``initial_data`` was flushed via
the same eager send path as ``sock_sendall``).
``open_connection(…, initial_send=…)`` passes ``connect_to`` and
``initial_data`` through this path for TCP and Unix ``path=`` connects.

`connect(sock, address)` completes with ``None`` on success or raises on
failure. Connect-time send is wired through
``ProactorIOManager.sock_connect(..., initial=...)`` via ``IOWaitGroup`` (connect
→ optional ``sock_sendall``), not a separate proactor ``initial`` parameter.

`SelectorProactor` probes immediate readiness with `select.select()` and
registers the fd with the internal selector when the fd is not ready yet. It
maps `POLLIN`, `POLLPRI`, `POLLOUT`, `POLLERR`, `POLLHUP`, and `POLLRDHUP`
(when the platform defines it) onto `select()` read/write/exception fd lists;
other `select.POLL*` bits are not supported and raise `ValueError`. `POLLERR`
and `POLLHUP` register for both read and write selector wakeups, and
immediate probing also checks the read fd list because Linux often reports
peer hangup as readability rather than an exception-set wakeup. It allows
at most one pending operation per fd per direction (so `poll(POLLIN)` conflicts
with an in-flight `recv_many` on the same socket).

`UringProactor` forwards `mask` and `fd` unchanged to io_uring; invalid
arguments surface as operation/CQE errors rather than pre-submit `ValueError`.
Poll results pass through `completion.res` as the kernel reports them and may
include bits outside the requested `mask`; the selector backend intersects
results with the request. Poll and socket receive/send operations may coexist
on the same fd — for example `poll_many(POLLIN)` alongside `recv_many()` on one
socket. That overlap is uncommon and rarely problematic; the uring path
deliberately does not enforce selector-style per-fd exclusivity.

`recv_many(sock, callback)` delivers each received byte chunk to `callback` as a
``MultishotDelivery(index, value, exception, more)``. For receive,
``index`` is the stream ordinal (``completion.sequence`` on uring, seeded by
``base_sequence`` at submit); ``value`` is a read-only ``memoryview`` when data
arrived; ``exception`` carries transport failures (for example ``errno.ENOBUFS``
when the provided-buffer pool is full on multishot uring); ``more`` mirrors
``IORING_CQE_F_MORE``. EOF is ``more=False`` with an empty ``value``. Chunk sizes
are backend-defined: multishot uring uses the operation ``BufGroup`` slot size
(16 KiB by default); degraded uring and selector paths read up to 8 KiB per leg.
``recv_many(sock, callback, *, buf_group)`` requires an explicit pool from
``create_recv_buffer_pool()`` or ``shared_recv_buffer_pool()``. Each proactor
lazily creates one shared pool for ``scheduler.io`` receive helpers (16 KiB × 256
on ``UringProactor`` backends by default, 16 KiB × 8 on selector backends).
``sock_recv_iter(sock, buffer_pool=None)`` and ``sock_recvall(sock, ...,
buffer_pool=None)`` use that shared pool when ``buffer_pool`` is omitted; pass a
dedicated pool from ``create_recv_buffer_pool()`` for isolated sizing. Concurrent
long-lived generators that share the same pool object therefore draw from the
same buffer pool: a slow consumer on one stream can trigger
``RECV_MANY_BUFFER_PRESSURE`` or stall another stream even when the second would
otherwise fit. Use separate pool objects when independent streams need isolated
buffer pools.

On uring multishot, pool exhaustion delivers ``errno.ENOBUFS`` through
``MultishotDelivery.exception`` at the terminal ``index`` and completes the
current leg; drop held views and start a fresh ``recv_many()`` with
``base_sequence`` set to that ``index``. ``SyntheticRecvBufferPool`` (selector
backends and uring when PBUF ring creation fails) mirrors lease accounting:
when the pool is already full at submit, ``recv_many()`` completes immediately
with ENOBUFS without reading the socket. Callbacks receive borrowed views: copy
with ``bytes(data)`` when you need to keep payload past the callback. Leased
chunks (uring ``BufView``, synthetic pool on degraded paths) require
``memoryview.release()`` (or dropping the last reference after copying) to return
slots to the pool; holding too many live views can pin the pool and stall
further receives.

When ``IORING_RECV_MULTISHOT`` is unavailable, ``UringProactor.recv_many()``
routes by pool type: real ``BufGroup`` pools use one ``submit_recv_buf()`` per
leg (leased ``BufView`` per chunk); ``SyntheticRecvBufferPool`` uses one-shot
``submit_recv()`` with synthetic leases over copied bytes. Each leg delivers one
``MultishotDelivery`` with ``more=False``; callers start a fresh ``recv_many()``
for the next chunk (``RecvIterBuffer`` / ``sock_recv_iter`` own that re-arm loop).
Direct ``recv_many`` callbacks do not receive ``RECV_MANY_BUFFER_PRESSURE``;
that token is only yielded by ``sock_recv_iter``.

When `IORING_ACCEPT_MULTISHOT` is unavailable, `UringProactor.accept_many()`
falls back to repeated one-shot `submit_accept()` after each accepted
connection. `SelectorProactor.accept_many()` uses the same one-shot re-arm
pattern. Direct `proactor.accept_many()` callers must resubmit after each
accept; `scheduler.io.accept_many(...).wait()` returns an `IOWaitable` that
unblocks when the current stream leg ends (one accept on oneshot backends), so
callers re-arm in a loop — `StreamServer` owns this accept-loop tealet.

Cancelling an operation is only through `scheduler.proactor.cancel(operation)`
(or `scheduler.io._cancel_operation()` / `SelectorScheduler.cancel_operation()`
wrappers). `Operation.cancel()` was removed. The proactor returns a teardown
`Operation[None]`; `wait()` on it when io_uring cancel must settle before
shutdown, or `forget()` when only the target's terminal state matters.
Exceptional `IOWaiter.wait()` exit uses `.forget()` on the teardown leg
(best-effort).

`scheduler.io.accept_many()` may start independent accept-time `recv`
operations when `recv_size` is set. That preread path does not apply to
`accept_many_streams()` or `start_server()`; stream accepts arm `recv_many` when
streams open. Cancelling an accept `IOWaiter` does not cancel in-flight work
started from accept callbacks; discard late deliveries after shutdown (as
`StreamServer` does via `_closed`).

When `IORING_POLL_MULTISHOT` is unavailable, `UringProactor.poll_many()` falls
back to repeated one-shot `submit_poll()` after each readiness event.
Multishot stop posts `submit_poll_remove()` and terminalises the continuous op
immediately; the oneshot fallback stops locally without `submit_cancel()` on
the pending poll SQE.

`UringProactor.capabilities` exposes the `uring_api.probe(entries=...,
flags=...)` result captured once at construction, so callers and the proactor
itself can gate behaviour without re-running runtime probes.

Backends may run these result callbacks from any worker thread; code that needs
thread affinity should marshal from the callback into the appropriate scheduler,
event loop, or application thread.

`scheduler.io.sock_recvall(sock, progress=None, *, buffer_pool=None)` joins
chunks from `sock_recv_iter(sock, buffer_pool)`. Each non-pressure chunk is
converted to `bytes` as the
iterator advances, so at most one leased `memoryview` is held per iteration
step. Provided-buffer pressure is handled inside `sock_recv_iter`; receive
restarts once ``leased_count < buffer_count / 2`` (low-water mark).
`sock_recvall` does not batch retain views itself. When
provided, `progress(chunk)` is
called after each received non-empty chunk with that chunk's `bytes` payload
(not a running total).

`scheduler.io.sock_recv_iter(sock, buffer_pool=None)` is a tealet-blocking iterator that
incrementally yields `(index, data)` chunks in stream-index order until EOF.
Each `data` is a read-only `memoryview`; copy with `bytes(data)` when owned
storage is required past the current iteration step. Unlike `recv_many`, it
does not yield a final `(index, empty_view)` EOF tuple; iteration ends when
the stream completes (the generator raises `StopIteration`). Use `recv_many`
directly when you need the documented EOF sentinel and exact `recv_many`
callback semantics.

Chunks already buffered in stream-index order are yielded before a terminal
stream error or cancellation is raised. When an ordered empty chunk (EOF) is
dequeued, iteration ends cleanly even if the underlying `recv_many` operation
recorded cancel or error concurrently — ordered EOF wins that race.

`(RECV_MANY_BUFFER_PRESSURE, memoryview(b""))` is yielded when the
buffer pool is exhausted (ENOBUFS from the underlying ``recv_many`` leg).
At most one pressure notification is pending until the consumer advances past
that yield; ``RecvIterBuffer`` re-arms ``recv_many`` once the pool drains below
the low-water mark (``leased_count < buffer_count / 2``). Consumers should
drop every receive `memoryview` they still hold when that token appears and
avoid keeping more views than needed between reads.

`scheduler.io.create_recv_buffer_pool(buffer_size, buffer_count)` returns a
`RecvBufferPool` for explicit sizing. Pass it to `sock_recv_iter(sock, pool)`
or `sock_recvall(sock, buffer_pool=pool)` to share a tuned pool across
generators or with a custom `recv_many` callback.
`scheduler.io.shared_recv_buffer_pool()` and
`scheduler.io.set_shared_recv_buffer_pool(pool)` delegate to the mounted
proactor's shared pool.

Out-of-order multishot completions are reordered before yield. The iterator
must be consumed from a scheduler tealet so `CrossThreadEvent.swait()` can
block cooperatively.

`scheduler.io.sock_sendall(sock, data, progress=None)` tries one non-blocking
`send` first. When the full buffer is accepted, it returns `IOWaiterSync` without
a proactor submit. On would-block it falls through to `proactor.send`; on a
partial send it reports `progress(sent)` (if provided) and submits the remainder
— the proactor continues the drain and reports further progress as cumulative
totals from the original buffer. Empty payloads go straight to the proactor.

`scheduler.io.sock_shutdown(sock, how)` and `scheduler.io.sock_close(sock)` call
stdlib `socket.shutdown` / `socket.close` on the calling thread and return
`IOWaiterSync` (no proactor submit), matching asyncio stream teardown.
`Proactor.shutdown` / `close_socket` remain for direct proactor use.

`scheduler.io.sock_send_iter(sock, chunks)` drains an iterable of `bytes`,
`bytearray`, or `memoryview` chunks through `sock_sendall`, sending each
non-empty chunk before pulling the next. Track send progress in the iterable or
generator you pass when you need it; use ``sock_sendall`` directly for
per-buffer ``progress=`` callbacks. Must be called from a scheduler tealet.

`Proactor` exposes `recv_many(sock, callback, *, buf_group)`,
`create_recv_buffer_pool`, and the lazy `shared_recv_buffer_pool()` used by
`scheduler.io` receive helpers when `buffer_pool=None`. Blocking socket
streaming helpers live on `scheduler.io` as `sock_recvall`, `sock_recv_iter`,
and `sock_send_iter`.

Proactor-backed schedulers expose blocking poll helpers on `scheduler.io`.
`scheduler.io.poll(fd, mask)` waits cooperatively and returns the readiness
bitmask. `scheduler.io.poll_many(fd, mask, callback)` starts a continuous poll
and forwards each readiness event to `callback`, implemented through
`wait_operation()` and proactor `poll`/`poll_many`. `SelectorScheduler` still
implements `poll` / `poll_many` directly on the scheduler surface via
selector-backed readiness waits and the same `select.POLL*` mask semantics as
`SelectorProactor`; a future `SelectorIOManager` could expose the same helpers
through `scheduler.io` without changing the proactor path. When a bidirectional
poll mask arms the same callback on both read and write, the selector scheduler
delivers at most one callback invocation per readiness event even if both
direction bits are set.

`add_reader` / `add_writer` follow the same rule: if both slots on an fd hold the
same callback and args, one combined selector wakeup schedules a single call.
Register distinct callbacks when you need separate per-direction invocations.

`scheduler.io.open(path, mode="rb")` returns an `IOFile` handle (today the
concrete `ProactorFile`, an unbuffered `io.RawIOBase`) for positioned binary I/O
through the proactor backend. Import the protocol as `from tealetio import IOFile`
(also re-exported from `tealetio.proactor`); it lives in `tealetio.files` for
implementation sharing but that module path is not the primary public import.
The handle tracks a logical file position and uses
`read_into()` for in-buffer reads, so `io.BufferedReader` and `io.TextIOWrapper`
can stack on top without an extra copy through `read()`. File helpers require a
proactor with `openat` support (`UringProactor` today). Low-level `openat` /
positioned `read` / `write` remain on `scheduler.proactor` for callers that need
explicit flags, offsets, `dfd`, or metadata via `stat(path=...)` / `stat(fd=...)`.

`IOFile.fileno()` exposes the raw OS descriptor. I/O through the handle uses the
tracked logical offset; calling `os.read()` / `os.write()` on that fd directly
bypasses position tracking and can desynchronise `tell()` and subsequent proactor
reads or writes.

Supported binary modes mirror the usual stdlib subset: `rb`, `wb`, `ab`, `r+b`,
`w+b`, and `a+b` (plus `rb+` / `wb+` / `ab+` spellings). Text modes (`t`) and
exclusive create (`x`) raise `ValueError`.

Append-mode opens still set `O_APPEND` on the fd (stdlib parity). `ProactorFile`
also tracks a logical offset in userspace and passes it to positioned proactor
writes. On Linux, kernel append handling may still redirect some writes to the
true end-of-file even when an explicit offset is supplied, which can diverge from
`_pos` when concurrent writers extend the file while `_pos_at_eof` is set and
sequential append writes skip `stat_fdsize()`. The handle keeps `_pos_at_eof` to
avoid redundant size lookups: while set, sequential append writes extend from
`_pos`; the flag clears after `seek()` (other than `seek(SEEK_END, 0)` when
already at EOF) or after reads in update/append modes, and the next append write
looks up file size again. `seek(SEEK_END, …)` uses `stat_fdsize()` when a fresh
EOF position is needed.

`UringProactor` submits io_uring `statx` / `statx_fdsize` when `IORING_OP_STATX`
is probed; other proactors complete `stat()` / `stat_fdsize()` immediately via
blocking `os.fstat()` / `os.stat()`. When `statx_fdsize` completes without a
parsed size, the uring completion path falls back to a rare blocking `os.fstat()`
on the completion thread before delivering the operation result.

`send(sock, data, progress=None)` drains stream buffers before completing and
accepts an optional progress callback. Use `sendto(sock, data, address)` for
datagram sockets.
Backends call `progress(total)` with the cumulative number of bytes sent as
progress becomes observable. Some backends may only expose a single completion
for the whole send, in which case they report one final total.

Proactors expose `wake_wait()` to unblock callers parked in `wait()` or
`wait_async()`. The proactor does not auto-wake on operation completion: when
using the raw API, register
`operation.add_done_callback(lambda _: proactor.wake_wait())` if the caller
blocks in `wait()` / `wait_async()`. Scheduler production code wakes through
`IOWaiter` / `call_soon_threadsafe` → `proactor.wake_wait()` instead.

`UringProactor.wake_wait()` always calls `ring.break_wait()`, which opens the
host `wait_idle` park immediately. The ring submits an internal NOP only when
completion service is idle (inline `ring.wait()` on an empty CQ); with service
workers the NOP is skipped. Threaded `wait()` parks on `ring.wait_idle()`;
`wait_async()` runs the same `wait` binding in a thread-pool executor (call
`bind_loop()` first).
`ThreadedSelectorProactor` still uses an inlined `EventWakeupManager` for sync
and async waits. `SelectorProactor.wait_async()` still runs `wait()` in a
thread-pool executor; optional `set_async_break()` can install a host-loop hook
for that path.

## Blocking IO facade (`scheduler.io`)

Proactor-backed schedulers (`SyncProactorScheduler`, `AsyncProactorScheduler`)
expose a composed `ProactorIOManager` at `scheduler.io`. This object owns
blocking tealet IO over the scheduler's `Proactor` backend: `wait_operation`,
asyncio-shaped `sock_*` helpers, `poll` / `poll_many`, positioned file `open`,
and receive-buffer pool helpers. Call it from scheduler-owned tealets; it blocks
the current tealet through `CrossThreadEvent.swait()` rather than yielding
asyncio futures.

```python
import socket

from tealetio import SyncProactorScheduler, get_running_scheduler, set_scheduler

scheduler = SyncProactorScheduler()
set_scheduler(scheduler)

def exercise(sock: socket.socket) -> bytes:
    return get_running_scheduler().io.sock_recv(sock, 4096)
```

`BasicScheduler` and other schedulers without an IO backend raise
`RuntimeError` when `.io` is accessed. Prefer `scheduler.io` (or module helpers
that resolve the running scheduler) over `isinstance(scheduler, ProactorScheduler)`
when you only need blocking IO capability.

`SelectorScheduler` is different today: blocking `sock_*` and `poll*` helpers
remain on the scheduler via `SelectorMixin` for the selector driving path.
`scheduler.io` is not wired for selector schedulers yet; a future
`SelectorIOManager` could provide the same capability gate without changing
proactor callers. `tealetio.streams` requires a proactor scheduler and always
goes through `scheduler.io`.

Low-level submission stays on `scheduler.proactor` (`Operation` returns,
raw `recv_many`, `accept_many`, and similar). Prefer `scheduler.io` for
application and stream code so you get the eager non-blocking try where it
applies. `ProactorFile` blocks through an `OperationWaiter` protocol implemented
by `ProactorIOManager`.

`SelectorProactor` is the simple single-threaded selector-backed prototype.
`ThreadedSelectorProactor` uses the same socket operation surface, but polls the
selector from a worker thread and queues completions for `wait(...)` or
`wait_async(...)`. That makes it useful for exercising the thread-callback shape
expected from future OS-backed proactors without making the default selector
prototype more complicated.

## Scheduler Accessors

`tealetio.set_scheduler(scheduler)` binds a scheduler as current in the active
context. Passing `None` clears the current scheduler binding.

`tealetio.get_scheduler()` returns the current scheduler, whether or not
it is actively running. It raises `RuntimeError` if no scheduler is currently
bound and never creates one implicitly.

`tealetio.get_running_scheduler()` returns the current scheduler only while it is
actively driving work. It raises `RuntimeError` if no scheduler is running and
never creates one implicitly.

`tealetio.get_current()` returns the currently running `Task`, or `None`
when the caller is outside a scheduler-owned tealet task. Asyncio tasks therefore
see `None` rather than an unrelated low-level tealet object. This includes
coroutines that a tealet task waits for through `BaseScheduler.await_(...)`.

`tealetio.asyncio_get_current()` returns the current `asyncio.Task`, but returns
`None` while execution is inside a scheduler-owned tealet task. Asyncio runners
hosted by `run_asyncio_in_tealet(...)` clear that tealetio task scope before
entering the asyncio entry point, so ordinary asyncio tasks remain visible.

`tealetio.sleep(delay)` suspends the current scheduler task on the running
scheduler. `sleep(0)` is the tealetio yield checkpoint, matching the familiar
`asyncio.sleep(0)` pattern without scheduling a timer.

`tealetio.spawn(func)` creates a `Task` on the current scheduler from a
zero-argument callable. `tealetio.create_task(func)` is an asyncio-style alias
for the same operation; `spawn(...)` is the native tealetio spelling.

## Scheduler Main Context

`scheduler.main_context()` is the low-level boundary for direct scheduler task
access from the process main tealet. It temporarily wraps the current main
tealet with the scheduler's configured task class, so operations that transfer
or inspect scheduler-owned tasks see a `Task`-shaped current tealet.

High-level driving APIs enter this context for you. That includes scheduler
drivers such as `run()`, `run_forever()`, `run_until_complete(...)`, and
their async counterparts, as well as `Runner.run()`, `Runner.close()`,
`AsyncRunner.run()`, and `AsyncRunner.aclose()`.

`AsyncScheduler.arun(yield_every=N)`,
`AsyncScheduler.arun_forever(yield_every=N)`, and
`AsyncScheduler.arun_until_complete(..., yield_every=N)` bound each scheduler
batch to at most `N` scheduling transfers before yielding to asyncio with
`asyncio.sleep(0)` if runnable scheduler work remains. With `yield_every=None`,
`arun(...)` and `arun_forever(...)` run each scheduler batch without an early
batch limit, while `arun_until_complete(...)` uses the runnable queue length at
batch entry.

Use it explicitly only when raw main code manipulates scheduler tasks directly:

```python
with scheduler.main_context():
    task.cancel()
    scheduler.run_until_complete(task)
```

Code already running inside a scheduler-owned `Task` does not need this
wrapper, and task transfer methods do not install it implicitly.

## Task Priorities

`PriorityTask` is a `Task` subclass for schedulers that use a priority
runnable queue. Its `priority` property is a float, and changing it notifies the
current task link so the queue can recompute runnable order when the task is
already waiting to run. `get_effective_priority()` returns the priority the
scheduler should use right now, including inherited priority from
`PriorityLock` waiters.

Priority values follow Python priority queue and Unix `nice` intuition: lower
numeric values run first. The public constants provide spaced bands with room
for intermediate values:

```python
TASK_PRIORITY_CRITICAL = -20.0
TASK_PRIORITY_HIGH = -10.0
TASK_PRIORITY_DEFAULT = 0.0
TASK_PRIORITY_LOW = 10.0
TASK_PRIORITY_IDLE = 20.0
```

For example, the built-in task factories can construct priority tasks directly:

```python
scheduler.set_task_factory(DefaultTaskFactory(task_constructor=PriorityTask))
scheduler.spawn(worker, priority=TASK_PRIORITY_HIGH)
```

The factory passes extra `spawn(...)` keyword arguments to the task constructor
before the scheduler makes the task runnable.

Schedulers accept a `runnable_queue_factory` for applications that need a
specific runnable policy. The default is `PrescheduledRunnableQueue`, which keeps
FIFO ordering with an immediate lane for explicit `reschedule(...)` and
`yield_to(...)` operations. `PriorityRunnableQueue` is the built-in priority
policy, and it is intended to be paired with `PriorityTask`:

```python
from tealetio import DefaultTaskFactory, PriorityRunnableQueue, PriorityTask, Scheduler

scheduler = Scheduler(runnable_queue_factory=PriorityRunnableQueue)
scheduler.set_task_factory(DefaultTaskFactory(task_constructor=PriorityTask))
scheduler.spawn(worker, priority=TASK_PRIORITY_HIGH)
```

The public runnable queue symbols are `FifoRunnableQueue`,
`PrescheduledRunnableQueue`, `PriorityRunnableQueue`, `RunnableQueue`, and
`RunnableQueueFactory`. Custom queue implementations should satisfy the
`RunnableQueue` protocol so the scheduler can add, discard, pop, reschedule, and
introspect runnable tasks without knowing the queue's concrete policy.

`PriorityLock` is the priority-aware counterpart to `Lock` for tealet code. It
supports `sacquire()` / `with lock:` from scheduler-owned tasks and
`acquire()` / `async with lock:` from asyncio tasks. It keeps the same FIFO lock
waiter policy as `Lock`. When `PriorityTask` instances participate, a
low-priority owner inherits the best waiter priority until it releases the lock,
which avoids the usual low/medium/high priority inversion. Regular tealet tasks
and asyncio tasks participate with the default priority.

## Scheduler Asyncio Bridge

`BaseScheduler.await_(awaitable) -> object` waits for an asyncio awaitable from
the current tealet task and returns its result.

Coroutine objects are driven directly through their await protocol with
`asynkit.coro_drive`. If the coroutine completes synchronously, `await_()`
returns without creating an asyncio task. If it yields an asyncio future-like
object, tealetio waits for that future and resumes the same coroutine driver
when it completes.

Coroutine objects run in the current `contextvars.Context`, with tealetio's
current-task marker temporarily cleared while the coroutine is driven. Existing
asyncio `Future` and `Task` objects keep the context they already captured.
Generic non-coroutine awaitables that `await_()` wraps in a new asyncio task run
in a copy of the current context.

## Scheduler Waiting Helpers

`scheduler.runnable_tasks()` returns the scheduler-owned `Task` instances
currently waiting to run, in scheduler order. It is an introspection helper for
advanced scheduling and debugging; blocked and completed tasks are not included.

`scheduler.reschedule(task, position=None)` moves a runnable task to a new
runnable queue position. By default, the task returns through normal runnable
policy. Passing an integer inserts the task into the immediate lane; position
`0` makes it the next scheduler-owned task to run. Negative positions count back
from the end of the immediate lane, with out-of-range values truncated like list
insertion. The task must belong to that scheduler and must already be runnable.

`scheduler.yield_to(task, insert_current_at=None)` yields from the current tealet
to a runnable task and keeps the current task runnable. The target is placed in
the immediate lane. By default, the current task is returned through the normal
runnable policy. Passing an integer inserts the current task into the immediate
lane after the target; position `0` means the current task is next once the
target blocks or completes. Negative values count from the end of the immediate
lane, with out-of-range values truncated like list insertion.

`scheduler.ensure_future(entry)` returns a scheduler `Future` for one entry.
Existing scheduler futures are returned unchanged, and zero-argument callables
are spawned as scheduler tasks.
`tealetio.ensure_future(entry)` delegates to the current scheduler.

`tealetio.gather(*entries, return_exceptions=False)` returns a scheduler
`Future` for ordered child results. Entries may be scheduler futures/tasks or
zero-argument callables, which are spawned as scheduler tasks.

`tealetio.wait(entries, *, timeout=None, return_when=ALL_COMPLETED)`
returns a scheduler `Future` whose result is `(done, pending)`. The
`return_when` value may be `FIRST_COMPLETED`, `FIRST_EXCEPTION`, or
`ALL_COMPLETED`. Timeout completion does not cancel pending children.

`tealetio.wait_for(entry, timeout)` returns a scheduler `Future` for one child
result. If the timeout expires, the wrapper raises `TimeoutError` and cancels the
child future.

`tealetio.as_completed(entries, *, timeout=None)` is a tealet-blocking
iterator that yields scheduler futures in child completion order. If the timeout
expires before all children finish, iteration raises `TimeoutError`; unfinished
children are not cancelled by `as_completed(...)`.

## Streams

`tealetio.streams` provides tealet-native stream endpoints backed by blocking
socket I/O through `scheduler.io` on proactor schedulers. `StreamReader` and
`StreamWriter` are the
default native types with synchronous methods. `AsyncStreamReader` and
`AsyncStreamWriter` mirror the asyncio stream API shape (`async def read`,
`drain`, and so on) so handlers written for asyncio-style web code can run under
`run_coro()` or `scheduler.await_()` without creating asyncio futures for socket
reads and writes.

```python
from tealetio.proactor import SyncProactorScheduler
from tealetio.streams import open_streams, run_coro

scheduler = SyncProactorScheduler()

async def handler(reader, writer):
    line = await reader.readline()
    writer.write(line.upper())
    await writer.drain()

def exercise(sock):
    reader, writer = open_streams(sock, async_=True)
    return run_coro(handler(reader, writer))
```

Stream connect and server helpers are module-level functions (like asyncio).
`open_connection(addr=(host, port))` resolves the running scheduler through
`get_running_scheduler()` unless you pass `scheduler=`. Helpers require a
proactor-backed scheduler (`ProactorScheduler` / `SyncProactorScheduler`); they
resolve blocking socket I/O through `scheduler.io`. `SelectorScheduler` still
exposes blocking `sock_*` on the scheduler surface, but stream helpers raise a
targeted `RuntimeError` until `SelectorIOManager` wires `scheduler.io` for
selector paths. `start_server` additionally needs proactor submission
(`accept_many` via the `ServerIO` slice: blocking `SocketIO` plus `proactor`
access; `ProactorSocketIO` is a backward-compatible alias). Implementation routes
through `streams._connect_tcp_streams(...)` /
`streams._connect_unix_streams(...)`; the only functional difference between
native and asyncio-shaped connect helpers at that layer is which default stream
factory `async_` selects.

`open_connection(addr=(host, port), async_=False)` connects a TCP socket and
returns `(reader, writer)`. The host may be a hostname or literal IPv4/IPv6
address; TCP connects resolve through `scheduler.ensure_resolved()`, which
fast-paths literal IPs and calls `getaddrinfo` on a worker thread for names.
Resolved addresses are tried in order (no happy eyeballs). Pass `path=` for
Unix-domain stream sockets without name resolution, e.g.
`open_connection(path="/tmp/sock")`. Pass `async_=True`
for asyncio-shaped `AsyncStream*` endpoints. `open_streams(sock, async_=False)`
wraps an existing non-blocking connected socket. The `async_` flag only selects
the default stream factory when `stream_factory` is omitted.
Under the hood, `StreamWriter` queues outbound data through an internal
`SendBuffer` that chains `scheduler.io.sock_sendall()` legs.
`close()` rejects further writes; `wait_closed()` flushes queued data, applies
deferred `SHUT_WR` when `write_eof()` was called, and closes the socket.
Default readers receive through `recv_many` via `RecvIterBuffer`.

Pass `stream_factory=` to `open_streams()`, `open_connection(...)`, or
`start_server(...)` to customise stream construction. Use `StreamFactory` for
native `(StreamReader, StreamWriter)` pairs and `AsyncStreamFactory` for
asyncio-shaped pairs. `default_stream_factory` and
`default_async_stream_factory` are the built-in implementations.
`pooled_default_stream_factory(async_=..., buffer_size=..., buffer_count=...,
pool=...)` delegates to the default factory with a per-connection or shared
provided-buffer pool.

Proactor socket operations accept `socket.socket` objects. `UringProactor`
submits the socket's file descriptor to io_uring internally; the public API
still expects a non-blocking `socket.socket` so accepted connections, peer
metadata, and selector-backed backends share one handle type.

`scheduler.io.sock_create(family, type, proto=0, *, flags=0, connect_to=None,
initial_data=None)` is the socket creation entry point for proactor-backed
blocking IO. It creates a non-blocking, close-on-exec socket with the stdlib
(not via ``Proactor.create_socket`` / ``IORING_OP_SOCKET``). When ``connect_to``
is set, ``ProactorIOManager`` composes connect (and optional initial send) via
``IOWaitGroup`` before the root completes. Connect/server helpers call
``scheduler.io.sock_create()`` so create policy stays behind one gate.

## Name resolution

`scheduler.getaddrinfo(host, port, ...)` and `scheduler.getnameinfo(sockaddr,
flags=0)` run the stdlib resolver on a worker thread through
`run_in_executor`, matching asyncio's `loop.getaddrinfo()` /
`loop.getnameinfo()`. `scheduler.ensure_resolved(address, ...)` skips the
executor when `host` is already a literal IP, like asyncio's `_ensure_resolved`.
Module helpers `tealetio.getaddrinfo(...)`, `tealetio.getnameinfo(...)`, and
`tealetio.ensure_resolved(...)` delegate to the running scheduler.

`open_connection(addr=...)` always calls `scheduler.ensure_resolved(...)` before
`sock_connect`; see the name-resolution section above for the literal-IP
fast path.

`start_server(client_handler, addr=(host, port), async_=False, limit=2**16)`
binds a TCP listening socket; use ``addr=(None, port)`` or ``addr=("", port)`` for
all interfaces. Pass ``path=`` for Unix-domain listeners, or ``sock=`` with a
caller-prepared stream socket (``addr``/``path`` must not be passed as well).
When binding via ``addr``, ``reuse_address`` and ``reuse_port`` mirror
``asyncio.loop.create_server()``: ``reuse_address=None`` defaults to ``True`` on
POSIX platforms other than Cygwin; ``reuse_port`` is off unless set to ``True``.
With ``sock=``, tealetio applies the scheduler listen-socket contract
(non-blocking, close-on-exec) and calls ``listen(backlog)``, like asyncio.
``limit`` sets the stream reader line-buffer cap for ``readline()`` (asyncio
semantics). When ``stream_factory`` is omitted, ``start_server()`` uses
``pooled_default_stream_factory`` (per-connection provided-buffer pools). Pass
``stream_factory=`` for custom stream types or alternate pool policy (for example
a shared pool across clients). Close listeners and
discard late deliveries in the accept callback after shutdown (``StreamServer``
uses ``_closed``).
Each accept loop call drains ready connections with direct `accept()` when
possible, then arms `proactor.accept_many` for the wait. On the continuous
delivery path, ``accept_many_streams()`` wraps the connection as streams and
starts ``recv_many`` before the stream pair is posted onto the scheduler reorder
buffer (one `call_soon_threadsafe()` hop per leg, with `immediate=True` when
already on the owner thread), so data can arrive while the handler is still
queued. Eager (ready-queue) deliveries open streams on the accept-loop thread.
A peer that connects without sending leaves ``recv_many`` pending; the handler
still receives the stream pair and can apply read timeouts or idle close policy.
The handler runs in a spawned tealet.
``async_=True`` selects asyncio-shaped streams and drives the handler through
``run_coro()``. On ``UringProactor``, accept uses multishot
`IORING_ACCEPT_MULTISHOT` when probed; otherwise the proactor falls back to
repeated one-shot accepts (see the `accept_many` notes above).

`start_server(...)` returns a `StreamServer`. ``close()`` cancels the accept-loop
tealet synchronously; listening sockets are closed when that tealet exits.
``wait_closed()`` blocks until the accept-loop tealet and in-flight handler
tealets finish. Use as a context manager for both. ``serve_forever()`` parks the
current tealet until ``close()`` is called (accept is already active); it does
not install signal handlers — use ``tealetio.run()`` / ``Runner`` for shutdown
signals.

Default stream readers receive through ``recv_many`` / ``RecvIterBuffer``.
`StreamReader.read(n)` for ``n > 0`` waits once for at least one byte, then
returns at most ``n`` available bytes without blocking to fill the remainder
(asyncio ``StreamReader.read`` semantics). ``readinto()`` blocks until the
caller buffer is full or EOF (short return only at EOF), suitable for fixed-size
protocol reads. Other read methods assemble data in an internal `bytearray`
before returning `bytes`.
Release leased chunk views after copying when holding data past a read call
(``RecvIterBuffer`` releases on ingest for ``read()`` / ``readline()`` paths).

This module does not integrate with stdlib `asyncio.StreamReader` instances or
the `ForwardingProactor` guest loop.

## Synchronisation Primitives

`tealetio` provides scheduler-aware synchronisation primitives modelled after
`asyncio` where practical. Plain method names follow the asyncio-facing API, and
`s`-prefixed methods are tealet-blocking variants for synchronous tealet code.

Common primitives include:
- `Event.wait()` / `Event.swait()`
- `Lock.acquire()` / `Lock.sacquire()`
- `Semaphore.acquire()` / `Semaphore.sacquire()`
- `Condition.wait()` / `Condition.swait()`
- `Barrier.wait()` / `Barrier.swait()`
- `Queue.put()` / `Queue.sput()`
- `Queue.get()` / `Queue.sget()`
- `Queue.join()` / `Queue.sjoin()`

`Queue.shutdown(immediate=False)` follows `asyncio.Queue.shutdown()` semantics
and raises `tealetio.QueueShutDown`. On Python versions with
`asyncio.QueueShutDown`, this is the standard-library exception; on older
versions, `tealetio` provides a same-named fallback exception.
- future `put()` / `put_nowait()` / `sput()` calls raise `QueueShutDown`;
- blocked async and tealet-blocking putters/getters are woken;
- graceful shutdown lets existing queued items drain, then future gets raise
  `QueueShutDown` once the queue is empty;
- immediate shutdown drains queued items, marks each drained item done for
  `join()` accounting, and wakes joiners if unfinished work reaches zero.
