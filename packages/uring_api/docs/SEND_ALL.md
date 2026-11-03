# Synthetic send-all

`prepare_send_all` is one waitable that drains a stream buffer with ordinary
`IORING_OP_SEND` legs. Send, shutdown, and close on that fd wait on a per-fd
conflict FIFO so they cannot pass a drain that is still in the submission
queue or in the kernel.

This note is why the queues are split. The user contract — pending count,
`prepared`, lazy submit, skip flags — is in `README.md`. tealetio already
submits stream sends through `construct_send_all`. `SendBuffer` still
coalesces small writes; the ring op does not.

`IOSQE_IO_LINK` is not a substitute: you cannot pre-build N linked sends for
one buffer, and linking send+close would close after a partial first send.
Kernel `MSG_WAITALL` is not a reliable stream send-all either. Copying send
only. Zero-copy legs are a different CQE machine; see **Still open**.

---

## Shape

A ring-wide userspace list in front of the submission queue was tried and
dropped. It duplicated the kernel queue, changed `auto_submit=False`, and
bought nothing send-all needs. **SQ size (`Ring(entries=…)`) is what controls
lazy batching.**

### Where a prepared Completion sits

`prepare()` fills a kernel SQE immediately when it can. `submit()` calls
`io_uring_submit`. That kernel queue is the lazy batch. A full queue follows
`make_room`, which is independent of `auto_submit` and on by default. When
it is on, `prepare()` flushes and then takes a slot. An enter the kernel
rejects is `OSError`. When it is off, `prepare()` raises
`SubmissionQueueFull` and does not enter.

The submission queue cannot host send-all serialisation by itself: anything
in it is published on the next enter, including a close sitting behind a
send-all that still has remaining legs. The one userspace queue in front of
it is the per-fd conflict FIFO. Any thread may fill a free slot. A full SQ
with `make_room` on enters.

| Place | What | Role |
| --- | --- | --- |
| **Kernel SQ** | liburing SQEs | Filled by `prepare()` / `prepare_one_constructed` as today. Next `io_uring_submit` publishes them. Size is the batch limit. |
| **Conflict FIFO** | Per-fd FIFO of `Completion *` | Only for ops that **conflict** with an already-filled send-all on that fd (SQ or in-kernel). Drain copies them into the SQ with the existing fill path when the fd is free. |

```text
construct_*                 cargo on Completion, caller-owned
      │
      ▼
prepare()
      │
      ├─ no conflict        get_sqe_try + prep_*  → kernel SQ
      │                     if send-all: fd becomes busy
      │                     make_room: flush SQ if full
      │                     a rejected enter is OSError; make_room off: SubmissionQueueFull
      └─ conflicts          append that fd’s conflict FIFO
                            (except cancel-of-active send-all → SQ)

submit() / wait flush       io_uring_submit of SQEs already in the queue

send-all completion         busy cleared; copy that fd’s FIFO into the SQ
                            until the next send-all is active or it is empty
                            a full queue enters (may start the next send-all)
```

Busy is set when a send-all **SQE is filled**, not after `io_uring_submit`.
`prepare([send_all, close])` therefore serialises in one batch: first item
fills an SQE and sets busy, second goes to conflict. `prepare(close)` then
`prepare(send_all)` publishes close first — caller order, same as today.

Conflict drain calls the same `prepare_one_constructed` as a normal prepare.
A full queue does not spill onto the conflict FIFO. That FIFO is fd-busy
serialisation, not SQ backpressure.

### Worked example — send-all, close, recv, second send-all

Same fd=5 except the recv, which is full-duplex and does not conflict.
`prepare()` fills SQEs as it goes. `make_room` decides whether a full SQ
flushes or raises. `auto_submit` does not.

**1. `prepare(send_all A)` (1 MiB)**

```text
conflict[5]: []
kernel SQ:   [A first-leg]
busy[5]:     A
```

**2. `prepare(close-nowait C)`, `prepare(recv R)`**

```text
conflict[5]: [C]
kernel SQ:   [A first-leg, R]
busy[5]:     A
```

Close is parked (fd busy). Recv is not a conflict, so it joins A in the SQ.

**3. `submit()`** (or wait with `auto_submit` on)

Kernel sees the send-all first leg and the recv. **Close is not published.**

**4. Partial CQE for A** (64 KiB of 1 MiB)

Same Completion A, offset advanced. Next-leg SQE into the kernel SQ when a
slot is free, including a flush when the queue is full. If that enter
fails, the send-all fails. C stays on
conflict.
Recv may complete independently.

**5. Terminal CQE for A**

`busy[5]` cleared. The completion copies `conflict[5]`: C fills an SQE, and
the housekeeping publish enters. Close follows the finished drain.

**6. Caller order `A, B, cancel(B), close`**

After A is in the SQ, later prepares park:

```text
conflict[5]: [B, cancel(B), close]
```

A terminals → drain:

| Pop | Decision |
| --- | --- |
| B send-all | SQ, `busy[5]=B` |
| cancel(B) | **cancel-of-active** → send-all-cancel SQE (abandon + `ASYNC_CANCEL` of B) |
| close | conflict[5] again (fd busy with B) |

If the caller had prepared close *before* B, close would drain first. That is
the order they asked for.

### Marking an fd busy

Busy means: this fd has a send-all whose SQE has **already been filled**
(current leg in the SQ or in-kernel). Recv, accept,
poll, and other fds stay independent (full-duplex).

Ops that **conflict** (`prepare()` parks them on that fd’s conflict FIFO
instead of filling an SQE):

- `send` / `send_zc` / `sendmsg` / `sendmsg_zc` / further `send_all`
- `shutdown` / `shutdown_nowait` (especially `SHUT_WR`)
- `close` / `close_nowait`
- `cancel` / `cancel_nowait` of a **FIFO-queued** target on this fd. Look the
  fd up on the target Completion. Cancel of the *active* send-all, or of an
  already-prepared (SQ / in-kernel) send on this fd, still fills an SQE.

A `close` on that FIFO stays graceful when the send-all drain finishes.
When the drain fails, each queued close sets `SO_LINGER` with a zero timeout
as it is filled, so the peer sees a reset after a partial or failed write.
That covers a negative send result, a zero-length send with bytes still
queued, and an abandoned drain. `shutdown` on the same FIFO is unchanged.
`abortive=True` on the close sets the option whether or not the drain succeeds.

`sendmsg` / `sendmsg_zc` stay in that set because on `SOCK_STREAM` they are
scatter-gather send, not datagram-only.

Non-conflicting (`prepare()` fills an SQE even while the fd is send-all-busy):

- recv / recv_buf / recv_multishot / recvmsg
- accept, poll, connect
- `sendto` (datagram; not mixed with stream `send_all`)
- cancel of a waitable on another fd
- send/close on a different fd

**Fd reuse** is the landmine. The busy entry must live until the **queued close
has actually been submitted** (ideally until its CQE). If Python
`socket.close()` bypasses the ring while a send-all is active, the table is
stale and the next occupant of that fd number is wrongly serialised. Contract:
once an fd has used send-all, subsequent send/shutdown/close on it go through
the ring until the fd is idle. Document that; do not try to intercept libc
close.

Connect-on-the-same-fd during send-all is rare; treat it as non-conflicting in
v1 (do not grow the conflict set without a test). `sendto` is the same class:
a datagram helper, not compatible with stream send-all, so it is not a
conflict.

### Conflict FIFO holds Completions

**Retrospect: should pending submissions have been stored as SQE structs and
memcpy’d into the kernel SQ?**

No, not for the existing lazy path, and not enough of a win for the new
conflict FIFO.

The kernel SQ **already is** the cyclic array of pending SQEs. `prepare()`
fills one in place (and `auto_submit` flushes if it is full); `submit()`
publishes. Do not add a second userspace SQE array or a ring-wide Completion
list in front of it.

For the **conflict FIFO**, a memcpy-of-SQE drain looks tempting
(`io_uring_get_sqe(); *sqe = saved;`). In practice you still need the Completion:
buffer views, nowait tagging, cancel_target, in-flight ref, Python identity.
Drain via the existing `prepare_one_constructed` (switch on kind, fill a real
SQE from cargo) reuses the path we already have.

**Conflict FIFO entries are `Completion *`.** Growable cyclic buffer of
pointers. Drain = `prepare_one_constructed` into the kernel SQ.

States of a Completion:

| State | Where | `prepared` |
| --- | --- | --- |
| constructed only | caller | false |
| conflict-queued | per-fd FIFO | false |
| in kernel SQ | liburing SQ | true |
| in kernel (submitted) | io_uring | true |

### Next-leg re-arm is the same Completion

A continuing send-all is the **same** Completion (same `user_data` pointer the
kernel already knows). Do not enqueue a second handle at the head.

Per-fd state should look like:

```text
active: Completion*          # the send-all in progress
queue: Completion*[]         # conflicting ops, FIFO
```

There is no ring-wide list and no continuation bit on a parked completion.
A next leg is filled directly. A continuation that cannot be submitted
fails the send-all.

On a partial send CQE (`res > 0`, bytes remain):

1. Advance the retained view offset on `active`.
2. Under the ring lock, if abandon is set, arm nothing. The caller completes
   `-ECANCELED`.
3. Otherwise take a slot with `flush_if_full` on, ignoring `make_room`. A
   free slot is filled from any thread. A full queue publishes the
   unpublished tail and then takes the slot. That counts as
   `submit_sq_full`. An enter the kernel rejects fails the send-all.
   Prep the next `IORING_OP_SEND` (same Completion pointer, `POLL_FIRST`
   on later legs). The handler's publish submits the leg with the other
   SQEs from this completion when the callback policy says to enter.
   Inline `wait()` submits a continuation this thread filled during the
   burst, on that same call, when this thread's waitable count moved.
   The harvest flush and host `submit()` publish a leg this thread did
   not prepare, or one the callback policy left queued.
4. If the flush cannot take a slot, or that submit fails, fail the
   send-all. `res` is `-ENOMEM` when the error is `MemoryError`,
   otherwise `-EIO`. The Python exception is what `wait()` raises, and
   what stops that `serve_completions` worker. A link-timeout allocation
   failure aborts the leg the same way. It does not leave the operation
   queued. A free slot that was filled, then fails on the later publish,
   is a different case. That enter is `OSError`. The send-all is not
   finished as `-EIO`. `wait()` raises. A `serve_completions` worker stops.

The conflict FIFO is separate. The send-all completion copies it until
the next send-all is active or the FIFO is empty. A full queue enters.
`make_room` is not read. A rejected enter is `OSError` and fails that
completion. Nothing else drains the FIFO.
A new prepare may take a free slot. A two-slot op that does not fit
does not take the first slot.

The issuer submits from `submit()` and from inline `wait()`. A service
thread submits after its handler when the callback policy allows that enter.

### A second send-all queues

A second `send_all` on a busy fd is parked on that fd’s conflict FIFO at
`prepare()`. When the active drain terminals, drain copies the FIFO into the
SQ: the next send-all fills an SQE, marks the fd busy, and later FIFO entries
for that fd stay parked (point 7).

### Cancel stays in FIFO order

Do **not** scan the conflict FIFO to complete a target locally. Same rule as
today’s lazy SQ: if you `prepare(send)` then `prepare(cancel(send))` then
`submit()`, the kernel sees send then cancel. Some bytes may go out. That is
the existing contract (“prepare the target first if one flush should publish
both in order”). The conflict FIFO is the same idea one stage earlier.

**How cancel finds the fd.** `prepare(cancel)` reads the fd off
`cancel_target`’s sidecar (`view_state.fd` / `scalar_state.fd`). Park the
cancel on **that fd’s conflict FIFO** only when the target is already
`CONFLICT_QUEUED`. If it is the active send-all, fill a send-all-cancel SQE
now. If the target is already `PREPARED` (SQ / in-kernel), fill ordinary
`ASYNC_CANCEL` now so cancel is not delayed until the drain terminals. No
extra hash of in-flight send-alls: `fd_table[fd].active == cancel_target`. A
reverse `Completion* → fd` map would only duplicate the sidecar fd.

**When the cancel SQE is filled:**

- Target is the **active send-all**: set the abandon bit before the cancel
  is queued, then `ASYNC_CANCEL` the current leg (the send-all Completion
  pointer, the same `user_data` on every leg). The leg CQE holds the ring
  critical section while it reads that bit and, when the bit is clear, arms
  the next leg. A cancel from another thread therefore either sees that new
  leg or the completion does not create one. There is no parked next leg.
  A full queue raises `SubmissionQueueFull` from `prepare_cancel` when
  `make_room` is off. That path does not enter. With `make_room` on the
  cancel enters, and an enter the kernel rejects is `OSError`. If that send's CQE
  arrives first and bytes remain, abandon finishes the drain with
  `-ECANCELED` and does not arm another leg. If the leg empties the buffer,
  the drain succeeds. A
  `send_all` still on a queue, with no leg submitted, is filled as a NOP
  when the drain reaches it. That CQE is what `wait()` and a callback
  deliver, and abandon finishes it `-ECANCELED`. The cancel follows the
  NOP. `-ENOENT` on the cancel is the lost race after that CQE has already
  been taken.
- Target is any other op (queued send, close, another send-all that we just
  moved into the SQ ahead of this cancel): ordinary
  `io_uring_prep_cancel(cancel_target)`. FIFO already submitted the target
  first, so the kernel can find it. Same as cancel sitting behind a target in
  the kernel SQ today.

**While the fd is send-all-busy, `prepare(cancel)` of the *active* send-all
still fills an SQE when one is free.** A full queue follows the same
`make_room` rule as any other prepare. Cancel of a
*queued* target parks behind it. Cancel of an already-prepared send on that
fd fills now (`ASYNC_CANCEL` is not delayed until the drain terminals).
Other prepares for that fd go to conflict.
See the worked example above for `B, cancel(B), close`.

Waitable cancel still completes only the **cancel** waitable (ack /
`-ENOENT`). The target completes from its own CQE (or send-all terminal).
Nowait cancel `-ENOENT` / `-EALREADY` stay silent.

### When the conflict FIFO drains

`prepare()` of a **non-busy** fd still fills an SQE immediately (`auto_submit`
/ SQ-full unchanged).

The send-all completion drains that fd’s conflict FIFO. It copies until
the next send-all is active or the FIFO is empty, and a full queue
enters. `prepare()`, `submit()`, and `wait()` do not drain it.

While draining a FIFO, the same conflict test applies: a dequeued send-all
marks the fd busy and later entries stay parked; cancel-of-active still fills
an SQE. Recv and other fds are unrelated `prepare()`s, not this drain.

No behaviour change for ordinary ops: `prepare()` still means “SQE is in the
SQ” unless the fd is send-all-busy.

### A filled send-all holds later conflicting ops

Not a separate “stop the world” flag. `prepare()` of a recv on another fd still
fills an SQE. `prepare()` of send/close/send-all on the busy fd goes to that
fd’s FIFO. Drain of that FIFO parks again as soon as it fills the next
send-all.

### Per-fd queues, not one global queue

| | Per-fd FIFO | One global FIFO |
| --- | --- | --- |
| Drain | O(that fd’s queued ops) | Skip-scan; busy fds leave holes |
| Next-leg | Continuation slot on that fd | Head-insert mixes fds |
| Cancel of active send-all | Peek this fd’s FIFO for cancel-of-active | Walk / skip other fds |
| Allocation | On first send-all or first conflict; free when idle | One buffer always |
| Starvation | Unrelated fds unaffected | A long busy-fd skip-scan on every drain |

**Recommendation:** hash table keyed by fd, value =
`{active, cyclic Completion* queue}`. The send-all completion already holds
that slot, and it is the only copy. Free the slot when the fd is not busy
and the FIFO is empty.

No hash of send-all Completions. Identity is `fd_table[target.fd].active ==
target`.

---

## Recommended Completion / pending model

One user-visible `Completion` for the whole drain (`COMPLETION_KIND_SEND_ALL`).
Intermediate partial CQEs are consumed internally, like `send_zc` NOTIF.

- Success: deliver the armed handle once. `res` is total bytes clamped to
  `INT_MAX` (`Completion.res` is a CQE-shaped `int`); `result` is the full
  unsigned count.
- Error: `res < 0` as today, including `-ECANCELED`.
- Zero-byte send: treat as today (`-EAGAIN` / fail the send-all). Do not spin.
- Buffer retained until terminal (copying send).
- **Counts as in-flight from first accept until terminal**, including nowait
  send-all and a waitable still only on the conflict FIFO.
  `pending_count()` must not drop between legs. This is a deliberate break from
  “nowait is excluded from pending_count”: a nowait send-all that still owns
  the fd is ring-busy. Increment when the send-all is accepted (first-leg SQE
  or conflict-enqueue), not only while an SQE is in the kernel.

`POLL_FIRST`: omit on first leg when the caller expects READY (mirror
`UringProactor._send_sqe_flags`); set on later legs when probed.

### v1: copying `IORING_OP_SEND` only

Do **not** use `IORING_OP_SEND_ZC` for send-all in this stack, even when
`probe()["IORING_OP_SEND_ZC"]` is true. `send_zc` is two CQEs per **leg**
(op + `IORING_CQE_F_NOTIF`) plus buffer lifetime until NOTIF. Mixing that
with continuation, fd-busy, and cancel is a later project — see Follow-up.
v1 always uses ordinary send. tealetio can keep zc for one-shot `sendto` /
`sendmsg`; stream send-all goes through the synthetic copying op.

---

## Threading and the CQE path

Prepare, the fd table, and both queues run under the ring critical section.

`wait()` and `serve_completions` take one kernel CQE at a time. There is no
harvest-then-package buffer. A send-all partial is re-armed before that CQE
is delivered, so Python sees one completion for the drain.

A next leg calls `get_sqe_try` with `flush_if_full` on. Return 1 fills the
SQE. Return 0 should not happen on that path. A failure fails the send-all
and propagates the Python exception. The FIFO copy uses the same enter.
A failure there fails the completion and stops the worker. A worker submits once after the callback
and that fill, under the callback policy, covering the continuation and
the parked copy. Inline `wait()` submits a continuation this thread filled
during the burst before it returns. With `auto_submit` off those automatic
points stay off for a caller prepare. A next leg, and a copy off
a per-fd queue, are still published from every thread. That publish can take an application SQE
already on the tail with it. `wait()` then keeps
reaping if that publish armed a leg and the burst delivered nothing. An
enter the kernel rejects is `OSError`.
The harvest flush and host `submit()` publish a waitable SQE this thread
did not prepare, or one the callback policy left queued.

Do not set `IORING_SETUP_SINGLE_ISSUER` without `SQPOLL` when a worker
must enter. A worker can fill a free slot. The following publish is
`OSError` and stops that worker. A full SQ on a next leg fails the
send-all. With `SQPOLL` the kernel thread is the issuer and any thread
may publish. Do not set `DEFER_TASKRUN` for a worker that enters.

---

## What stays in tealetio

`SendBuffer` still coalesces. One nowait send-all per tiny `write()` would
defeat `min_write` and multiply SQEs.

`UringProactor` stream send, including `send_close_nowait`, is
`construct_send_all`. Close parks on the conflict FIFO. `pending_count()`
stays non-zero for the whole drain, so scheduler idle does not treat a live
send-all as a quiet ring. Oneshot `poll_many` between legs is a separate gap
and is not this op.

---

## Cases that must keep working

1. **Cancel vs success race** on the last remaining bytes. The abandon bit
   stops a racing success CQE from re-arming.
2. **Queued cancel behind queued send-all** — FIFO submits the send-all, then
   send-all-cancel of the now-active drain; no unlink.
3. **SQ full, `make_room=False`** — a user `prepare` raises
   `SubmissionQueueFull` and does not enter. A send-all next leg, and
   the FIFO copy that the send-all completion starts, still enter to
   take a slot. That completion copies until the next send-all is
   active or the FIFO is empty. If making room fails, the completion
   fails and the worker stops. A filled copy is published from every
   thread, together with the rest of the unpublished tail.
4. **Two fds with concurrent send-alls** plus a close on one of them.
5. **Nowait send-all error** after the Python caller has moved on —
   `skip_success` delivers the handle on failure; `skip_all` uses
   `nowait_error_handler`. `pending_count` until terminal. Success stays
   silent either way.
6. **Fd reuse after ring close** of the previous occupant.
7. **`prepare()` of a mixed batch** — each item is SQ or conflict in order
   (`prepare([send_all, close])` fills send-all then parks close). A full
   SQ still raises or flushes as today; it does not spill onto the
   conflict FIFO. A full SQ with `make_room` on enters. A rejected enter
   is `OSError`.
8. **Worker-thread CQE** — a free-slot next leg is filled by the worker.
   The following publish enters. Without `SQPOLL` that enter is
   `OSError` and the worker stops. A full SQ fails the send-all.
9. **Buffer lifetime** if the Python caller drops the Completion while
   the drain is still running. The ring holds one reference for that
   completion: on the per-fd FIFO while it is parked, or as the in-flight
   ref once a leg is filled. Moving it from the FIFO onto an SQE does not
   take another. A next leg is that same completion, not a second park.

---

## API sketch (uring-api)

```python
# construct then prepare, same cargo-then-user_data rule
c = ring.construct_send_all(fd, data, flags, user_data)
n = ring.prepare(c)  # SQE, or conflict FIFO if fd is send-all-busy
# convenience
c = ring.prepare_send_all(fd, data, flags, user_data)

c.skip_all = True
ring.prepare(c)  # fire-and-forget drain; still pending_count until done

# error-only: skip_success keeps the handle; user_data is just a token
c = ring.construct_send_all(fd, data, flags, token)
c.skip_success = True
ring.prepare(c)
```

C capsule: `ring_construct_send_all` + existing `ring_prepare` / `ring_submit`.
No per-op submit slot.

### `skip_success` and `skip_all`

Delivery flags, not `user_data`. `user_data` is only the caller's token.

| | meaning |
| --- | --- |
| neither | normal waitable: deliver success and error |
| `skip_success` | keep the handle; skip successful delivery; **complete on error** |
| `skip_all` | no user delivery (implies `skip_success`); errors → `nowait_error_handler` |

`skip_all` is the tagged nowait protocol (kernel `CQE_SKIP_SUCCESS` when
available) except for `send_all`, which still keeps the `Completion*` to
re-arm. `skip_success` never uses kernel skip-success: the CQE drops the
in-flight ref.

`prepare_*_nowait` sets `skip_all`. Error-only send_all is
`skip_success = True` plus whatever token you want.

No public “deferred queue” type. No `flush_deferred()` unless tests need a
hook. `submit()` publishes SQEs already in the queue.

`probe()` does not need a kernel key — this is userspace. Optional `"SEND_ALL"`
compile-time feature bit is unnecessary while the package is pre-release.

---

## Prepare and submit

`IORING_SETUP_SINGLE_ISSUER` means one task may `io_uring_enter` to
submit. Filling an SQE is not that. `IORING_SETUP_DEFER_TASKRUN` (which
requires `SINGLE_ISSUER`) also pins completion reaping to that thread.
The extension does not pre-check either flag. A forbidden enter is
`OSError` with `errno` `EEXIST`.

- Any thread may `prepare` into a free slot. `make_room` decides whether
  a full SQ is flushed from prepare. With `make_room` off that prepare
  raises `SubmissionQueueFull` and does not enter. With `make_room` on
  it enters, and a rejected enter is `OSError`.
- Do not set `SINGLE_ISSUER` without `SQPOLL` when a worker needs to
  enter. `SQPOLL` with that flag still lets any thread publish. Do not
  set `DEFER_TASKRUN` for a worker that enters.
- `submit()`, and the `wait()` / `serve_completions` flush, enter from
  the thread that called them. `submit()` from a task the kernel rejects
  is `OSError`. There is nothing to queue: the submission queue already
  holds the work.
- A send-all next-leg fills from any thread that finds a slot, including a
  completion worker. It flushes when the queue is full, ignoring
  `make_room`. Every thread then tries to publish it. A leg that cannot
  be submitted fails the send-all. A filled leg whose later publish is
  rejected stops that worker with `OSError`.
- The send-all completion is the only conflict-FIFO drain. It copies
  until the next send-all is active or the FIFO is empty, and a full
  queue enters. `make_room` is not read. A failure fails that
  completion and stops the worker.
- The conflict FIFO is fd-busy serialisation. A recv on a send-all-busy fd
  is not a conflict. A worker recv with a full SQ and `make_room` on
  enters, and a rejected enter is `OSError`.

`UringProactor` does not default `SINGLE_ISSUER`. One thread enters; the
ring critical section already serialises who writes an SQE.

---

## Still open

- Default `SINGLE_ISSUER` on `SyncUringProactor` only if that host is
  the only task that enters. A worker enter is `OSError`. There is no
  path that publishes a slot the worker filled and could not enter.
- **send-all + `send_zc`.** Use `IORING_OP_SEND_ZC` for legs when probed
  (kernel 6.0+). This is not a flag on the current op: it changes the CQE
  machine.

  Why it waits:

  - **Two CQEs per partial.** Today one send CQE either re-arms or
    terminals. Zc posts an op CQE (bytes / error) and a later
    `IORING_CQE_F_NOTIF`. The in-flight ref already has a NOTIF rule for
    oneshot `send_zc`; send-all must keep that ref until **every** leg’s
    NOTIF, including after a racing cancel.
  - **`user_data` is busy until NOTIF.** Both CQEs of one zc SQE use the
    same `user_data` (today the `Completion*`, same as oneshot `send_zc`).
    That pointer must not go on a **new** SQE until the NOTIF of the
    previous SQE has been reaped — otherwise op/NOTIF CQEs from two legs
    alias, and `ASYNC_CANCEL` of the send-all handle is ambiguous. Waiting
    for NOTIF before the next leg keeps today’s identity. Overlapping the
    next leg needs a **per-in-flight-SQE** token (small leg object or
    tagged id linked back to the send-all `Completion`); both CQEs of that
    SQE share the token; the next SQE gets a new one. Cancel-of-active
    then cancels the current token, not the `Completion*`.
  - **Next-leg vs pin.** The kernel pins the zc range until NOTIF. Re-arming
    the remainder of the **same** `Py_buffer` before NOTIF is overlapping
    pins; waiting for NOTIF before the next leg doubles enter/CQE cost per
    partial and stalls the drain. Either policy is extra slot state
    (`outstanding_notifs`) on top of `continuation_pending`.
  - **Cancel and close.** Abandon + `ASYNC_CANCEL` of the current leg still
    leaves a NOTIF to reap before the fd is idle. Conflict-FIFO close must
    wait for that, not only the op CQE — otherwise close races the pin.
    An abandoned drain must not fill another zc SQE.
  - **Fallback is common.** `IORING_SEND_ZC_REPORT_USAGE` /
    `IORING_NOTIF_USAGE_ZC_COPIED` means the kernel copied anyway (typical
    on `AF_UNIX`). Then we paid the two-CQE machine for a copy send. A
    mixed policy (zc first leg, copy if remaining is small or last NOTIF
    said copied) is more state again.
  - **Probe floor.** Copying send-all works wherever `IORING_OP_SEND`
    does. Zc is 6.0+ and still `-EOPNOTSUPP` on some protocol/fd pairs;
    the drain would have to fall back mid-flight.

  Public shape when we do it: same `COMPLETION_KIND_SEND_ALL` waitable;
  choose zc vs copy at first-leg fill (probe + maybe an opt-in). Do not
  add a second completion kind. Tests: one-CQE drain; multi-leg with
  NOTIF-before-next-leg or overlap policy spelled out; cancel mid-drain
  still delivers one terminal `res` and then the leftover NOTIFs; close
  behind a zc drain does not run until the last NOTIF.

---

## Key decisions

1. **Do it in uring-api**, not another Python emulator — one waitable, honest
   pending_count, fire-and-forget close.
2. **No ring-wide lazy list.** `prepare()` fills an SQE. SQ size is the
   batch limit.
3. **Conflict check at `prepare()`.** If the fd is send-all-busy and the op
   conflicts, park on that fd’s FIFO; otherwise `get_sqe_try` and prep
   (`auto_submit` unchanged).
4. **Conflict FIFO holds Completions**, published later by the same
   `prepare_one_constructed` path.
5. **Per-fd FIFO** + continuation slot. Next-leg is not a second FIFO entry.
6. **Cancel is FIFO**, like cancel sitting behind a target in the kernel SQ. No
   instant unlink of queued ops. Cancel of the *active* send-all still fills
   an SQE while busy, using send-all-cancel (abandon + current-leg
   `ASYNC_CANCEL`). Fd comes from the target Completion, not a send-all hash.
7. **v1 is copying send only** — zc is a second lifetime protocol.
8. **Nowait send-all still counts as pending** until terminal.
9. **tealetio calls `construct_send_all`.** `SendBuffer` still coalesces.
10. **Do not default `SINGLE_ISSUER`.** Any thread may prepare into a
    free slot. A full SQ with `make_room` on enters and is `OSError` if
    the kernel rejects it. `make_room` off is `SubmissionQueueFull` with
    no enter. A next leg that cannot be submitted fails the send-all.
    A worker that must enter needs `SQPOLL` with `SINGLE_ISSUER`, and
    must not use `DEFER_TASKRUN`.

---

## Out of scope

- Vector / `sendmsg` scatter-gather send-all (`ROADMAP.md` item 10).
- Recv-side bundling, poll_many between-leg pending_count.
- Automatic SQ-full FIFO for *all* prepares (explicitly rejected in
  `ROADMAP.md` Queue Pressure Notes). The conflict FIFO is **fd-busy
  serialisation**, not SQ backpressure. SQ-full still raises
  `SubmissionQueueFull` when `make_room` is off, or flushes when it is on.
- A ring-wide userspace list in front of the kernel SQ.
- Storing conflict entries as copied `io_uring_sqe` structs. Completions on
  the per-fd FIFO are enough; drain fills the kernel SQE.
