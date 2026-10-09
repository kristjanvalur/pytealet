# Synthetic send-all

`prepare_send_all` is one waitable that drains a stream buffer with ordinary
`IORING_OP_SEND` legs. Send, shutdown, and close on that fd wait on a per-fd
conflict FIFO so they cannot pass a drain that is still in the submission
queue, in the kernel, or parked on fill-wait.

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
`io_uring_submit`. That kernel queue is the lazy batch. `auto_submit` still
makes room when the queue is full.

The submission queue cannot host send-all serialisation by itself: anything
in it is published on the next enter, including a close sitting behind a
send-all that still has remaining legs. Two userspace queues sit in front of
it, and they are not the same queue.

| Place | What | Role |
| --- | --- | --- |
| **Kernel SQ** | liburing SQEs | Filled by `prepare()` / `prepare_one_constructed` as today. Next `io_uring_submit` publishes them. Size is the batch limit. |
| **Conflict FIFO** | Per-fd FIFO of `Completion *` | Only for ops that **conflict** with an already-filled send-all on that fd (SQ, in-kernel, or next-leg on fill-wait). Drain copies them into the SQ with the existing fill path when the fd is free. |
| **Fill-wait** | Ring-wide FIFO of `Completion *` | Non-issuer `prepare` that would have to enter, and send-all next-leg when there is no SQ slot. Drain fills without `io_uring_enter` except when `ring_can_submit()`. |

```text
construct_*                 cargo on Completion, caller-owned
      │
      ▼
prepare()
      │
      ├─ no conflict        get_sqe + prep_*  → kernel SQ
      │                     if send-all: fd becomes busy
      │                     auto_submit: flush SQ if full (issuer)
      │                     non-issuer SQ-full: ring fill-wait list
      └─ conflicts          append that fd’s conflict FIFO
                            (except cancel-of-active send-all → SQ)

submit() / wait flush       io_uring_submit, as today
                            plus drain fill-wait, then conflict FIFOs
                            of fds that are free

send-all terminal           busy cleared; drain that fd’s conflict FIFO
                            into the SQ (may start the next send-all)
```

Busy is set when a send-all **SQE is filled**, not after `io_uring_submit`.
`prepare([send_all, close])` therefore serialises in one batch: first item
fills an SQE and sets busy, second goes to conflict. `prepare(close)` then
`prepare(send_all)` publishes close first — caller order, same as today.

Fill-wait is enter / SQ-full backpressure, not a lazy SQ: do not park a
non-issuer `prepare` when a slot already exists. Conflict drain calls the same
`prepare_one_constructed` as a normal prepare. Issuer SQ-full still raises
`SubmissionQueueFull` when `auto_submit` is off; it does not spill onto the
conflict FIFO (that FIFO is fd-busy serialisation, not SQ backpressure). A
non-issuer that would have to enter parks on fill-wait.

### Worked example — send-all, close, recv, second send-all

Same fd=5 except the recv, which is full-duplex and does not conflict.
`prepare()` fills SQEs as it goes (`auto_submit` on or off only changes
whether a full SQ flushes or raises).

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

Same Completion A, offset advanced. Next-leg SQE into the kernel SQ (or the
fill-wait list if this thread cannot enter). C stays on conflict.
Recv may complete independently.

**5. Terminal CQE for A**

`busy[5]` cleared. Drain `conflict[5]`: C fills an SQE, then submit. Close
follows the finished drain.

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

**Rejected alternative — ring-wide lazy list.** `prepare()` would only enqueue
Completions; drain would fill SQEs and park conflicts. That duplicated the
kernel SQ, delayed `auto_submit=False` SQ fill, and let callers enqueue past
`sq_entries` (an SQ-full FIFO, which the roadmap already rejected). Dropped.

### Marking an fd busy

Busy means: this fd has a send-all whose SQE has **already been filled**
(current leg in the SQ or in-kernel, or next-leg on the fill-wait list). Recv, accept,
poll, and other fds stay independent (full-duplex).

Ops that **conflict** (`prepare()` parks them on that fd’s conflict FIFO
instead of filling an SQE):

- `send` / `send_zc` / `sendmsg` / `sendmsg_zc` / further `send_all`
- `shutdown` / `shutdown_nowait` (especially `SHUT_WR`)
- `close` / `close_nowait`
- `cancel` / `cancel_nowait` of a **FIFO-queued** target on this fd. Look the
  fd up on the target Completion. Cancel of the *active* send-all, or of an
  already-prepared (SQ / in-kernel) send on this fd, still fills an SQE.

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
(`get_sqe(); *sqe = saved;`). In practice you still need the Completion:
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
| fill-wait | ring-wide list | false |
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

Ring-wide: a **fill-wait** list of `Completion*` (send-all next-leg with
`SEND_ALL_CONT`, or a non-issuer `prepare` that would have to enter).

On a partial send CQE (`res > 0`, bytes remain):

1. Advance the retained view offset on `active`.
2. If `get_sqe_try` succeeds: prep the next `IORING_OP_SEND` (same Completion
   pointer, `POLL_FIRST` on later legs). If this thread may enter and a unique
   waiter is already held (may be in `wait_cqe`), submit so the continuation
   cannot stall; otherwise leave the SQE for
   the next harvest flush or host `submit()`. Never enter from a non-issuer.
3. Otherwise park the **active** handle on fill-wait (`SEND_ALL_CONT`).
   **Do not raise** `SubmissionQueueFull` out of CQE drain.

Drain order: **fill-wait first**, then that fd’s FIFO. That is the
“head of the queue” requirement without mixing the active handle into the
FIFO.

Any thread may **fill** an SQE if a slot exists. Only the issuer **submits**.

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

- Target is the **active send-all**: do **not** blindly `io_uring_prep_cancel`
  as if it were a oneshot. Set the abandon/cancel bit so a racing success CQE
  cannot re-arm, then `ASYNC_CANCEL` the **current leg** identity (the send-all
  Completion pointer, same `user_data` on every leg). If the next-leg is only
  on fill-wait (no SQE in kernel), still issue cancel-of-user_data; `-ENOENT`
  is the lost-race case, and the abandon bit is what stops the continuation
  from being published. That is the only send-all-specific cancel code.
- Target is any other op (queued send, close, another send-all that we just
  moved into the SQ ahead of this cancel): ordinary
  `io_uring_prep_cancel(cancel_target)`. FIFO already submitted the target
  first, so the kernel can find it. Same as cancel sitting behind a target in
  the kernel SQ today.

**While the fd is send-all-busy, `prepare(cancel)` of the *active* send-all
still fills an SQE.** Cancel of a *queued* target parks behind it. Cancel of
an already-prepared send on that fd fills now (`ASYNC_CANCEL` is not delayed
until the drain terminals). Other prepares for that fd go to conflict.
See the worked example above for `B, cancel(B), close`.

Waitable cancel still completes only the **cancel** waitable (ack /
`-ENOENT`). The target completes from its own CQE (or send-all terminal).
Nowait cancel `-ENOENT` / `-EALREADY` stay silent.

### When the conflict FIFO drains

`prepare()` of a **non-busy** fd still fills an SQE immediately (`auto_submit`
/ SQ-full unchanged).

Also drain a fd’s conflict FIFO (continuation first, then FIFO):

- on the next user `prepare()` that needs an SQE (parked next-legs take the
  slot first; `auto_submit` / SQ-full as for that prepare)
- when that send-all terminals
- from `submit()`, and from `wait()` / serve when `auto_submit` is on
- when a worker **may** submit after a send-all CQE

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

| | Per-fd FIFO + a “fds with work” list | One global FIFO |
| --- | --- | --- |
| Drain | O(that fd’s queued ops) | Skip-scan; busy fds leave holes |
| Next-leg | Continuation slot on that fd | Head-insert mixes fds |
| Cancel of active send-all | Peek this fd’s FIFO for cancel-of-active | Walk / skip other fds |
| Allocation | On first send-all or first conflict; free when idle | One buffer always |
| Starvation | Unrelated fds unaffected | A long busy-fd skip-scan on every drain |

**Recommendation:** hash table keyed by fd, value =
`{active, cyclic Completion* queue}`. Next-leg park lives on the ring-wide
fill-wait list, not a per-fd bool. A short list of fds that have drainable
work so `submit()` does not iterate the hash. Free the slot when the fd is
not busy and the FIFO is empty.

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

Progress callbacks stay **out of uring-api**. tealetio can progress per
submitted send-all chunk (`SendBuffer` already batches). A C progress hook can
wait.

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

Next-leg and leftover drain call `get_sqe_try`. Return 0 parks on fill-wait.
An issuer `prepare` with `auto_submit` off raises `SubmissionQueueFull`
instead. Return 1 fills the SQE. `io_uring_enter` runs only when
`ring_can_submit()` is true: `auto_submit` is on and this thread may submit.
A filled next-leg is submitted when this thread may enter and a unique waiter
is already held. Otherwise the next harvest flush or host `submit()`
publishes it.

Do not default `IORING_SETUP_SINGLE_ISSUER` on `UringProactor`. A worker can
fill a next-leg and cannot enter; the issuer has to keep calling `submit()`.

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
3. **SQ full, `auto_submit=False`** — next-leg parks; user `submit()` later.
   `wait()` does not submit (same lazy-submit policy as other prepares).
   `submit()` fills the parked continuation: if the SQ is still full of
   unsubmitted SQEs it kernel-submits them (SQPOLL may wait) rather than
   raising `SubmissionQueueFull`. A wait-only loop will not unstick a parked
   continuation or an unsubmitted next-leg SQE.
4. **Two fds with concurrent send-alls** plus a close on one of them.
5. **Nowait send-all error** after the Python caller has moved on —
   `skip_success` delivers the handle on failure; `skip_all` uses
   `nowait_error_handler`. `pending_count` until terminal. Success stays
   silent either way.
6. **Fd reuse after ring close** of the previous occupant.
7. **`prepare()` of a mixed batch** — each item is SQ or conflict in order
   (`prepare([send_all, close])` fills send-all then parks close). Issuer
   SQ-full still raises or auto-flushes as today; it does not spill onto the
   conflict FIFO. Non-issuer SQ-full parks on fill-wait.
8. **Worker-thread CQE + issuer-only submit** — fill-wait next-leg
   observed by the issuer.
9. **Buffer lifetime** if the Python caller drops the Completion while
   deferred (in-flight ref must cover queued and continuation, not only
   SQE-in-kernel).

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
hook; `submit()` drains.

`probe()` does not need a kernel key — this is userspace. Optional `"SEND_ALL"`
compile-time feature bit is unnecessary while the package is pre-release.

---

## Prepare and submit

`IORING_SETUP_SINGLE_ISSUER` means one thread may `io_uring_enter`. Filling
an SQE is not that. `IORING_SETUP_DEFER_TASKRUN` (which requires
`SINGLE_ISSUER`) also pins completion reaping to that thread.

- Any thread may `prepare` when it finds an SQ slot. `auto_submit` still
  decides whether a full SQ is flushed from prepare, or the issuer raises
  `SubmissionQueueFull`.
- A non-issuer that would have to enter parks on fill-wait. Do not park it
  when a slot is already free, and do not park an issuer `prepare` on
  SQ-full.
- `submit()`, and the `wait()` / `serve_completions` flush, stay on the
  issuer when the setup flags say so. `submit()` from a non-issuer is an
  error. There is nothing to queue: the submission queue already holds the
  work.
- A send-all next-leg fills from any thread that finds a slot, including a
  completion worker. It is submitted only when this thread may enter and a
  unique waiter is already held. Otherwise the next harvest flush or host
  `submit()` publishes it.
- Fill-wait and the conflict FIFO stay separate. Conflict is fd-busy
  serialisation. Fill-wait is thread/enter affinity. A recv on a
  send-all-busy fd is not a conflict. A worker recv with a full SQ is a
  fill-wait park.

`UringProactor` does not default `SINGLE_ISSUER`. One thread enters; the
ring critical section already serialises who writes an SQE.

---

## Still open

- **`SINGLE_ISSUER` + workers.** A worker can fill a next-leg and cannot
  enter. The issuer must keep calling `submit()`. An unbounded `wait_idle`
  on a quiet ring can stall a multi-leg drain. Later, low urgency: a
  `need_submit` callback the worker could fire so the host hooks
  `break_wait`.
- Default `SINGLE_ISSUER` on `SyncUringProactor` only, once that stall has
  a host-side answer.
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
    Parked continuations must not fill a new zc SQE after abandon.
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
2. **No ring-wide lazy list.** `prepare()` still fills an SQE. SQ size is the
   lazy-batching control. A userspace list in front of the SQ was over-design
   (duplicated the kernel SQ, changed `auto_submit=False`, looked like an
   SQ-full FIFO).
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
10. **Do not default `SINGLE_ISSUER`.** Any thread may prepare. The issuer
    still submits. A worker fills a next-leg and parks when it cannot enter.

---

## Out of scope

- Vector / `sendmsg` scatter-gather send-all (`ROADMAP.md` item 10).
- Recv-side bundling, poll_many between-leg pending_count.
- Automatic SQ-full FIFO for *all* prepares (explicitly rejected in
  `ROADMAP.md` Queue Pressure Notes). The conflict FIFO is **fd-busy
  serialisation**, not SQ backpressure. SQ-full still raises
  `SubmissionQueueFull` or flushes via `auto_submit` as today.
- A ring-wide userspace lazy list in front of the kernel SQ. Dropped: SQ size
  already controls batching.
- Storing conflict entries as copied `io_uring_sqe` structs. Completions on
  the per-fd FIFO are enough; drain fills the kernel SQE.
