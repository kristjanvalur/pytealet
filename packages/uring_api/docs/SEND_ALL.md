# Synthetic send-all and deferred submissions in uring-api

Design for moving stream send-all into `uring-api` as one waitable, with
send/close/shutdown on the same fd serialised in C.

**Status (2026-08-24):** the copying `send_all` op itself is on
`feat/uring-send-all` (`construct_send_all` / `prepare_send_all`, internal
next-leg re-arm, `pending_count` for the whole drain). The per-fd conflict
FIFO, fd-busy tracking, and send-all-specific cancel are **not** done yet.

---

## Verdict

This is worth doing, and the motivation is right: moving stream send-all into
`uring-api` so a drain is one waitable, with send/close/shutdown on that fd
serialised in C, would remove the most expensive Python-side send protocol we
have today.

It is larger than a new opcode helper. The hard part is not the send loop. It
is **fd-busy tracking, a per-fd conflict FIFO in front of the existing kernel
SQ, and send-all-specific cancel at SQ-fill time** — while leaving today’s
`prepare()` → SQE path, `auto_submit`, and SQ-sized lazy batching alone.

Do it **uring-api first**. tealetio adoption is a follow-up PR once the C
contract is stable. Do not land send-all and the Python-layer rewrite in one
PR.

`IOSQE_IO_LINK` is not a substitute: you cannot pre-build N linked sends for
one buffer, and linking send+close would close after a partial first send.
Kernel `MSG_WAITALL` is also not a reliable stream send-all.

---

## What exists today (the cost of staying in Python)

`tealetio.UringProactor.send()` already emulates send-all:

- First leg: `construct_send` / `construct_send_zc`, arm reverse, `prepare`.
- Partial CQE: `_complete_uring_sendall` re-prepares the remainder under
  `_multi_leg_lock`.
- Cancel: abandon the reverse link, then `ASYNC_CANCEL` the current leg. A
  racing success CQE must not re-arm.
- `send_close_nowait`: send-all then close in a done-callback. Documented
  constraint: **do not submit another send on that sock until the drain has
  finished**.
- `SendBuffer`: at most one in-flight `sock_sendall`; later writes coalesce
  in Python.

Known fallout of doing this above the ring:

- `ring.pending_count()` can be **zero between legs**, so scheduler `run()` /
  idle may return while a drain is still live (`scheduler.py` documents this).
- Next-leg prepare runs on completion workers, which is why `UringProactor`
  cannot default `IORING_SETUP_SINGLE_ISSUER`.
- Python must serialise send vs close vs a second send with locks, waiter
  callbacks, and “in flight” flags. That is the opposite of fire-and-forget.

A C send-all that stays pending until the buffer is exhausted, and that queues
conflicting ops on that fd, fixes all three.

---

## Evaluation of the proposed system

The eight points below are the right shape. A ring-wide userspace lazy list in
front of the SQ is **not**. That hop was tried and dropped: it duplicated the
kernel SQ, changed `auto_submit=False`, and bought nothing send-all needs.
**SQ size (`Ring(entries=…)`) is what controls lazy batching**, as today.

### Two places a prepared Completion can sit

Today `prepare()` fills a kernel SQE immediately; `submit()` only calls
`io_uring_submit`. That kernel SQ **is** the lazy batch. `auto_submit` still
makes room inside `get_sqe` when the SQ is full. Keep that.

The SQ cannot host send-all serialisation by itself: anything in it is
published on the next enter, including a close sitting behind a send-all that
still has remaining legs. The only new structure is a **per-fd conflict FIFO**
of Completions that must not enter the SQ yet.

| Place | What | Role |
| --- | --- | --- |
| **Kernel SQ** | liburing SQEs | Filled by `prepare()` / `prepare_one_constructed` as today. Next `io_uring_submit` publishes them. Size is the batch limit. |
| **Conflict FIFO** | Per-fd FIFO of `Completion *` | Only for ops that **conflict** with an already-filled send-all on that fd (SQ, in-kernel, or `continuation_pending`). Drain copies them into the SQ with the existing fill path when the fd is free. |

```text
construct_*                 cargo on Completion, caller-owned
      │
      ▼
prepare()
      │
      ├─ no conflict        get_sqe + prep_*  → kernel SQ
      │                     if send-all: fd becomes busy
      │                     auto_submit: flush SQ if full (unchanged)
      └─ conflicts          append that fd’s conflict FIFO
                            (except cancel-of-active send-all → SQ)

submit() / wait flush       io_uring_submit, as today
                            plus drain conflict FIFOs of fds that are free
                            (and continuations)

send-all terminal           busy cleared; drain that fd’s conflict FIFO
                            into the SQ (may start the next send-all)
```

Busy is set when a send-all **SQE is filled**, not after `io_uring_submit`.
`prepare([send_all, close])` therefore serialises in one batch: first item
fills an SQE and sets busy, second goes to conflict. `prepare(close)` then
`prepare(send_all)` publishes close first — caller order, same as today.

No ring-wide lazy list. Conflict drain calls the same
`prepare_one_constructed` as a normal prepare. SQ-full still raises
`SubmissionQueueFull` when `auto_submit` is off; it does not spill onto the
conflict FIFO (that FIFO is fd-busy serialisation, not SQ backpressure).

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

Same Completion A, offset advanced. Next-leg SQE into the kernel SQ (or
`continuation_pending` if this thread cannot `get_sqe`). C stays on conflict.
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

### 1. Mark an fd busy with send-all — yes

Busy means: this fd has a send-all whose SQE has **already been filled**
(current leg in the SQ or in-kernel, or `continuation_pending`). Recv, accept,
poll, and other fds stay independent (full-duplex).

Ops that **conflict** (`prepare()` parks them on that fd’s conflict FIFO
instead of filling an SQE):

- `send` / `send_zc` / `sendmsg` / `sendmsg_zc` / further `send_all`
- `shutdown` / `shutdown_nowait` (especially `SHUT_WR`)
- `close` / `close_nowait`
- `cancel` / `cancel_nowait` whose **target’s fd** is this fd (send-all, a
  queued send, close, …). Look the fd up on the target Completion.

Non-conflicting (`prepare()` fills an SQE even while the fd is send-all-busy):

- recv / recv_buf / recv_multishot / recvmsg
- accept, poll, connect
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
v1 (do not grow the conflict set without a test).

### 2. Conflict FIFO of Completions — yes. SQE copies would not have been simpler

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
| in kernel SQ | liburing SQ | true |
| in kernel (submitted) | io_uring | true |

### 3. Next-leg re-arm — yes, but not as a second queue entry

A continuing send-all is the **same** Completion (same `user_data` pointer the
kernel already knows). Do not enqueue a second handle at the head.

Per-fd state should look like:

```text
active: Completion*          # the send-all in progress
continuation_pending: bool   # next leg ready, no SQE yet
queue: Completion*[]         # conflicting ops, FIFO
```

On a partial send CQE (`res > 0`, bytes remain):

1. Advance the retained view offset on `active`.
2. If this thread **may submit** (`ring_check_submit_thread` quiet) and
   `get_sqe` succeeds: prep the next `IORING_OP_SEND` immediately (same
   Completion pointer, `POLL_FIRST` on later legs).
3. Otherwise set `continuation_pending` (cannot submit: SINGLE_ISSUER /
   DEFER_TASKRUN worker, or SQ full with `auto_submit` off). **Do not raise**
   `SubmissionQueueFull` out of CQE drain.

Drain order for that fd: **continuation first**, then the FIFO. That is the
“head of the queue” requirement without mixing the active handle into the
FIFO.

`get_sqe` today raises if the worker is not the issuer. Next-leg must treat
that as defer, not as a Python exception.

### 4. Other send-alls queue — yes

A second `send_all` on a busy fd is parked on that fd’s conflict FIFO at
`prepare()`. When the active drain terminals, drain copies the FIFO into the
SQ: the next send-all fills an SQE, marks the fd busy, and later FIFO entries
for that fd stay parked (point 7).

### 5. Cancel is queueable and FIFO — no instant unlink

Do **not** scan the conflict FIFO to complete a target locally. Same rule as
today’s lazy SQ: if you `prepare(send)` then `prepare(cancel(send))` then
`submit()`, the kernel sees send then cancel. Some bytes may go out. That is
the existing contract (“prepare the target first if one flush should publish
both in order”). The conflict FIFO is the same idea one stage earlier.

**How cancel finds the fd.** `prepare(cancel)` reads the fd off
`cancel_target`’s sidecar (`view_state.fd` / `scalar_state.fd`). If that fd is
send-all-busy and the target is not the active drain, park the cancel on
**that fd’s conflict FIFO**. If it *is* the active send-all, fill a
send-all-cancel SQE now. No extra hash of in-flight send-alls:
`fd_table[fd].active == cancel_target`. A reverse `Completion* → fd` map would
only duplicate the sidecar fd.

**When the cancel SQE is filled:**

- Target is the **active send-all**: do **not** blindly `io_uring_prep_cancel`
  as if it were a oneshot. Set the abandon/cancel bit so a racing success CQE
  cannot re-arm, then `ASYNC_CANCEL` the **current leg** identity (the send-all
  Completion pointer, same `user_data` on every leg). If only
  `continuation_pending` is set (no SQE in kernel), still issue
  cancel-of-user_data; `-ENOENT` is the lost-race case, and the abandon bit is
  what stops the continuation from being published. That is the only
  send-all-specific cancel code.
- Target is any other op (queued send, close, another send-all that we just
  moved into the SQ ahead of this cancel): ordinary
  `io_uring_prep_cancel(cancel_target)`. FIFO already submitted the target
  first, so the kernel can find it. Same as cancel sitting behind a target in
  the kernel SQ today.

**While the fd is send-all-busy, `prepare(cancel)` of the *active* send-all
still fills an SQE.** Other prepares for that fd go to conflict (more sends,
close, cancel of a *queued* send-all). Drain of the FIFO uses the same rule.
See the worked example above for `B, cancel(B), close`.

Waitable cancel still completes only the **cancel** waitable (ack /
`-ENOENT`). The target completes from its own CQE (or send-all terminal).
Nowait cancel `-ENOENT` / `-EALREADY` stay silent.

### 6. Drain points — conflict FIFO into the SQ, then the usual submit

`prepare()` of a **non-busy** fd still fills an SQE immediately (`auto_submit`
/ SQ-full unchanged).

Also drain a fd’s conflict FIFO (continuation first, then FIFO):

- when that send-all terminals
- from `submit()`, and from `wait()` / serve when `auto_submit` is on
- when a worker **may** submit after a send-all CQE

While draining a FIFO, the same conflict test applies: a dequeued send-all
marks the fd busy and later entries stay parked; cancel-of-active still fills
an SQE. Recv and other fds are unrelated `prepare()`s, not this drain.

No behaviour change for ordinary ops: `prepare()` still means “SQE is in the
SQ” unless the fd is send-all-busy.

### 7. Stop publishing conflicting ops once a send-all SQE is filled — yes

Not a separate “stop the world” flag. `prepare()` of a recv on another fd still
fills an SQE. `prepare()` of send/close/send-all on the busy fd goes to that
fd’s FIFO. Drain of that FIFO parks again as soon as it fills the next
send-all.

### 8. Per-fd queues vs one global queue — recommend per-fd

| | Per-fd FIFO + a “fds with work” list | One global FIFO |
| --- | --- | --- |
| Drain | O(that fd’s queued ops) | Skip-scan; busy fds leave holes |
| Next-leg | Continuation slot on that fd | Head-insert mixes fds |
| Cancel of active send-all | Peek this fd’s FIFO for cancel-of-active | Walk / skip other fds |
| Allocation | On first send-all or first conflict; free when idle | One buffer always |
| Starvation | Unrelated fds unaffected | A long busy-fd skip-scan on every drain |

**Recommendation:** hash table keyed by fd, value =
`{active, continuation_pending, cyclic Completion* queue}`. A short list of
fds that have drainable work so `submit()` does not iterate the hash. Free the
slot when the fd is not busy and the FIFO is empty.

No hash of send-all Completions. Identity is `fd_table[target.fd].active ==
target`.

---

## Recommended Completion / pending model

One user-visible `Completion` for the whole drain (`COMPLETION_KIND_SEND_ALL`).
Intermediate partial CQEs are consumed internally, like `send_zc` NOTIF.

- Success: deliver the armed handle once, `res == total bytes`.
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

`send_zc` is two CQEs per leg (op + `IORING_CQE_F_NOTIF`) plus buffer lifetime
until NOTIF. Mixing that with send-all continuation and cancel is a second
project. v1 send-all always uses ordinary send. tealetio can keep zc for
one-shot `sendto` / `sendmsg`; stream send-all goes through the synthetic op.

---

## Threading, locks, CQE path

This is the riskiest implementation surface.

- Ring CS already serialises prepare. Fd table + queues live under that CS (or
  the same lock prepare already takes).
- CQE drain currently stages, then packages, then delivers. Next-leg re-arm
  should happen **when the send CQE is packaged**, before Python delivery, so
  Python never sees partial send-all CQEs.
- Nested `get_sqe` during drain: `get_sqe` may flush (`auto_submit`) or wait
  (SQPOLL). Doing that while holding `cqe_drain_lock` is the lock-order bug to
  design first. Likely: decide continuation vs enqueue **without** calling a
  flushing `get_sqe` from inside drain; try a non-flushing `io_uring_get_sqe`;
  on failure set `continuation_pending` and let the next `submit()` / wait
  flush path drain.
- Worker + SINGLE_ISSUER: workers only set `continuation_pending` / enqueue;
  the issuer thread’s `submit()` / `wait()` publishes. This is the mechanism
  that later lets `UringProactor` consider SINGLE_ISSUER; **do not** flip that
  default in the send-all work.

---

## What this does *not* replace in tealetio

Even with atomic send-all, `SendBuffer` should still coalesce. Firing a nowait
send-all on every tiny `write()` defeats `min_write` and multiplies SQEs. The
Python win is:

- `UringProactor.send()` becomes one `prepare_send_all` (drop
  `_complete_uring_sendall`, send abandon, `_multi_leg_lock` for send).
- `send_close_nowait` is `send_all` + `close_nowait`; close queues behind the
  drain — **no “don’t send until finished” contract**.
- `StreamWriter.wait_closed` no longer special-cases in-flight vs queued at
  the proactor layer.
- `pending_count()` stays non-zero for the whole drain — scheduler idle
  becomes honest for send-all (oneshot `poll_many` is a separate gap).

Keep tealetio adoption on a follow-up PR so uring-api can ship tests against
the C contract alone.

---

## Risks (must be tested, not just documented)

1. **Cancel vs success race** on the last remaining bytes — same as today;
   specify and test.
2. **Queued cancel behind queued send-all** — FIFO submits the send-all, then
   send-all-cancel of the now-active drain; no unlink.
3. **SQ full, `auto_submit=False`** — next-leg parks; user `submit()` later;
   wait() with auto_submit off must still be able to publish a parked
   continuation or the drain sticks forever.
4. **Two fds with concurrent send-alls** plus a close on one of them.
5. **Nowait send-all error** after the Python caller has moved on —
   `nowait_error_handler`; pending_count until terminal.
6. **Fd reuse after ring close** of the previous occupant.
7. **`prepare()` of a mixed batch** — each item is SQ or conflict in order
   (`prepare([send_all, close])` fills send-all then parks close). SQ-full
   still raises or auto-flushes as today; it does not spill onto the conflict
   FIFO.
8. **Worker-thread CQE + issuer-only submit** — `continuation_pending`
   observed by the issuer.
9. **Buffer lifetime** if the Python caller drops the Completion while
   deferred (in-flight ref must cover queued and continuation, not only
   SQE-in-kernel).

---

## API sketch (uring-api)

```python
# construct then prepare, same cargo-then-user_data rule
c = ring.construct_send_all(fd, data, flags, user_data)
n = ring.prepare(c)          # SQE, or conflict FIFO if fd is send-all-busy
# convenience
c = ring.prepare_send_all(fd, data, flags, user_data)

c.nowait = True
ring.prepare(c)              # fire-and-forget drain; still pending_count until done
```

C capsule: `ring_construct_send_all` + existing `ring_prepare` / `ring_submit`.
No per-op submit slot.

No public “deferred queue” type. No `flush_deferred()` unless tests need a
hook; `submit()` drains.

`probe()` does not need a kernel key — this is userspace. Optional `"SEND_ALL"`
compile-time feature bit is unnecessary while the package is pre-release.

---

## Implementation plan (stacked PRs)

Work on `feat/uring-send-all` (based on `main`). Land as independently
reviewable PRs.

A ring-wide lazy list (prepare no longer fills an SQE) was **rejected** and
must not land. SQ size remains the batch limit.

### PR 1 — Synthetic `send_all` (copying send)

- New completion kind + view state with offset.
- First-leg `prepare()` fills an SQE as today.
- CQE path: consume partials, `continuation_pending` vs immediate prep,
  terminal delivery.
- `pending_count` covers the whole drain (including nowait).
- Later legs `POLL_FIRST` when probed.
- Tests: full accept in one CQE; multi-leg with a small `SO_SNDBUF` / no
  reader; zero-byte; error; nowait + handler; pending_count never 0 between
  legs.

**Done** on this branch (uncommitted at the time of writing), except fd-busy
marking which belongs with PR 2.

### PR 2 — Fd table and conflict FIFO

- Hash of fd → `{active, continuation_pending, conflict FIFO}`.
- `prepare()` of send/close/shutdown/cancel on a busy fd enqueues; drain into
  SQ when the fd is free (`submit()` / wait / send-all terminal).
- Drain stops parking further once a dequeued op starts send-all; still fill
  cancel-of-active while busy.
- Tests: send_all then close_nowait; two send_alls; cancel of active; cancel
  queued behind a queued send-all; cancel of a normal send still in the FIFO;
  worker CQE + issuer submit; SQ-full still raises with `auto_submit` off (no
  spill onto conflict).

### PR 3 — Docs, C API, changelog

- `README.md`, `AGENTS.md` submit/cancel invariants, `ROADMAP.md` (send-all
  done; note zc send-all and SINGLE_ISSUER still open).
- `_uring_api.pyi`, `uring_api_capi.h`, kinds header, `tests/capi_client`.
- `CHANGELOG.md`.

PR 1 already touched most of these for the public `send_all` surface. PR 3
covers conflict-FIFO behaviour.

### Follow-up

- tealetio: `UringProactor.send` / `send_close_nowait` use `send_all`; delete
  Python sendall re-arm and send abandon.
- Optional later: send-all + `send_zc`; default SINGLE_ISSUER on
  `SyncUringProactor` only.

---

## Key decisions

1. **Do it in uring-api**, not another Python emulator — one waitable, honest
   pending_count, fire-and-forget close.
2. **No ring-wide lazy list.** `prepare()` still fills an SQE. SQ size is the
   lazy-batching control. A userspace list in front of the SQ was over-design
   (duplicated the kernel SQ, changed `auto_submit=False`, looked like an
   SQ-full FIFO).
3. **Conflict check at `prepare()`.** If the fd is send-all-busy and the op
   conflicts, park on that fd’s FIFO; otherwise `get_sqe` + prep as today
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
9. **tealetio consumption is a follow-up** so the C contract can freeze first.
10. **Do not default SINGLE_ISSUER** in this work; only make next-leg safe when
    the worker cannot submit.

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
