# Issuer-only deferred submission (proposal)

Status: **Draft** — design only; not implemented.

## Summary

Move `UringProactor` deferred SQ handling onto the **issuer thread only**
(scheduler driver: submit, cancel, wait). Drop worker-side
`_retry_deferred_submissions`, and remove the deferred-queue lock when no
worker path mutates that queue.

Emulated continuous legs (oneshot fallbacks for multishot) **try** to arm the
next leg from delivery. If that resubmit cannot be armed — including SQ-full
backpressure that would only queue work — **finish the continuous op with a
terminal leg** (`more=False`) and **swallow** the failure so delivery stays
clean. Callers that want another edge re-arm at a higher layer (as
`accept_many` and `RecvIterBuffer` already do for recv).

**IOManager chains** that start a **second** proactor submit from a completion
callback (accept-time preread, connect + initial send) follow the same rule:
workers never own the deferred queue. The follow-up either arms immediately,
fails the chain, or returns an incomplete waitable for the **issuer** to finish
(including deferred enqueue/drain).

## Motivation

Today deferred submissions are drained at the end of every completion batch
(`_deliver_uring_completion` → `_retry_deferred_submissions`). With
`completion_threads > 0` that runs on **worker** threads while the issuer may
be in cancel or submit. That forces:

- a deferred queue lock across enqueue / cancel-remove / drain
- careful “hold lock across submit” or head-identity tricks so cancel never
  sees a gap between “off the deferred list” and “reverse link installed”
- extra complexity for fakes and nested re-entry stories

Thread contract already says **submit and cancel are issuer-only**. Completing
the same rule for **deferred drain** matches that contract and simplifies the
proactor.

A single SQ-full with no further activity is rare either way: completion-only
retry stalls without CQEs; submit-only retry stalls without a later submit or
wait entry. We address the idle case by draining when the issuer **settles into
wait**.

## Thread model (unchanged ownership)

| Role | Thread | Touches deferred queue? |
|------|--------|-------------------------|
| First submit / cancel / wait | Issuer (driver) | Yes — enqueue, remove, drain |
| CQE delivery | Issuer (inline) or workers | No drain; may **try** one eager follow-up arm (see below) |
| IOManager chain advance (accept→recv, connect→send) | Often worker-side callback | Eager arm only; never deferred enqueue/drain |
| Kernel multishot | Kernel | No deferred list |

**Rule:** only the issuer mutates the deferred list. Off-issuer paths may
**attempt** a direct arm (`pre_submit` + SQE). On `SubmissionQueueFull` they must
**not** call `_enqueue_deferred_operation` / drain. They either fail the chain,
terminalise (emulated continuous), or hand the incomplete step back to the
issuer.

## Deferred queue (issuer only)

A FIFO list of armed ops (`sq_impl` already set) waiting for an SQE.

### Submit path

```text
_submit_uring_op(op):
    if deferred non-empty:
        append op
        drain_deferred()          # until SQ-full or empty
        return whether op is now ring-armed

    try:
        arm(op)                   # pre_submit installs reverse link
        return True
    except SubmissionQueueFull:
        append op
        return False
```

**FIFO:** when backlog exists, the new op goes to the tail and the drain runs
from the head. New work never steals SQEs ahead of older deferred legs.

### Drain

```text
drain_deferred():
    while deferred non-empty:
        head = deferred[0]
        try:
            arm(head)
        except SubmissionQueueFull:
            leave head in place
            break
        except Exception as exc:
            pop head
            fail(head, exc)       # after structural pop; no user code mid-list
            continue
        pop head                  # reverse link live
```

No re-enqueue of the same op on SQ-full: it remains at the head.

### Wait entry

When the issuer is about to park (`wait` / `wait_idle` / async park entry):

```text
if deferred non-empty:
    drain_deferred()
# then park as today
```

This is the recovery path after completions free SQEs while the driver was idle
and workers no longer drain deferred work.

Optional: if drain arms new ops, the next park may see more CQEs soon — same as
today after worker drain.

### Cancel

Issuer-only, no lock:

- If `op.completion is None` and op is on the deferred list → remove and
  terminalise (same local cancel semantics as today).
- If reverse link is set → post `ASYNC_CANCEL` / `poll_remove` as today.
- Never terminalise under a “mid-submit gap”: only the issuer drains, and
  reverse link is installed before the next cancel can run on that thread.

## Emulated continuous legs

### What is “emulated”?

When multishot is unavailable, the proactor fakes a continuous stream with
one-shot SQEs.

| Op | Emulated behaviour today | This proposal |
|----|--------------------------|---------------|
| `accept_many` oneshot | One accept, **`more=False`**, caller re-arms | Unchanged |
| `recv_many` oneshot / `recv_buf` | One leg, **`more=False`**, `RecvIterBuffer` resubmits | Unchanged |
| `poll_many` oneshot | Emit **`more=True`**, `_queue_op_resubmit` from delivery | **Change** (below) |

Kernel multishot paths are unchanged: one arm, many CQEs with `F_MORE`.

### Continuation rule (poll_many oneshot and any similar auto-resubmit)

After a successful oneshot leg, while the continuous op is not yet user-stopped:

1. **Try** to arm the next leg immediately on the delivery path (same `sq_impl`
   recipe: same fd/mask, next index).
2. **On success:** emit the leg with **`more=True`**, leave the continuous op
   open (same observable stream as today when SQEs are available).
3. **On failure** (including `SubmissionQueueFull`, or any exception from the
   arm path that means “could not place the next SQE”):
   - **do not** enqueue a deferred continuation
   - emit / finish as a **terminal leg** (`more=False`) for the value already
     delivered (or the error policy already used for terminal continuous legs)
   - **swallow** the resubmit failure (do not fail the stream with SQ-full;
     SQ pressure is not a stream error)
   - clear reverse link / deactivate as for a finished continuous leg

Callers that need another readiness edge re-arm `poll_many` (or a helper) with
the next `base_sequence` / index, analogous to accept and recv.

### Why swallow?

SQ-full is backpressure, not a permanent poll failure. Surfacing it as a stream
exception would force every consumer to special-case a condition the proactor
already handles for ordinary submits via the deferred queue. Terminal
`more=False` is the existing “paused; re-arm if you want more” signal (recv
docs already use that language for non-EOF terminal data legs).

### What we remove

- `_queue_op_resubmit` as a deferred-list path from delivery
- Worker (or delivery-end) `_retry_deferred_submissions`
- `_deferred_lock` once nothing off-issuer mutates the list

## IOManager follow-up submits (worker-started)

Several `ProactorIOManager` paths start a **second** proactor op when the first
completes. Today that often runs on a **completion worker** (accept preread) or
via `IOWaitGroup` advance after connect (initial send). Under this proposal those
paths still run their *logic* wherever they do today, but they **must not** put
work on the deferred SQ queue or run drain.

### Cases in tree

| Chain | First leg | Follow-up | Where follow-up is started today |
|-------|-----------|-----------|----------------------------------|
| Accept-time preread | `accept_many` / accept delivery | `proactor.recv(conn, n)` after eager `recv` miss | `_accept_preread_on_worker` on worker delivery |
| Connect + initial | `proactor.connect` | `sock_sendall` / proactor send remainder | `sock_connect(initial=…)` / `sock_create(…, initial_data=…)` via `_attach_sock_sendall` on connect advance |
| Create-connect-send | same pattern | same | `sock_create_streams` / stream connect helpers |

Eager non-blocking try first (already policy for preread and sendall) stays:
if the follow-up completes without a proactor submit, there is no deferred issue.

### When the follow-up proactor arm fails (SQ-full or “would only defer”)

Off-issuer code has two allowed outcomes — **never** “enqueue on deferred and
hope a worker drains it.”

#### (a) Fail the whole chain

Treat the composed waitable as failed:

- Accept+preread: post merged leg `(conn, None, exc)` (or close via
  `finalize_accept_recv_error` policy), same as other recv errors; do not leave
  a half-open preread without a posted disposition.
- Connect+initial: complete the `IOWaitGroup` with the error; existing
  `on_cleanup` / abortive close paths run as on any connect-chain failure.

Simple, predictable, no incomplete ops left for the issuer. Cost: transient
SQ pressure aborts a connection that might have succeeded a moment later.

#### (b) Hand incomplete work to the issuer

Return the follow-up as an **incomplete** operation/waitable to the main
(issuer) path so only the issuer may enqueue deferred / drain:

- Accept+preread: do not finish the merged leg on the worker; marshal “recv not
  armed, please submit” (or an already-constructed but unarmed recipe) to the
  scheduler/issuer; issuer runs normal `_submit_uring_op` (FIFO deferred rules).
- Connect+initial: connect advance that cannot arm send returns control so the
  group’s next step is attached/submitted on the issuer thread (e.g. re-enter
  `_attach_sock_sendall` only after marshal, or submit send via issuer-only
  helper).

The chain stays **one logical waitable** from the caller’s point of view; only
the **submit ownership** of the second leg moves to the issuer.

### Preferred default

- **Emulated continuous poll continuation:** terminalise + swallow (above) —
  not a multi-step IOManager group.
- **IOManager multi-step chains (accept+recv, connect+initial):** prefer
  **(b)** when we care about not dropping connections under SQ pressure;
  **(a)** is acceptable as a first implementation cut if marshal plumbing is
  larger than the win. Document which is shipped per API.

Either way: **no resubmit pool / deferred list management on worker threads.**

### Relationship to `IOWaitGroup`

`IOWaitGroup` already composes sequential legs. The change is not the group
model; it is that **`advance` handlers must not call deferred enqueue**. If
advance runs on a worker (or on any non-issuer thread), arm is eager-only; on
SQ-full choose (a) or (b). If advance is guaranteed issuer-only, full submit
path including deferred is fine.

## Orphan / single-failure edge case

If the **only** activity is one SQ-full and nothing else happens:

| Trigger | Behaviour |
|---------|-----------|
| Later issuer submit | Drain runs as part of submit |
| Issuer enters wait | Drain at wait entry |
| Completions free SQEs, driver parked | Wait returns on CQE / wake; **next** wait entry or submit drains |

We do **not** rely on workers to pull the deferred list after CQEs. We **do**
rely on wait-entry drain so a driver parked solely for that deferred op still
makes progress when woken.

A pure “submit once, SQ-full, never wait again, no further submits” hang is
the same class of idle misuse as never waiting for a normal pending op.

## API / observable changes

- **`poll_many` without multishot:** streams may stop after a successful mask
  with `more=False` under SQ pressure (or if a continuation arm fails). Clients
  that loop forever on one continuous op without handling terminal non-error
  legs must re-arm. Prefer documenting this next to accept/recv emulation.
- **Accept-time preread / connect+initial:** under SQ pressure on the second
  leg, either a chain error (a) or slightly longer latency while the issuer
  finishes the leg (b) — not a silent hang and not worker-side deferred.
- **Multishot kernels:** no change.
- **Cancel / `poll_remove`:** issuer-only; cancel of a deferred head remains
  local terminalise without ring cancel.
- **Stats:** `submit_queue_full` / deferred peak remain useful; worker-side
  retry counters go away if any existed only for that path.

## Implementation sketch (later)

1. Introduce issuer-only `drain_deferred()`; call from `_submit_uring_op` (with
   FIFO enqueue-when-backlog) and from wait entry (`_wait_inline` /
   `_wait_workers` / async wait entry as appropriate).
2. Stop calling `_retry_deferred_submissions` from `_deliver_uring_completion`.
3. Change `_deliver_uring_poll_many_oneshot` to try-arm-or-terminalise; delete
   deferred `_queue_op_resubmit` (or reduce it to the eager try helper).
4. Audit IOManager chains (`_accept_preread_on_worker`, `_attach_sock_sendall` /
   connect advance, stream connect with `initial_send`): eager arm only off
   issuer; implement (a) or (b) on SQ-full.
5. Remove `_deferred_lock` and locked helpers once all list access is
   issuer-thread only; keep a single list + peak counter.
6. Tests: SQ-full FIFO ordering; wait-entry drain; poll oneshot continuation
   success vs terminalise on SQ-full; cancel of deferred head; accept+preread
   and connect+initial under forced SQ-full; no deferred mutation from workers.

## Alternatives considered

### Keep worker drain

Best latency when CQEs free SQEs without the driver leaving park, but keeps
the cross-thread lock and cancel/submit races this proposal removes.

### Always terminalise emulated legs (never try continuation)

Simpler, matches accept/recv, one re-arm per event always. Slightly more
submit churn when SQ is empty. This proposal keeps a **best-effort**
continuation try for poll so unloaded systems keep today’s streaming shape.

### Enqueue continuation deferred from workers

Restores lock (or SPSC + issuer drain). Rejected: the point is to keep deferred
mutation on the issuer.

## Open questions

1. **Async wait entry:** exact hook for drain (before `wait_idle` vs before
   executor `wait`) for both inline and worker completion modes.
2. **Wake policy:** after a worker delivers a CQE that frees SQ capacity while
   deferred is non-empty, should we always `wake_wait` so the driver re-enters
   wait entry and drains, even if other pending ops remain? Likely yes when
   deferred non-empty after a batch that did not drain.
3. **Hard arm failures (emulated continuous):** swallow only
   `SubmissionQueueFull`, or also unexpected exceptions from `sq_impl`?
   Proposal: swallow SQ-full and treat other exceptions as terminal continuous
   error (fail the stream), not silent drop — unless we find a reason to
   swallow more broadly.
4. **IOManager default (a) vs (b):** ship fail-chain first for accept+preread and
   connect+initial, or invest in issuer handoff immediately? Preference in
   text above leans (b) for production quality under load.
5. **Connect advance thread:** confirm whether `IOWaitGroup` advance after
   connect always runs on issuer today; if yes, connect+initial may already be
   issuer-only and only needs “do not enqueue deferred from a future
   worker-moved advance.”
6. **Selector proactor:** out of scope; no uring SQ deferred list.

## References

- Current deferred + cancel notes in `UringProactor.cancel` docstring
  (issuer-only submit/cancel).
- Emulated accept: `_deliver_uring_accept_many_oneshot` (`more=False`).
- Emulated recv: `_deliver_uring_recv_oneshot` / `RecvIterBuffer._schedule_resubmit`.
- Emulated poll today: `_deliver_uring_poll_many_oneshot` + `_queue_op_resubmit`.
- Accept-time preread: `ProactorIOManager._accept_preread_on_worker`,
  `OPERATION_CALLBACKS.md` accept composition diagram.
- Connect + initial: `sock_connect(initial=…)`, `sock_create(…, initial_data=…)`,
  `_attach_sock_sendall`; streams `initial_send` on connect helpers.
