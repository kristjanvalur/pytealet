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
| CQE delivery | Issuer (inline) or workers | No drain; emulated continuous may **try** one continuation submit (see below) |
| Kernel multishot | Kernel | No deferred list |

If emulated continuation is implemented as “try issuer-style submit from the
delivery thread,” that path must not take a cross-thread deferred lock: either
continuation is **eager only** (succeeds or terminalise — never enqueue from a
worker), or continuation is **marshalled** to the issuer. The preferred rule in
this proposal is **eager try only; never enqueue deferred work from delivery**.

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
4. Remove `_deferred_lock` and locked helpers once all list access is
   issuer-thread only; keep a single list + peak counter.
5. Tests: SQ-full FIFO ordering; wait-entry drain; poll oneshot continuation
   success vs terminalise on SQ-full; cancel of deferred head; no lock
   required under threaded completions for deferred list (stress optional).

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
3. **Hard arm failures:** swallow only `SubmissionQueueFull`, or also unexpected
   exceptions from `sq_impl`? Proposal: swallow SQ-full and treat other
   exceptions as terminal continuous error (fail the stream), not silent
   drop — unless we find a reason to swallow more broadly.
4. **Selector proactor:** out of scope; no uring SQ deferred list.

## References

- Current deferred + cancel notes in `UringProactor.cancel` docstring
  (issuer-only submit/cancel).
- Emulated accept: `_deliver_uring_accept_many_oneshot` (`more=False`).
- Emulated recv: `_deliver_uring_recv_oneshot` / `RecvIterBuffer._schedule_resubmit`.
- Emulated poll today: `_deliver_uring_poll_many_oneshot` + `_queue_op_resubmit`.
