# Frontend deferred SQ and backend TLS

Status: **Implemented** on branch `design/issuer-deferred-submit`.

## Summary

`UringProactor` owns a **deferred submission queue** (FIFO list of armed ops
waiting for an SQE). Who may enqueue or drain that list is decided by **role**,
not by a per-op API flag:

| Role | Who | Capability |
|------|-----|------------|
| **Frontend** | Driver / wait path, and any client that serialises on the proactor API | Full: submit, deferred enqueue + drain, cancel, wait |
| **Backend** | Completion service workers (`serve_completions`) | Limited: deliver CQEs; **eager-arm only**; never touch the deferred list |

Multiple frontend threads are allowed **if only one is inside the proactor API
at a time** (external lock or a single scheduler driver). Backend workers are
never frontend.

Backend identity is a **proactor-owned thread-local** set for the worker thread
lifetime. When a backend thread cannot eager-arm (SQ-full, or deferred backlog
already non-empty), the proactor raises **`RetryOnFrontend`**. That is a policy
exception, **not** buffer-pool `ENOBUFS` and not a permanent stream failure.

Higher layers (IOManager chains) catch `RetryOnFrontend` and **redo the arm on
the frontend** (marshal to the scheduler). In single-threaded / inline
completion mode there is no backend TLS, so sendall partial-CQE resubmit may
**defer** normally — only true worker threads forbid deferred submit.

## Motivation

Earlier designs drained deferred work at the end of every completion batch.
With `completion_threads > 0` that ran on workers while the frontend might be
in submit or cancel, which forced:

- a deferred-queue lock across enqueue / cancel-remove / drain
- careful mid-submit gaps so cancel never races reverse-link install
- complex nested re-entry and fakes

Completing the rule **“only frontend mutates deferred state”** removes the lock
and matches a simple single-threaded-frontend + worker-backend mental model.

Wait-entry drain recovers the idle case: SQEs free via CQEs while the driver is
parked; the next wait (or submit) drains deferred heads without workers owning
the list.

## Frontend / backend (TLS)

### Thread-local

Each `UringProactor` owns `self._thread_state` (`threading.local` subclass).
Per-thread `__init__` sets `backend = False` (frontend). Workers flip it for
their lifetime:

```text
_service_thread_main:
    _thread_state.backend = True
    try:
        apply nice; ring.serve_completions()
    finally:
        _thread_state.backend = False
```

Inline completion mode (`completion_threads == 0`) never sets the flag: delivery
runs on the driver with full submit capability.

### Submit policy

```text
_submit_uring_op(op):
    if deferred non-empty:
        if _thread_state.backend: raise RetryOnFrontend
        append op
        drain_deferred()              # until SQ-full or empty
        return whether op is ring-armed

    try:
        arm(op)                       # pre_submit installs reverse link
        return True
    except SubmissionQueueFull:
        note stats
        if _thread_state.backend: raise RetryOnFrontend  # no enqueue
        append op
        return False
```

| Context | SQ-full / non-empty deferred |
|---------|------------------------------|
| Frontend | Enqueue FIFO + drain |
| Backend | Raise `RetryOnFrontend` (no enqueue, no drain) |
| Send partial-CQE on frontend | May defer remainder |
| Send partial-CQE on backend | `RetryOnFrontend` → complete with **short** byte count |

There is **no** `allow_defer=` parameter on public proactor ops. Policy is
entirely role TLS + submit path.

### `RetryOnFrontend` (signalling)

Defined in `operations.py`, re-exported from `proactor`:

- Not an `OSError`
- Not buffer-pool `ENOBUFS` (recv pressure / leased views)
- Means: *this arm cannot complete under backend rules; redo on a frontend
  thread that may defer and drain*

IOManager chain sites catch it and `_marshal_on_scheduler` the same arm. Other
failures still fail the chain normally.

## Deferred queue (frontend only)

A FIFO list of ops with `sq_impl` already set, waiting for an SQE. **No lock**:
backend never mutates the list; frontend API access is serialised by contract.

### Drain

```text
drain_deferred():
    while deferred non-empty:
        head = deferred[0]
        try:
            arm(head)
        except SubmissionQueueFull:
            leave head; break
        except Exception as exc:
            pop head; fail(head, exc); continue
        pop head                  # reverse link live
```

### Wait entry

Before the frontend parks (`wait` / `wait_idle` / async park entry):

```text
if deferred non-empty:
    drain_deferred()
# then park
```

Recovery after completions free SQEs while the driver was idle and workers do
not drain.

### Cancel

Frontend only:

- `completion is None` and op on deferred list → remove and terminalise
- reverse link set → `ASYNC_CANCEL` / `poll_remove` as today
- No mid-submit gap: only frontend drains, and reverse link is installed before
  the next frontend cancel can run

## Emulated continuous legs

When multishot is unavailable, continuous ops use one-shot SQEs.

| Op | Behaviour |
|----|-----------|
| `accept_many` oneshot | One accept, **`more=False`**, caller re-arms |
| `recv_many` / `recv_buf` oneshot | One leg, **`more=False`**, `RecvIterBuffer` (or similar) resubmits |
| `poll_many` oneshot | One readiness mask, **`more=False`**, finish; **`stop_poll`** uses `ASYNC_CANCEL` on the live poll SQE (not multishot `POLL_REMOVE`) |

No in-proactor deferred resubmit from delivery. Kernel multishot paths are
unchanged (one arm, many CQEs with `F_MORE`).

## Proactor-internal multi-leg: send only

The only proactor completion path that **re-arms another SQE for the same
logical op** is stream **send** (`_complete_uring_sendall` →
`_resubmit_sendall_remainder`):

- Same submit path as any other arm (subject to frontend/backend TLS)
- Frontend: may defer the remainder
- Backend: `RetryOnFrontend` → deliver **short** total; bytes already on the wire
- IOManager `sock_sendall` re-arms a **new** send for the unsent tail and loops
  until full (or hard error); backend `RetryOnFrontend` on that arm → marshal

`write`, oneshot `recv*`, `connect`, `accept`, etc. do not chain SQEs inside the
proactor. Cross-op composition lives in IOManager.

## IOManager chained ops

| Chain | First | Follow-up | Backend handoff |
|-------|-------|-----------|-----------------|
| Accept-time preread | `accept_many` / accept | `proactor.recv` after eager miss | `_accept_preread_on_worker`: catch `RetryOnFrontend` → marshal recv |
| Oneshot accept + recv | `sock_accept` + group | `proactor.recv` on advance | catch → marshal attach |
| Connect + initial | `proactor.connect` | `_attach_sock_sendall` (eager + `proactor.send` legs) | catch → marshal send arm |
| `sock_sendall` | eager `send` loop | proactor send + short re-arm | catch → marshal arm |
| Accept + streams | `accept_many` | **Happy path:** `open_streams` / `recv_many` on delivery thread. **Recovery:** `RetryOnFrontend` → marshal open to frontend and retry (may be awkward; rare) |

Eager non-blocking try first (preread, sendall) stays: if no proactor submit is
needed, there is no deferred issue.

Socket **create** is stdlib on the frontend; it is not a uring chain leg.

### Hand-off rule (shipped)

For accept→recv and connect→send (and sock_sendall re-arm):

1. Try submit on whatever thread the advance/delivery runs.
2. On success → continue.
3. On **`RetryOnFrontend`** → leave the chain open; `_marshal_on_scheduler` and
   redo the same arm on the frontend (full defer/drain).
4. On true errors → fail the group / post error disposition as today.

Do **not** enqueue deferred from a backend thread and hope a worker drains it.

### `IOWaitGroup`

Still composes sequential legs. Advance may run on a backend thread after a CQE;
arms then inherit backend TLS. Handoff is exception + marshal, not “advance must
be frontend-only.”

## Orphan / idle deferred

| Trigger | Behaviour |
|---------|-----------|
| Later frontend submit | Drain as part of submit |
| Frontend enters wait | Drain at wait entry |
| CQEs free SQEs, driver parked | Wait returns; next wait entry or submit drains |

Workers never pull the deferred list. A pure “submit once, SQ-full, never wait
or submit again” hang is idle misuse of the same class as never waiting for a
normal pending op.

## API / observable notes

- **No** public `allow_defer` / “do not defer” flags on proactor methods
- **`RetryOnFrontend`**: backend cannot place or defer this arm; redo on frontend
- **`ENOBUFS`**: buffer / pool pressure only (recv path)
- **Send short counts**: proactor may complete short under backend `RetryOnFrontend`
  on a continuation; `sock_sendall` loops until full
- **Emulated `poll_many`**: terminal `more=False` after one leg; re-arm at a
  higher layer if more edges are needed
- **Stats**: `submit_queue_full` / deferred peak remain useful
- **Selector proactor**: no deferred SQ; TLS exception path does not apply
- **Multishot kernels**: unchanged

## Alternatives considered

### Worker drain of deferred list

Best latency when CQEs free SQEs without the driver leaving park, but keeps a
cross-thread lock and cancel/submit races. Rejected.

### Per-op `allow_defer=` flag

Explicit but noisy (Protocol, mocks, every chain site). Easy to forget on a new
worker path. Rejected in favour of backend TLS + `RetryOnFrontend`.

### Issuer-only by worker-tid set without a named exception

Operationally similar for workers, but conflates “SQ-full” with “policy refused
defer” and does not spell the recovery (`RetryOnFrontend` → marshal). Revised
to role TLS + dedicated exception.

### Always fail IOManager chains on second-leg SQ pressure

Simpler, but drops connections under transient load. Shipped path prefers
frontend handoff for accept→recv and connect→send.

## Implementation history (branch)

Approximate commit series on `design/issuer-deferred-submit` (order may vary
slightly in git log):

1. FIFO deferred drain on the submit path; stop completion-batch drain
2. Emulated continuous: no delivery deferred resubmit; poll oneshot terminalises
3. Wait-entry deferred drain
4. Chain handoff (worker must not enqueue deferred)
5. Drop deferred list lock; backend cannot enqueue/drain
6. Emulated poll as single terminal leg; stop cancels the SQE
7. Proactor send returns short count on best-effort stop
8. Backend TLS + `RetryOnFrontend`; remove `allow_defer`; IOManager marshal;
   `sock_sendall` loops short sends; connect remainder on frontend

## References

- `RetryOnFrontend`: `operations.py` (re-exported from `proactor`)
- TLS + submit: `UringProactor._thread_state`, `_service_thread_main`,
  `_submit_uring_op`
- Send drain: `_complete_uring_sendall`, `_resubmit_sendall_remainder`
- IOManager: `_accept_preread_on_worker`, `_attach_sock_sendall`,
  `_sock_sendall_from_offset`, `sock_connect` / `sock_create` / `sock_accept`
- Emulated accept/recv/poll oneshot complete handlers in `proactor.py`
- Accept composition: `OPERATION_CALLBACKS.md`
