# Auto-submit

`Ring(auto_submit=False)` means the application decides when an SQE it
prepared becomes visible to the kernel. `True` (the default) publishes
those SQEs at a few fixed points, and when a full submission queue has to
make a slot. Housekeeping the library arms later is neither of those
choices.

`workers_may_submit` is a separate bit, cooked from the setup flags. It
answers which thread may call `io_uring_submit`. `auto_submit` does not
change it.

## Application SQEs

An application SQE is one `prepare()` filled, on the driving thread or
inside a completion callback.

With `auto_submit` off:

- `wait()`, `serve_completions`, and a full queue do not publish it.
- `poll()` never publishes.
- A thread that may submit raises `SubmissionQueueFull` when `prepare()`
  cannot take a slot without entering.
- A thread that must not submit parks that `prepare()` on the fill-wait
  list instead of raising.
- The SQE stays unpublished until `Ring.submit()`.

With `auto_submit` on, the same SQE is published:

- before `wait()` or `serve_completions` reaps, when the queue holds a
  waitable SQE (under `SQPOLL`, whenever the tail is unpublished);
- after a `wait()` burst that prepared one;
- when a `serve_completions` callback returns, under the waiter rule, or
  at end of turn under `SQPOLL`;
- when `prepare()` needs a free slot and the queue is full.

`Ring.submit()` publishes either way. It does not read `auto_submit`.

Make-room stays on that list. Publishing to free a slot submits the
application's own unpublished batch, which is the case `auto_submit` off
exists to prevent. A housekeeping SQE that cannot fit without that
publish parks until `submit()`, or until a slot frees on its own under
`SQPOLL`.

One boolean covers both the strategic points and make-room. The middle
setting (publish at the end of a turn, but raise when the queue is full)
is not a public flag. Nothing in the tree needs a callback batch to stay
atomic past a full queue; a linked operation and its timeout already
reserve both slots together.

## Housekeeping SQEs

Some SQEs are not a `prepare()` the application can submit:

- a send-all next leg, including an abandon NOP;
- a copy off the fill-wait list or a per-fd conflict FIFO of an operation
  `submit()` already covered while it was still parked.

Those are published when this thread's housekeeping flag is set, even if
`auto_submit` is off. The application never sees the SQE, so it cannot
call `submit()` at the moment the SQE appears. A service thread that
leaves it unpublished stalls the driver inside `wait_idle`: the driver
will not `submit()` again until some completion wakes it, and the
completion is the one that was never published.

A `prepare()` that no `submit()` has covered stays unpublished, including
when a later completion copies it onto the queue. Otherwise a `prepare()`
after `submit()` would run by itself.

### The owed mark

`submit()` drains what it can, publishes that tail, then marks every
completion still on fill-wait or a per-fd FIFO. A later copy of a marked
completion is housekeeping. The mark is internal. A `prepare()` after
that `submit()` returns is not marked.

A next leg does not need the mark. It continues a send-all that was
already submitted.

The copy does not enter to make a slot. `submit()` does, so a parked
next leg that is only waiting for space is usually placed by `submit()`
itself. What remains marked is work that still cannot run: the fd is
busy, or a slot did not free.

### Ordered tail

`io_uring_submit` publishes the whole unpublished tail. A housekeeping
publish therefore submits any application SQE already queued, ahead of
the housekeeping SQE or behind it. An application that needs a batch to
stay invisible should not leave those SQEs queued across a completion
that will arm housekeeping. SQEs prepared after that publish are not
included.

## No service thread

The driving thread is the only reaper. `wait()` publishes housekeeping
filled while handling that burst, then keeps reaping when the burst
delivered nothing user-visible and the housekeeping flag is set. One
`wait()` can finish a multi-leg send while another thread reads the
socket. The same thread cannot `recv` during that repark. `wait(0)` does
not repark.

Application prepares made before `wait()` stay unpublished when
`auto_submit` is off. The driving thread calls `submit()` for those.

## Service threads

The classes are the same. Who is awake is not.

The policy cooks two housekeeping flags. The owner's flag is always on.
The worker's flag follows `workers_may_submit`: on for a normal ring and
for `SQPOLL` (including `SQPOLL` with `SINGLE_ISSUER`), off for
`DEFER_TASKRUN` and for `SINGLE_ISSUER` without `SQPOLL`.

The worker that filled a housekeeping SQE publishes it when its flag is
set. The host may already be inside `wait_idle`.

An application `prepare()` on that worker still follows `auto_submit`.
Off means the prepare stays unpublished, and a full queue raises
`SubmissionQueueFull` when the worker is allowed to submit. The callback
calls `submit()` itself, or those SQEs wait until the driving thread
next submits. Housekeeping on the same thread does not raise and does
not wait for that wake.

A thread whose housekeeping flag is off may still copy a parked
operation into a free slot. It does not publish. The owner's `submit()`
does. That is the `SINGLE_ISSUER` ring without `SQPOLL`, and
`DEFER_TASKRUN`.

## Policy fields

`ring_submission_policy_apply` cooks `UringApiSubmissionPolicy` from
`auto_submit` and the setup flags. Call sites read one flag for this
thread. The owner reads the issuer flag. Any other thread reads the
worker flag.

| Field | Follows |
| --- | --- |
| `workers_may_submit` | Setup flags only. Off for `DEFER_TASKRUN`, and for `SINGLE_ISSUER` unless `SQPOLL` is set. |
| `issuer_on_sq_full`, `issuer_before_reap`, `issuer_callback_sqes` | `auto_submit` |
| `worker_on_sq_full`, `worker_before_reap`, `worker_callback_sqes` | `auto_submit` and `workers_may_submit` |
| `callback_submit` | Off when `auto_submit` is off. `END_OF_TURN` under `SQPOLL`. Otherwise the waiter rule. |
| `issuer_housekeeping` | Always on. |
| `worker_housekeeping` | `workers_may_submit` |

`submit()` and `flush_if_full` do not read `auto_submit`. They do reject
a thread that must not submit.
