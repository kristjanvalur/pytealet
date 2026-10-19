# Auto-submit

`Ring(auto_submit=False)` means the application calls `submit()` for the
SQEs it prepares. That is the normal rule, not a guarantee that a queued
SQE stays invisible. Housekeeping the library arms later is published on
its own, and that publish copies the whole unpublished tail, so an
application SQE already sitting there can go out with it.

`True` (the default) publishes application SQEs at a few fixed points.
It does not decide what happens when the submission queue is full. That
is `make_room`, also fixed at construction and also on by default. The
two flags are independent. `ring_new` leaves both on.

`workers_may_submit` is a separate bit, cooked from the setup flags. It
answers which thread may call `io_uring_submit`. `auto_submit` does not
change it.

## Application SQEs

An application SQE is one `prepare()` filled, on the driving thread or
inside a completion callback.

With `auto_submit` off:

- `wait()`, `serve_completions`, and `poll()` do not publish it on
  their own.
- The SQE stays unpublished until `Ring.submit()`, until `make_room`
  submits a tail it is already on, or until a housekeeping publish
  copies that tail.

With `auto_submit` on, the same SQE is published:

- before `wait()` or `serve_completions` reaps, when the queue holds a
  waitable SQE (under `SQPOLL`, whenever the tail is unpublished);
- after a `wait()` burst that prepared one;
- when a `serve_completions` callback returns, under the waiter rule, or
  at end of turn under `SQPOLL`.

`Ring.submit()` publishes either way. It does not read `auto_submit`
or `make_room`.

## Make-room

`make_room` is what `prepare()` does when the submission queue has no
free slot. It is not a signal that the kernel is behind on socket
buffers. The wait is the issuer catching up: inside `submit()` on a
plain ring, and after the publish under `SQPOLL`, until the kernel
thread has consumed a slot. Both look the same to the caller.
`prepare()` blocks until the new SQE is queued, or the stuck-queue
timeout fires.

With `make_room` on (the default):

- A thread that may submit publishes the unpublished tail and waits for
  a slot. The SQE that did not fit is queued after that publish, so it
  starts the next unpublished batch.
- A housekeeping fill uses the same rule. A next leg that finds the
  queue full submits that tail and then takes a slot.
- A thread that must not submit still parks on the fill-wait list.
  `make_room` does not override `workers_may_submit`.

With `make_room` off:

- A thread that may submit raises `SubmissionQueueFull`.
- A thread that must not submit parks.
- A housekeeping SQE that cannot fit parks until `submit()`, or until a
  slot frees on its own under `SQPOLL`.

`auto_submit=True` with `make_room=False` still publishes at the points
above and raises when `prepare()` cannot fit. `auto_submit=False` with
`make_room` left on still requires `submit()` for a normal batch, and
submits on its own when that batch has filled the queue. A linked
operation and its timeout still reserve both slots together.

## Housekeeping SQEs

Some SQEs are filled by the library, not by a `prepare()` the application
can submit at that moment:

- a send-all next leg, including an abandon NOP;
- a copy off the fill-wait list or a per-fd conflict FIFO.

Those are published when this thread's housekeeping flag is set, even if
`auto_submit` is off. The application is not in a call that can `submit()`
when the SQE appears. Leaving it unpublished stalls a driver inside
`wait_idle`: the driver will not `submit()` again until some completion
wakes it, and the completion is the one that was never published.

For a single-threaded ring that is mostly send-all next legs and the
per-fd queue. A multi-threaded ring adds the same work on a worker that
is allowed to submit.

`io_uring_submit` publishes the whole unpublished tail. A housekeeping
publish therefore includes any application SQE already queued, ahead of
the housekeeping SQE or behind it, including one `prepare()` just added
in the same call that drained the parked op. SQEs prepared after that
publish are not included. They wait for `submit()`, or for a later
housekeeping publish. An application that needs a batch to stay invisible
should not leave those SQEs queued across a completion that will arm
housekeeping.

There is no separate mark for "this parked op was already covered by
`submit()`". A `prepare()` that parked because the fd was busy, or
because this thread could not enter, is published when a later drain
copies it onto the queue.

With `make_room` off, the copy does not enter to make a slot.
`submit()` does, so a parked next leg that is only waiting for space is
usually placed by `submit()` itself. With `make_room` on, that next leg
submits the tail that filled the queue and then takes a slot. What a
completion copies later is work that still could not run: the fd was
busy, or this thread must not submit.

## No service thread

The driving thread is the only reaper. `wait()` publishes housekeeping
filled while handling that burst, then keeps reaping when the burst
delivered nothing user-visible and the housekeeping flag is set. A
callback that raises, or packaging that fails after a copy is already
queued, still gets that publish. The callback or packaging error is the
one `wait()` raises. One
`wait()` can finish a multi-leg send while another thread reads the
socket. The same thread cannot `recv` during that repark. `wait(0)` does
not repark.

An application prepare made before `wait()` stays unpublished when
`auto_submit` is off, unless that wait publishes housekeeping and the
prepare is already on the tail. Otherwise the driving thread calls
`submit()`.

## Service threads

The classes are the same. Who is awake is not.

The policy cooks two housekeeping flags. The owner's flag is always on.
The worker's flag follows `workers_may_submit`: on for a normal ring and
for `SQPOLL` (including `SQPOLL` with `SINGLE_ISSUER`), off for
`DEFER_TASKRUN` and for `SINGLE_ISSUER` without `SQPOLL`.

The worker that filled a housekeeping SQE publishes it when its flag is
set. The host may already be inside `wait_idle`. The serve waiter also
publishes a copy it places before it blocks in the kernel, so a slot
that frees between completions does not leave the SQE in userspace.

An application `prepare()` on that worker still follows `auto_submit`
for the strategic points, and `make_room` for a full queue. Off for
`auto_submit` means the prepare does not publish by itself. Off for
`make_room` means a full queue raises `SubmissionQueueFull` when the
worker is allowed to submit. If the same turn fills housekeeping, that
prepare can ride along. Otherwise the callback calls `submit()`, or
those SQEs wait until the driving thread next submits. Housekeeping on
the same thread does not raise and does not wait for that wake. When
`make_room` is on, a full queue submits instead of raising.

A thread whose housekeeping flag is off may still copy a parked
operation into a free slot. It does not publish. The owner's `submit()`
does. That is the `SINGLE_ISSUER` ring without `SQPOLL`, and
`DEFER_TASKRUN`.

The housekeeping count is per thread, shared by every ring that thread
touches. Housekeeping filled on one ring during a handler can publish
another ring's unpublished tail on the same thread.

## Policy fields

`ring_submission_policy_apply` cooks `UringApiSubmissionPolicy` from
`auto_submit`, `make_room`, and the setup flags. Call sites read one
flag for this thread. The owner reads the issuer flag. Any other thread
reads the worker flag.

| Field | Follows |
| --- | --- |
| `workers_may_submit` | Setup flags only. Off for `DEFER_TASKRUN`, and for `SINGLE_ISSUER` unless `SQPOLL` is set. |
| `issuer_on_sq_full` | `make_room` |
| `worker_on_sq_full` | `make_room` and `workers_may_submit` |
| `issuer_before_reap`, `issuer_callback_sqes` | `auto_submit` |
| `worker_before_reap`, `worker_callback_sqes` | `auto_submit` and `workers_may_submit` |
| `callback_submit` | Off when `auto_submit` is off. `END_OF_TURN` under `SQPOLL`. Otherwise the waiter rule. |
| `issuer_housekeeping` | Always on. |
| `worker_housekeeping` | `workers_may_submit` |

`submit()` and `flush_if_full` do not read `auto_submit`. They do reject
a thread that must not submit.
