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

Setup flags are not stored on this policy. `IORING_SETUP_SINGLE_ISSUER`
and `IORING_SETUP_DEFER_TASKRUN` are the caller's contract with the
kernel. A thread that must not `io_uring_enter` finds out when it does:
the call raises `OSError` with `errno` `EEXIST`. A path that does not
enter does not raise. Filling a free SQ slot is not an enter.

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
or `make_room`. An enter the kernel rejects is `OSError` on every one
of these paths.

## Make-room

`make_room` is what `prepare()` does when the submission queue has no
free slot. It is not a signal that the kernel is behind on socket
buffers. The wait is the issuer catching up: inside `submit()` on a
plain ring, and after the publish under `SQPOLL`, until the kernel
thread has consumed a slot. Both look the same to the caller.
`prepare()` blocks until the new SQE is queued, or the stuck-queue
timeout fires.

With `make_room` on (the default):

- `prepare()` publishes the unpublished tail and waits for a slot. The
  SQE that did not fit is queued after that publish, so it starts the
  next unpublished batch. An enter the kernel rejects is `OSError`.
- A send-all next leg uses a stricter rule. A full queue submits that
  tail and then takes a slot, even when `make_room` is off. There is no
  caller to retry the leg. That enter is `OSError` when the kernel
  rejects it, and the send-all fails.
- A free slot is filled with no enter, from any thread.

With `make_room` off:

- A full queue raises `SubmissionQueueFull`. That path does not enter,
  so a setup flag the kernel would reject does not show up here.
- A send-all next leg, and the FIFO copy that completion starts, still
  flush. Any other failure of that submit fails the handling. `make_room`
  is not consulted on either path.

`auto_submit=True` with `make_room=False` still publishes at the points
above and raises when `prepare()` cannot fit. `auto_submit=False` with
`make_room` left on still requires `submit()` for a normal batch, and
submits on its own when that batch has filled the queue. A linked
operation and its timeout still reserve both slots together.

## Housekeeping SQEs

Some SQEs are filled by the library, not by a `prepare()` the application
can submit at that moment:

- a send-all next leg, including one filled after the drain was abandoned;
- a copy off a per-fd conflict FIFO.

Every thread tries to publish those, even if `auto_submit` is off. The
application is not in a call that can `submit()` when the SQE appears.
Leaving it unpublished stalls a driver inside `wait_idle`: the driver
will not `submit()` again until some completion wakes it, and the
completion is the one that was never published. An enter the kernel
rejects is `OSError` and stops that worker. A next leg that cannot be
submitted also fails the send-all.

On a single-threaded ring that work is mostly send-all next legs and the
per-fd queue. A multi-threaded ring does the same publish from the thread
that filled the SQE. Use `SQPOLL` with `SINGLE_ISSUER` when that thread
must enter. Do not set `DEFER_TASKRUN` for a worker that enters, and do
not set `SINGLE_ISSUER` without `SQPOLL` when it must.

`io_uring_submit` publishes the whole unpublished tail. A housekeeping
publish therefore includes any application SQE already queued, ahead of
the housekeeping SQE or behind it, including one `prepare()` just added
in the same call that drained the parked op. SQEs prepared after that
publish are not included. They wait for `submit()`, or for a later
housekeeping publish. An application that needs a batch to stay invisible
should not leave those SQEs queued across a completion that will arm
housekeeping.

There is no separate mark for "this parked op was already covered by
`submit()`". A `prepare()` that parked because the fd was busy is
copied when that send-all completes. The copy runs until the next
send-all is active or the FIFO is empty, and a full queue enters.
`make_room` is not read. `prepare()`, `submit()`, and `wait()` do not
walk the FIFO again.

If that enter cannot take a slot, the completion fails. `wait()` raises.
A `serve_completions` worker stops. The head is not left for a later
call. A next leg that cannot be submitted fails that send-all the same
way.

## No service thread

The driving thread is the only reaper. `wait()` publishes housekeeping
filled while handling that burst, then keeps reaping when the burst
delivered nothing user-visible and that publish armed another leg. A
callback that raises, or packaging that fails after a copy is already
queued, still gets that publish. The callback or packaging error is the
one `wait()` raises. A flush error beside it is reported separately.
One `wait()` can finish a multi-leg send while another thread reads the
socket. The same thread cannot `recv` during that repark. `wait(0)` does
not repark.

An application prepare made before `wait()` stays unpublished when
`auto_submit` is off, unless that wait publishes housekeeping and the
prepare is already on the tail. Otherwise the driving thread calls
`submit()`.

## Service threads

The classes are the same. Who is awake is not.

Every thread tries the housekeeping publish. The host may already be
inside `wait_idle`. The serve waiter also publishes a copy it places
before it blocks in the kernel, so a slot that frees between
completions does not leave the SQE in userspace.

An application `prepare()` on that worker still follows `auto_submit`
for the strategic points, and `make_room` for a full queue. Off for
`auto_submit` means the prepare does not publish by itself. Off for
`make_room` means a full queue raises `SubmissionQueueFull` and does
not enter. If the same turn fills housekeeping, that prepare can ride
along when the housekeeping publish enters. Otherwise the callback
calls `submit()`, or those SQEs wait until the driving thread next
submits. When `make_room` is on, a full queue submits instead of
raising.

A free slot is still filled from any thread, with no enter.
`SINGLE_ISSUER` without `SQPOLL` then fails the following publish: the
worker stops with `OSError` (`EEXIST`), and the filled SQE stays
unpublished. With `SQPOLL` the kernel thread is the issuer, so any
thread may publish. `DEFER_TASKRUN` rejects an enter from another
thread, including a blocking `wait()`, `poll()`, or
`serve_completions()`. A peek (`wait(0)` / `poll(0)`) that does not
enter does not raise. `break_wait()` does not pre-check. With nobody
inside `io_uring_enter` it returns without entering. A NOP it cannot
submit is dropped, and the call does not raise.

The housekeeping count is per thread, shared by every ring that thread
touches. Housekeeping filled on one ring during a handler can publish
another ring's unpublished tail on the same thread.

## Policy fields

`ring_submission_policy_apply` fills `UringApiSubmissionPolicy` from
`auto_submit`, `make_room`, and `IORING_SETUP_SQPOLL`. Flags that
restrict which thread may enter are not stored. The same fields apply
on every thread.

| Field | Follows |
| --- | --- |
| `auto_submit` | The constructor. On by default. `ring_policy_before_reap` and `ring_policy_callback_sqes`. |
| `make_room` | The constructor. On by default. `ring_policy_on_sq_full`. |
| `callback_submit` | Off when `auto_submit` is off. `END_OF_TURN` under `SQPOLL`. Otherwise the waiter rule. |

`submit()` and a send-all next leg do not read `auto_submit`.
Housekeeping is always attempted. A forbidden enter is `OSError` from
the kernel.
