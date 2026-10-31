# uring-api Changelog

All notable changes to the `uring-api` package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Highlights

### Added

- ``Ring.abort_queue(completion)`` marks the pipeline of a completion that
  is still on its per-fd queue, or of the ``send_all`` active on that
  queue, and cancels that ``send_all`` when one is in progress. The mark
  is set before the cancel, so a drain that finishes successfully still
  leaves the fd abortive. The caller posts the close. The call returns
  true when it issued a cancel. The C capsule entry is
  ``ring_abort_queue(ring, completion, &cancelled)``. On success it
  stores 1 in ``*cancelled`` when a cancel was posted, and 0 otherwise.
- ``ACCEPT_TCP_NODELAY`` in the ``prepare_accept`` / ``prepare_accept_multishot``
  flags word. The bit is removed before ``accept4``. On success the extension
  sets ``TCP_NODELAY`` before the accepted fd is delivered, including a
  multishot ``MORE`` leg. A ``setsockopt`` failure is ignored and the fd
  is still delivered. An ``AF_UNIX`` listener rejects the option.
- ``prepare_close``, ``construct_close``, and the nowait close helpers take
  keyword-only ``abortive=False``. ``abortive=True`` sets ``SO_LINGER`` with
  a zero timeout when the close SQE is filled, so a TCP connection resets.
  A ``setsockopt`` failure is ignored. A close parked behind a nowait
  (``skip_all``) ``send_all`` does the same when that drain fails. A
  waitable failure leaves that close graceful.

### Fixed

- ``wait_idle()`` delivers ``KeyboardInterrupt`` when ``SIGINT`` interrupts
  the park, including a park with no timeout. Any other interrupt returns
  false and leaves the gate closed.
- A new prepare waits behind whatever is already on fill-wait. One free
  submission slot used to be taken by that prepare while a parked link
  timeout, which needs two slots, stayed queued. ``submit()`` copies the
  head first. The same rule applies to a send-all next leg and to a
  nowait prepare. None of the three enters to make room for a head that
  does not already fit, including when ``make_room`` is on. A shortfall
  parks the new op, so an in-flight send stays in progress. ``submit()``
  and ``wait()`` place the head. When fill-wait is empty, ``make_room``
  still enters for the operation itself.
- Cancelling a ``send_all`` whose next leg is waiting for a submission
  slot sends that leg and queues the cancel behind it. The queue no
  longer rewrites the parked leg as a NOP, and ``prepare_cancel`` no
  longer raises ``SubmissionQueueFull`` for that case. The leg
  completion reads abandon under the ring lock before it arms another
  send, so a cancel from another thread is not followed by a new leg.
  The drain finishes with
  ``-ECANCELED`` unless that leg already emptied the buffer. A
  ``send_all`` still queued, with no leg submitted, is filled as a NOP
  when the drain reaches it. That CQE is what ``wait()`` and a callback
  deliver, and abandon finishes it ``-ECANCELED``. The cancel follows
  the NOP.
- A nowait helper does not post while fill-wait already holds a
  completion. Close, shutdown, cancel, and ``poll_remove`` allocate a
  ``skip_all`` ``Completion`` and use the same prepare path as a waitable
  op. A head that already fits is copied first. The nowait parks at the
  tail when that head does not fit.
- A completion callback can ``memoryview.release()`` the buffer it
  submitted. The export drops before the callback runs, once the kernel
  is finished with those bytes. A free-threaded waiter resumes inside
  that callback; holding the export until the completion was destroyed
  raised ``BufferError``. A zero-copy send keeps the export until that
  ``Completion`` is destroyed. The ring drops its reference on the
  internal ``NOTIF``; a caller who kept the object holds the export
  after the send result. ``memoryview.release()`` can fail after the
  send has completed. Dropping the reference does not. ``bytes`` cannot
  be overwritten in that window. Mutable buffers such as ``bytearray``
  stay accepted: the caller must not store into them until the
  completion releases the buffer.
- Destroying a ``Ring`` releases a completion parked on a per-fd queue or
  the fill-wait list, and the buffer that completion owns. The ring keeps
  one reference and moves it with the completion. A second reference used
  to keep that object alive after the ring was gone.
- An abortive close sets ``SO_LINGER`` only once its close SQE stays
  queued. A nowait close that has to park, and a prepare that fails
  before that, leave the socket unchanged. A link timeout that cannot
  be filled rolls the close back without the option set. A later fill
  of that same close still applies it.
- ``wait()`` still publishes housekeeping queued while it handles a
  burst when a completion callback raises, or when packaging that
  completion fails after a copy is already queued. The callback or
  packaging error is the one ``wait()`` raises. A flush error beside it
  is reported separately.

### Changed

- Only a nowait ``send_all`` (``Completion.skip_all``) that fails leaves
  that fd abortive. A negative result, a zero-length send with bytes
  left, and an abandoned incomplete drain all count, and only when
  ``skip_all`` is set. A waitable drain,
  and ``skip_success`` alone, leave the flag alone. The caller sees the
  failure and can call ``Ring.abort_queue`` before posting the close. A close already queued
  behind a waitable failure stays graceful. The per-fd slot stays until
  a close is copied out of its conflict FIFO. A close already on that FIFO
  is not rewritten. When it is filled from an abortive slot it gets
  ``SO_LINGER`` at a zero timeout, and the mark then drops. With nothing
  in progress the queue is empty, so a close prepared after the failure
  is filled immediately, including ``prepare_close_nowait``, and a send
  is filled immediately with fd ``-1``. The ring does not submit a close
  on its own.
- On an abortive fd, a completion popped off the conflict FIFO for submit,
  other than ``close``, is submitted with fd ``-1`` and finishes
  ``-EBADF``. A queued ``send_all`` is still the active drain: later
  entries wait, its SQE uses fd ``-1``, and ``-EBADF`` finishes that drain
  on the normal send-all path. The mark stays until a close is filled.
  The close keeps the descriptor.
  With nothing in progress, an operation that sees the abortive slot is
  filled immediately with fd ``-1`` instead of being parked. A close is
  filled immediately and drops the mark. Already-submitted work finishes
  on the real fd.
- ``make_room`` is fixed when the ring is created and is independent of
  ``auto_submit``. Both default to true. When ``make_room`` is on,
  ``prepare()`` submits and waits until it can take a slot, on a plain
  ring and under ``SQPOLL``. When it is off, a thread that may submit
  raises ``SubmissionQueueFull`` and a thread that must not submit parks.
  A housekeeping fill uses the same flag. ``Ring(auto_submit=False)``
  still makes room. Pass ``make_room=False`` to keep a full queue for an
  explicit ``submit()``. Assigning ``ring.make_room`` raises
  ``AttributeError``. The C capsule reads it with ``ring_make_room``.
  ``ring_new`` leaves it on.
- ``auto_submit`` is fixed when the ring is created. ``Ring(auto_submit=False)``
  is the way to turn it off. Assigning ``ring.auto_submit`` raises
  ``AttributeError``. The C capsule no longer has ``ring_set_auto_submit``.
  ``ring_auto_submit`` still reads the value, and ``ring_new`` still leaves
  it on.
- ``skip_owner_break_wait`` is fixed when the ring is created. The default
  stays true, so ``break_wait()`` from the creating thread does nothing.
  ``Ring(skip_owner_break_wait=False)`` keeps that wake sticky for a later
  ``wait()`` or ``wait_idle()`` on another thread. Assigning the attribute
  raises ``AttributeError``. ``URING_API_SKIP_OWNER_BREAK_WAIT`` no longer
  overrides the constructor.
- With ``auto_submit`` off, the application calls ``submit()`` for SQEs
  it prepares. That is the normal rule, not a guarantee. Housekeeping is
  still published when this thread's housekeeping flag is set: a send-all
  next leg, and a copy off fill-wait or a per-fd queue. The publish copies
  the whole unpublished tail, so an application SQE already queued can go
  out with it. ``wait()`` keeps reaping while that publish armed another
  leg and nothing user-visible was delivered. Making room when the
  queue is full is the separate ``make_room`` flag. See
  ``docs/AUTO_SUBMIT.md``. The
  flag is cooked on the submission policy: the owner always, a worker
  when it may ``submit()``.
- ``Ring.close()`` with ``IORING_SETUP_SQPOLL`` waits until the kernel
  thread has consumed SQEs that ``submit()`` already published, then
  exits the ring. ``io_uring_queue_exit`` otherwise drops that tail, so
  a detached fd from a nowait close stays open. Unpublished SQEs are
  still dropped. The wait is the same few seconds as a full-SQ slot
  wait. A stuck poller raises ``RuntimeError`` and leaves the ring open.
- ``Ring()`` uses ``IORING_SETUP_SQPOLL`` when ``flags`` is omitted. Pass
  ``flags=0`` for a ring without the kernel poller. An explicit ``flags``
  value is used as given. ``probe()`` and ``is_available()`` still create
  a plain ring. The C API passes its flags integer through, so ``0`` does
  not gain the poller.
- With ``IORING_SETUP_SQPOLL``, ``IORING_SETUP_SINGLE_ISSUER`` does not
  restrict which thread may ``submit()``, and it does not turn off a
  worker's full-SQ make-room. The kernel polling thread is the issuer.
  The ring stores that answer as ``workers_may_submit`` on its submission
  policy, from the creation flags, and later checks read the bit.
  ``DEFER_TASKRUN`` still pins the creating thread. The single-issuer
  checks still apply when ``SQPOLL`` is off.
- ``IORING_SETUP_SQPOLL`` keeps the same automatic publish points as a
  normal ring, including the three ``wait()`` publishes: before the
  first reap, after a burst, and before a silent repark. Each of those
  publishes an unpublished tail before the thread blocks, including a
  nowait-only queue. A full submission queue still publishes so
  ``prepare`` can take a slot. ``io_uring_submit`` copies the tail; the
  kernel poller issues. A completion handler also publishes when it
  returns if it prepared any SQE, including a direct nowait close,
  shutdown, or poll_remove. The unique-waiter and host-park gates stay
  off for that handler publish.
- With ``IORING_SETUP_SQPOLL``, each publish point calls
  ``io_uring_submit``. liburing copies the tail when its userspace head
  is behind, and it does not enter when that copy published nothing.
  ``Ring.submit()`` and the ``submit_*_sqes`` counters count how far
  ``sqe_tail`` was ahead of that head. They do not count SQEs the kernel
  thread has not issued. A call that copies nothing adds nothing. A
  normal ring still counts what ``io_uring_submit`` submitted.
- ``Ring.stats()`` and ``UringApiRingStats`` no longer have
  ``submit_next_events`` or ``submit_next_sqes``. A send-all continuation
  is prepared with the other SQEs from that completion and submitted by
  the same enter.
- ``Ring.wait`` submits after handling a burst when this thread prepared
  a waitable SQE during that handling, the same per-thread count a
  service thread uses after its callback. The flush before the first
  reap is unchanged: a queue that is already waitable is still
  submitted, including SQEs another thread prepared before the call.
  Before a silent burst parks again, that same harvest runs for a
  waitable SQE this thread did not prepare.
- A service thread enters after a completion callback only when that
  handling prepared a waitable SQE: a completion, a cancel that marks
  the queue waitable, or a close, shutdown, or poll_remove filled from
  a park. A direct close, shutdown, poll_remove, or other non-waitable
  stays queued. ``submit()``, a full submission queue, or a later flush
  that also carries a waitable SQE publishes it, and one already in
  that queue is submitted with the waitable SQE. A link-timeout SQE
  does not count on its own. A host that only polls, such as Qt
  ``turn()``, does not submit that non-waitable batch either:
  ``wait(0)`` flushes only a waitable queue.

### Fixed

- A completion callback submits once after it returns, when a
  completion-service waiter is already held and that handling prepared
  a waitable SQE. That waiter flushes, then blocks in
  ``io_uring_wait_cqe``. Work the callback queued afterwards — the next
  send, or a send-all's first leg — used to sit in the userspace queue.
  The host parked in ``wait_idle`` does not submit again until something
  completes, so the operation never started. Free-threaded builds hit
  this whenever a callback arms the next operation. A callback that
  drops the GIL before ``prepare()`` can hit it on a GIL build too.
  ``prepare()`` itself stays lazy, so several operations from one
  callback share that single enter. The thread counts waitable SQEs it
  committed during the handling. A callback that committed none does
  not enter, so another thread's queued SQEs stay for the harvest flush
  or for ``submit()``. Once the host has blocked in ``wait_idle`` with
  a timeout other than zero, or opened a ``note_driver_waiting(True)``
  span, that enter also requires the host to be inside the span. The
  host submits on the way into the next blocking wait, so the callback
  leaves the queue alone while the host is running. Until that first
  blocking wait the sleep check stays off. An external poller such as
  Qt never parks, so a waitable SQE prepared in the callback is still
  entered.

- Collecting a ``Completion`` that still holds a ``memoryview`` export no
  longer segfaults on CPython 3.10, 3.11, and 3.12. Those versions drop a
  memoryview's managed buffer while the export is still outstanding. The
  completion keeps that export out of cyclic GC and releases it when the
  completion itself is cleared.

### Removed

- ``BufGroup.release_callback`` and ``BufGroup.release_invoked``.
  ``close()`` unregisters the group. An idle group frees the buf ring
  immediately. An armed group stays registered until the last terminal
  CQE, and a second ``close()`` before that CQE does nothing. Hand a
  live group to a cache with that cache's own call.

## [0.1.0rc5] - 2026-10-09

### Changed
- ``Ring.wait`` and ``serve_completions`` submit prepared SQEs only when
  ``sq_waitable`` is set. A cancel sets that bit, so the next wait submits
  it, unless the cancel is nowait and its target is a ``recv_multishot``
  with ``no_deliver_multi`` set: that pair posts nothing a caller waits on.
  A direct close, shutdown, or poll_remove does not set the bit. The same
  op copied out of a park (the ``send_all`` conflict FIFO, or fill-wait)
  does, so the wait that releases that tail submits it. A direct
  non-waitable SQE stays queued until ``submit()``, a full submission
  queue, or a later flush that also carries a waitable SQE. One already
  ahead of that waitable SQE is submitted with it.
- ``Ring.wait`` parks again after a burst that delivered nothing, when this
  thread may submit and the burst was not ``break_wait``. Waitable SQEs
  prepared while handling that burst (including a send-all next leg) are
  submitted before the next park. A timed wait keeps the caller's original
  deadline and parks with the time still left, instead of restarting the
  full timeout. ``wait(0)`` still returns after one harvest. One external
  ``wait()`` is still one ``wait_calls`` count.
- ``BufGroup.close()`` does not take ``close_mu`` when ``release_callback``
  is set. The flag check and the hook call are plain loads and stores.
  Reading or writing ``release_invoked`` does not take that lock either.
  ``close_mu`` remains the hard release against the terminal completion's
  inflight drop.

### Fixed
- Replacing ``Completion.user_data`` is safe while a multishot MORE shell is
  built and while another thread reads the slot. The shell copy and the
  Python and C getters incref under the same mutex as the setter, then
  release it before allocating. A shell keeps the object it copied. The
  terminal leg still reads the armed slot when its callback loads
  ``user_data``. On a free-threaded build ``Py_NewRef`` only makes the
  refcount update atomic. The pointer load is separate, and the object can
  be freed before that update. CPython has no helper that does both.
  GC traverse still borrows the pointer: the mutex belongs to the ring, and
  a completion can be visited after that ring is gone.

### Added
- ``BufGroup.in_use()`` is true when ``inflight_count`` or ``leased_count``
  is non-zero. An armed receive counts before it selects a buffer, and a
  leased ``BufView`` still counts after that receive has completed.
- ``BufGroup.inflight_count``: armed ``recv_buf`` / ``recv_multishot``
  requests on that group. Incremented when the SQE is filled, decremented
  once on the terminal ``!MORE`` CQE (even if that CQE is not delivered).
  Not the number of leased ``BufView``s. With ``release_callback`` set,
  ``close()`` calls the hook on the calling thread and does not unregister.
  The hook is not deferred to the terminal CQE. A hard close (hook unset)
  unregisters immediately when idle, or on the last terminal CQE while a
  receive is still armed. ``close()`` returns ``None``.
- ``BufGroup.release_invoked``. With ``release_callback`` set, the first
  ``close()`` sets this and calls the hook. Later ``close()`` calls do not
  call the hook until the owner sets the flag false. Clearing the hook and
  calling ``close()`` still unregisters; that path ignores the flag.
- ``Completion.no_deliver_multi`` replaces ``no_deliver_cancel``. It only
  drops later CQEs of a ``recv_multishot`` (MORE legs, EOF, errors, and
  the terminal ``-ECANCELED``) without a callback, once the caller wants
  nothing more from the connection. Accept multishot, poll multishot, and
  oneshot completions, including ``-ECANCELED``, are still delivered. The
  flag may be set on any completion; it has no effect unless the kind is
  ``recv_multishot``. Buffers and the in-flight ref are still released.
  An omitted MORE leg does not allocate a shell ``Completion``. An omitted
  terminal leg does not allocate a ``BufView``. The flag can be set after
  ``prepare`` (unlike ``skip_success``) and is not copied onto MORE shells.
  ``construct_cancel``, ``prepare_cancel``, and the ``*_nowait`` pair take
  keyword-only ``no_deliver_multi=False`` and set the flag on the
  **target** before the cancel SQE is submitted. That does not hide the
  cancel request. Later ``recv_multishot`` CQEs are dropped even if that
  cancel never enters the kernel. If that call fails before a cancel SQE
  exists, a bit it just set is cleared. ``no_deliver_multi=False`` does not
  clear a flag already set on the target, and neither does a failed call.
- ``Ring.stats()``: cumulative counters (no reset; subtract two snapshots).
  ``sqe`` SQEs obtained, ``cqe`` CQEs consumed (including wake NOPs, nowait,
  multishot legs, and zero-copy notifications), ``sq_full`` fill attempts
  whose first peek found no free slot, ``next_leg`` send-all continuation
  sends filled. Submit traffic is split into ``submit_main_*`` (the owning
  thread: ``submit()``, the ``wait()`` flush, and the ``submit()`` before
  ``wait_idle``), ``submit_worker_*`` (the ``serve_completions`` harvest
  flush), ``submit_next_*`` (a deliberate continuation enter), and
  ``submit_sq_full_*`` (one bucket for a make-room flush on any thread).
  ``poll()`` does not submit. Each source has an event count and an SQE
  total, so
  ``sqes / events`` is the average batch. ``sqe`` is not that sum: the
  difference is still in the SQ. ``next_leg_park`` counts continuations that
  parked on fill-wait. ``wait_front_*`` / ``wait_back_*`` are harvest bursts
  from ``Ring.wait()`` and the ``serve_completions`` reaper (an event is a
  reap that returned a CQE; ``*_cqes`` is how many that drain consumed; an
  empty wait is not an event; ``poll()`` is neither). ``wait_calls`` counts
  every ``Ring.wait()`` that reached the reap, empty or not.
  ``cqe`` matches the sum
  of the two ``*_cqes`` totals. ``cq_overflow`` copies the kernel overflow
  count (``0`` after ``close()``). C API: ``ring_stats`` (appended;
  pre-release ABI stays 1).
- ``Ring.poll(timeout=None)``: same park as ``wait()`` without harvesting
  and without submitting (``io_uring_wait_cqe`` / peek, no ``cqe_seen``).
  Prepared SQEs stay queued until ``submit()`` or ``wait()``. A later
  ``wait()`` reaps.
  Same timeout, thread rules, and unique-waiter slot as ``wait()``. C API:
  ``ring_poll`` (appended; pre-release ABI stays 1).
- ``Ring(..., cq_entries=N)``: create-time ``IORING_SETUP_CQSIZE``. Must be
  greater than SQ ``entries``; omitted, liburing sizes the CQ at about 2× SQ.
- ``Completion.user_data`` is settable (and clearable with ``None`` / ``del``).
  Clients may drop the payload after delivery to break cycles with waitables;
  kernel SQE identity remains the Completion pointer. C API:
  ``completion_set_user_data`` (appended vtable slot).
- Multishot delivery: an intermediate ``IORING_CQE_F_MORE`` leg delivers a
  shell ``Completion`` with a copied ``user_data``. The terminal ``!MORE``
  leg delivers the armed submit handle.
- ``Completion.take_user_data()``: return the payload and drop the slot.
  Completion callbacks that reverse-link waitables take possession in one
  call. Assign ``None`` or ``del`` to drop without taking. Both share the
  deferred MORE-shell window (``USER_DATA_CLEAR`` while ``aux_refcount > 0``).
  C API: ``completion_take_user_data`` (appended; pre-release ABI stays 1).
- ``prepare_send_all`` / ``construct_send_all``: one waitable that drains a
  stream buffer with repeated ``IORING_OP_SEND`` legs. Partial CQEs are
  consumed internally; success ``res`` is total bytes, clamped to
  ``INT_MAX``; ``result`` holds the full unsigned count. Later legs set
  ``POLL_FIRST`` when probed. ``pending_count()`` stays non-zero until the
  drain terminals, including ``skip_success`` send-all (unlike tagged
  nowait helpers). ``prepare_cancel`` of the handle abandons further legs so a
  parked continuation completes ``-ECANCELED`` instead of flushing another
  send. ``Completion.skip_success`` skips successful delivery and completes
  the handle on error. ``Completion.skip_all`` (implies ``skip_success``)
  skips user delivery entirely; errors go to ``nowait_error_handler``.
  ``user_data`` is only a token. ``prepare_*_nowait`` sets ``skip_all``.
  C API: ``ring_construct_send_all``.
  A filled send-all next-leg is submitted when this thread may enter and a
  unique waiter is already held (may be in ``wait_cqe``); otherwise it stays
  until harvest flush or host ``submit()``
  / ``wait()``, or parks on fill-wait if there is no slot. ``submit()`` fills parked next-legs
  even when ``auto_submit`` is off (kernel-submit a full SQ rather than
  raising ``SubmissionQueueFull``). The count ``submit()`` returns includes
  those room-making flushes, not only the last enter. The next user
  ``prepare`` fills parked next-legs before taking an SQE.
  While a send-all is busy on an fd (SQE filled, in-kernel, or next-leg
  on the fill-wait list), ``prepare`` of send/close/shutdown/further
  send-all on that fd parks on a per-fd conflict FIFO instead of the
  kernel SQ. Recv and other fds stay independent. Drain (fill-wait first,
  then per-fd FIFO) runs from the next user ``prepare``, ``submit()`` /
  ``wait()``, and when the send-all terminals. A conflicting ``prepare``
  parks on the FIFO before leftover drain, so a full SQ does not raise
  ``SubmissionQueueFull`` for send/close on a busy fd. Leftover drain still
  runs for recv / other fds so parked next-legs take the next SQ slot. A
  non-empty FIFO still counts as busy even if ``active`` is already NULL.
  The fd-busy slot is allocated before the SQE is reserved; ``active`` is
  set only after fill. FIFO drain peeks until fill succeeds. CQE drain
  fills SQEs while a slot exists without ``io_uring_enter`` unless
  ``ring_can_submit()``
  (``auto_submit`` and this thread may submit). Cancel of the *active*
  send-all still fills an SQE (abandon + ``ASYNC_CANCEL``); abandon is set
  before leftover drain so a parked next-leg is a NOP rather than another
  send. Cancel of a queued op stays in the FIFO behind its target. SQ-full
  does not spill onto the conflict FIFO. Waitable ops take the in-flight
  ref (and ``pending_count()``) at SQ fill, conflict-FIFO enqueue, or
  fill-wait enqueue; ordinary nowait still does not count. A full SQ while
  draining that FIFO after a terminal send-all CQE does not fail
  ``wait()`` / ``serve_completions()``
  packaging: the completed handle is delivered and the slot stays on the
  drain list for the next issuer ``submit()`` / ``prepare()``.
- ``IORING_RECVSEND_POLL_FIRST`` and ``IORING_CQE_F_SOCK_NONEMPTY``.
  ``prepare_recv`` / ``construct_recv`` and recvmsg take ``flags`` (cargo
  then ``user_data``). ``POLL_FIRST`` is applied to SQE ``ioprio`` (not
  ``msg_flags``). Bit 0 is also ``MSG_OOB``: that value is poll-first,
  not out-of-band (send already took ``flags``, so this is a behaviour
  change for any existing ``MSG_OOB`` caller).
  ``probe()["IORING_RECVSEND_POLL_FIRST"]`` is the kernel 5.19 floor.
  C API: ``ring_construct_recv`` / ``recvmsg`` take ``flags``.
  ``POLL_FIRST`` on ``recv_multishot`` is unsupported (untested in
  liburing; can strand a ``MORE`` handle with no EOF CQE) and is not
  applied to that SQE.

### Removed
- ``Ring.experimental_send_all_submit_next`` and
  ``URING_API_SEND_ALL_SUBMIT_NEXT``. A filled next-leg is submitted when
  this thread may enter and a unique waiter is already held; otherwise the
  next harvest flush or host ``submit()`` publishes it.
- ``Completion.clear_user_data()`` and C ``completion_clear_user_data``.
  ``take_user_data()`` is the same deferred-clear and also returns the
  payload. Rebuild C clients that cached ``offsetof``.

### Changed
- Drop dead ``from_delivery_thread`` wait path (serve no longer calls
  ``wait()``). Collapse duplicate parked-SQE drain helpers; inline ``wait()``
  post-callback flush uses ``wait_flush_pending_sqes``.
- Internal SQ fill uses ``get_sqe_try`` (1 when a slot was taken, 0 when
  full with no exception, -1 on error). ``prepare()`` raises
  ``SubmissionQueueFull`` when that returns 0 and the operation is not
  parked. Next-leg, leftover drain, and non-issuer park no longer
  build-and-clear that exception as control flow.
- ``serve_completions`` consumes one CQE at a time. A lone worker packs
  immediately; extra workers take copies from a CQE FIFO (they never enter
  the CQ). TAKE does not ``io_uring_submit`` after a CQE (avoids unbatching
  the SQ). Next-leg still uses ``auto_submit`` to make SQ room when this
  thread may enter; it parks on fill-wait when it cannot. A filled next-leg
  is submitted when a unique waiter is already held and this thread may
  enter.
- ``Ring.wait()`` consumes one CQE to completion (package, next-leg /
  fill-wait, optional callback) and peeks the rest. The harvest-then-package
  staging buffer is gone; threaded workers keep a CQE FIFO only as a work
  list.
- Split ``uring_api_prepare.c``: construct factories and ``prepare_*`` sugar
  live in ``uring_api_construct.c``; fill-wait / conflict parks in
  ``uring_api_park.c``; send-all drain in ``uring_api_send_all.c``; SQE fill
  stays in ``uring_api_prepare.c``. ``skip_success_omit_delivery`` is a
  packaging filter in ``uring_api_dispatch.c``.
- Cancel / poll_remove no longer default ``user_data`` to the target handle.
  The target is ``cancel_target``. Pass a token if the ack needs one.
- Internal drain helpers are ``drain_parked`` / ``clear_parked`` (fill-wait
  then conflict FIFOs). Docs use **fill-wait** for the ring-wide park list,
  not "issuer-fill". README has a three-seat table (SQ, conflict FIFO,
  fill-wait).
- ``IORING_SETUP_SINGLE_ISSUER``: any thread may ``prepare`` if the SQ has a
  slot. When a non-issuer would have to enter to make a slot, the handle
  parks on a ring-wide fill-wait list (same circular ``Completion*`` FIFO as
  the conflict queue). Send-all next-leg parks the active handle there.
  ``submit()`` / wait auto-flush stay issuer-only. The owner is the thread
  that created the ring (not the first ``submit()``). Issuer
  ``auto_submit=False`` still raises ``SubmissionQueueFull``.
- C API / docs: ``ring_prepare`` counts conflict-FIFO and fill-wait parks as
  accepted; ``completion_prepared`` / ``Completion.prepared`` stay true only
  after an SQE fill. README / AGENTS / ROADMAP spell out leftover drain
  (fill-wait first, then per-fd FIFO), CQE fill without enter, and conflict
  FIFO vs SQ-full overflow.
- On Python 3.15+, keyword methods use ``METH_FASTCALL`` with
  ``PyArg_ParseArrayAndKeywords``. Older Pythons keep
  ``PyArg_ParseTupleAndKeywords``. ``Ring.__init__`` stays tuple+dict
  (``tp_init`` cannot switch calling convention).
- **Cargo then ``user_data``.** ``prepare_*`` / ``construct_*`` take SQE cargo
  first and ``user_data`` last. METH_FASTCALL three-arg send / accept is now
  ``(fd, data, flags)`` / ``(fd, flags)``, not a token after the fd. ``openat``
  is positional ``dfd, path, flags, mode=0, user_data=None`` (no
  ``dfd=`` keyword-only tail). C construct signatures already used this order.
- Python ``construct_*_multishot`` / ``prepare_*_multishot`` (recv, accept,
  poll) take optional last positional ``base_sequence=0`` after ``user_data``.
  That seeds ``completion.sequence`` before the SQE is filled, so
  auto_submit / SQPOLL cannot harvest a CQE still numbered 0. Existing
  positional cargo-then-``user_data`` calls stay valid. C construct stays
  cargo-only; C clients use ``completion_set_sequence`` after construct.
  Setting ``completion.sequence`` after construct (before ``Ring.prepare``)
  still works.
- C capsule appends ``completion_set_sequence`` and ``ring_wait_idle``.
  Pre-release ABI version stays 1; rebuild C clients that cache
  ``offsetof``. ``ring_wait_idle(timeout)`` parks until ``break_wait``
  (``timeout < 0`` blocks).

### Fixed
- A terminal send-all CQE always releases the fd-busy slot, even if
  ``complete()`` / result packaging fails (OOM). FIFO close/send must not wait
  on a CQE that will never come.
- Ring GC traverse visits conflict-FIFO Completions, so a parked nowait whose
  ``user_data`` points at the ring is collectable.
- Conflicting ``prepare`` (send/close/shutdown/further send-all) parks on the
  per-fd FIFO before leftover drain, so ``auto_submit=False`` plus a full SQ
  does not drop a close/send that should have queued behind a parked next-leg.
- ``prepare_cancel`` of a send-all sets ``SEND_ALL_ABANDON`` before flushing
  parked next-legs, so a continuation is filled as a NOP rather than another
  send. Previously abandon was set only when filling the cancel SQE, after
  that flush (and not at all if the flush raised ``SubmissionQueueFull``).
- Nowait cancel CQEs with ``-ENOENT`` or ``-EALREADY`` (lost race against an
  already-completed or already-completing target) do not invoke
  ``nowait_error_handler``. Waitable cancel still reports those as
  ``res < 0``. Other negative nowait cancel results still fail.
- ``Completion.take_user_data()`` (and ``user_data = None``) defers
  clearing the armed handle while ``aux_refcount > 0``. Two
  ``serve_completions`` workers could deliver ``!MORE`` and nerf
  ``user_data`` before an earlier MORE shell copied it (dropped CQE).
  The pending clear is applied after the last staged leg is packaged —
  same window as the in-flight handle ref, without a second long-lived
  pointer. Shells and idle handles still clear immediately.
- Docs: ``README.md`` / ``AGENTS.md`` describe the multishot in-flight
  ``DECREF`` (``aux_refcount`` / ``AUX_DECREF``) and the deferred
  ``USER_DATA_CLEAR`` flag. ``take_user_data()`` after ``Ring``
  deallocation is undefined (borrowed ring mutex; no per-handle lock).

### Added
- ``Ring.pending_count()``: number of waitable Completions still in flight
  (the prepare in-flight ref). Construct-only and nowait ops are not
  counted; a multishot handle counts as one until its terminal CQE is
  packaged. C API: ``ring_pending_count``.
- **Construct then prepare** for every waitable op. ``construct_*`` binds cargo
  (sidecar or ``cancel_target``) without an SQE so reverse links can be armed
  first. ``Ring.prepare(completion_or_sequence)`` fills SQEs and returns the
  count. Python ``prepare_*`` is construct+prepare sugar. Nowait
  close/shutdown/cancel/poll_remove are temporary ``Completion`` holds
  (``construct_*_nowait`` or ``completion.skip_all = True``); prepare stamps a
  tagged SQE and drops the handle. Cross-ring prepare is undefined; a failed
  batch may leave the prefix prepared. C API: ``ring_construct_*`` +
  ``ring_prepare`` (no per-op submit slots).
- ``Ring.auto_submit`` (constructor keyword and property; default ``True``) and
  ``SubmissionQueueFull``. When on, a full SQ flushes from prepare, and
  ``wait()`` / ``serve_completions()`` flush before parking. When off, prepare
  raises ``SubmissionQueueFull`` instead of flushing, and wait/serve do not
  submit — call ``Ring.submit()`` first. C API: ``ring_auto_submit`` /
  ``ring_set_auto_submit``.

### Removed
- ``Ring.pre_submit`` and C API ``ring_set_pre_submit`` / ``ring_set_c_pre_submit``.
  Arm reverse links after construct (or after prepare returns) on the thread
  that will cancel; serialise with multi-leg re-arm (see tealetio
  ``UringProactor``).
- C API per-op ``ring_submit_*`` / ``ring_submit_*_nowait``. C clients
  construct then ``ring_prepare()``; ``ring_submit`` remains the flush. Rebuild
  C clients.

### Changed
- The unique CQ waiter always ``io_uring_submit``s before harvest when this
  thread may enter. There is no ``worker_auto_submit`` switch (the
  pre-release C accessors are removed; rebuild C clients). TAKE workers
  never ``io_uring_submit``. ``auto_submit`` still gates host ``wait()`` and
  whether this thread may enter. A filled send-all next-leg is submitted
  when that waiter is already held.
- Python ``Ring.submit_*`` / ``submit_*_nowait`` renamed to ``prepare_*`` /
  ``prepare_*_nowait``. They only fill SQEs; ``Ring.submit()`` is the flush.
- **Lazy submit:** ``prepare_*`` and nowait helpers only fill SQEs. Flush with
  ``Ring.submit()``, or — when ``auto_submit`` is on — ``wait()`` / serve, a
  full SQ, or post-delivery. ``break_wait`` still submits its wake NOP.
  ``auto_submit=False`` raises ``SubmissionQueueFull`` on a full SQ. Cancel and
  poll_remove are fully lazy like other prepares. C API: ``ring_submit``.
- Some ``prepare_*`` methods are positional-only (``METH_FASTCALL``): close,
  shutdown, cancel, poll_remove, accept, poll, poll_multishot.

### Added
- ``IORING_SETUP_SQPOLL`` exported for ``Ring(..., flags=...)``. Opt-in kernel
  SQ polling; creation may fail (privileges, container policy). No special
  probe — handle ``OSError`` at ring construction. Liburing's submit path
  wakes a sleeping poller (``IORING_SQ_NEED_WAKEUP``) automatically.
- Nowait prepares (no ``Completion``, no delivery; return
  ``None``). Internal nowait SQE token; ``IOSQE_CQE_SKIP_SUCCESS`` when
  ``IORING_FEAT_CQE_SKIP`` is available; failure CQEs (``res < 0``) invoke
  ``Ring.nowait_error_handler`` when set:
  - ``prepare_close_nowait(fd)``
  - ``prepare_shutdown_nowait(fd, how)``
  - ``prepare_cancel_nowait(completion)`` — cancel ack only; target is still a
    waitable handle
  - ``prepare_poll_remove_nowait(completion)`` — remove ack only
  C clients use ``ring_construct_*`` + ``completion_set_nowait`` +
  ``ring_prepare`` (no dedicated nowait vtable slots).
- `Ring.nowait_error_handler`: optional ``hook(context)`` when a nowait
  CQE fails (``res < 0`` only). Successful nowait CQEs — which still arrive when
  ``IOSQE_CQE_SKIP_SUCCESS`` is unavailable — are dropped silently and never
  invoke the hook. Nowait cancel ``-ENOENT`` / ``-EALREADY`` are also silent
  (waitable cancel still reports them). Context keys: ``message``, ``ring``, ``res``, ``flags``,
  ``kind`` (``COMPLETION_KIND_*`` from the tagged SQE), ``fd`` (advisory int, or
  ``None`` for cancel/poll_remove; may truncate huge fds). Invoked after CQ
  drain (same GIL window as packaging/delivery; not under the drain lock). Must
  not re-enter ring wait/serve. If the hook raises, ``exception_handler`` is
  used (same shape as delivery-callback failures, with ``completion=None``);
  if that is unset or also raises, the error is written as unraisable
  and the drain continues. C API: ``ring_set_nowait_error_handler()`` (appended
  vtable slot).
- SQE ``user_data`` tagging: ``Completion*`` keeps bits 1:0 clear; specials use
  bit 0 set (wake ``…01``, nowait ``…11`` with kind+fd payload). Replaces static
  token addresses for wake/nowait.

- `Ring.wait_idle(timeout=None)`: host-side idle park separate from CQ reaping.
  Parks until `break_wait` or `close` (or timeout). Returns `True` if signalled,
  `False` on timeout.
- `Ring.break_wait()`: opens the `wait_idle` park **immediately**. When
  completion service is idle, best-effort submits **one** internal NOP CQE so a
  blocking `wait()` on an empty CQ can return; while serve workers are active the
  NOP is skipped (idle only). The NOP uses tagged wake ``user_data`` (``…01``;
  no ``Completion`` object); reaping marks it seen and discards it. Duplicate
  in-flight wake tokens are acceptable. NOP failure still succeeds after
  signalling.
- `BufGroup.release_callback` and `BufGroup.close()`: optional owner hook for
  pool reuse. When `release_callback` is set, `close()` calls
  `release_callback(group)` and leaves the provided-buffer ring intact. When it
  is unset, `close()` frees the kernel buf ring (same work as finalization).
  Dealloc still frees the group if nothing called `close()`, and does not invoke
  the callback. Owners that truly dispose a group (for example a cache drain)
  should clear `release_callback` before `close()`.
- `Ring.exception_handler`: optional callback invoked when a delivery callback
  raises (Python or C). The handler receives a context dict with `message`,
  `exception`, `ring`, and `completion` (the CQE being delivered, or `None` for
  nowait-handler failures). When it returns normally, that worker continues
  serving; when it is unset or raises, `serve_completions()` exits with the
  exception and only that worker stops. C API: `ring_set_exception_handler()`.
- `prepare_accept_multishot(..., base_sequence=0)`: optional start index for
  multishot accept leg numbering, matching `prepare_recv_multishot`. The first
  successful accept CQE uses `completion.sequence == base_sequence`, then
  increments. C API: `ring_construct_accept_multishot(..., base_sequence)`.

### Fixed
- (Historical, pre_submit era) After an SQE was reserved and linked to a
  ``Completion``, failure in the removed ``pre_submit`` path rewrote that SQE
  as a wake NOP before the caller dropped the ``Completion`` ref, avoiding a
  dangling pointer on a later successful submit. With hooks gone, prepare either
  links successfully or never reserves; that neutralize path is removed.
- `Ring.break_wait()` opens the `wait_idle` park before (and independent of) the
  internal NOP submit, so a full submission queue cannot drop scheduler wakeups.

### Changed
- Delivery callbacks (`Ring.callback` and `UringApi_CCompletionCallback`) are
  invoked **once per user-visible CQE**, not once per drain with a Python list.
  Drain still peeks extra CQEs, but uring-api iterates them in C. Typical
  waits deliver one or two CQEs; wrapping those in a list and iterating again
  in Python was wasteful and locked the C side into batching. `wait()` without
  a callback still returns a list (built lazily; internal CQEs such as
  zero-copy NOTIF never allocate one). ``break_wait`` wake NOPs are discarded
  at staging. Empty drains (timeout, internals-only) skip the callback and
  `wait` still returns `None`. Each `wait` snapshots the callback once, so a
  blocked pull-mode waiter cannot have its list discarded if another thread
  sets `Ring.callback` (`WAITING` does not lock the setter). Callback-mode
  drain returns `None`, not an empty list; finish does not re-read the
  property. An unrecovered callback error still invokes later CQEs in that
  drain (later failures are unraisable). Inline ``wait()`` callback drain
  does not drop the GIL between CQEs (that hand-off was for worker
  interleaving; workers now take one CQE from the queue with the mutex
  dropped). `exception_handler`
  context uses `completion` for the CQE that raised. Callback pointers must
  not be changed while `serve_completions()` workers are active.
  `URING_API_CAPI_ABI_VERSION` remains **1** while the package is pre-release;
  clients must check `struct_size` and null-check vtable pointers they rely on.
- ``serve_completions`` workers share a staged-CQE work queue (mutex + condvar).
  One thread is the unique kernel waiter (``wait_cqe`` + peek); it publishes
  staged CQEs and wakes the others. Each worker takes **one** CQE, drops the
  mutex, and runs the callback, then circles back. The last worker to exit
  packages leftover queued CQEs (first callback error still propagates;
  later ones are unraisable). Publish steals the harvest buffer when the
  queue is empty so a second grow cannot drop already-seen CQEs.
  Pull-mode ``wait()`` is unchanged (exclusive with workers). The old
  ``cqe_drain_lock`` is gone.
- `prepare_accept()` and `prepare_accept_multishot()` no longer pass a peer
  sockaddr buffer to the kernel. Delivered completions expose the accepted fd
  only; resolve peer addresses with `getpeername()` when needed.

## [0.1.0rc4] - 2026-07-12

### Fixed
- Release workflow: publish job checks out the repository before creating the
  GitHub release (rc3 wheels reached PyPI but the release step failed).

## [0.1.0rc3] - 2026-07-12

### Added
- `Completion.cancel_target` exposes the operation being cancelled on cancel and
  poll_remove completions.

### Changed
- `submit_cancel()` and `submit_poll_remove()` accept optional `user_data` for
  the cancel completion CQE, matching other submit helpers.

## [0.1.0rc2] - 2026-07-03

### Added
- Provided-buffer receive: `BufGroup`, `BufView`, `submit_recv_buf()`, and
  `submit_recv_multishot()` with leased `BufView` completions.
- Poll operations: `submit_poll()`, `submit_poll_multishot()`, and
  `submit_poll_remove()`.
- Positional file I/O: `submit_read()`, `submit_write()`, and `submit_openat()`.
- Async file metadata: `submit_statx()`, `submit_statx_fdsize()`, `statx_st_size()`,
  and statx layout constants.
- `CompletionKind` `IntEnum` exported from `uring_api`.
- Kernel version floor gating for named `probe()` capabilities (#38).

### Changed
- C API clients call `_impl` submit paths directly instead of bouncing through
  Python methods.
- Consolidated provided-buffer completion kinds (`RECV_BUF`, `RECV_MULTISHOT`).
- `submit_recv_multishot()` requires an explicit caller-owned `BufGroup`.
- Enforce `IORING_SETUP_SINGLE_ISSUER` and `IORING_SETUP_DEFER_TASKRUN` thread
  ownership for submit, `wait()`, `break_wait()`, and `serve_completions()`.
- Moved native extension sources into `src/_uring_api/`, matching core `tealet`'s
  `src/_tealet/` layout.
- Split the monolithic test suite into focused modules with shared `conftest.py`
  and `helpers.py` (see `tests/README.md`).
- Aligned `pyproject.toml` package-data with `setup.py` so wheels ship
  `uring_api_completion_kinds.h` alongside `uring_api_capi.h`.

### Fixed
- `BufGroup` / `BufView` lifecycle, GC traversal, and buffer recycle hardening.
- Per-ring `BufGroup` ID recycling after group release (#33).

## [0.1.0rc1] - 2026-06-29

### Notes
- First `uring-api` release candidate with ring lifecycle, socket submit/wait
  operations, callback-thread completion delivery, and the public C API capsule.

