# tealetio Changelog

All notable changes to the `tealetio` package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- ``UringProactor(..., cq_entries=N)``: CQ depth at ring create (must exceed SQ
  ``entries``). Default CQ is ``max(1024, 2 * entries)``.

### Fixed
- Selector write FIFO: cancel of a queued send drops it from the queue so
  drain cannot re-arm a done ``Operation``, and drain isolates ``start()`` /
  ``run()`` failures so a send behind close fails that op instead of the wait
  loop. Nowait close swallows ``OSError`` like waitable close.

### Fixed
- Uring ``accept_many`` / ``recv_many`` seed ``completion.sequence`` on the
  constructed handle **before** ``Ring.prepare`` fills the SQE. Staging
  reads that field when the CQE is harvested (drain lock, no GIL). Setting
  it after ``prepare_*`` raced with auto_submit workers and SQPOLL.


### Changed
- ``UringProactor`` default ring is SQ 256 / CQ 1024
  (``DEFAULT_URING_SQ_ENTRIES`` / ``DEFAULT_URING_CQ_ENTRIES``), sized for a
  256-connection recv-multishot plus send burst. Override with ``entries=``
  and ``cq_entries=``.
- ``SelectorProactor`` queues further send, ``shutdown``, and close behind an
  in-flight send on the same fd (write-side FIFO, same order as uring send-all
  conflict). Recv on that fd is unchanged.
- ``StreamServer`` / ``start_server`` spawn connection handlers with an
  explicit ``eager_start=False`` (was true). Accept delivery already opens
  streams and arms ``recv_many``; eager start ran the handler on the
  marshal/CQE stack and could starve queued work under uring-sync. Pass
  ``handler_eager_start=True`` to opt in. The ``False`` is explicit so a
  scheduler-wide eager task factory cannot re-enable it.
- ``UringProactor`` ring delivery receives one ``Completion`` per CQE. uring-api
  no longer packages a drain as a Python list on the callback path. Cancel and
  poll_remove CQEs use the same complete path as other waitables (the teardown
  op is ``user_data``); delivery no longer special-cases those kinds.
- ``UringProactor._prepare`` passes ``sequence`` as a last positional after
  ``user_data`` on native multishot ``prepare_recv_multishot`` /
  ``prepare_accept_multishot``, so the first-leg index is set before the SQE
  is filled. Oneshot fallback still assigns ``completion.sequence`` after
  prepare. Requires workspace ``uring-api`` with optional construct/prepare
  ``base_sequence`` on the Python multishot APIs.
- Uring delivery takes possession with ``completion.take_user_data()``
  (get-and-clear). Deferred-clear still applies on an armed multishot
  handle while CQEs are staged.
- Tests: ``test_delivery.py`` covers reorder/count helpers;
  RecvIterBuffer tests live in ``test_io_buffers.py``; oneshot ``_arm``
  results are ``box``/``handle`` rather than ``operation``.
- Export hub: ``tealetio`` star-imports ``types``, ``delivery``,
  ``io_waiter``, and ``io_manager``. ``proactor`` no longer re-exports the
  IO facade. Renamed ``operations.py`` + ``continuous_callbacks.py`` to
  ``delivery.py``. ``IOWaiter`` / ``IOHandle`` / ``IOWaitable`` are
  top-level exports. ``InvalidStateError`` for waiters lives in
  ``io_waiter`` (distinct from ``locks.InvalidStateError``).
- Dropped ``IOWaitGroupChildProtocol`` (advance callbacks take
  ``IOWaitGroupChild``). Dropped unused ``IOWaiter.cancelled`` /
  ``IOWaiterSync.cancelled``. ``SocketIO`` return types name the concrete
  waiter (``IOWaiter`` / ``IOWaiterSync``) except where the helper really
  returns a union (``IOWaitable``).
- Dropped unused ``RecvManyHandle`` / ``AcceptManyHandle`` / ``PollManyHandle``
  (they were ``OpHandle``) and unused ``AcceptManyResult``.
- Selector oneshot handles are a private ``_SelectorOpHandle`` in
  ``proactor.py``. ``Operation`` and ``SupportsOperation`` are gone;
  ``tealetio.Operation`` is no longer exported. Submit still returns an
  opaque ``OpHandle``.
- Dropped ``SelectorScheduler.poll_many`` and ``cancel_operation``. Selector
  schedulers keep oneshot ``poll``; continuous poll is ``scheduler.io.poll_many``
  / ``proactor.poll_many`` on proactor-backed schedulers.
  ``SelectorScheduler`` no longer constructs ``Operation``.
- ``IOWaiter.complete`` is the proactor submit callback (was ``accept``).
  ``IOWaitGroup.attach`` takes an ``IOWaiter`` (not a raw ``Operation``).
  Dropped the unused ``IOOperation`` protocol.
- Proactor submit/cancel/stop signatures use ``OpHandle``. Selector
  oneshots all go through ``_spawn_operation``. Dropped the unused
  ``SupportsOperation`` re-export from ``proactor``.
- Dropped ``ContinuousOperation``, ``finish_continuous_delivery``,
  ``_wrap_continuous_delivery``, and the ``SupportsContinuousOperation`` /
  ``SupportsStreamFinish`` protocols. ``CountFinalizer.finish`` is optional
  (accept_many passes an ``IOWaiter`` closer). ``MultishotDelivery`` no
  longer carries an ``operation`` owner field.
- Selector oneshot complete sites invoke the submit callback directly
  (``_finish_selector_oneshot``), not via ``Operation.add_done_callback``.
- Selector proactor fd tracking uses ``_fd_slots`` / ``_FdSlot.handle``
  (not ``_fd_operations`` / ``.operation``). ``_poll`` tracks progress
  with a boolean instead of a completed-ops list.
- Renamed the opaque submit-handle alias from ``CancelHandle`` to
  ``OpHandle``. It is a handle to the submitted operation; cancel (and
  ``stop_poll``) is how you use it.
- Dropped ``UringOperation``, the uring waitable freelist, ``_prepare`` /
  ``_void_cqe`` / ``_complete_uring_operation``, and ``recycle_operation``.
  Uring CQEs are tuple ``user_data`` only. Selector oneshots still return an
  ``Operation`` object internally; callers treat it as the opaque
  ``OpHandle`` alias.
- ``OpHandle`` is one opaque alias for every proactor submit
  (uring ``Completion`` or ``None``, selector oneshot token,
  selector ``SelectorCancelHandle``, emulated poll holder).
  Do not call ``done()`` / ``result()`` on the handle.
- ``IOWaiter`` keeps a single opaque ``_handle`` (``OpHandle``). Selector
  ``Operation`` is that handle, not a wrapped waitable; results always come
  from ``accept()``.
- ``proactor.cancel(handle, callback)`` and ``proactor.stop_poll(handle,
  callback)`` both take ``callback(None, exception)`` and return nothing.
  The callback is the cancel/stop *request* completion (uring cancel /
  ``POLL_REMOVE`` CQE, or immediate on selector). ``cancel_nowait`` stays
  fire-and-forget with no callback.
- ``proactor.poll_many`` returns an opaque handle (native: armed
  ``Completion``; selector: ``SelectorCancelHandle``; emulated oneshot:
  reverse-link holder), not ``ContinuousOperation``. Stop with
  ``proactor.stop_poll(handle, callback)``, a oneshot like other ops
  (``callback(None, exception)``, cancel token). Native posts
  ``POLL_REMOVE``; oneshot abandons reverse and ``ASYNC_CANCEL``s the live
  poll; selector deregisters locally. No runtime kind check — callers pass
  a poll handle. Manager ``IOHandle.close()`` maps to ``stop_poll``;
  ``closed`` follows terminal ``!MORE``, not ``operation.done()``.
- Selector / emulated ``accept_many`` deliver transient accept errors
  (``EMFILE``, ``ENFILE``, ``ECONNABORTED``, ``EPROTO``, ``ENOBUFS``,
  ``ENOMEM``) as ordinary terminal ``OSError`` on the ``IOWaiter``, same
  as native multishot. ``StreamServer`` catches them in the accept loop:
  aborted clients are skipped; fd/memory pressure logs via the scheduler
  exception handler and pauses ``ACCEPT_RETRY_DELAY`` (1s) before re-arm.
- Oneshot proactor submits take ``callback(result, exception)`` and
  return an opaque ``OpHandle`` (uring: the armed ``Completion``, or
  ``None`` when the callback already ran). Covers ``recv`` / ``recv_into``
  / ``recvfrom*`` / ``send`` / ``sendto`` / ``accept`` / ``connect`` /
  ``poll`` / file ops / ``create_socket`` / ``shutdown`` / ``close_*``.
  ``IOWaiter`` is built in the manager: construct, pass ``complete`` as
  the callback, ``bind`` the handle. Selector still parks internally on
  an ``Operation`` used as that same handle. ``recv`` still delivers
  ``RecvResult``; ``sock_recv`` maps to bytes.
- ``proactor.recv_many`` / ``proactor.accept_many`` return ``OpHandle``s
  instead of ``ContinuousOperation``. Both are cancellable callback
  streams, not waitables: chunks go to the submit-time ``callback``, and
  callers cancel via ``proactor.cancel`` / ``cancel_nowait``.
  ``RecvIterBuffer`` holds the recv handle.
  Manager ``accept_many`` / ``accept_many_streams`` still return an
  ``IOWaiter`` (callback mode) so ``StreamServer`` can park on stream-end
  for oneshot re-arm and close; ``CountFinalizer`` settles that waiter
  rather than ``finish_operation``. Handles have no ``kind`` / ``fileobj``
  (poll is stopped with ``stop_poll``), and no ``done()`` / ``exception()``
  — stream state is on the callback deliveries. Uring callback CQEs store
  ``user_data = (handler, user_cb, extra)`` (``extra`` is ``()`` or a
  frozen cargo tuple) and return the armed ``Completion``. Delivery calls
  ``ud[0](completion, ud[1], ud[2])``. Selector uses
  ``SelectorCancelHandle``. Stream recv
  arms ``proactor.recv_many`` directly (no manager ``_recv_many`` hop).
- Selector / emulated continuous cancel emits ``ECANCELED`` at
  ``SelectorCancelHandle._next_index`` for selector recv/accept-many
  (oneshot accept: ``base_sequence``), matching uring ``-ECANCELED`` CQE
  sequence. Uring recv/accept-multi handles do not store ``_next_index``.
  ``index=None`` is no longer a backend cancel encoding.
- ``accept_many`` / ``accept_many_streams`` use ``CountFinalizer`` instead of
  strict ``ReorderBuffer``. User callbacks run in completion/marshal order,
  not index order. A numeric ``!MORE`` defers settling the manager
  ``IOWaiter`` until every sequenced leg through that terminal has been
  delivered, counting in ``finally`` so a raising user callback cannot stall
  ``wait()``. Stream-end (cancel or accept ``OSError``) settles the waiter
  only — it is not raised into the scheduler handler and never reaches the
  per-connection callback. Requires a numeric delivery index (no
  ``index=None`` branch). ``RecvIterBuffer`` and ``poll_many`` stay on
  ``ReorderBuffer``. ``LenientReorderBuffer`` is still gone.
- ``ReorderBuffer`` requires a numeric ``delivery.index`` (no ``index=None``
  passthrough, no ``flush_pending``). ``MultishotDelivery.index`` is ``int``
  (no longer ``int | None``). ``RecvIterBuffer.close`` with no live unfinished
  leg posts sequenced ``ECANCELED`` at the next expected index. Close while
  ``recv_many`` is still installing marks ``_closed`` and cancels after return
  (or sequenced close if that op already finished).
- Scheduler driver batches are bounded like asyncio ``_run_once``: with
  ``yield_every=None`` each batch snapshots the runnable queue after
  timer/threadsafe drain; ``yield_every=N`` still caps cooperative
  transfers. ``yield_to`` is not stolen by that cap. If work remains, the
  driver ``wait(0)`` / ``select(0)`` without consulting the next timer;
  idle still blocks in ``wait``.
- Callback drain is re-entrant-safe: ``_run_ready_timers`` sets a scheduler
  ``_in_callback_drain`` flag for the whole drain, including while the
  draining tealet is suspended inside a callback that switched out. A later
  resume does not start a nested drain. Callbacks must not block-wait; they
  may switch via eager ``spawn``. While the flag is set,
  ``_make_runnable`` of the drain tealet prepends on FIFO / prescheduled
  queues, and a ``PriorityTask`` drain tealet is temporarily raised to
  ``TEALET_PRI_CALLBACK`` so the priority heap and ``on_modified`` stay
  consistent. ``RunnableQueue`` includes ``add_front``. Eager spawn outside
  drain is unchanged.
- Timer and threadsafe callbacks drain on the runner at the start and end of
  each ``_run_ready_batch``, not on every ``_schedule`` resume.
  ``_skip_post_switch_callbacks`` is removed. A ``CancelledError`` raised by
  a callback during drain goes to the exception handler instead of
  cancelling the runner.

### Fixed
- ``gather`` marks child task exceptions retrieved when it takes them onto
  the gather result (same as ``result()`` / ``exception()``). Previously
  the child stayed ``_log_traceback`` and GC printed ``Task exception was
  never retrieved`` after the parent had already raised.

### Added
- Fire-and-forget ``Task`` / ``Future`` exceptions that nobody retrieves
  via ``result()``, ``exception()``, or ``wait()`` are reported through
  the scheduler exception handler on GC (``Task exception was never
  retrieved``). ``CancelledError`` is not logged. Awaited tasks still
  store the exception on the Future and re-raise to the waiter.
- ``Proactor.cancel_nowait(operation) -> None`` and
  ``ProactorIOManager.cancel_nowait``: cancel without a teardown waitable.
  Uring uses ``prepare_cancel_nowait`` and posts whenever a reverse
  ``Completion`` exists (already-done / reverse-idle is left to the kernel;
  ``-ENOENT`` is silent via skip-success). Not valid for ``poll_many``
  (use ``poll_remove``); that is not checked. Selector deregisters and
  terminalises. ``RecvIterBuffer.close`` and exceptional ``IOWaiter`` /
  ``IOWaitGroup`` cancel use it so stream teardown does not allocate a
  cancel ``Operation``. ``cancel()`` remains waitable.
- ``Proactor.close_socket_nowait(sock) -> None``: close without a waitable
  completion. ``UringProactor`` detaches and ``prepare_close_nowait``
  (lazy, same as ``close_socket``);
  selector backends call ``sock.close()``. ``close_socket`` remains
  waitable for ordered teardown.
- ``UringProactor`` installs ``ring.nowait_error_handler`` and routes failed
  nowait CQEs (``res < 0``) to the delivery exception handler. Successful
  nowait CQEs are skipped by ``uring-api`` (``IOSQE_CQE_SKIP_SUCCESS`` when
  ``IORING_FEAT_CQE_SKIP`` is available).
- ``IoExpect`` (``READY`` / ``BLOCK``): first-attempt hint on
  ``Proactor.send(..., expect=)``. Uring omits ``POLL_FIRST`` on a
  ``READY`` first leg and sets it for ``BLOCK`` and for later sendall
  legs. ``sock_sendall`` passes ``BLOCK`` after an eager would-block or
  partial send, ``READY`` when there is no prior try (empty payload).
  Other ops will take the same flag later.
- ``IoMore`` / ``RecvResult``: portable ``IORING_CQE_F_SOCK_NONEMPTY`` on
  oneshot ``Proactor.recv`` (``RecvResult.data`` + ``RecvResult.more``).
  ``more`` defaults to ``MORE`` so it pairs with ``IoExpect.READY``.
  ``sock_recv`` still waits to ``bytes``. Not a ``MultishotDelivery`` field
  (``delivery.more`` remains continuous-stream ``CQE_F_MORE``; re-arm stays
  in the proactor).

### Changed
- ``UringProactor.send`` always uses ``uring-api`` ``send_all`` (one waitable;
  C re-arms partial CQEs). Drop the in-proactor sendall loop and the
  ``TEALETIO_URING_SEND_ALL`` experiment flag. Stream send is copying only
  (no ``SEND_ZC``). ``send_close_nowait`` prepares nowait ``send_all`` then
  nowait close on the same-fd conflict FIFO. Cancel is ``ASYNC_CANCEL`` of
  the live send_all handle. Progress fires once with the total at the
  terminal CQE. Requires workspace ``uring-api`` with ``send_all``.
- ``ProactorIOManager`` no longer first-tries accept or recv. Those always
  submit to the proactor (selector and uring share the same manager policy).
  ``sock_sendall`` still tries one non-blocking ``send``. Drop
  ``TEALETIO_EAGER_ACCEPT`` / ``TEALETIO_EAGER_RECV``.
- Uring stream send passes the original buffer to ``construct_send_all``
  (no extra ``memoryview`` wrap; ``uring-api`` ``GetBuffer``s).
- ``Proactor.send_close_nowait(sock, data)``: fire-and-forget sendall then
  nowait close. No ``Operation``. Close runs after the last send leg
  (or a terminal send error). Later failures go to the delivery
  exception handler. ``UringProactor`` prepares nowait ``send_all`` then
  nowait close together (close parks on the send-all conflict FIFO);
  ``SelectorProactor`` closes after send completion.
  ``ProactorIOManager.sock_send_close`` is the pass-through (same layer
  naming as ``sock_close`` vs ``close_socket_nowait``).
  ``StreamWriter.wait_closed()`` uses it for queued bytes (or closes when
  an in-flight send finishes) and does not park.
- ``ProactorIOManager.sock_close(sock) -> None``: fire-and-forget, raises
  ``OSError`` from detach or stdlib close. No ``IOWaiterSync`` / ``forget()``.

### Fixed
- Asyncio guest ``loop.sock_recv`` / ``ForwardingProactor.recv`` wait to
  payload bytes (``RecvResult.data``), matching ``scheduler.io.sock_recv``.
- ``readexactly`` hang after ``open_connection`` on the default two-worker
  ``UringProactor``: a ``recv_many`` MORE CQE could be packaged after the
  terminal ``!MORE`` already nerfed ``completion.user_data``, so the data
  chunk was dropped and ``ReorderBuffer`` never released the parked
  ``PulseEvent``. Delivery now calls ``completion.clear_user_data()``,
  which defers the armed-handle clear until every staged CQE is packaged
  (``uring-api``). Requires a workspace ``uring-api`` with that method.

### Changed
- Skip extra Python ``memoryview`` wraps on uring ``sendto`` / ``write``,
  empty ``sock_sendall``, ``sock_send_iter``, and connect ``initial`` /
  ``initial_data``. ``uring-api`` already ``GetBuffer``s; wrap only for
  eager-send remainder slices.
- ``Operation`` / ``ContinuousOperation`` and ``CrossThreadEvent`` use
  ``__slots__`` (no per-op ``__dict__``). ``IOWaiter`` and the other
  waitable wrappers already did.
- ``StreamWriter.wait_closed()`` full teardown uses ``sock_close`` only (no
  ``SHUT_WR``). Matches asyncio selector transports: half-close remains
  ``write_eof()`` via ``SendBuffer``; ``close()`` / ``wait_closed()`` destroy
  the fd without an extra ``shutdown`` syscall.
- ``UringProactor`` passes ``IORING_RECVSEND_POLL_FIRST`` on oneshot
  recv/send prepares when ``probe()["IORING_RECVSEND_POLL_FIRST"]`` is
  true. ``POLL_FIRST`` + ``recv_multishot`` is unsupported (liburing
  does not test it; can strand ``MORE`` with no EOF CQE) and is not
  applied.
- ``UringProactor._prepare`` is ``prepare(*args, op)`` with optional
  ``sequence=``. No ``extra`` / ``**kwargs``: accept flags and
  send flags are ordinary positional cargo. Multishot ``prepare_*`` take
  ``sequence`` after ``user_data``; oneshot fallback assigns
  ``completion.sequence`` after prepare. Multishot
  ``poll_many`` sets ``operation.poll_remove`` at the call site. Stream send
  is one ``send_all`` prepare. Requires
  workspace ``uring-api`` with cargo-then-``user_data``.
- ``UringProactor.has_pending_operations()`` reads ``ring.pending_count()``
  instead of a per-proactor list append/pop on each waitable. Nowait
  prepares are not counted; a multishot handle counts as one until its
  terminal CQE is packaged. Between oneshot ``poll_many`` legs the count
  can be zero (CQE packaged before the next prepare). Stream ``send_all``
  stays counted until its terminal CQE. ``run()`` / ``arun()`` treat a
  zero count as idle and may return while a oneshot ``poll_many`` is still
  in flight — best-effort; prefer ``run_until_complete``.
- Collapse uring oneshot prepare: one ``_prepare`` stamps the complete
  handler, calls ``ring.prepare_*`` with the waitable as ``user_data``, and
  arms reverse. Shared shapers ``_complete_uring_void`` / ``_complete_uring_res``
  / ``_complete_uring_bytes`` / ``_complete_uring_socket`` replace per-op
  copies. ``send_all``, stat, recvfrom, and continuous/multishot paths stay
  specialised. Drop ``_prepare_uring_op``, ``_prepare_ring``, and
  ``_prepare_recvmsg``.
- Rename proactor ``_submit_*`` helpers to ``_prepare_*`` (uring
  ``_prepare`` / ``_prepare_async_cancel_op`` / ``_prepare_poll_remove_op``,
  and the selector arming helpers). They prepare or register; they do not flush.
- ``UringProactor`` no longer stores a submit recipe (``sq_impl`` / ``sq0``…``sq4``)
  on every waitable. One-shot ops call ``ring.prepare_*`` directly. Only
  oneshot ``poll_many`` keeps ``leg_fd`` / ``leg_arg`` for next-leg re-arm.
  The deferred-SQ retry path that needed a replayable recipe is already gone.
- Uring stream ``send`` uses ``construct_send_all`` then ``Ring.prepare``:
  reverse is armed on the constructed handle before any SQE exists, so send
  does not take ``_multi_leg_lock``. Requires a workspace ``uring-api`` with
  ``send_all`` construct/prepare.
- Uring delivery takes ``op = completion.user_data`` then calls
  ``completion.clear_user_data()`` — the sole op↔completion cycle breaker
  (defers on an armed multishot handle while CQEs are still staged). Requires
  **``uring-api>=0.1.0rc5``** with ``clear_user_data``. Finish handlers no
  longer clear ``op.completion`` for hygiene; reverse may still point at a
  nerfed Completion until freelist
  scrub or prepare-fail. Client-held incomplete waitables are reverse-armed
  before the public prepare method returns (oneshot ``poll_many`` replace
  under ``_multi_leg_lock``). ``cancel(poll_many)`` always fails the teardown
  waitable (no ring/selector effect) on both uring and selector; stop continuous
  poll with ``poll_remove()`` only. Stream ``send`` cancel is ``ASYNC_CANCEL``
  of the live ``send_all`` handle (C abandon stops further legs). Other oneshot
  cancel is ``ASYNC_CANCEL`` only (no unarmed local-terminal path). Multishot MORE legs
  are shells; terminal ``!MORE`` is the armed parent. Freelist reclaim when
  reverse is ``None`` or nerfed; abandon blocks reclaim until oneshot
  ``poll_many`` CQE paths clear it.

### Added
- Size-keyed receive buffer pool cache on ``ProactorIOManager``:
  ``acquire_recv_buffer_pool`` / ``release_recv_buffer_pool`` reuse idle pools
  by ``(buffer_size, buffer_count)`` with an LRU free-pool cap (default 16,
  ``max_free_recv_buffer_pools``). Checked-out pools install
  ``release_callback`` so ``pool.close()`` returns to the cache; free pools
  keep that hook so a second ``close()`` is a soft no-op (required for uring
  ``BufGroup``, where no-callback ``close()`` hard-frees the ring).
- ``RecvIterBuffer`` / stream factories take ``owns_pool``: only the layer that
  checked out a lease for the buffer lifetime calls ``pool.close()`` on
  teardown. ``pooled_default_stream_factory`` uses per-connection cache leases
  by default; an explicit shared ``pool=`` is borrowed and not closed per
  connection.

### Removed
- Uring deferred SQ backpressure path: no ``SubmissionQueueFull`` catch, no
  deferred FIFO / ``_deferred_lock`` / ``deferred_cancelled`` mark-and-skip, and
  no ``UringSubmissionStats`` / ``submission_stats``. After uring-api lazy
  prepare (#84), ``get_sqe`` flushes (and SQPOLL waits) instead of signalling
  recoverable SQ-full; stuck SQ is ``RuntimeError``. Oneshot continuous next-leg
  prepare (``_submit_next_leg``) arms the next SQE immediately after a CQE
  (skips if already terminal) — not a failure retry.
- Emulated oneshot ``poll_many``: reverse link is not cleared to ``None``
  between legs; after emit, a single ``_multi_leg_lock`` section handles
  abandon/error/done cleanup or next-leg prepare (reverse link replaced after
  prepare returns). Stop abandons the link (sentinel + ``ASYNC_CANCEL``);
  freelist refuses reclaim until the CQE clears the sentinel. Kernel multishot
  unchanged.

### Changed
- ``UringProactor`` no longer installs ``Ring.pre_submit``. Reverse link
  ``operation.completion`` is set after prepare returns only when still idle
  (``None``), except stream send which arms the constructed handle first.
  Emulated oneshot ``poll_many`` first legs still hold ``_multi_leg_lock``
  across prepare+arm; send next-leg and poll next-leg assign reverse under
  that lock after abandon is ruled out. Delivery still routes via
  ``completion.user_data``.
- ``_LeasedChunk.__release_buffer__`` swallows ``AttributeError`` so a
  half-torn-down instance after cyclic GC does not emit unraisable errors.
- ``ProactorFile`` append open: if the initial ``stat_fdsize`` fails after the
  fd is stored, detach ``_fd`` before re-raising so ``__del__`` does not
  ``close_fd`` again after ``make_file`` already closed the descriptor.
- Break the proactor↔scheduler bound-method cycle on close: proactor
  ``_detach_owner_hooks()`` restores ``_clock`` to ``time.monotonic`` and
  clears delivery/async-break handlers; ``ProactorScheduler.close()`` /
  ``ProactorIOManager.close()`` null ownership links (idempotent).
- ``scheduler.io.poll_many`` returns ``IOHandle`` (``close()`` / ``closed``),
  not ``IOWaitable``. Stop with ``handle.close()`` (``poll_remove``); deliveries
  stay callback-only. ``IOWaiter`` exceptional cancel uses ``proactor.cancel``
  only — no poll_many kind dispatch. Removed ``ProactorIOManager._cancel_operation``.
- ``Proactor.poll_remove(operation)`` stops continuous ``poll_many`` (uring
  multishot posts ``POLL_REMOVE``; oneshot fallback abandons the reverse link
  and ``ASYNC_CANCEL``s the live poll leg). ``Proactor.cancel()`` posts
  ``ASYNC_CANCEL`` on the armed reverse (multi-leg ``send`` abandons first).
  Continuous poll stop is no longer routed through a separate cancel-submit API.
- Multishot ``poll_many`` stop no longer eagerly terminalises when
  ``submit_poll_remove`` posts. The target finishes from its terminal CQE
  (typically ``-ECANCELED`` with ``!MORE``), delivered through the same
  reorder buffer as readiness chunks so listeners always see a terminal
  cancel delivery. After the terminal CQE nerfs ``user_data``,
  ``poll_many`` may be freelisted like other continuous ops. Oneshot poll
  freelist is blocked by the abandoned-leg sentinel until the poll CQE
  clears it. Selector paths still stop locally.
- Hot-path typing: drop runtime ``cast(IOWaiter[None] | IOWaiterSync[None], …)``
  in ``SendBuffer._submit_leg`` (building that union every send leg was
  ~4 µs). Type the active waiter as ``IOWaitable`` and expose ``exception()``
  on the protocol / ``IOWaitGroup``. Prebind ``Operation[T]`` /
  ``ContinuousOperation[T]`` aliases used in ``cast()`` and constructors on
  uring paths so each call does not re-evaluate generics (~0.45 µs each).
  Uring completion-side cargo (``cq0``…``cq3``) is ``Any``. CQE complete
  paths trust those slots with no ``cast()``. CQE ``user_data`` and
  continuous complete handlers use
  ``assert isinstance`` to document internal invariants and narrow types
  (no identity ``cast`` helpers). Public ``Proactor`` methods remain the
  typed boundary. Scheduler/locks: replace ``cast(Any, task/self)`` with
  ``assert isinstance`` ownership checks, ``getattr`` for optional priority
  hooks, and direct mixin ``self`` use after asserting ``BaseScheduler``.
- ``Proactor.poll_remove(operation)`` stops continuous ``poll_many`` (uring
  multishot posts ``POLL_REMOVE``; oneshot fallback stops resubmit without
  ``ASYNC_CANCEL``). ``Proactor.cancel()`` stays real cancel only (deferred local
  terminal or ``ASYNC_CANCEL``), including an in-flight oneshot poll leg;
  poll stop is no longer shoehorned into cancel submit. Selector maps both APIs
  to interest deregistration with the matching teardown kind.
- ``UringProactor.wait_async`` splits by completion mode (mirroring sync
  ``wait``): threaded mode parks on ``EventWakeupManager`` only (workers own
  CQ reaping); inline ``completion_threads=0`` still runs ``ring.wait`` in a
  thread-pool executor so the asyncio loop services the ring without blocking
  the event-loop thread. ``wake_wait()`` signals both ``break_wait`` and the
  threaded async waiter.
- Docs: ``IO_MANAGER_DESIGN.md`` / ``PYTHON_API.md`` document the
  **eager non-blocking first** policy on ``scheduler.io`` as send-only.
  Accept and recv always submit to the proactor (same for selector and uring);
  a backend that wants a first try can do it internally. ``TEALETIO_EAGER_IO``
  / ``TEALETIO_EAGER_SEND`` remain for the send try.
- ``ProactorIOManager.sock_accept``, ``accept_many``, and ``accept_many_streams``
  always arm ``proactor.accept`` / ``accept_many``. No manager-side non-blocking
  accept drain.
- Internal ``ProactorIOManager._recv_many`` is a thin wrap of
  ``proactor.recv_many`` (same callback, no marshal/reorder, no manager-side
  ``recv`` drain). ``RecvIterBuffer`` starts legs via this helper and still
  cancels unfinished ops on the real proactor.
- ``ProactorIOManager.sock_recv`` and accept-time preread (``sock_accept`` /
  ``accept_many`` with ``recv_size``) always use ``proactor.recv``.
  ``sock_recv_into`` / ``recvfrom`` are unchanged.
- ``ProactorIOManager.sock_sendall`` tries one non-blocking ``send`` before
  ``proactor.send``. A full buffer completes as ``IOWaiterSync``; partial sends
  report ``progress`` then hand the remainder to the proactor (which continues
  the drain). Empty payloads still go straight to the proactor.
- ``SendBuffer`` owns outbound backlog without double materialise: an empty
  backlog keeps the first ``bytes`` payload by reference (mutable inputs are
  snapshotted once); further writes promote to a ``bytearray`` and extend.
  ``_take_pending`` detaches that buffer into ``sock_sendall`` as-is. While a
  leg is in flight, writes still coalesce for the next leg (asyncio proactor
  style), so line-at-a-time stream writes become one next send rather than N
  tiny legs. On leg complete, any pending ships even below ``min_write`` so
  ``flush()`` cannot strand a tiny tail. Scatter/gather vector send remains a
  follow-up when the proactor exposes multi-buffer submit.
- Connect-time ``initial`` / ``initial_data`` (``sock_connect``, ``sock_create``,
  ``sock_create_streams``) chain through ``sock_sendall`` after connect, so the
  first post-connect bytes get the same eager send try. Connect itself still
  always uses the proactor.
- ``ProactorIOManager.sock_shutdown`` / ``sock_close`` run
  ``socket.shutdown`` / ``socket.close`` on the calling thread and return
  ``IOWaiterSync`` (no proactor submit), matching asyncio stream teardown.
  ``Proactor.shutdown`` / ``close_socket`` remain for direct proactor callers.
- ``ProactorIOManager.sock_create`` / ``sock_create_streams`` create sockets
  directly via stdlib ``socket.socket()`` (scheduler contract) instead of
  ``Proactor.create_socket``. Create-only results use ``IOWaiterSync`` (no
  synthetic ``Operation``). Connect and optional send still go through the
  proactor; uring ``IORING_OP_SOCKET`` remains available on
  ``Proactor.create_socket`` for direct callers. Blocking create is the faster
  path for the io_manager hot entry point.
- ``UringProactor``: multi-threaded ``wait()`` parks on ``ring.wait_idle()``;
  ``wake_wait()`` always calls ``ring.break_wait()`` (inline and threaded).
  ``break_wait`` signals the idle park immediately and skips the internal NOP
  while completion service workers own CQ reaping. Removes the separate
  ``EventWakeupManager`` host for uring driver waits. ``wait_async()`` uses a
  thread-pool executor on the same ``wait`` binding.
- Inlined ``WakeupManager`` / ``EventWakeupManager`` into ``proactor.py`` (removed
  standalone ``wakeup.py``). ``ThreadedSelectorProactor`` parks on
  ``EventWakeupManager``; ``bind_loop()`` prepares its asyncio waiter.
- Uring multishot CQEs are delivered without gating on ``operation.done()`` in
  the completion worker; out-of-order terminal ordering defers to scheduler-thread
  ``ReorderBuffer``.
- Removed ``LenientReorderBuffer``; accept and poll continuous paths use the same
  strict ``ReorderBuffer`` as ``RecvIterBuffer``. On unsequenced cancel
  (``index=None``), accept/poll flush heaped legs before the terminal so
  sockets are not stranded; ``recv_many`` does not flush (no gap-skipped data).
  After that flush, late gap indices pass through immediately (exception-safe
  pop-one flush; no happy-path cost).
- ``start_server()`` without an explicit ``stream_factory`` now uses
  ``pooled_default_stream_factory`` (per-connection provided-buffer pools)
  instead of the scheduler shared pool, so concurrent clients do not share
  ``recv_many`` backpressure.
- ``StreamReader.readinto()`` / ``AsyncStreamReader.readinto()`` block until the
  caller buffer is full or EOF (short return only at EOF), including across
  multiple ``recv_many`` chunks.
- ``StreamWriter.wait_closed()`` still flushes queued sends, then runs direct
  ``sock_shutdown`` / ``sock_close`` with ``forget()`` instead of ``wait()``
  (same pattern as ``SHUT_WR``), so handler tealets do not park on teardown.

### Breaking Changes
- Removed ``Proactor.break_wait()`` and ``Proactor.set_completion_callback()``.
  Register ``operation.add_done_callback(lambda _: proactor.wake_wait())`` when
  blocking in raw ``wait()`` / ``wait_async()``; scheduler production wakes
  through ``IOWaiter`` / ``call_soon_threadsafe`` → ``wake_wait()`` unchanged.
- ``accept_many_streams()`` and ``start_server()`` no longer accept ``recv_size``,
  ``recv_timeout``, or ``on_recv_error``. Stream accepts no longer perform
  accept-time preread or ``feed_initial`` on readers; each connection arms
  ``recv_many`` through ``RecvIterBuffer`` when streams open.
- ``StreamReader.feed_initial()`` and ``AsyncStreamReader.feed_initial()`` are
  removed.
- ``open_streams()`` no longer accepts ``recv_buffer_pool``. Pass a
  ``stream_factory`` (for example ``pooled_default_stream_factory``) for
  dedicated provided-buffer pools on stream endpoints.
- ``StreamFactory`` / ``AsyncStreamFactory`` no longer declare a
  ``recv_buffer_pool`` parameter; pool policy belongs to the factory
  implementation, not per-call framework injection.
- Default ``StreamReader`` / ``AsyncStreamReader`` construction no longer takes a
  ``transport`` argument; receive is exclusively through ``RecvIterBuffer``.
- ``SocketTransport`` is send/metadata-only; ``recv`` and ``recv_into`` are
  removed from the transport surface.
- One-shot `ProactorIOManager` helpers (`sock_recv`, `sock_connect`, `open`,
  etc.) return `IOWaiter`; call `.wait()` to block (or `.forget()` to drop
  interest). `streams` / `files` call `.wait()` internally. Convenience helpers
  (`sock_recvall`, `sock_recv_iter`, `sock_send_iter`) remain blocking and return
  values directly.
- `sock_create_streams()` requires `connect_to`; the unconnected create-only
  path was removed. Use `sock_create()` plus `open_streams()` when needed.
- `accept()` / ``sock_accept()`` and ``accept_many`` callbacks no longer return
  peer addresses. One-shot ``sock_accept()`` returns ``(conn, initial_data)``
  (``AcceptDelivery``). Without ``recv_size`` / ``n``, ``initial_data`` is
  ``None``. With accept-time pre-read, ``initial_data`` holds the bytes read;
  empty ``initial_data`` (``b""``) means EOF before any payload. ``accept_many``
  still delivers ``(conn, initial_data, recv_error)``; call
  ``socket.getpeername()`` when the peer address is needed. Close the socket
  when ``recv_error`` is set unless a higher-level helper (for example
  ``start_server``) handles teardown.
- Removed `Operation.cancel()`, `set_cancel()`, `complete()`, and suboperation
  tracking. Cancellation is only through `Proactor.cancel(operation)` (and
  `scheduler.io._cancel_operation()` / `SelectorScheduler.cancel_operation()`
  wrappers). Removed private backend fields `_proactor`, `_attempt`, and
  `_cancel_target`; use proactor-side structures (`_FdSlot` for selector fd
  drivers, `_uring_entry()` for io_uring) instead of reaching into operation
  attributes.
- Blocking proactor IO (`wait_operation`, `sock_*`, `poll*`, positioned file
  `open`, and receive-buffer pool helpers) moved from the scheduler surface to
  `scheduler.io` (`ProactorIOManager`). Use `scheduler.io.sock_recv(...)` instead
  of `scheduler.sock_recv(...)`. ``BasicScheduler.io`` and ``SelectorScheduler.io``
  raise when the scheduler has no proactor IO facade (selector schedulers get a
  targeted error). Non-IO schedulers raise ``RuntimeError`` on ``.io`` access
  (not ``NotImplementedError`` from per-method stubs).
- Custom `stream_factory` / `StreamFactory` callables now receive a `SocketIO`
  facade as the first argument (`io=...`) instead of a `ProactorScheduler`.
  Update factories to use `io.sock_*` helpers; see `tealetio.streams.StreamFactory`.
- `tealetio.streams` helpers require a proactor scheduler. Passing a
  `SelectorScheduler` raises a targeted ``RuntimeError`` (selector blocking IO
  remains on ``scheduler.sock_*`` until ``SelectorIOManager``).
- `recv_many(sock, callback, *, buf_group)` now requires an explicit
  provided-buffer pool; there is no per-operation default at the proactor level.
- `recv_many` continuous-operation callbacks now receive
  ``MultishotDelivery(index, value, exception, more)`` instead of separate
  callback arguments. For receive, ``index`` is stream-global
  (``completion.sequence`` on uring, seeded by ``base_sequence`` at submit).
- `recv_many` provided-buffer exhaustion delivers ``errno.ENOBUFS`` through
  ``MultishotDelivery.exception`` on uring multishot, or completes immediately
  with ENOBUFS when a ``SyntheticRecvBufferPool`` is already full at submit.
  ``RecvIterBuffer`` / ``sock_recv_iter`` map pool pressure to
  ``(RECV_MANY_BUFFER_PRESSURE, memoryview(b""))`` and re-arm receive once
  ``leased_count < buffer_count / 2``; direct ``recv_many`` callers must drop
  held views and start a fresh ``recv_many()`` with ``base_sequence`` set
  appropriately.
- `Proactor.recvall` and `Proactor.recvgen` are removed. Use
  `scheduler.io.sock_recvall` and `scheduler.io.sock_recv_iter` from
  scheduler-owned tealets instead (blocking helpers, not `Operation` returns).
- `sock_recvall(..., progress=...)` now calls `progress(chunk)` with each
  non-empty chunk's `bytes` payload instead of a cumulative byte count.
- `sock_recvgen` renamed to `sock_recv_iter` on the blocking IO facade
  (`scheduler.io`).
- `sock_recv_iter` always yields `(index, memoryview)` and
  `(RECV_MANY_BUFFER_PRESSURE, memoryview(b""))`; the `allow_memview` option
  is removed.
- `Proactor.sendall()` is removed. Use `Proactor.send()` for stream sends that
  drain the full buffer before completing (`Operation[None]`). Datagram sends
  remain `Proactor.sendto()`.
- `scheduler.io.sock_create()` now returns a ``socket.socket`` only. The former
  ``(socket, is_connected, initial_sent)`` tuple is removed. Connect and
  connect-time send are always chained by ``ProactorIOManager`` when
  ``connect_to`` / ``initial_data`` are set; callers no longer inspect hint
  honour flags.
- Chained ``connect`` operations (``sock_connect(..., initial=...)``,
  ``sock_create(..., connect_to=...)``) complete with ``None``, not ``True``.
- ``accept_many`` / ``poll_many`` on ``scheduler.io`` return ``IOWaitable[None]``
  instead of ``ContinuousOperation``. ``wait()`` ends the current stream leg;
  on non-multishot backends that is one accept or poll event — re-arm in a loop
  (``StreamServer`` accept tealet) or hold ``waiter.operation`` for the raw
  ``Operation`` handle. Direct ``proactor.accept_many()`` on oneshot backends
  **finishes** after each accept; oneshot ``poll_many`` fallbacks still resubmit
  inside the proactor until cancel — do not assume the same auto-resubmit model.
- Accept-time ``recv`` legs started by ``accept_many(..., recv_size=...)`` are
  independent of the parent waiter. Cancelling the accept stream does not cancel
  in-flight recvs; callers must discard late deliveries after shutdown.

### Changed
- ``StreamReader.read(n)`` for ``n > 0`` matches asyncio partial-read semantics
  (one chunk at a time, no read-ahead to ``n``).
- ``sock_create_streams()`` / ``open_connection()`` open streams in
  ``IOWaitGroup`` advance hooks on the completion worker thread, arming
  ``recv_many`` before ``wait()`` returns to the scheduler tealet.

### Fixed
- ``StreamServer.wait_closed()`` waits for the accept-loop tealet to exit, not
  only handler tealets.
- ``StreamServer.close()`` cancels the accept-loop tealet synchronously and no
  longer closes listening sockets itself; the accept tealet's ``finally`` block
  closes them on exit.
- Accept-time ``recv_timeout`` no longer leaks scheduler timers when the recv
  completes before the arm callback runs on the scheduler thread.
- ``UringProactor`` deactivates uring entries promptly when ``submit()`` returns
  on an already-cancelled target, keeping ``has_pending_operations()`` accurate.

### Added
- ``pooled_default_stream_factory()`` builds default sync or asyncio-shaped
  stream pairs with a per-connection or shared provided-buffer pool.
- ``accept_many_streams()`` wraps each accept as streams on the accept delivery
  thread and starts ``recv_many`` before marshalling the user callback onto the
  scheduler thread.
- Default stream readers receive through ``recv_iter`` / ``recv_many`` chunk
  delivery; consumers release leased ``memoryview`` chunks after ingest.
- `Proactor.create_socket()` and `scheduler.io.sock_create()` to create
  scheduler-contract sockets through the proactor. Optional ``connect_to`` and
  ``initial_data`` are chained by ``ProactorIOManager`` (create → connect →
  send). ``open_connection(..., initial_send=...)`` uses this path for TCP and
  Unix ``path=`` connects.
- `IOFile` handle protocol for positioned binary file objects returned by
  `FileIO.open()`; `ProactorFile` is the proactor-backed implementation. Static
  typing only (not ``@runtime_checkable`` on Python 3.10–3.11); import from
  ``tealetio`` / ``tealetio.proactor``.
- `ServerIO` protocol slice for stream servers (`SocketIO` plus proactor
  submission); `ProactorSocketIO` remains a backward-compatible alias.
- Streams internals: `_AcceptedConnection` / `SocketAddress` typing and
  `SupportsProactorIO` documentation for the capability gate.
- `ProactorIOManager` and `scheduler.io` on proactor-backed schedulers: composed
  blocking IO facade over `Proactor` (`wait_operation`, `sock_*`, `poll*`,
  positioned file `open`, receive-buffer pool helpers).
- `SocketIO`, `PollIO`, and `FileIO` protocols for the blocking IO facade slices;
  `tealetio.streams` socket paths depend on `SocketIO` rather than the concrete
  manager type.
- `tealetio.streams` module helpers `open_connection`, `open_streams`, and
  `start_server` with optional `scheduler=`; blocking socket IO routes through
  `scheduler.io`.
- `scheduler.io.sock_recv_iter(sock, buffer_pool=None)` as a tealet-blocking
  incremental consumer of `recv_many`, yielding stream-ordered `(index, data)`
  chunks with the same provided-buffer pressure policy as `sock_recvall`.
  ``None`` uses the proactor shared pool.
- `scheduler.io.sock_send_iter(sock, chunks)` to drain an iterable of buffer
  chunks through `sock_sendall`. Track send progress in the iterable or generator
  you pass; there is no separate progress callback on the helper.
- `scheduler.io.create_recv_buffer_pool(buffer_size, buffer_count)` for explicit
  provided-buffer pool sizing shared by `sock_recv_iter` and `recv_many`.
- `Proactor.shared_recv_buffer_pool()` as the lazy proactor-owned shared
  `BufGroup` used by `sock_recvall`; pass it explicitly to `sock_recv_iter` when
  sharing the default pool.
- `scheduler.io.set_shared_recv_buffer_pool(pool)` and
  `Proactor.set_shared_recv_buffer_pool(pool)` to replace the shared default pool
  before `sock_recvall` or explicit `sock_recv_iter` calls.
- `RECV_MANY_BUFFER_PRESSURE` result index so `recv_many` consumers can release
  held views when the shared provided-buffer pool is exhausted.
- Published runnable queue policies (`FifoRunnableQueue`,
  `PrescheduledRunnableQueue`, and `PriorityRunnableQueue`) for explicit
  scheduler construction, including priority-scheduling applications.
- Added a proactor IO layer with `Operation`, `Proactor`, `SelectorProactor`,
  `ThreadedSelectorProactor`, and proactor-backed sync/async scheduler drivers.
- Added selector-backed sync/async scheduler drivers that share a common
  selector core while preserving the existing readiness API.
- Added tealet-hosted asyncio loop adapters for both loop families:
  `ForwardingSelector`/`TealetSelectorEventLoop` and
  `ForwardingProactor`/`TealetProactorEventLoop`.
- Added top-level `tealetio.await_()` for awaiting asyncio awaitables from the
  current scheduler task without reaching through the scheduler object.
- Added integration coverage for multi-step async socket send/receive coroutines
  awaited by sibling tealet tasks in both host modes.

### Changed
- Default proactor-backed schedulers (`Scheduler`, `SyncProactorScheduler`,
  `AsyncProactorScheduler` without an explicit factory) now construct
  `UringProactor` when `uring_api.is_available()` is true, and fall back to
  `SelectorProactor` otherwise.
- `UringProactor.create_socket()` submits ``IORING_OP_SOCKET`` for inet and
  ``AF_UNIX`` when probed, ORing ``SOCK_NONBLOCK | SOCK_CLOEXEC`` into the
  socket ``type`` (matching ``socket(2)``). Unix connect legs use
  ``ProactorBase._sync_unix_connect()`` because uring ``submit_connect()`` is
  inet-only today.
- `ProactorIOManager` holds a direct scheduler reference; ``wait_operation()``
  always parks the current tealet through ``CrossThreadEvent`` and is torn down
  from ``ProactorScheduler.close()``.
- `UringProactor.connect()` and ``recv(..., 0)`` use stdlib fast paths for
  ``AF_UNIX`` and zero-length reads respectively on the uring backend.
- ``run_asyncio_in_tealet()`` hosts asyncio socket helpers on a
  ``SelectorProactor`` bridge when the scheduler proactor is
  ``UringProactor``; tealet blocking IO still uses ``scheduler.proactor``.
- `ProactorFile` blocks through the `OperationWaiter` protocol; production opens
  pass `ProactorIOManager` from `scheduler.io.open(...)`.
- `sock_recv_iter` always yields `(index, memoryview)` chunks and
  `(RECV_MANY_BUFFER_PRESSURE, memoryview(b""))` pressure tokens; consumers
  release held views between reads so leased buffers can return to the pool.
  ``RecvIterBuffer`` re-arms ``recv_many`` once ``leased_count < buffer_count / 2``
  (low-water mark). At most one pressure notification is pending until receive
  restarts.
- Removed the `n` chunk-size argument from `recv_many`, `sock_recvall`, and
  `sock_recv_iter`; chunk sizes are backend-defined
  (`SelectorProactor` reads up to 8 KiB per `recv()`, `UringProactor` uses the
  shared `BufGroup` slot size).
- `UringProactor.recv_many` delivers leased `memoryview` chunks instead of
  copied `bytes`; `sock_recvall` converts each chunk to `bytes` as
  `sock_recv_iter` advances, with shared-pool pressure handled inside
  `sock_recv_iter`.
- `SelectorProactor.recv_many` uses ``SyntheticRecvBufferPool`` lease
  accounting; pool exhaustion surfaces as submit-time ENOBUFS (or the
  ``RECV_MANY_BUFFER_PRESSURE`` token via ``sock_recv_iter``). When uring
  ``create_buf_group()`` fails (pre-5.19 PBUF rings), ``UringProactor`` falls
  back to the same synthetic pool and one-shot ``submit_recv()`` path.
- `UringProactor.recv_many` routes by capability: multishot provided-buffer
  receive when available, ``submit_recv_buf()`` per leg on real ``BufGroup``
  pools without multishot, and ``submit_recv()`` with synthetic leases on
  ``SyntheticRecvBufferPool``.
- Made `Scheduler` use the proactor-backed synchronous scheduler by default,
  while keeping explicit selector-backed schedulers available.
- Changed `run_asyncio_in_tealet(...)` to choose the hosted asyncio loop from
  the active scheduler type by default, using the proactor loop for proactor
  schedulers and the selector loop for selector schedulers.
- Protected proactor operation completion state and threaded selector-proactor
  mutations for free-threaded Python builds.
- Relaxed the `tealet` dependency to the compatible `0.1` range
  (`tealet>=0.1.0rc2,<0.2`) instead of pinning one release candidate exactly.
- Optimised `await_()` coroutine handling so coroutine await protocols are driven
  directly, with yielded asyncio futures waited by the scheduler instead of
  delegating ordinary coroutines to `asyncio.Task` immediately.
- Uses the typed `asynkit.coro_drive` implementation directly for coroutine
  driving.
- Raised the `asynkit` dependency floor to `asynkit>=0.19.2` for the native
  coroutine driver implementation, typed `coro_drive` export, and `syncmethod`
  descriptor.

## [0.1.0rc1] - 2026-06-25

### Added
- First release candidate for `tealetio`, the synchronous asyncio-like runtime built on `tealet`.
- Added scheduler, task, synchronisation, selector, runner, and asyncio coexistence APIs for ordinary tealet code.
- Added package-local tests and documentation for the public API.

### Changed
- Depends on the compatible `tealet` 0.1 release-candidate line for the split workspace release.

### Notes
- Pre-release candidate for validating the package boundary and publication workflow before `0.1.0`.