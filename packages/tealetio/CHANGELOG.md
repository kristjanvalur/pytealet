# tealetio Changelog

All notable changes to the `tealetio` package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
- Emulated multishot (oneshot continuous next-leg) serialises arm vs
  ``cancel`` / oneshot ``poll_remove`` on a short-held ``_emulated_leg_lock``.
  Armed oneshot poll stop posts ``ASYNC_CANCEL`` and sets reverse link to an
  abandoned-leg sentinel (blocks freelist until the poll CQE clears it); the
  CQE path treats sentinel as cancel-won cleanup. Local continuous cancel also
  marks ``done()`` / ``cancelled()`` so further legs stop without a per-op
  flag. Kernel multishot paths are unchanged.

### Changed
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
  multishot posts ``POLL_REMOVE``; oneshot fallback stops next-leg arming without
  ``ASYNC_CANCEL``). ``Proactor.cancel()`` is real cancel only (unarmed local
  terminal or ``ASYNC_CANCEL``), including an in-flight oneshot poll leg;
  continuous poll stop is no longer routed through cancel submit.
- Multishot ``poll_many`` stop no longer eagerly terminalises when
  ``submit_poll_remove`` posts. The target finishes from its terminal CQE
  (typically ``-ECANCELED`` with ``!MORE``), delivered through the same
  reorder buffer as readiness chunks so listeners always see a terminal
  cancel delivery. After that deactivate (``completion is None``),
  ``poll_many`` may be freelisted like other continuous ops. One-shot poll
  fallback and selector paths still stop locally.
- Hot-path typing: drop runtime ``cast(IOWaiter[None] | IOWaiterSync[None], …)``
  in ``SendBuffer._submit_leg`` (building that union every send leg was
  ~4 µs). Type the active waiter as ``IOWaitable`` and expose ``exception()``
  on the protocol / ``IOWaitGroup``. Prebind ``Operation[T]`` /
  ``ContinuousOperation[T]`` aliases used in ``cast()`` and constructors on
  uring paths so each call does not re-evaluate generics (~0.45 µs each).
  Uring ring-leg cargo (``sq0``…``sq4``, ``cq0``…``cq3``) is ``Any``: SQ
  helpers and CQE complete paths pass slots straight to the ring with no
  ``cast()``. CQE ``user_data`` and continuous complete handlers use
  ``assert isinstance`` to document internal invariants and narrow types
  (no identity ``cast`` helpers). Public ``Proactor`` methods remain the
  typed boundary. Scheduler/locks: replace ``cast(Any, task/self)`` with
  ``assert isinstance`` ownership checks, ``getattr`` for optional priority
  hooks, and direct mixin ``self`` use after asserting ``BaseScheduler``.
- ``UringProactor.wait_async`` splits by completion mode (mirroring sync
  ``wait``): threaded mode parks on ``EventWakeupManager`` only (workers own
  CQ reaping); inline ``completion_threads=0`` still runs ``ring.wait`` in a
  thread-pool executor so the asyncio loop services the ring without blocking
  the event-loop thread. ``wake_wait()`` signals both ``break_wait`` and the
  threaded async waiter.
- ``UringProactor`` installs ``Ring.pre_submit`` so ``operation.completion`` is
  reverse-linked before ``io_uring_submit``, replacing the
  ``_URING_SUBMIT_PENDING`` / post-claim install protocol. Cancel treats a
  missing reverse link as not yet armed (deferred or pre-submit not run).
- Docs: ``IO_MANAGER_DESIGN.md`` / ``PYTHON_API.md`` /
  ``SCHEDULER_RUNTIME_API_SPEC.md`` document the **eager non-blocking first**
  policy on ``scheduler.io`` (try the socket, fall through to the proactor only
  when needed) as the performance-oriented design for stream accept/recv/send.
- ``ProactorIOManager.sock_accept``, ``accept_many``, and ``accept_many_streams``
  try non-blocking ``accept()`` on the calling thread while the listen socket is
  ready, then fall through to the proactor continuous/one-shot path when it
  would block. Ready backlog is drained without a proactor submit per connection.
  Eager accepts use sequential multishot indices; continuous
  ``proactor.accept_many(..., base_sequence=N)`` continues numbering after the
  drain (uring multishot seeds ``completion.sequence`` the same way as
  ``recv_many``).
- Internal ``ProactorIOManager._recv_many`` drains ready data with non-blocking
  ``recv()`` then arms ``proactor.recv_many(..., base_sequence=N)`` with the same
  callback, returning a ``ContinuousOperation`` like the proactor (thin wrap: no
  marshal/reorder). Intermediate eager legs may deliver with ``operation=None``;
  pure-eager EOF/error finishes a synthetic done operation; the proactor path
  always returns a real op. ``RecvIterBuffer`` starts legs via this override and
  still cancels unfinished ops on the real proactor.
- ``ProactorIOManager.sock_recv`` and accept-time preread (``sock_accept`` /
  ``accept_many`` with ``recv_size``) share a non-blocking ``recv`` try before
  ``proactor.recv``. Ready first-bytes or EOF complete without a oneshot submit.
  ``sock_recv_into`` / ``recvfrom`` are unchanged.
- ``ProactorIOManager.sock_sendall`` tries one non-blocking ``send`` before
  ``proactor.send`` (unchanged single-eager policy). Empty payloads still go
  straight to the proactor.
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