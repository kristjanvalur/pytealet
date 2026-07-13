# tealetio Changelog

All notable changes to the `tealetio` package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- ``start_server()`` without an explicit ``stream_factory`` now uses
  ``pooled_default_stream_factory`` (per-connection provided-buffer pools)
  instead of the scheduler shared pool, so concurrent clients do not share
  ``recv_many`` backpressure.

### Breaking Changes
- ``accept_many_streams()`` and ``start_server()`` no longer accept ``recv_size``,
  ``recv_timeout``, or ``on_recv_error``. Stream accepts no longer perform
  accept-time preread or ``feed_initial`` on readers; each connection arms
  ``recv_many`` through ``RecvIterBuffer`` when streams open.
- ``StreamReader.feed_initial()`` and ``AsyncStreamReader.feed_initial()`` are
  removed.
- ``open_streams()`` no longer accepts ``recv_buffer_pool``. Pass a
  ``stream_factory`` (for example ``pooled_default_stream_factory``) for
  dedicated provided-buffer pools on stream endpoints.
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
  always parks the current tealet through ``ThreadsafeEvent`` and is torn down
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