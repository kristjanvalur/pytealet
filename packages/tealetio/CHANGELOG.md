# tealetio Changelog

All notable changes to the `tealetio` package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking Changes
- `accept_many` result callbacks now receive
  ``(socket, address, initial_data, recv_error)`` instead of ``(socket, address)``.
  Close the socket when ``recv_error`` is set unless a higher-level helper (for
  example ``start_server``) handles teardown.
- Removed private `Operation` backend fields `_proactor`, `_attempt`, and
  `_cancel_target`. Use `operation.cancel()` (backend hooks via `set_cancel()`)
  and proactor-side structures (`_FdSlot` for selector fd drivers,
  `_uring_entry()` cancel binding for io_uring) instead of reaching into
  operation attributes.
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
- `recv_many` provided-buffer exhaustion now delivers
  `(RECV_MANY_BUFFER_PRESSURE, resume)`; consumers must drop held views and
  call `resume()` to continue (no automatic resubmission).
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

### Added
- `Proactor.create_socket()` and `scheduler.io.sock_create()` to create
  scheduler-contract sockets through the proactor, returning
  ``(socket, is_connected, initial_sent)``. Optional ``connect_to`` and
  ``initial_data`` hints let `UringProactor` chain ``IORING_OP_SOCKET``,
  connect, and sendall-style flushing of ``initial_data`` when supported; other
  backends may ignore the hints and `sock_create()` falls back to
  ``sock_connect()``. ``open_connection(..., initial_send=...)`` uses this path
  for TCP and Unix ``path=`` connects.
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
- `ProactorFile` blocks through the `OperationWaiter` protocol; production opens
  pass `ProactorIOManager` from `scheduler.io.open(...)`.
- `sock_recv_iter` always yields `(index, memoryview)` chunks and
  `(RECV_MANY_BUFFER_PRESSURE, memoryview(b""))` pressure tokens; consumers
  release held views between reads so leased kernel buffers can return to the
  pool. At most one pressure notification is pending until receive restarts.
- Removed the `n` chunk-size argument from `recv_many`, `sock_recvall`, and
  `sock_recv_iter`; chunk sizes are backend-defined
  (`SelectorProactor` reads up to 8 KiB per `recv()`, `UringProactor` uses the
  shared `BufGroup` slot size).
- `UringProactor.recv_many` delivers leased `memoryview` chunks instead of
  copied `bytes`; `sock_recvall` converts each chunk to `bytes` as
  `sock_recv_iter` advances, with shared-pool pressure handled inside
  `sock_recv_iter`.
- `SelectorProactor.recv_many` (Python 3.12+) uses a synthetic `BufGroup` and
  the same `(RECV_MANY_BUFFER_PRESSURE, resume)` backpressure contract as uring.
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