# uring-api Changelog

All notable changes to the `uring-api` package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
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

### Changed
- On Python 3.15+, keyword methods use ``METH_FASTCALL`` with
  ``PyArg_ParseArrayAndKeywords``. Older Pythons keep
  ``PyArg_ParseTupleAndKeywords``. ``Ring.__init__`` stays tuple+dict
  (``tp_init`` cannot switch calling convention).
- **Cargo then ``user_data``.** ``prepare_*`` / ``construct_*`` take SQE cargo
  first and ``user_data`` last. METH_FASTCALL three-arg send / accept is now
  ``(fd, data, flags)`` / ``(fd, flags)``, not a token after the fd. ``openat``
  is positional ``dfd, path, flags, mode=0, user_data=None`` (no
  ``dfd=`` keyword-only tail). C construct signatures already used this order.
- Drop ``base_sequence`` from ``construct`` / ``prepare`` for
  ``recv_multishot`` and ``accept_multishot``. Seed the first leg with
  ``completion.sequence = N`` after construct (or after ``prepare_*`` returns).
  C API: ``ring_construct_*_multishot`` no longer takes the start index.
- C capsule appends ``completion_set_sequence``, ``completion_clear_user_data``,
  and ``ring_wait_idle``. Pre-release ABI version stays 1; rebuild C clients
  that cache ``offsetof``. ``ring_wait_idle(timeout)`` parks until
  ``break_wait`` (``timeout < 0`` blocks).

### Fixed
- Nowait cancel CQEs with ``-ENOENT`` or ``-EALREADY`` (lost race against an
  already-completed or already-completing target) do not invoke
  ``nowait_error_handler``. Waitable cancel still reports those as
  ``res < 0``. Other negative nowait cancel results still fail.
- ``Completion.clear_user_data()`` (and ``user_data = None``) defers
  clearing the armed handle while ``aux_refcount > 0``. Two
  ``serve_completions`` workers could deliver ``!MORE`` and nerf
  ``user_data`` before an earlier MORE shell copied it (dropped CQE).
  The pending clear is applied after the last staged leg is packaged —
  same window as the in-flight handle ref, without a second long-lived
  pointer. Shells and idle handles still clear immediately.
- Docs: ``README.md`` / ``AGENTS.md`` describe the multishot in-flight
  ``DECREF`` (``aux_refcount`` / ``AUX_DECREF``) and the deferred
  ``USER_DATA_CLEAR`` flag. ``clear_user_data()`` after ``Ring``
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
  (``construct_*_nowait`` or ``completion.nowait = True``); prepare stamps a
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
  used (same shape as delivery-callback failures, with an empty ``completions``
  list); if that is unset or also raises, the error is written as unraisable
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
  `exception`, `ring`, and `completions`. When it returns normally, that worker
  continues serving; when it is unset or raises, `serve_completions()` exits with
  the exception and only that worker stops. C API: `ring_set_exception_handler()`.
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
- `Ring.wait()` / `ring_wait()`: when a delivery callback (Python or C) is set,
  non-empty user batches are delivered through that callback and `wait` returns
  `None`. Empty batches (timeout, internals-only) skip the callback and still
  return `None`. With no callback, `wait` still returns a list (possibly empty).
  User-visible completion lists are built lazily: internal CQEs (e.g. zero-copy
  NOTIF) never allocate a delivery list. ``break_wait`` wake NOPs are discarded
  at staging and never enter list packaging.
- C API: `UringApi_CCompletionCallback` now receives a `list` of completions per
  kernel drain batch (was a single completion). Callback pointers must not be
  changed while `serve_completions()` workers are active.
  `URING_API_CAPI_ABI_VERSION` remains **1** while the package is pre-release;
  clients must check `struct_size` and null-check vtable pointers they rely on.
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

## [0.1.0rc5] - 2026-08-13

### Added
- ``Completion.user_data`` is settable (and clearable with ``None`` / ``del``).
  Clients may drop the payload after delivery to break cycles with waitables;
  kernel SQE identity remains the Completion pointer. C API:
  ``completion_set_user_data`` (appended vtable slot).
- Documented multishot delivery contract: intermediate ``IORING_CQE_F_MORE``
  legs deliver a shell ``Completion`` (copied ``user_data``, no
  ``pre_submit``); terminal ``!MORE`` delivers the armed submit handle itself.
  Documented in README, AGENTS.md, and ``build_completion_result``.

