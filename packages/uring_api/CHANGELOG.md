# uring-api Changelog

All notable changes to the `uring-api` package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `Ring.pre_submit`: optional ring-level Python hook ``hook(completion)``
  invoked after an SQE is prepared (``completion.user_data`` already set, may be
  ``None``) and before ``io_uring_submit``. Internal ``break_wait`` NOPs do not
  create a ``Completion`` and never invoke the hook. No failure/retract
  callback — a failed submit may leave the ``Completion`` on a reverse link
  without a CQE. The hook must not re-enter ring submit/wait/serve APIs.
  Intended for clients that store ``operation.completion`` before the kernel
  can complete the op.
- C API: ``ring_set_pre_submit()`` (Python callable) and
  ``ring_set_c_pre_submit()`` / ``UringApi_CPreSubmitCallback`` (pure C). When
  both are set, the C hook runs first, then the Python hook. Vtable fields are
  appended; check ``struct_size`` / null pointers.
- `Ring.wait_idle(timeout=None)`: host-side idle park separate from CQ reaping.
  Parks until `break_wait` or `close` (or timeout). Returns `True` if signalled,
  `False` on timeout.
- `Ring.break_wait()`: opens the `wait_idle` park **immediately**. When
  completion service is idle, best-effort submits **one** internal NOP CQE so a
  blocking `wait()` on an empty CQ can return; while serve workers are active the
  NOP is skipped (idle only). The NOP uses the address of a static token as SQE
  data (no ``Completion`` object); reaping marks it seen and discards it.
  Duplicate in-flight wake tokens are acceptable. NOP failure still succeeds
  after signalling.
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
- `submit_accept_multishot(..., base_sequence=0)`: optional start index for
  multishot accept leg numbering, matching `submit_recv_multishot`. The first
  successful accept CQE uses `completion.sequence == base_sequence`, then
  increments. C API: `ring_submit_accept_multishot(..., base_sequence)`.

### Fixed
- After an SQE is reserved and linked to a ``Completion``, failure in
  ``pre_submit`` or ``io_uring_submit`` rewrites that SQE as a wake NOP before
  the caller drops the ``Completion`` ref. Previously the SQE kept a dangling
  pointer (use-after-free on a later successful submit).
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
- `submit_accept()` and `submit_accept_multishot()` no longer pass a peer
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