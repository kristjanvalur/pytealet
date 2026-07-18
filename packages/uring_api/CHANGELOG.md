# uring-api Changelog

All notable changes to the `uring-api` package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `Ring.wait_idle(timeout=None)`: host-side idle park separate from CQ reaping.
  Parks until `break_wait` or `close` (or timeout). Returns `True` if signalled,
  `False` on timeout.
- `Ring.break_wait()` is the single wakeup API: submits **one** internal NOP CQE
  (never delivered as a user completion; serve workers ignore it) so a blocking
  `wait()` in a thread-free reaper loop can return, and also opens the
  `wait_idle` park.
- `Ring.exception_handler`: optional callback invoked when a delivery callback
  raises (Python or C). The handler receives a context dict with `message`,
  `exception`, `ring`, and `completions`. When it returns normally, that worker
  continues serving; when it is unset or raises, `serve_completions()` exits with
  the exception and only that worker stops. C API: `ring_set_exception_handler()`.
- `submit_accept_multishot(..., base_sequence=0)`: optional start index for
  multishot accept leg numbering, matching `submit_recv_multishot`. The first
  successful accept CQE uses `completion.sequence == base_sequence`, then
  increments. C API: `ring_submit_accept_multishot(..., base_sequence)`.

### Changed
- `Ring.wait()` / `ring_wait()`: when a delivery callback (Python or C) is set,
  non-empty user batches are delivered through that callback and `wait` returns
  `None`. Empty batches (timeout, `break_wait`, wake-only) skip the callback and
  still return `None`. With no callback, `wait` still returns a list (possibly
  empty). User-visible completion lists are built lazily: wake / internal CQEs
  never allocate a delivery list.
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