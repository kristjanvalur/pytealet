# uring-api Changelog

All notable changes to the `uring-api` package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- `submit_accept()` and `submit_accept_multishot()` no longer pass a peer
  sockaddr buffer to the kernel. Delivered completions expose the accepted fd
  only; resolve peer addresses with `getpeername()` when needed.

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