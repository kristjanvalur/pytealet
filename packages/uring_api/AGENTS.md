# Agent Instructions for uring-api

## Scope

`uring-api` is a low-level Linux `io_uring` wrapper. It exposes ring lifecycle,
socket submit operations, completion waiting, and optional callback-thread
delivery. It does **not** implement an event loop, scheduler, proactor, or
asyncio compatibility layer.

For API behaviour and examples, read `README.md`. For planned extensions, read
`ROADMAP.md`. This file covers build constraints, invariants, and conventions
agents should follow when changing the package.

## Build and Test

### Prerequisites

- Linux only (`Operating System :: POSIX :: Linux`)
- `liburing` development headers **>= 2.4** (`liburing-dev` on Debian/Ubuntu)
- No dependency on core `tealet` or other workspace packages

On non-Linux hosts or without liburing headers, `setup.py` skips the native
extension. The pure-Python `uring_api` package still installs, but `probe()`
returns `{}` and ring operations are unavailable.

### Commands

From the workspace root:

```bash
uv sync --active --locked --dev --package uring-api
timeout 30 uv run --active --package uring-api python -m pytest packages/uring_api/tests/ -v
```

Run unit tests with a **30-second timeout** (`timeout 30` on Linux). The full
suite should finish well inside that budget on a desktop; a hang usually means a
deadlock or a stuck `wait()` rather than slow hardware.

Install liburing on Ubuntu:

```bash
sudo apt-get install -y liburing-dev
```

C formatting for this package is included in the root Makefile:

```bash
make format-c
make format-c-check
```

### C compile policy

Match core `tealet` extension builds:

```text
-std=c17 -pedantic-errors -Wall -Wno-unused-function
```

`setup.py` defines `EXTENSION_C_COMPILE_ARGS`; keep
`tests/conftest.py` in sync. Ad-hoc compiles (`build_c_api_client()` in
`tests/helpers.py`,
public header checks) must use the same flags.

For intentional CPython `void*` slot conversions (for example `PyModuleDef_Slot`
with `Py_mod_exec`), use the local pedantic pragma pattern from
`uring_api_module.c` and `tests/capi_client/uring_api_capi_client.c`.

## Runtime Availability

Import success does not imply `io_uring` works. Ring creation can fail because
of kernel policy, containers, seccomp (`EPERM`), memory limits (`ENOMEM`), or
missing kernel support.

- Use `uring_api.probe()` for availability and named capabilities.
- Use `uring_api.is_available()` only when a boolean is enough.
- Prefer **runtime operation probes** for behavioural optional features such as
  `IORING_RECV_MULTISHOT` when a real submission is the only reliable check.
- **Version-gated** capabilities use `uname(2)` against documented
  `io_uring_enter(2)` floors: `IORING_OP_STATX` (5.6), `IORING_OP_SEND_ZC` and
  `IORING_OP_SENDMSG_ZC` (6.0). Protocol-specific zerocopy failures are handled
  above `uring_api` (for example `AF_UNIX` uses copying sends in `tealetio`).
- Production code must still handle `OSError` when creating real rings with
  larger `entries` or setup flags than the tiny probe ring.

## Test Policy

Tests live in `packages/uring_api/tests/`, split by concern (see
`tests/README.md`). Shared skip helpers are in `conftest.py`; socket helpers,
C API client build, and kernel-version utilities are in `helpers.py`. When
verifying changes locally or in automation, wrap pytest in a 30-second process
timeout so stuck tests fail fast instead of blocking the session.

Follow existing patterns:

- Gate on availability with `require_uring()`.
- Gate optional features with `require_uring_capability("NAME")`.
- Skip with `pytest.skip(...)` when the environment lacks support; do not treat
  unavailable `io_uring` as a code defect.
- For errno-sensitive paths (`EINVAL`, `ENOSYS`, `EOPNOTSUPP`, `ENOBUFS`), skip
  when the platform rejects an optional operation rather than weakening asserts.
- Do not hide segfaults or memory corruption by skipping tests unless explicitly
  requested.

`tests/capi_client/` validates the public C API capsule by compiling a small
downstream extension at test time. Keep C API changes compatible with ABI checks
in those tests.

## Design Invariants

### Buffer and fd ownership

- Ordinary sends retain the submitted Python buffer until the operation CQE
  completes.
- `submit_send_zc()` and `submit_sendmsg_zc()` deliver the user `Completion`
  on the operation CQE; the later `IORING_CQE_F_NOTIF` lifetime CQE is consumed
  internally before the retained buffer is released.
- `submit_recv_multishot()` requires a caller-owned `BufGroup`, delivers leased
  `BufView` completions, and assigns `completion.sequence` so out-of-order
  callback delivery can be reconstructed. When the buffer ring is empty the
  multishot terminates with `-ENOBUFS`; callers return buffers and resubmit.
- **Multishot delivery contract** (accept / poll / recv): intermediate
  `IORING_CQE_F_MORE` legs deliver a **shell** `Completion` that copies
  `user_data` from the armed handle (shells do not re-arm reverse links). Terminal `!MORE`
  (cancel, poll_remove, EOF, `-ENOBUFS`, natural end) delivers the **armed
  handle itself**. Keep this shape; clients break waitable cycles by clearing
  `completion.user_data` on each delivery (only the terminal clear hits the
  reverse-linked object).
- `submit_close()` is for **caller-owned detached fds** only (for example after
  `socket.detach()`). Do not close fds still owned by Python socket objects.
- **Lazy submit:** ordinary `submit_*` and all nowait helpers only prepare SQEs
  (including cancel / poll_remove). Flush with `Ring.submit()`, **`wait()` /
  serve (flush pending at entry when this thread may submit)**, SQ-full
  `get_sqe`, or after each delivery callback batch. tealetio threaded parks
  (`wait_idle` / async event) call `ring.submit()` because they never enter
  `ring.wait`; inline `ring.wait` flushes itself. SQPOLL `get_sqe` may hold the
  ring CS while waiting for a slot (GIL released); intended for
  SINGLE_ISSUER-style exclusive prep.
- **Construct then prepare:** `construct_send` / `construct_send_zc` /
  `construct_recv` / `construct_read` / `construct_write` /
  `construct_sendto` / `construct_recvmsg` / `construct_sendmsg` /
  `construct_sendmsg_zc` / `construct_connect` / `construct_recv_buf` /
  `construct_recv_multishot` / `construct_openat` / `construct_statx` /
  `construct_statx_fdsize` / `construct_accept` / `construct_poll` /
  `construct_close` / `construct_shutdown` / `construct_socket` /
  `construct_cancel` / `construct_poll_remove` bind cargo on the matching
  sidecar (VIEW, VIEW_SOCKADDR, MSG, SOCKADDR, BUF_GROUP, PATH, STATX,
  STATX_FDSIZE, SCALAR) or, for cancel/poll_remove, ``cancel_target`` with no
  SQE so clients can arm
  reverse links first. `prepare` (one Completion or a sequence) does get_sqe +
  the matching `io_uring_prep_*`. The matching Python `submit_*` is construct +
  prepare of that handle. The C capsule exposes `ring_construct_*` +
  `ring_prepare` + `ring_submit` (flush) only — no per-op `ring_submit_*`
  slots; nowait is `completion_set_nowait` then `ring_prepare`. `prepare` is not transactional: a later `get_sqe` failure can leave
  the prefix prepared (and possibly flushed). Cancel/poll_remove may be
  constructed against an unprepared target: the target's identity is the
  `Completion` pointer. The kernel only sees that identity after the target
  SQE is prepared; prepare the target first if one flush should publish both
  in order. Dropping an unprepared handle just releases its cargo. Preparing a
  completion on a different ring than the one used to construct it is undefined.
- **Cancel / poll_remove:** same construct-then-prepare as other waitable
  submits. Cargo is `cancel_target`. If the target is still in the SQ, cancel
  prepared after it publishes in order on the next flush. No special pre/post
  flush until a real need appears.
- Nowait helpers (`submit_close_nowait`, `submit_shutdown_nowait`,
  `submit_cancel_nowait`, `submit_poll_remove_nowait`): construct a temporary
  `Completion` with `nowait` set, prepare a **tagged** nowait SQE (not the
  `Completion*`), then drop the handle. No client delivery. Use
  `construct_*_nowait` (or `construct_close` + `completion.nowait = True`)
  to put a nowait op in a `prepare` batch. Prefer when the result/ack is unused.
  Successful ops may post no CQE (`IOSQE_CQE_SKIP_SUCCESS` when
  `IORING_FEAT_CQE_SKIP`); failures (`res < 0`) invoke
  `Ring.nowait_error_handler` when set (after CQ drain, not under the drain lock).
  Handler errors go through `exception_handler`; the drain never fails for nowait.

### Provided-buffer receive (`BufGroup` / `BufView`)

- Create pools with `Ring.create_buf_group()`; submit with `submit_recv_buf()`
  or `submit_recv_multishot()`. Neither `BufGroup` nor `BufView` is directly
  instantiable.
- A `BufGroup` must belong to the `Ring` that created it. Reject cross-ring use
  with `ValueError`.
- Provided-buffer completion paths always return `BufView`, never `b""`. EOF (`res == 0`)
  yields an empty `BufView` (`length == 0`, falsy). Kernel-selected zero-length
  buffers are still leased and recycle on `close()` / last `memoryview` release.
- `BufView` buffer exports are read-only. Set `format` only when
  `PyBUF_FORMAT` is requested. Recycle leased slots when exports drop to zero
  or on explicit `close()`.
- `tp_clear` / `tp_dealloc` must free provided-buffer rings and recycle leased
  slots without raising exceptions during cyclic GC.

### Queue backpressure

`get_sqe` flushes when the SQ is full, then retries. With `IORING_SETUP_SQPOLL`,
if a slot is still unavailable after the second flush it waits for SQ space
(`io_uring_sqring_wait`) and retries until a slot appears or a few seconds
elapse. Non-SQPOLL must free a slot after one successful flush. Either way, if
a slot cannot be obtained after flush (or after the SQPOLL timeout), raise
`RuntimeError` — a stuck queue / dead poller, not recoverable backpressure.
Do not treat SQ full as “wait for CQEs.” There is no recoverable
`SubmissionQueueFull` backpressure exception; stuck SQ is `RuntimeError`.

**SQPOLL slot-wait and the ring critical section:** prepare paths call `get_sqe`
under `Py_BEGIN_CRITICAL_SECTION` so the reserved SQE stays exclusive through
prep. The SQPOLL wait therefore runs **while the ring CS is still held** (up to
the timeout window). The GIL is released around `io_uring_sqring_wait` / EINVAL
backoff so other Python threads can run, but free-threaded builds still serialise
other ring ops behind that CS. That matches **SINGLE_ISSUER**-style exclusive
prep; multi-thread submit without SINGLE_ISSUER plus SQPOLL is supported only
with that bound. Restructuring wait outside the CS is not done (would need a
get_sqe/re-validate protocol across prepare).

### Threading and serving

- One thread should reap with `wait()`; submit methods may be called from other
  threads.
- `break_wait()` opens the `wait_idle` park immediately. When completion service
  is idle it also best-effort submits one internal NOP to wake `wait()` on an
  empty CQ; while serve workers are active the NOP is skipped (idle only).
  `stop_serving()` forces a NOP. SQ-full NOP failure still wakes `wait_idle`.
- While `serve_completions()` workers are running, public `wait()` raises
  `RuntimeError`. Join worker threads and call `stop_serving()` before `close()`.
- Delivery callback exceptions invoke `exception_handler` when set; handler
  failures (or no handler) propagate from `serve_completions()` and stop only
  that worker.
- `IORING_SETUP_DEFER_TASKRUN` pins submit and completion reaping to one thread.
  `wait()`, `serve_completions()`, and `break_wait()` must run on that same
  thread; worker-thread `serve_completions()` is rejected at entry.

### Setup flags

`IORING_SETUP_SINGLE_ISSUER` and similar flags impose application contracts.
Use `IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN` when deferring
taskrun; the extension enforces the combined single-thread contract.
Check `probe(flags=...)` before constructing a real `Ring(flags=...)`.
`tealetio.UringProactor` does not default this flag; see
`ROADMAP.md` (`UringProactor` submission threading and
`IORING_SETUP_SINGLE_ISSUER`) for why worker-thread submission stays enabled.

## C Extension Layout

Native sources live under `src/_uring_api/` (mirroring core `tealet`'s
`src/_tealet/` layout). They are split by concern:

| Area | Files |
| --- | --- |
| Module entry | `_uring_api/uring_api_module.c` |
| Ring lifecycle | `_uring_api/uring_api_ring.c`, `_uring_api/uring_api_core.c` |
| Submit path | `_uring_api/uring_api_submit.c`, `_uring_api/uring_api_submit.h` |
| Completions | `_uring_api/uring_api_completion.c` |
| Provided buffers | `_uring_api/uring_api_bufgroup.c`, `_uring_api/uring_api_bufview.c` |
| Probing | `_uring_api/uring_api_probe.c` |
| Callback service | `_uring_api/uring_api_dispatch.c` |
| C API capsule | `_uring_api/uring_api_capi.c`, `_uring_api/uring_api_capi_impl.h` |
| Completion kinds | `uring_api/include/uring_api_completion_kinds.h` |

Submission follows an `_impl` + thin Python wrapper pattern:

- `UringApiRing_submit_*_impl(...)` hold the io_uring prep/submit logic.
- `UringApiRing_submit_*(self, args, kwargs)` parse arguments and delegate.
- The C API calls `_impl` functions directly where appropriate.

Public native headers: `src/uring_api/include/uring_api_capi.h` and
`uring_api_completion_kinds.h`. Python stubs: `src/_uring_api.pyi`. High-level
constants and types live in `src/uring_api/__init__.py`.

### Completion kinds and C API ABI

- Stable public kind values live in `URING_API_COMPLETION_KIND_*` macros
  (`uring_api_completion_kinds.h`). Internal pending kinds must stay aligned.
  Provided-buffer receive uses `RECV_MULTISHOT` (13) for multishot and
  `RECV_BUF` (16) for one-shot `submit_recv_buf()`.
- `Completion.kind` on the native `Completion` object is an `int`. Export
  `CompletionKind` (`enum.IntEnum`) from `uring_api/__init__.py` only — not from
  `_uring_api.pyi` or the extension module namespace.
- C API clients must check `abi_version` / `URING_API_CAPI_ABI_VERSION` and
  `struct_size` before use, and null-check pointers they rely on. While the
  package remains pre-release, keep `URING_API_CAPI_ABI_VERSION` at **1** but
  vtable layout may change freely: insert or reorder entries in
  `uring_api_capi.h`, keep `uring_api_probe.c` and `tests/capi_client/` in
  sync. Reserve ABI version bumps for the first stable release; after that,
  append-only growth is the default compatibility rule.

Preserve refcount and buffer-lifetime invariants when touching completion
delivery or pending-operation state.

### Internal C invariants

Every submitted SQE stores a `Completion` pointer retrievable from its CQE. A
NULL `cqe_get_completion()` result means our drain/submit bookkeeping is broken,
not a user or kernel error. Document that contract with `assert` — do not raise
`SystemError` or add release-build guards that imply the caller can recover.

## What Not to Add Here

Keep this package narrow:

- No poll/readiness event loop or asyncio integration.
- No scheduler or task model.
- No broad liburing opcode surface without a clear Python ownership contract.

Track wider kernel features and specialised tuning in `ROADMAP.md` rather than
expanding the baseline API opportunistically.

## Documentation Hygiene

- Update `README.md` for user-visible API or behaviour changes.
- Update `ROADMAP.md` when deferring or adopting new kernel/liburing features.
- Update `src/_uring_api.pyi`, `uring_api_capi.h`, and
  `uring_api_completion_kinds.h` together with C API or completion-kind changes.
- Bump package version in `pyproject.toml` and record changes in
  `CHANGELOG.md` before release tags (`uring-api-vX.Y.Z`).

## References

- Package README: `README.md`
- Feature roadmap: `ROADMAP.md`
- CI workflow: `.github/workflows/uring-api-ci.yml`
- Workspace-wide rules: `../../AGENTS.md`