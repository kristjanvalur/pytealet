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
uv run --active --package uring-api python -m pytest packages/uring_api/tests/ -v
```

Install liburing on Ubuntu:

```bash
sudo apt-get install -y liburing-dev
```

C formatting for this package is included in the root Makefile:

```bash
make format-c
make format-c-check
```

## Runtime Availability

Import success does not imply `io_uring` works. Ring creation can fail because
of kernel policy, containers, seccomp (`EPERM`), memory limits (`ENOMEM`), or
missing kernel support.

- Use `uring_api.probe()` for availability and named capabilities.
- Use `uring_api.is_available()` only when a boolean is enough.
- Prefer **runtime operation probes** over kernel version checks for optional
  features (`IORING_RECV_MULTISHOT`, `IORING_OP_SEND_ZC`, etc.).
- Production code must still handle `OSError` when creating real rings with
  larger `entries` or setup flags than the tiny probe ring.

## Test Policy

Tests live in `packages/uring_api/tests/`. Follow existing patterns:

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
- `submit_recv_multishot()` owns an internal provided-buffer ring, copies each
  receive into a new `bytes` object, recycles the kernel buffer immediately, and
  assigns `completion.sequence` so out-of-order callback delivery can be
  reconstructed.
- `submit_close()` is for **caller-owned detached fds** only (for example after
  `socket.detach()`). Do not close fds still owned by Python socket objects.

### Queue backpressure

`SubmissionQueueFull` means the submission queue has no free SQE after flushing
prepared work. Treat it as backpressure: wait for completions, then retry. It is
not a permanent ring failure.

### Threading and serving

- One thread should reap with `wait()`; submit methods may be called from other
  threads.
- `break_wait()` is safe while another thread blocks in `wait()`.
- While `serve_completions()` workers are running, public `wait()` raises
  `RuntimeError`. Join worker threads and call `stop_serving()` before `close()`.
- Callback exceptions are reported as unraisable and stop the serving group.

### Setup flags

`IORING_SETUP_SINGLE_ISSUER` and similar flags impose application contracts.
Check `probe(flags=...)` before constructing a real `Ring(flags=...)`.

## C Extension Layout

Sources are split by concern under `src/`:

| Area | Files |
| --- | --- |
| Module entry | `uring_api_module.c` |
| Ring lifecycle | `uring_api_ring.c`, `uring_api_core.c` |
| Submit path | `uring_api_submit.c`, `uring_api_submit.h` |
| Completions | `uring_api_completion.c` |
| Probing | `uring_api_probe.c` |
| Callback service | `uring_api_dispatch.c` |
| C API capsule | `uring_api_capi.c`, `uring_api_capi_impl.h` |

Submission follows an `_impl` + thin Python wrapper pattern:

- `UringApiRing_submit_*_impl(...)` hold the io_uring prep/submit logic.
- `UringApiRing_submit_*(self, args, kwargs)` parse arguments and delegate.
- The C API calls `_impl` functions directly where appropriate.

Public native header: `src/uring_api/include/uring_api_capi.h`. Python stubs:
`src/_uring_api.pyi`. High-level constants and types live in
`src/uring_api/__init__.py`.

Preserve refcount and buffer-lifetime invariants when touching completion
delivery or pending-operation state.

## What Not to Add Here

Keep this package narrow:

- No poll/readiness event loop or asyncio integration.
- No scheduler or task model.
- No broad liburing opcode surface without a clear Python ownership contract.

Track wider kernel features and specialised tuning in `ROADMAP.md` rather than
expanding the baseline API opportunistically. Provided-buffer / leased-buffer
receive models belong here only when they have an explicit `BufGroup` /
`BufView`-style ownership story (see `ROADMAP.md`).

## Documentation Hygiene

- Update `README.md` for user-visible API or behaviour changes.
- Update `ROADMAP.md` when deferring or adopting new kernel/liburing features.
- Update `src/_uring_api.pyi` and `uring_api_capi.h` together with C API
  changes.
- Bump package version and `CHANGELOG.md` before release tags (`uring-api-vX.Y.Z`).

## References

- Package README: `README.md`
- Feature roadmap: `ROADMAP.md`
- CI workflow: `.github/workflows/uring-api-ci.yml`
- Workspace-wide rules: `../../AGENTS.md`