# uring-api test layout

Tests are split by concern. Shared skip helpers live in `conftest.py`.
Socket setup, the C API client build, kernel-version checks, `drain_until`,
and `break_wait_from_other_thread` live in `helpers.py`.

## Modules

- `test_module_exports.py`: package metadata, constant exports, header compile checks, import-without-extension fallback
- `test_probe.py`: `probe()` behaviour and kernel version gates
- `test_parse_keywords.py`: keyword acceptance on probe, construct, wait, and poll
- `test_setup_flags.py`: `IORING_SETUP_SINGLE_ISSUER` and `IORING_SETUP_DEFER_TASKRUN` threading contracts
- `test_ring_lifecycle.py`: ring create/close, `pending_count`, and invalid-parameter handling
- `test_ring_stats.py`: `Ring.stats()` SQ/CQ and submit-path counters
- `test_lazy_submit.py`: `submit()`, `auto_submit`, and `SubmissionQueueFull`
- `test_ring_socket.py`: socket/datagram send/recv, accept, connect, cancel, shutdown, close
- `test_ring_poll.py`: poll, multishot poll, and poll remove
- `test_ring_cq_ready.py`: `Ring.poll()` CQ-ready wait (does not harvest)
- `test_wait_resume.py`: `Ring.wait` parks again after a silent burst; a timed wait keeps its deadline
- `test_ring_file.py`: read/write, openat, and statx
- `test_ring_serving.py`: `serve_completions`, callbacks, and `break_wait`
- `test_buf_group.py`: `BufGroup` / `BufView` lifecycle and provided-buffer receive paths
- `test_buf_group_inflight.py`: `inflight_count` while a provided-buffer receive is armed
- `test_no_deliver_multi.py`: `no_deliver_multi` drops later `recv_multishot` CQEs
- `test_recv_link_timeout.py`: oneshot recv link timeout (`Completion.timeout`)
- `test_gc_cycles.py`: cyclic GC collectability for user data, ring callbacks, and buf-group callbacks
- `test_c_api.py`: downstream C API capsule client checks (`tests/capi_client/`)
- `test_construct.py`: `construct_*` then `prepare` for waitable ops (no SQE until prepare)
- `test_send_all.py`: synthetic send-all drain (one waitable, multi-leg send)

## Running

From the workspace root:

```bash
uv sync --active --locked --dev --package uring-api
timeout 30 uv run --active --package uring-api python -m pytest packages/uring_api/tests/ -v
```

Gate on availability with `require_uring()` and optional features with
`require_uring_capability("NAME")`. Skip when the environment lacks support;
do not treat unavailable `io_uring` as a code defect.
