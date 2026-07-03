# uring-api test layout

Tests are split by concern. Shared fixtures and skip helpers live in
`conftest.py`; socket helpers, C API client build, and kernel-version
utilities live in `helpers.py`.

## Modules

- `test_module_exports.py`: package metadata, constant exports, header compile checks, import-without-extension fallback
- `test_probe.py`: `probe()` behaviour and kernel version gates
- `test_setup_flags.py`: `IORING_SETUP_SINGLE_ISSUER` and `IORING_SETUP_DEFER_TASKRUN` threading contracts
- `test_buf_group.py`: `BufGroup` / `BufView` lifecycle and provided-buffer receive paths
- `test_ring_lifecycle.py`: ring create/close and invalid-parameter handling
- `test_ring_socket.py`: socket/datagram send/recv, accept, connect, cancel, shutdown, close
- `test_ring_poll.py`: poll, multishot poll, and poll remove
- `test_ring_file.py`: read/write, openat, and statx
- `test_ring_serving.py`: `serve_completions`, callbacks, and `break_wait`
- `test_gc_cycles.py`: cyclic GC collectability for user data and callbacks
- `test_c_api.py`: downstream C API capsule client checks (`tests/capi_client/`)

## Running

From the workspace root:

```bash
uv sync --active --locked --dev --package uring-api
timeout 30 uv run --active --package uring-api python -m pytest packages/uring_api/tests/ -v
```

Gate on availability with `require_uring()` and optional features with
`require_uring_capability("NAME")`. Skip when the environment lacks support;
do not treat unavailable `io_uring` as a code defect.
