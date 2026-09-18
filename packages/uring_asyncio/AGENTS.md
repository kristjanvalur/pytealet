# Agent Instructions for uring-asyncio

## Scope

`uring-asyncio` is an asyncio event loop that uses `uring-api` as its
completion multiplexer. Asyncio owns the run loop; this package does **not**
use tealet or tealetio.

Workspace root `AGENTS.md` covers monorepo tooling. Prefer this file when
working under `packages/uring_asyncio/`.

Slice 1 (current) is an `IocpProactor`-shaped adapter plugged into
`asyncio.proactor_events.BaseProactorEventLoop`: oneshot recv/send/accept/
connect and `Ring.wait(timeout)`. Slice 2 (multishot transports, `add_reader`,
pidfd subprocess) is not implemented yet. Design, remaining work, and
`uring-api` boundaries: **`ROADMAP.md`**.

## Quality checks before push / PR

From the workspace root:

```bash
make check
```

Package tests can pass while `ruff` / `ty` still fail. Root CI runs
`make check` on every PR.

## Dev and test

```bash
uv sync --active --locked --dev --package uring-asyncio
timeout 30 uv run --active --package uring-asyncio python -m pytest packages/uring_asyncio/tests/ -v
```

Needs Linux and a working `uring-api` native build. Skip when
`uring_api.is_available()` is false.

## Package boundaries

- Depend only on `uring-api`. Do not import `tealet` or `tealetio`.
- Do not add an event loop to `uring-api`.
- Copy CQE shaping / close / cancel rules from `tealetio.UringProactor` when
  needed; do not wrap that class.
- Complete asyncio Futures on the loop thread via inline `ring.wait()`. Do not
  use `serve_completions()` workers.
- `proactor.send` must drain with `prepare_send_all`, not a single
  `prepare_send`.
