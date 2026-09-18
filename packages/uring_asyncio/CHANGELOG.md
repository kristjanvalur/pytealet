# Changelog

## Unreleased

### Added

- First slice: `UringProactor` (IOCP-shaped) and `UringProactorEventLoop` on
  `asyncio.proactor_events.BaseProactorEventLoop`, driven by `uring-api`.
  `asyncio.run(..., loop_factory=UringProactorEventLoop)` works on Python 3.12+;
  `uring_asyncio.run()` covers 3.10+. Remaining work is in `ROADMAP.md`.
