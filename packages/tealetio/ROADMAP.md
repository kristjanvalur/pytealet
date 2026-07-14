# tealetio Roadmap

Tracks architectural follow-ups above day-to-day API fixes. Not a release
commitment list.

## Streams and servers

### Callback-driven `StreamServer`

Today `StreamServer` runs a dedicated accept-loop tealet that blocks on
`accept_many_streams().wait()`, re-arms after each selector leg, and dispatches
clients from scheduler-marshalled delivery callbacks. Multishot accept on uring
already streams connections through the proactor result callback; the extra
tealet exists mainly to own the wait/re-arm loop and `wait_closed()` joining.

A future refactor could make the server entirely callback-driven: one continuous
`accept_many` submission whose callback handles each accept (and optional
accept-time preread) without a parking tealet re-issuing after every leg.
Shutdown would cancel the continuous op and join handler tealets only. Mostly an
architectural simplification — behaviour should stay the same for callers of
`start_server()` / `serve_forever()`.

### Consolidate stream reader/writer modules

`RecvIterBuffer` and `SendBuffer` now live together in `io_buffers.py` under the
`scheduler.io` bridge layer (`open_recv_iter_buffer` / `open_send_buffer`;
`ProactorIOManager` delegates through `_open_sock_recv_iter` /
`_open_send_buffer`). A later refactor could still split `streams.py` into a
small `streams/` package when reader/writer cores outgrow a single module.

## References

- `packages/tealetio/src/tealetio/streams.py` — `StreamServer`, `StreamWriter`
- `packages/tealetio/src/tealetio/io_buffers.py` — `RecvIterBuffer`, `SendBuffer`
- `packages/tealetio/docs/IO_MANAGER_DESIGN.md` — io_manager layout and consolidation note
- `packages/tealetio/docs/OPERATION_CALLBACKS.md` — continuous delivery disposition