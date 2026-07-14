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

### Clean `StreamWriter` shutdown

`StreamWriter.close()` and transport teardown still need a coherent shutdown
story: flush/drain semantics, half-close vs full close, routing socket shutdown
through proactor `sock_shutdown` / `sock_close` rather than direct
`socket.close()` where appropriate, and predictable interaction with in-flight
send operations. Related preparation work lives in io_manager proactor close
paths; stream-level lifecycle policy remains to be designed and implemented.
`SendBuffer` (`send_buffer.py`) is the outbound analogue of `RecvIterBuffer`
(`recv_iter.py`): callback-driven legs, blocking `drain()` / `take_next()` on
the scheduler thread, wired via `ProactorIOManager._open_send_buffer` and
`_open_sock_recv_iter` today.

### Consolidate stream buffer helpers and reader/writer modules

Module count and file granularity are getting high. A later refactor should
group related pieces instead of one tiny concept per file:

- **Stream socket buffers** — `RecvIterBuffer`, `SendBuffer`, and similar
  `scheduler.io` bridge objects (callback pump ↔ blocking producer/consumer API)
  belong together, probably out of `io_manager.py` proper. Factories such as
  `_open_sock_recv_iter` / `_open_send_buffer` move with them; `io_manager`
  stays a thin facade over `Proactor` submission and `IOWaiter` composition.
- **Stream readers and writers** — `StreamReader`, `StreamWriter`, transport
  cores, and asyncio-shaped wrappers can share one module (or a small
  `streams/` package) rather than growing `streams.py` indefinitely alongside
  separate `recv_iter.py` / `send_buffer.py` imports.

Naming is open (`stream_buffers.py`, `sock_stream_io.py`, etc.); the intent is
cohesion by data direction and lifecycle, not one file per class. Do not fold
`SendBuffer` into `recv_iter.py` — recv-specific `recv_many` machinery stays
distinct from outbound `sock_sendall` chaining even when colocated.

## References

- `packages/tealetio/src/tealetio/streams.py` — `StreamServer`, `StreamWriter`
- `packages/tealetio/src/tealetio/recv_iter.py` — `RecvIterBuffer`
- `packages/tealetio/src/tealetio/send_buffer.py` — `SendBuffer`
- `packages/tealetio/docs/IO_MANAGER_DESIGN.md` — io_manager layout and consolidation note
- `packages/tealetio/docs/OPERATION_CALLBACKS.md` — continuous delivery disposition