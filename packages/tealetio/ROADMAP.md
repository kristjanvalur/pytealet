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

## References

- `packages/tealetio/src/tealetio/streams.py` — `StreamServer`, `StreamWriter`
- `packages/tealetio/docs/IO_MANAGER_DESIGN.md` — io_manager open follow-ups
- `packages/tealetio/docs/OPERATION_CALLBACKS.md` — continuous delivery disposition