# tealetio Roadmap

Tracks architectural follow-ups above day-to-day API fixes. Not a release
commitment list.

## Task groups

v1 is `TaskGroup` as a synchronous nursery: one-shot `Task.throw()` of a
private `CancelledError` tagged with the group, no sticky cancel. Follow-ups
if we need them:

- **Sticky cancel at park.** `Task.cancel()` / `TaskGroup` abort is a one-shot
  `throw()`. A task that swallows `CancelledError` and parks again is not
  re-cancelled. Sticky cancel (`_cancel_requested` checked in `_park_current`)
  would only be for applications that ignore cancel and hang a group in
  practice. Do not add that semantic speculatively.
- **Cancel scopes.** Nested deadlines and shielding as scopes, not only
  `timeout()` plus `shield()` on a Future. The `RawTimeoutError` /
  `_TaskGroupCancelled` tagging is the seed; a stack of scopes is the rest.

`StreamServer` handler tealets should stay outside a `TaskGroup`: one failed
client must not cancel the others. `gather` stays the fixed-set helper.

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

## References

- `packages/tealetio/src/tealetio/streams/` — `StreamServer`, `StreamReader`, `StreamWriter`
- `packages/tealetio/src/tealetio/io_buffers.py` — `RecvIterBuffer`, `SendBuffer`
- `packages/tealetio/docs/IO_MANAGER_DESIGN.md` — io_manager layout and consolidation note
- `packages/tealetio/docs/OPERATION_CALLBACKS.md` — continuous delivery disposition