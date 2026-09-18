# uring-asyncio Roadmap

Tracks the intended event-loop design above day-to-day API fixes. Not a
release commitment list.

`uring-asyncio` lets stock `async def` code run on Linux `io_uring` through
`uring-api`. Asyncio owns the run loop; this package is the completion
multiplexer. That is the inverse of `tealetio.TealetProactorEventLoop`, which
hosts asyncio inside a tealetio scheduler.

## Boundaries

- Depend only on `uring-api`. Do not import `tealet` or `tealetio`.
- Do not add an event loop to `uring-api`.
- Copy CQE shaping, close, and cancel rules from `tealetio.UringProactor`
  when needed; do not wrap that class. Its completion worker threads, tealet
  `OpHandle` model, and scheduler wait are the wrong shape for asyncio.
- Do not extend `TealetProactorEventLoop` to “just use uring”. That loop’s
  `select()` parks a tealet; this loop’s `select()` *is* `ring.wait()`.

## Slice 1 — current bootstrap

An `IocpProactor`-shaped adapter plugged into
`asyncio.proactor_events.BaseProactorEventLoop`.

That mapping works: oneshot `prepare_recv` / `prepare_send_all` /
`prepare_accept` / `prepare_connect`, Futures completed from `Ring.callback`
during `wait(timeout)`, stdlib self-pipe for `call_soon_threadsafe`. It is a
correctness vehicle, not the performance design. Stdlib proactor transports
keep one outstanding read and one outstanding write, re-arm after each CQE,
and leave `add_reader` / `add_writer` unimplemented. Slice 1 will sit near
stock asyncio, not uvloop, because those Python transport and Future frames
remain.

Invariants for this slice (keep them when rewriting internals):

- `proactor.send` drains with `prepare_send_all`, not a single
  `prepare_send`. asyncio `sock_sendall` awaits one Future.
- Complete asyncio Futures on the loop thread via inline `ring.wait()`. Do
  not use `serve_completions()` workers. Off-thread `set_result` is illegal
  without `call_soon_threadsafe`, and `IORING_SETUP_DEFER_TASKRUN` forbids
  off-thread reaping.
- Default ring flags: `IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN`
  when `probe(flags=...)` accepts them. This is the opposite of
  `tealetio.UringProactor`, which keeps worker-thread submit enabled.
- Future cancel posts `prepare_cancel_nowait`. A later CQE may still arrive
  (`-ECANCELED` or a lost-race success); ignore it if the Future is already
  done.
- `close()` must reap cancelled CQEs before tearing the ring down (the IOCP
  “pending overlapped at dealloc” class of bug).
- `prepare_close` is for detached fds only.
- If `io_uring` cannot be created (`probe()` empty, seccomp `EPERM`),
  constructing the loop raises `UringUnavailableError`. Do not silently fall
  back to a selector loop.

## Slice 2 — custom loop

Keep asyncio’s public API (`create_connection`, `start_server`, `sock_*`,
timers, SSL via `sslproto`). Replace the IO internals so uring features are
actually used.

1. **Stream transport.** Arm `prepare_recv_multishot` with a `BufGroup`; each
   CQE is a leased `BufView` fed to `protocol.data_received` (copy to `bytes`
   unless a documented memoryview contract is added). Pause/resume is
   cancel/re-arm. Writes go through `prepare_send_all` (later, size-gated
   `send_zc`). Close detaches the fd and uses nowait send/close so Python
   cannot close an in-flight fd (same rule as tealetio `send_close_nowait`).
2. **Accept.** One `prepare_accept_multishot`; each MORE leg becomes a
   transport. Peer address comes from `getpeername()` because multishot
   accept passes a NULL addr.
3. **`add_reader` / `add_writer`.** `prepare_poll` / `prepare_poll_multishot`
   plus `prepare_poll_remove`. Required for Unix drop-in use (psycopg, pyzmq,
   prompt_toolkit). Stop a poll stream with `POLL_REMOVE`, not
   `ASYNC_CANCEL`.
4. **Subprocess.** pidfd plus poll, matching CPython 3.14’s Unix path.
5. **Wakeup.** Drop the socketpair self-pipe. `call_soon_threadsafe` should
   use `ring.break_wait()` or an eventfd polled on the ring. Point
   `signal.set_wakeup_fd` at an fd the ring actually waits on (eventfd, not
   signalfd: signalfd changes the signal mask and leaks into subprocesses).
6. **Timers.** Keep asyncio’s heap and pass the next deadline into
   `ring.wait(timeout)`. `IORING_OP_TIMEOUT` is optional later.

**Re-entrancy.** A protocol callback invoked while draining CQEs may
`write()`, `close()`, or `loop.stop()`. Decide before implementing custom
transports: defer new SQEs until the drain finishes, or make every path
re-entrant. Document and test the chosen rule. tealetio avoids this by
marshalling onto the scheduler; an inline asyncio reap does not have that
hop.

`BufferedProtocol` does not combine cleanly with kernel-selected provided
buffers (`get_buffer()` versus `BufView`). Start that path with oneshot
`recv_into(get_buffer())`. Do not claim zero-copy multishot plus
`BufferedProtocol` without a separate design.

SSL stays on CPython `sslproto` sitting on the stream transport.

## uring-api

No opcode work is required for slice 1 or the first parts of slice 2. Poll,
multishot recv/accept, provided buffers, `send_all`, cancel, and
`break_wait` are already there.

Respect existing contracts rather than changing them:

- Seed `completion.sequence` *before* `prepare`, never after.
- Multishot MORE legs are shells; `!MORE` is the armed handle. Call
  `take_user_data()` on every delivered object.
- `auto_submit=True` matches asyncio: prepare during callbacks, flush at
  `wait()`.

Later, only if measurements ask for them:

| Possible addition | Why |
|---|---|
| `IORING_OP_SPLICE` / sendfile | Native `loop.sock_sendfile` instead of the copy fallback |
| `IORING_OP_TIMEOUT` | Timers on the ring instead of `wait(timeout)` plus the Python heap |
| Ring resize | CQ overflow under multishot accept plus recv (already on the uring-api roadmap; this loop would consume it) |

## Tests and performance

CPython’s `test.test_asyncio` instantiates loop classes directly and cannot
be run unmodified. Own tests first (streams, cancel races, close with
in-flight ops, thread-safe wakeup, UDP). A later imported subset can use
`loop_factory` injection and a skip list.

Do not claim uvloop parity until slice 2 transports exist and there is a
named echo benchmark. tealetio’s wrk harnesses can grow a third server:
stock asyncio versus this loop versus tealetio.

## Suggested order

1. Slice 1 (landed): Iocp-shaped proactor, `UringProactorEventLoop`,
   loop factory, self-pipe kept.
2. `add_reader` / `add_writer` via poll; pidfd subprocess; replace the
   self-pipe with `break_wait` / eventfd.
3. Replace `_ProactorSocketTransport` and `_start_serving` with multishot
   plus provided buffers plus `send_all`. Decide CQE-drain re-entrancy
   first.
4. Only then consider uring-api opcode additions.
