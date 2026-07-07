# Experiment: arm `recv_many` on accept

Branch: `experiment/accept-recv-many-on-connect`

## Motivation

`accept_many(recv_size=...)` arms a one-shot `submit_recv` after each accept and
defers the accept callback until the first byte arrives (or error/EOF). That
suits client-speaks-first protocols such as HTTP, but it blocks server-speaks-first
handlers and it does not integrate with the provided-buffer `recv_many` path.

This experiment adds an optional `recv_sink_factory` hint. When supported, the
proactor arms `recv_many` immediately on each accepted socket and delivers a
user-built receive sink in the accept callback. Data may already be buffered in
the sink when the callback runs.

## API

```python
AcceptManyResult = tuple[
    socket.socket,
    bytes | None,          # initial_data (legacy recv_size only)
    BaseException | None,  # recv_error
    Any | None,            # recv_sink
]
```

```python
def accept_many(
    sock,
    callback,
    *,
    recv_size: int | None = None,
    recv_sink_factory: AcceptRecvSinkFactory | None = None,
) -> ContinuousOperation[AcceptManyResult]:
    ...
```

`recv_size` and `recv_sink_factory` are mutually exclusive.

### Sink factory

```python
AcceptRecvSinkFactory = Callable[[socket.socket, RecvBufferPool], AcceptRecvSink | None]
```

- Called once per accepted connection.
- Returns `None` when the backend cannot honour the hint (same optional-flag
  pattern as other accept hints).
- Otherwise returns an object exposing `recv_many_callback()` for wiring into
  `proactor.recv_many()`.

`recv_iter_buffer_factory()` builds a `RecvIterBuffer` per connection.

Optional `attach_recv_many(stream)` binds the continuous `recv_many` operation.

## Behaviour

| Backend | `recv_size` | `recv_sink_factory` |
|---------|-------------|------------------------|
| `UringProactor` (multishot accept) | one-shot recv, deferred callback | immediate callback + armed `recv_many` |
| `UringProactor` (oneshot accept fallback) | ignored (`initial_data=None`) | immediate callback + armed `recv_many` |
| `SelectorProactor` | ignored (`initial_data=None`) | immediate callback + armed `recv_many` |

`recv_sink` is always `None` when the factory is omitted or returns `None`.

## Push-oriented receive

`RecvIterBuffer` is fed by `recv_many` callbacks (`on_result`) before the
application calls `take_next()`. That is push-at-the-transport layer even though
the consumer API still pulls chunks. A future `RecvStream` abstraction may
expose explicit push delivery to handlers; this experiment only wires the
accept-time arm and returns the sink object.

## `start_server`

`start_server(..., recv_sink_factory=...)` forwards the hint to `accept_many`.
The default `StreamReader` path does not consume `recv_sink` yet; use
`proactor.accept_many()` directly when experimenting with sink-driven protocols.

## Status

Experimental. Not part of a release changelog until the API stabilises.