# SSL / TLS in tealetio

Status: **experimental**. Native TLS wraps an already connected
`(ReadStream, WriteStream)` pair. It is not hooked into `open_connection` /
`start_server`. `wrap_ssl` lives in `tealetio.streams.ssl`.

## Choice

TLS lives in userspace above the stream buffers, using the same crypto engine
as asyncio: `ssl.MemoryBIO` plus `SSLContext.wrap_bio` → `SSLObject`. Ciphertext
bytes are ordinary `StreamReader.read` / `StreamWriter.write` traffic. The
proactor (`UringProactor` or `SelectorProactor`) never sees a TLS record layer;
it only sees bytes on the fd.

`ReadStream` and `WriteStream` are the native pair interfaces.
`StreamReader` / `StreamWriter` are the default concrete types. `SSLStream`
implements both, so `wrap_ssl` returns `(stream, stream)` — one object in both
slots. Handshake, unwrap, close, and extra info stay on that object. A
plaintext `read` or `write` can move ciphertext on both inner streams, so the
record layer cannot be split.

Ownership matches the inner streams: a single tealet owns the pair. There is
no `tealetio.Lock`. OpenSSL calls return `WantRead` / `WantWrite` before the
tealet parks, so another tealet never runs with `SSL_read` on the C stack.

Two alternatives were rejected:

- **`ssl.wrap_socket` / `SSLSocket`.** The resulting object intercepts
  `recv`/`send` on the Python socket. Uring `recv`/`send` (and the selector
  proactor's socket helpers) talk to the fd directly, so they would read and
  write ciphertext while the application believed it was speaking plaintext —
  or they would bypass the SSLSocket state machine entirely. Asyncio already
  refuses this: `BaseEventLoop._check_ssl_socket` raises if the sock is an
  `SSLSocket`.
- **Lifting `asyncio.sslproto.SSLProtocol` onto native streams.** That class is
  a `BufferedProtocol` sitting between an application `Protocol` and a socket
  `Transport`. It is driven by `connection_made` / `get_buffer` /
  `buffer_updated` / `data_received` / `eof_received`. Native tealetio is
  blocking tealets on `StreamReader`/`StreamWriter`, not a callback protocol
  stack. Reusing the object would force native streams to grow asyncio's
  transport/protocol seam. The engine (`MemoryBIO` + `SSLObject` +
  `SSLWantReadError` / `SSLWantWriteError` retry) is what we want; the protocol
  object is not.

kTLS is out of scope. It would still need uring/selector awareness of offloaded
records, and it does not replace the userspace wrap for ordinary sockets.

## How asyncio hooks SSL

`BaseEventLoop.create_connection(..., ssl=ctx)` connects a **plain** socket,
then calls `_make_ssl_transport` (abstract on `BaseEventLoop`; implemented on
`_UnixSelectorEventLoop` in `selector_events.py` and `BaseProactorEventLoop` in
`proactor_events.py`). Both implementations construct `sslproto.SSLProtocol`
around the application protocol and attach a **plain** socket transport
(`_SelectorSocketTransport` or `_ProactorSocketTransport`) to that SSL
protocol. The object returned to the application is
`ssl_protocol._app_transport`, not the raw socket transport.

`SSLProtocol` owns the engine:

```text
app Protocol  <-->  _SSLProtocolTransport  <-->  SSLProtocol
                                                    incoming MemoryBIO
                                                    SSLObject (wrap_bio)
                                                    outgoing MemoryBIO
                                              <-->  socket Transport  <-->  fd
```

Handshake starts in `connection_made` → `_do_handshake`. Want-read / want-write
are `SSLAgainErrors = (ssl.SSLWantReadError, ssl.SSLSyscallError)`. Outgoing
BIO bytes are written to the transport; incoming ciphertext arrives via
`buffer_updated` into the incoming BIO.

`BaseEventLoop.start_tls` upgrades an existing plain transport: it pauses
reading, `set_protocol(ssl_protocol)`, schedules `connection_made`, resumes
reading, and awaits the handshake waiter. `StreamWriter.start_tls` drains
buffered plaintext first, then calls `loop.start_tls`.

The same `_make_ssl_transport` hook is what `loop.start_server(..., ssl=ctx)`
and `asyncio.open_connection(..., ssl=ctx)` use. There is no SSLSocket on that
path.

## Hosted tealetio asyncio

`TealetSelectorEventLoop` subclasses `asyncio.SelectorEventLoop`.
`TealetProactorEventLoop` subclasses `asyncio.proactor_events.BaseProactorEventLoop`.
Neither overrides `_make_ssl_transport`, so hosted asyncio already inherits
stdlib TLS:

- `asyncio.open_connection(..., ssl=ctx)` inside a tealet-hosted loop
- `asyncio.start_server(..., ssl=ctx)`
- `StreamWriter.start_tls`

`run_asyncio_in_tealet(...)` / `TealetProactorEventLoop()` plus those stdlib
calls is the proof that the inheritance works. Native tealetio streams are a
separate wrap.

Caveat: `run_asyncio_in_tealet` on a `UringProactor` scheduler currently gives
the asyncio loop a **separate** `SelectorProactor` (see `asyncio.py` around the
`TealetProactorEventLoop` factory). Asyncio's self-pipe and `loop.sock_*`
helpers still expect selector-style readiness; the host tealet IO stays on
`scheduler.proactor`. That path is therefore not uring ciphertext I/O.
Constructing `TealetProactorEventLoop()` with no argument uses
`current.proactor` directly (including `UringProactor` when that is the host),
which is the stronger hosted-loop check.

## Native wrap

`tealetio.streams.ssl.wrap_ssl(reader, writer, sslcontext, ...)` builds one
`SSLStream` over an already connected pair and returns it twice. Handshake is
explicit (`do_handshake`). The inner streams keep doing ciphertext I/O;
`proactor.py` is untouched.

Retry loop (blocking tealet I/O, not callbacks):

```text
while True:
    try:
        return sslobj.do_handshake() / read() / write() / unwrap()
    except (ssl.SSLWantReadError, ssl.SSLSyscallError):
        flush outgoing BIO → writer.write + writer.drain
        chunk = reader.read(~16KiB)   # not read(-1); that waits for TCP EOF
        if not chunk: incoming.write_eof()
        else: incoming.write(chunk)
    except ssl.SSLWantWriteError:
        flush outgoing
```

Successful `write` / `do_handshake` / `unwrap` / `read` also flush the outgoing
BIO (a read can emit handshake or KeyUpdate bytes without `WantWrite`).
`read(-1)` loops until TLS EOF (`SSLZeroReturnError` or empty). `write_eof` is
not implemented: TLS has `close_notify` via `unwrap`/`close`, not TCP
half-close. `can_write_eof` is false.

One `SSLObject` is shared by read and write. The owning tealet calls both;
there is no lock. Sharing the stream across tealets needs the same external
locking the inner reader and writer would need.

## Experiment API

```python
from tealetio.streams.ssl import wrap_ssl

reader, writer = wrap_ssl(reader, writer, ctx, server_side=False, server_hostname="localhost")
assert reader is writer
reader.do_handshake()
writer.write(b"ping\n")
writer.drain()
payload = reader.readexactly(5)
writer.close()
writer.wait_closed()
```

Not done, deliberately:

- public `ssl=` on `open_connection` / `start_server`
- exporting `wrap_ssl` / `SSLStream` from `streams.__all__`
- kTLS
- `write_eof` on TLS
- wrapping `ssl.SSLSocket`
- instantiating `asyncio.sslproto.SSLProtocol` on native streams
