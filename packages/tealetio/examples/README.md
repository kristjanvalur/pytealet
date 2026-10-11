# Examples

`stdlib_http.py` runs the stdlib `HTTPServer` on one tealet scheduler. `socket` and `poll` are patched first. The accept loop waits with `selectors.PollSelector`. Each connection is finished on its own tealet, and a client tealet on the same scheduler performs three requests.

Stock `serve_forever` still handles one connection at a time. The spawn sits in `process_request`, which is the same hook `ThreadingMixIn` uses for a thread. A real thread has no running scheduler, so `ThreadingHTTPServer` is not a substitute.

From a checkout:

```bash
uv run --active --package tealetio python packages/tealetio/examples/stdlib_http.py
```
