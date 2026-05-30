"""Compatibility shim for implementation-specific ``greenlet._greenlet`` APIs.

Most functions in this module exist to satisfy greenlet compatibility tests and
downstream callers that probe internal greenlet APIs. They do not implement core
tealet switching semantics.

In upstream greenlet, the optional-cleanup APIs (added in the 2.x series) control
and measure a GC-assisted cleanup path used after thread exit to reclaim leaked
greenlet references. That cleanup can be expensive on large heaps, so greenlet
exposes a runtime toggle and a clock-tick counter. This shim keeps API shape and
observable behavior expected by tests, but does not implement the full cleanup
algorithm.
"""

import threading


_tls = threading.local()
# Real greenlet tracks whether optional GC-assisted cleanup is enabled by using
# a sentinel clock value. Here we keep a simple bool and expose the same
# high-level API contract.
_optional_cleanup_enabled = True

# Exposed by greenlet._greenlet and used by compat tests.
CLOCKS_PER_SEC = 1_000_000


def _thread_local_dict(create=False):
    d = getattr(_tls, "values", None)
    if d is None and create:
        d = {}
        _tls.values = d
    return d


def set_thread_local(key, value):
    # Mirror greenlet._greenlet debugging helper API.
    d = _thread_local_dict(create=True)
    d[key] = value


def get_thread_local(key, default=None):
    d = _thread_local_dict(create=False)
    if d is None:
        return default
    return d.get(key, default)


def del_thread_local(key):
    d = _thread_local_dict(create=False)
    if d is not None:
        d.pop(key, None)


def enable_optional_cleanup(enabled):
    # Compatibility toggle only. We currently do not run a separate optional
    # cleanup pass; this just controls metric visibility.
    global _optional_cleanup_enabled
    _optional_cleanup_enabled = bool(enabled)


def get_clocks_used_doing_optional_cleanup():
    # Upstream returns CPU clock ticks spent in optional cleanup and None when
    # cleanup is disabled. Keep that shape for compat tests.
    if not _optional_cleanup_enabled:
        return None
    return 0


def get_pending_cleanup_count():
    # Test-compat placeholder. Real greenlet returns size of pending
    # thread-state cleanup queue.
    return 0


def get_total_main_greenlets():
    # Test-compat placeholder. Real greenlet returns count of extant main
    # greenlets across thread states.
    return 1
