import contextvars
import gc
import threading
import weakref

import pytest

import _tealet

from ._tealet_test_helpers import join_thread_or_fail
class TestTealetContext:
    def test_new_tealet_context_is_none(self):
        t = _tealet.tealet()
        assert t.context is None
        t.context = None
        assert t.context is None

    def test_context_rejects_invalid_type(self):
        t = _tealet.tealet()
        with pytest.raises(TypeError):
            t.context = object()

    def test_direct_run_default_context_is_null(self):
        var = contextvars.ContextVar("ctx", default=None)
        var.set(7)

        def worker(current, _arg):
            assert current.context is None
            assert var.get() is None
            return current.main()

        t = _tealet.tealet()
        t.run(worker, None)

    def test_set_context_on_new_before_run(self):
        var = contextvars.ContextVar("ctx", default=None)
        ctx = contextvars.Context()
        ctx.run(var.set, 11)

        def worker(current, _arg):
            assert var.get() == 11
            return current.main()

        t = _tealet.tealet()
        t.context = ctx
        t.run(worker, None)

    def test_set_context_on_stub_before_run(self):
        var = contextvars.ContextVar("ctx", default=None)
        ctx = contextvars.Context()
        ctx.run(var.set, 17)

        def worker(current, _arg):
            assert var.get() == 17
            return current.main()

        t = _tealet.tealet()
        t.stub()
        t.context = ctx
        t.run(worker, None)

    def test_running_context_creation_and_clear(self):
        var = contextvars.ContextVar("ctx", default=None)

        def worker(current, _arg):
            assert current.context is None
            assert var.get() is None
            var.set(1)
            ctx = current.context
            assert ctx is not None
            assert ctx[var] == 1
            current.context = None
            assert current.context is None
            assert var.get() is None
            return current.main()

        _tealet.tealet().run(worker, None)

    def test_set_context_while_suspended(self):
        var = contextvars.ContextVar("ctx", default=None)
        suspended = []

        def worker(current, _arg):
            var.set(1)
            current.main().switch("paused")
            suspended.append(var.get())
            current.main().switch("paused")
            suspended.append(var.get())
            return current.main()

        t = _tealet.tealet()
        assert t.run(worker, None) == "paused"

        ctx = contextvars.Context()
        ctx.run(var.set, 2)
        t.context = ctx
        assert t.switch() == "paused"
        assert suspended[0] == 2

        t.context = None
        t.switch()
        assert suspended[1] is None

    def test_context_access_different_thread_fails(self):
        var = contextvars.ContextVar("ctx", default=None)
        holder = []
        started = threading.Event()
        ready = threading.Event()

        def worker(current, _arg):
            var.set(1)
            started.set()
            ready.wait(2.0)
            current.main().switch()
            return current.main()

        def thread_fn():
            t = _tealet.tealet()
            holder.append(t)
            t.run(worker, None)

        thread = threading.Thread(target=thread_fn, daemon=True)
        thread.start()
        assert started.wait(2.0), "worker thread did not signal started"
        t = holder[0]

        try:
            with pytest.raises(_tealet.InvalidError):
                _ = t.context
            with pytest.raises(_tealet.InvalidError):
                t.context = None
        finally:
            ready.set()
            join_thread_or_fail(thread, timeout=2.0)

    def test_context_cross_thread_running_forbidden_suspended_allowed(self):
        var = contextvars.ContextVar("ctx", default=None)
        holder = []
        observed = []
        running_ready = threading.Event()
        allow_suspend = threading.Event()
        suspended_ready = threading.Event()
        allow_resume = threading.Event()

        def worker(current, _arg):
            var.set(1)
            running_ready.set()
            assert allow_suspend.wait(2.0), "did not receive suspend signal"
            current.main().switch("paused")
            observed.append(var.get())
            return current.main()

        def thread_fn():
            t = _tealet.tealet()
            holder.append(t)
            first = t.run(worker, None)
            assert first == "paused"
            suspended_ready.set()
            assert allow_resume.wait(2.0), "did not receive resume signal"
            t.switch()

        thread = threading.Thread(target=thread_fn, daemon=True)
        thread.start()
        assert running_ready.wait(2.0), "worker thread did not enter running state"
        t = holder[0]

        # While running in a foreign thread, context access is forbidden.
        with pytest.raises(_tealet.InvalidError):
            _ = t.context
        with pytest.raises(_tealet.InvalidError):
            t.context = None

        # Once suspended in the foreign thread, context mutation is allowed.
        ctx = contextvars.Context()
        ctx.run(var.set, 2)
        allow_suspend.set()
        assert suspended_ready.wait(2.0), "worker thread did not suspend"
        t.context = ctx
        assert t.context is ctx

        allow_resume.set()
        join_thread_or_fail(thread, timeout=2.0)
        assert observed == [2]

    def test_context_weakref_cleanup(self):
        ctx = contextvars.Context()
        try:
            ref = weakref.ref(ctx)
        except TypeError:
            pytest.skip("contextvars.Context does not support weakref")

        t = _tealet.tealet()
        t.context = ctx
        del t
        del ctx
        gc.collect()

        assert ref() is None

    def test_cleanup_idempotent_on_empty_lineage(self):
        """Calling cleanup multiple times on empty lineage is idempotent."""
        _tealet.main()
        nerfed1 = _tealet.thread_reap()
        assert nerfed1 == []

        # Second cleanup should be idempotent (no error, no main)
        nerfed2 = _tealet.thread_reap()
        assert nerfed2 == []

        # Recreate main for subsequent tests
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_thread_sweep_reports_nerfed_from_dead_thread_lineage(self):
        holder = []
        ready = threading.Event()

        def parked(current, arg):
            current.main().switch("paused")
            return current.main()

        def worker():
            t = _tealet.tealet()
            holder.append(t)
            assert t.run(parked, None) == "paused"
            ready.set()

        th = threading.Thread(target=worker)
        th.start()
        assert ready.wait(timeout=1.0)
        join_thread_or_fail(th)

        t = holder[0]
        nerfed = _tealet.thread_sweep()
        assert any(id(x) == id(t) for x in nerfed)
        assert t.state == _tealet.STATE_EXIT

        # Re-running sweep should be idempotent once stale lineages are gone.
        assert _tealet.thread_sweep() == []

    def test_thread_sweep_skips_live_thread_lineage(self):
        holder = []
        ready = threading.Event()
        release = threading.Event()

        def parked(current, arg):
            current.main().switch("paused")
            return current.main()

        def worker():
            t = _tealet.tealet()
            holder.append(t)
            assert t.run(parked, None) == "paused"
            ready.set()
            release.wait(timeout=1.0)

        th = threading.Thread(target=worker)
        th.start()
        assert ready.wait(timeout=1.0)

        t = holder[0]

        try:
            nerfed_live = _tealet.thread_sweep()
            assert all(id(x) != id(t) for x in nerfed_live)
            assert t.state == _tealet.STATE_RUN
        finally:
            release.set()
            join_thread_or_fail(th)

        nerfed_dead = _tealet.thread_sweep()
        assert any(id(x) == id(t) for x in nerfed_dead)
        assert t.state == _tealet.STATE_EXIT


