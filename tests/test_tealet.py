import pytest
import math
import sys
import traceback
import types
import threading
import queue
import weakref
import gc

import _tealet
import contextvars
import random
random.seed(0)


def join_thread_or_fail(th, timeout=1.0):
    th.join(timeout=timeout)
    assert not th.is_alive(), "worker thread did not terminate in time"

# Utility stuff for creating tealets
def tealet_new_descend(descend, func=None, arg=None, klass=_tealet.tealet, retarg=False):
    while descend > 0:
        return tealet_new_descend(descend-1, func, arg, klass=klass, retarg=retarg)
    t = klass()
    if func:
        r = t.run(func, arg)
    else:
        t.stub()
        r = None
    return (t, r) if retarg else t

def tealet_new_rnd(func=None, arg=None, klass=_tealet.tealet, retarg=False):
    return tealet_new_descend(random.randint(0, 20), func, arg, klass, retarg)

def stub_new(func=None, arg=None, klass=_tealet.tealet, retarg=False):
    stub = tealet_new_descend(random.randint(0, 20), klass=klass, retarg=False)
    if func:
        r = stub.run(func, arg)
    else:
        r = None
    return (stub, r) if retarg else stub

def stub_new2(func=None, arg=None, klass=_tealet.tealet, retarg=False):
    stub = tealet_new_descend(random.randint(0, 20), klass=klass, retarg=False)
    dup = stub.duplicate()
    if func:
        r = dup.run(func, arg)
    else:
        r = None
    return (dup, r) if retarg else dup

the_stub=[None]
def stub_new3(func=None, arg=None, klass=_tealet.tealet, retarg=False):
    if (random.randint(0, 10) == 0):
        the_stub[0] = None
    if not the_stub[0]:
        the_stub[0] = tealet_new_descend(random.randint(0, 20), klass=klass)
    dup = the_stub[0].duplicate()
    if func:
        r = dup.run(func, arg)
    else:
        r = None
    return (dup, r) if retarg else dup

newmode = 0
newarray = [tealet_new_rnd, stub_new, stub_new2, stub_new3]
def get_new():
    if newmode >= 0:
        return newarray[newmode]
    return newarray(random.randint(0, len(newarray)-1))


class TestModule:
    def test_main(self):
        assert _tealet.main() == _tealet.current()

    def test_main2(self):
        assert _tealet.main() == _tealet.current().main()

    def test_main3(self):
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_frame_introspection_toggle(self):
        compiled = bool(getattr(_tealet, "PYTEALET_WITH_PENDING_FRAME_INTROSPECTION", 1))
        original = _tealet.frame_introspection()

        assert isinstance(original, bool)

        try:
            assert _tealet.frame_introspection(False) is False
            if compiled:
                assert _tealet.frame_introspection(True) is True
            else:
                with pytest.raises(RuntimeError):
                    _tealet.frame_introspection(True)
        finally:
            _tealet.frame_introspection(original)

    def test_hide_frame_hides_callers(self):
        def inner():
            return [frame.name for frame in traceback.extract_stack()]

        def wrapper_normal():
            return inner()

        def wrapper_hide():
            return _tealet.hide_frame(inner)

        normal_names = wrapper_normal()
        hidden_names = wrapper_hide()

        assert "wrapper_normal" in normal_names
        assert "wrapper_hide" not in hidden_names

    def test_hide_frame_accepts_none_kwargs(self):
        def inner(*args, **kwargs):
            return args, kwargs

        result = _tealet.hide_frame(inner, (1, 2), None)
        assert result == ((1, 2), {})

    def test_hide_frame_rejects_non_dict_kwargs(self):
        with pytest.raises(TypeError, match="kwargs must be a dict or None"):
            _tealet.hide_frame(lambda: None, (), 42)


class TestThreadCleanup:
    """Tests for thread cleanup semantics and edge cases."""

    @staticmethod
    def _count_dead_weakrefs():
        gc.collect()
        gc.collect()
        return sum(1 for o in gc.get_objects() if isinstance(o, weakref.ReferenceType) and o() is None)

    def test_run_tealets_do_not_accumulate_dead_weakrefs(self):
        """Regression: exited non-main tealets must not strand weakrefs in lineage tracking."""

        def worker(current, _arg):
            return current.main()

        def run_batch(rounds=5, batch_size=4):
            for _ in range(rounds):
                tealets = [_tealet.tealet() for _ in range(batch_size)]
                for t in tealets:
                    t.run(worker, None)
                del tealets

        before = self._count_dead_weakrefs()
        run_batch()
        after_first = self._count_dead_weakrefs()
        run_batch()
        after_second = self._count_dead_weakrefs()

        growth_first = after_first - before
        growth_second = after_second - after_first

        # Prior leak behavior grew by ~4 per round (20 per batch).
        # Keep a small tolerance for unrelated interpreter noise.
        assert growth_second <= 4

    def test_thread_kill_then_thread_cleanup_is_empty(self):
        def parked(current, arg):
            current.main().switch("paused")
            return current.main()

        t1 = _tealet.tealet()
        t2 = _tealet.tealet()
        assert t1.run(parked, None) == "paused"
        assert t2.run(parked, None) == "paused"

        active = _tealet.thread_active()
        active_ids = {id(x) for x in active}
        assert id(t1) in active_ids
        assert id(t2) in active_ids

        remaining = _tealet.thread_kill()
        assert remaining == []
        assert t1.state == _tealet.STATE_EXIT
        assert t2.state == _tealet.STATE_EXIT

        assert _tealet.thread_reap() == []
        # Recreate main for subsequent tests.
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_thread_kill_cleanup_passes(self):
        catches = [0]

        def stubborn(current, arg):
            while True:
                try:
                    current.main().switch("paused")
                except _tealet.TealetExit:
                    catches[0] += 1
                    if catches[0] >= 2:
                        raise

        t = _tealet.tealet()
        assert t.run(stubborn, None) == "paused"

        remaining = _tealet.thread_kill(1)
        assert any(id(x) == id(t) for x in remaining)
        assert t.state == _tealet.STATE_RUN

        remaining2 = _tealet.thread_kill(3)
        assert all(id(x) != id(t) for x in remaining2)
        assert t.state == _tealet.STATE_EXIT

        assert _tealet.thread_reap() == []
        # Recreate main for subsequent tests.
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_kill_active_tealets_then_cleanup_is_empty(self):
        def parked(current, arg):
            current.main().switch("paused")
            return current.main()

        t1 = _tealet.tealet()
        t2 = _tealet.tealet()
        assert t1.run(parked, None) == "paused"
        assert t2.run(parked, None) == "paused"

        active = _tealet.thread_active()
        active_ids = {id(x) for x in active}
        assert id(t1) in active_ids
        assert id(t2) in active_ids

        for t in active:
            assert t.throw(_tealet.TealetExit()) is None

        assert t1.state == _tealet.STATE_EXIT
        assert t2.state == _tealet.STATE_EXIT

        assert _tealet.thread_reap() == []
        # Recreate main for subsequent tests.
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_active_tealets_returns_only_run_tealets(self):
        thread_main = _tealet.main()
        stub = _tealet.tealet()
        stub.stub()

        def switch_back(current, arg):
            thread_main.switch()

        t = _tealet.tealet()
        t.run(switch_back, None)

        active = _tealet.thread_active()
        active_ids = {id(x) for x in active}

        assert id(thread_main) not in active_ids
        assert id(stub) not in active_ids
        assert id(t) in active_ids

        # Cleanup current lineage so following tests start with a fresh main.
        _tealet.thread_reap()
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_active_tealets_allowed_from_non_main_tealet_context(self):
        def run(current, arg):
            active = _tealet.thread_active()
            assert any(id(x) == id(current) for x in active)
            return current.main()

        _tealet.tealet().run(run, None)

    def test_thread_kill_allowed_from_non_main_skips_caller(self):
        def parked(current, arg):
            current.main().switch("peer-paused")
            return current.main()

        def run(current, arg):
            peer = _tealet.tealet()
            assert peer.run(parked, None) == "peer-paused"

            remaining = _tealet.thread_kill()
            remaining_ids = {id(x) for x in remaining}

            assert id(current) not in remaining_ids
            dup = data["stub"].duplicate()
            assert current.state == _tealet.STATE_RUN
            assert peer.state == _tealet.STATE_EXIT
            return current.main()

        caller = _tealet.tealet()
        caller.run(run, None)

    def test_thread_kill_accepts_callable_exception_factory(self):
        def parked(current, arg):
            current.main().switch("paused")
            return current.main()

        t = _tealet.tealet()
        assert t.run(parked, None) == "paused"

        remaining = _tealet.thread_kill(1, _tealet.TealetExit)
        assert all(id(x) != id(t) for x in remaining)
        assert t.state == _tealet.STATE_EXIT

        assert _tealet.thread_reap() == []
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_thread_kill_rejects_exception_instance_spec(self):
        with pytest.raises(TypeError, match="callable or None"):
            _tealet.thread_kill(1, _tealet.TealetExit())

    def test_thread_kill_rejects_non_exception_factory_result(self):
        def parked(current, arg):
            current.main().switch("paused")
            return current.main()

        t = _tealet.tealet()
        assert t.run(parked, None) == "paused"

        with pytest.raises(TypeError, match="must return an exception instance"):
            _tealet.thread_kill(1, lambda: 42)

        # Cleanup lineages to avoid cross-test contamination.
        _tealet.thread_kill(3)
        assert _tealet.thread_reap() == []
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_thread_cleanup_accepts_callable_exception_factory(self):
        def parked(current, arg):
            current.main().switch("paused")
            return current.main()

        t = _tealet.tealet()
        assert t.run(parked, None) == "paused"

        assert _tealet.thread_reap(1, _tealet.TealetExit) == []
        assert t.state == _tealet.STATE_EXIT
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_thread_cleanup_kills_run_tealets_before_force_cleanup(self):
        """Cleanup first kills active RUN tealets, so nerfed excludes them."""

        thread_main = _tealet.main()
        stub = _tealet.tealet()
        stub.stub()
        
        # Create a tealet that switches back to main and stays suspended in RUN.
        def switch_back(current, arg):
            # Switch back to main and return
            thread_main.switch()
        
        t = _tealet.tealet()
        t.run(switch_back, None)  # t runs and switches back, stays suspended
        
        nerfed = _tealet.thread_reap()
        nerfed_ids = {id(x) for x in nerfed}

        # thread_reap() now performs thread_kill() first, so suspended RUN
        # tealets are exited before forced handle teardown and are not in nerfed.
        # STUB tealets are not returned (can be safely collected).
        # Main is cleanly deleted, not forcibly invalidated.
        assert id(thread_main) not in nerfed_ids
        assert id(stub) not in nerfed_ids  # STUB not in nerfed
        assert id(t) not in nerfed_ids
        assert t.state == _tealet.STATE_EXIT

        # Recreate main for this thread so subsequent tests keep the usual baseline.
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_thread_cleanup_requires_main_tealet_context(self):
        def run(current, arg):
            with pytest.raises(_tealet.StateError):
                _tealet.thread_reap()
            return current.main()

        _tealet.tealet().run(run, None)

    def test_cleanup_suspended_tealet_cannot_switch(self):
        """A suspended RUN tealet is killed by cleanup and cannot be switched."""
        def parked(current, arg):
            current.main().switch("paused")
            return current.main()

        t = _tealet.tealet()
        assert t.run(parked, None) == "paused"

        nerfed = _tealet.thread_reap()
        _tealet.tealet() # create a new lineage for this thread
        assert all(id(x) != id(t) for x in nerfed)
        assert t.state == _tealet.STATE_EXIT

        with pytest.raises(_tealet.StateError):
            t.switch()

    def test_cleanup_empty_lineage(self):
        """Cleanup with only main tealet (no non-main tealets) returns empty list."""
        _tealet.main()  # ensure main exists
        nerfed = _tealet.thread_reap()
        assert nerfed == []
        # Recreate main for subsequent tests
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_cleanup_stub_tealets_not_returned(self):
        """STUB tealets are not returned in nerfed (can be safely collected)."""
        _tealet.main()
        stub1 = _tealet.tealet()
        stub1.stub()
        stub2 = _tealet.tealet()
        stub2.stub()
        
        nerfed = _tealet.thread_reap()
        nerfed_ids = {id(x) for x in nerfed}
        
        # STUB tealets are not in nerfed (they can be safely garbage collected)
        assert id(stub1) not in nerfed_ids
        assert id(stub2) not in nerfed_ids
        assert nerfed == []
        
        # Recreate main for subsequent tests
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_cleanup_fresh_start_after_cleanup(self):
        """After cleanup, can create new main and tealets in same thread."""
        # First lineage - only STUB, so nerfed will be empty
        _tealet.main()
        stub1 = _tealet.tealet()
        stub1.stub()
        nerfed = _tealet.thread_reap()
        assert nerfed == []

        # New lineage in same thread
        main2 = _tealet.main()
        assert main2.state == _tealet.STATE_RUN
        stub2 = _tealet.tealet()
        stub2.stub()
        assert stub2.state == _tealet.STATE_STUB
        nerfed2 = _tealet.thread_reap()
        assert nerfed2 == []  # STUB not in nerfed
        
        # Recreate main for subsequent tests
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_cleanup_with_finished_tealets(self):
        """Finished tealets (not ACTIVE) are not included in nerfed."""
        _tealet.main()
        
        def finish_work(current, arg):
            return current.main()
        
        stub = _tealet.tealet()
        stub.stub()
        stub.run(finish_work, None)  # Run to completion, becomes inactive
        
        nerfed = _tealet.thread_reap()
        # Finished tealet should not be in nerfed (not ACTIVE status)
        assert isinstance(nerfed, list)
        assert nerfed == []
        
        # Recreate main for subsequent tests
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_cleanup_with_dead_weakrefs(self):
        """Cleanup gracefully handles dead weakrefs (GC'd wrappers)."""
        _tealet.main()
        stub = _tealet.tealet()
        stub.stub()
        
        # Delete local reference to stub to allow GC
        del stub
        gc.collect()
        
        # Cleanup should handle dead weakrefs without crashing
        nerfed = _tealet.thread_reap()
        assert isinstance(nerfed, list)
        # The stub was GC'd and wasn't RUN anyway, so won't be in nerfed
        
        # Recreate main for subsequent tests
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_cleanup_main_never_in_nerfed(self):
        """Main tealet is never in nerfed, only non-main RUN tealets."""
        main = _tealet.main()
        stub = _tealet.tealet()
        stub.stub()

        nerfed = _tealet.thread_reap()
        nerfed_ids = {id(x) for x in nerfed}

        assert id(main) not in nerfed_ids
        assert id(stub) not in nerfed_ids

        # Recreate main for subsequent tests
        assert _tealet.main().state == _tealet.STATE_RUN


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


class TestThreadOwnership:
    def test_new_tealet_has_owner_tid_and_belongs(self):
        t = _tealet.tealet()
        assert t.thread_id == _tealet.current().thread_id
        assert t.belongs_to_current() is True

    def test_stub_rejected_from_foreign_thread(self):
        data = {}
        created = threading.Event()
        done = threading.Event()

        def worker():
            data["t"] = _tealet.tealet()
            created.set()
            done.wait(timeout=1.0)

        th = threading.Thread(target=worker)
        th.start()
        assert created.wait(timeout=1.0)
        try:
            with pytest.raises(_tealet.InvalidError):
                data["t"].stub()
        finally:
            done.set()
            join_thread_or_fail(th)

    def test_run_rejected_from_foreign_thread(self):
        data = {}
        created = threading.Event()
        done = threading.Event()

        def worker():
            data["t"] = _tealet.tealet()
            created.set()
            done.wait(timeout=1.0)

        th = threading.Thread(target=worker)
        th.start()
        assert created.wait(timeout=1.0)
        try:
            with pytest.raises(_tealet.ThreadMismatchError) as exc:
                data["t"].run(lambda current, arg: current.main(), None)
            assert "thread mismatch: run() not allowed from a different thread" in str(exc.value)
            assert exc.value.operation == "run()"
            assert exc.value.current_tid == _tealet.current().thread_id
            assert exc.value.target_tid == data["t"].thread_id
            assert isinstance(exc.value.target_alive, bool)
        finally:
            done.set()
            join_thread_or_fail(th)

    def test_switch_rejected_from_foreign_thread(self):
        data = {}
        ready = threading.Event()
        release = threading.Event()

        def parked(current, arg):
            current.main().switch("paused")
            return current.main()

        def worker():
            t = _tealet.tealet()
            t.stub()
            data["t"] = t
            data["first"] = t.run(parked, None)
            ready.set()
            if release.wait(timeout=1.0):
                t.switch()

        th = threading.Thread(target=worker)
        th.start()
        assert ready.wait(timeout=1.0)
        try:
            assert data["first"] == "paused"
            with pytest.raises(_tealet.ThreadMismatchError):
                data["t"].switch()
        finally:
            release.set()
            join_thread_or_fail(th)

    def test_duplicate_stub_allowed_from_foreign_thread(self):
        data = {}
        ready = threading.Event()
        release = threading.Event()

        def worker():
            t = _tealet.tealet()
            t.stub()
            data["owner_tid"] = t.thread_id
            data["stub"] = t
            ready.set()
            release.wait(timeout=1.0)

        th = threading.Thread(target=worker)
        th.start()
        assert ready.wait(timeout=1.0)
        try:
            assert data["owner_tid"] != _tealet.current().thread_id
            dup = data["stub"].duplicate()
            assert dup.thread_id == data["owner_tid"]
            assert dup.belongs_to_current() is False
            with pytest.raises(_tealet.ThreadMismatchError):
                dup.run(lambda current, arg: current.main(), None)
        finally:
            release.set()
            join_thread_or_fail(th)

    def test_dealloc_allowed_from_foreign_thread(self):
        q = queue.Queue()

        def worker():
            t = _tealet.tealet()
            t.stub()
            q.put((t.thread_id, t))
            t = None

        th = threading.Thread(target=worker)
        th.start()
        owner_tid, foreign_tealet = q.get(timeout=1.0)
        join_thread_or_fail(th)

        assert owner_tid != _tealet.current().thread_id
        ref = weakref.ref(foreign_tealet)
        del foreign_tealet
        gc.collect()
        assert ref() is None

    def test_traversal_rejected_from_foreign_thread(self):
        data = {}
        ready = threading.Event()
        release = threading.Event()

        def parked(current, arg):
            current.main().switch("paused")
            release.wait(timeout=1.0)
            current.main().switch("done")
            return current.main()

        def worker():
            t = _tealet.tealet()
            t.stub()
            data["t"] = t
            data["first"] = t.run(parked, None)
            ready.set()
            if release.wait(timeout=1.0):
                t.switch()

        th = threading.Thread(target=worker)
        th.start()
        assert ready.wait(timeout=1.0)
        try:
            assert data["first"] == "paused"
            with pytest.raises(_tealet.InvalidError):
                data["t"].current()
            with pytest.raises(_tealet.InvalidError):
                data["t"].main()
            with pytest.raises(_tealet.InvalidError):
                data["t"].previous()
        finally:
            release.set()
            join_thread_or_fail(th)


class TestTealetTraversalMethods:
    def test_methods_fail_on_new_tealet(self):
        t = _tealet.tealet()
        with pytest.raises(_tealet.StateError):
            t.current()
        with pytest.raises(_tealet.StateError):
            t.main()
        with pytest.raises(_tealet.StateError):
            t.previous()

    def test_current_main_previous_inside_running_tealet(self):
        seen = {}

        def run(current, arg):
            seen["self_is_current"] = (current.current() == current)
            seen["main"] = current.main()
            seen["previous"] = current.previous()
            return _tealet.main()

        _tealet.tealet().run(run, None)

        assert seen["self_is_current"] is True
        assert seen["main"] == _tealet.main()
        assert seen["previous"] == _tealet.main()

    @pytest.mark.skip(reason="Deferred-delete post-exit behavior is experimental; re-enable when PYTEALET_DEFER_DELETE is being exercised")
    def test_main_on_exited_tealet_depends_on_defer_delete_flag(self):
        def run_and_exit(current, arg):
            return _tealet.main()

        t = _tealet.tealet()
        t.run(run_and_exit, None)
        assert t.state == _tealet.STATE_EXIT

        if getattr(_tealet, "PYTEALET_DEFER_DELETE", 0):
            assert t.main() == _tealet.main()
        else:
            with pytest.raises(_tealet.StateError):
                t.main()

class TestSimple:
    def test_simple(self):
        status = [0]
        def run(current, arg):
            status[0] = 1
            return arg
        get_new()(run, _tealet.current())
        assert status[0] == 1

    def test_return_none_is_invalid_exit_target(self):
        def run(current, arg):
            return None

        seen = []
        original_hook = sys.unraisablehook

        def capture_unraisable(unraisable):
            seen.append(unraisable)

        sys.unraisablehook = capture_unraisable
        try:
            t = _tealet.tealet()
            assert t.run(run, None) is None
            assert t.state == _tealet.STATE_EXIT
        finally:
            sys.unraisablehook = original_hook

        assert seen, "expected unraisable error for None return target"
        assert any(
            isinstance(u.exc_value, TypeError) and "tealet object expected" in str(u.exc_value)
            for u in seen
        )


class TestPrepare:
    def test_prepare_returns_self_for_chaining(self):
        seen = []

        def worker(current, arg):
            seen.append(arg)
            return current.main(), "done-chain"

        t = _tealet.tealet().prepare(worker)
        assert isinstance(t, _tealet.tealet)
        assert t.switch("payload") == "done-chain"
        assert seen == ["payload"]
        assert t.state == _tealet.STATE_EXIT

    def test_prepare_new_first_switch_runs_callable(self):
        seen = []

        def worker(current, arg):
            seen.append(arg)
            return current.main(), "done-new"

        t = _tealet.tealet()
        assert t.state == _tealet.STATE_NEW

        t.prepare(worker)
        assert t.switch("payload") == "done-new"
        assert seen == ["payload"]
        assert t.state == _tealet.STATE_EXIT

    def test_prepare_stub_first_switch_runs_callable(self):
        seen = []

        def worker(current, arg):
            seen.append(arg)
            return current.main(), "done-stub"

        t = _tealet.tealet()
        t.stub()
        assert t.state == _tealet.STATE_STUB

        t.prepare(worker)
        assert t.switch(123) == "done-stub"
        assert seen == [123]
        assert t.state == _tealet.STATE_EXIT

    def test_prepare_requires_callable(self):
        t = _tealet.tealet()
        with pytest.raises(TypeError, match="must be callable"):
            t.prepare(42)

    def test_prepare_latest_callable_wins(self):
        seen = []

        def first(current, arg):
            seen.append(("first", arg))
            return current.main(), "first"

        def second(current, arg):
            seen.append(("second", arg))
            return current.main(), "second"

        t = _tealet.tealet()
        t.prepare(first)
        t.prepare(second)

        assert t.switch("x") == "second"
        assert seen == [("second", "x")]
        assert t.state == _tealet.STATE_EXIT

    def test_explicit_run_overrides_prepared_callable(self):
        seen = []

        def prepared(current, arg):
            seen.append(("prepared", arg))
            return current.main(), "prepared"

        def direct(current, arg):
            seen.append(("direct", arg))
            return current.main(), "direct"

        t = _tealet.tealet()
        t.prepare(prepared)

        assert t.run(direct, "r") == "direct"
        assert seen == [("direct", "r")]
        assert t.state == _tealet.STATE_EXIT

    def test_prepare_cycle_is_collectable_by_gc(self):
        t = _tealet.tealet()

        def worker(current, arg, _t=t):
            return current.main(), arg

        t.prepare(worker)
        ref = weakref.ref(t)

        del worker
        del t

        gc.collect()
        gc.collect()

        assert ref() is None

class TestStatus:
    def test_status_run(self):
        t = _tealet.current()
        assert t.main() == _tealet.main()
        assert t.state == _tealet.STATE_RUN

    @pytest.mark.stub
    def test_status_stub(self):
        stub = get_new()()
        status = [None]
        assert stub.state == _tealet.STATE_STUB
        def run(current, arg):
            status[0] = current.state
            return arg
        stub.run(run, _tealet.current())
        assert status[0] == _tealet.STATE_RUN

class TestSubclass:
    class sc(_tealet.tealet):
        dude = [0]
        def __repr__(self):
            return "<myrepr %r>"%super(TestSubclass.sc, self).__repr__()
        def __del__(self):
            self.dude[0] = 1

    class scinit(_tealet.tealet):
        def __init__(self, payload, *, label=None):
            self.payload = payload
            self.label = label

    def test_subclass(self):
        def foo(current, arg):
            arg.switch(current)
            return arg
        t = get_new()(foo, _tealet.current(), klass=self.sc)
        assert repr(t)[:7] == "<myrepr"
        assert self.sc.dude[0] == 0
        t.switch()
        assert self.sc.dude[0] == 0
        del t
        assert self.sc.dude[0] == 1

    def test_subclass_init_can_accept_custom_args(self):
        payload = {"a": 1}
        t = self.scinit(payload, label="demo")

        assert isinstance(t, _tealet.tealet)
        assert t.payload is payload
        assert t.label == "demo"
        assert t.state == _tealet.STATE_NEW

    def test_exact_tealet_constructor_stays_no_args(self):
        with pytest.raises(TypeError, match=r"tealet\(\) takes no arguments"):
            _tealet.tealet(123)

class TestSwitch:
    def test_switch_panic_keyword(self):
        assert _tealet.current().switch(panic=False) is None
        with pytest.raises(_tealet.PanicError) as exc:
            _tealet.current().switch("panic-value", panic=True)
        assert exc.value.result() == "panic-value"
        assert exc.value.exception() is None

        with pytest.raises(_tealet.PanicError) as exc2:
            _tealet.current().switch(panic=True)
        assert exc2.value.result() is None
        assert exc2.value.exception() is None

    def test_switch_panic_payload_identity_from_tealet(self):
        payload = {"kind": "panic", "n": 7}

        def worker(current, main):
            main.switch(payload, panic=True)
            return _tealet.main()

        with pytest.raises(_tealet.PanicError) as exc:
            _tealet.tealet().run(worker, _tealet.main())
        assert exc.value.result() is payload
        assert exc.value.exception() is None

    def test_switch_panic_carries_pending_throw_exception(self):
        pending = RuntimeError("boom-pending")
        t = _tealet.tealet()
        t.set_exception(pending)

        with pytest.raises(_tealet.PanicError) as exc:
            _tealet.current().switch("panic", panic=True)

        assert exc.value.exception() is pending
        with pytest.raises(RuntimeError, match="boom-pending"):
            exc.value.result()

    def test_switch(self):
        status = [0]
        t = [None, None]
        def t2(current, arg):
            assert current != _tealet.main()
            assert current != t[0]
            t[1] = current
            assert status[0] == 1
            status[0] = 2
            assert _tealet.current() == current
            t[0].switch()
            assert status[0] == 3
            status[0] = 4
            assert _tealet.current() == current
            t[0].switch()
            assert status[0] == 5
            status[0] = 6
            assert current == t[1]
            assert _tealet.current() == current
            t[1].switch() #noop
            assert status[0] == 6
            status[0] = 7
            assert _tealet.current() == current
            return _tealet.main()

        def t1(current, arg):
            assert current != _tealet.main()
            t[0] = current
            assert status[0] == 0
            status[0] = 1
            assert current == _tealet.current()
            get_new()(t2)
            assert status[0] == 2
            status[0] = 3
            assert current == _tealet.current()
            t[1].switch()
            assert status[0] == 4
            status[0] = 5
            assert current == _tealet.current()
            return t[1]

        get_new()(t1)
        assert status[0] == 7


    @pytest.mark.stub
    def test_switch_new(self):
        # 1 is high on the stack.  We then create 2 lower on the stack
        # the execution is : m 1 m 2 1 m 2 m */
        def new1(current, arg):
            # switch back to the creator
            arg.switch()
            # now we want to trample the stack
            stub = tealet_new_descend(50)
            del stub
            # back to main
            return _tealet.main()

        def new2(current, arg):
            # switch to tealet 1 to trample the stack
            arg.switch();
            # back to main
            return _tealet.main()

        tealet1 = get_new()(new1, _tealet.current())
        # the tealet is now running
        tealet2 = tealet_new_descend(4, new2, tealet1)

        assert tealet2.state == _tealet.STATE_RUN;
        tealet2.switch()

    @pytest.mark.stub
    def test_switch_arg(self):
        # 1 is high on the stack.  We then create 2 lower on the stack
        # the execution is : m 1 m 2 1 m 2 m */
        def new1(current, arg):
            # switch back to the creator
            r = arg.switch(2)
            assert r == 4
            # now we want to trample the stack
            stub = tealet_new_descend(50)
            del stub
            # back to main
            return _tealet.main(), 5

        def new2(current, arg):
            # switch to tealet 1 to trample the stack
            r = arg.switch(4);
            assert r == 6
            # back to main
            return _tealet.main(), 7

        tealet1, r = get_new()(new1, _tealet.current(), retarg=True)
        assert r == 2
        # the tealet is now running
        tealet2, r = tealet_new_descend(4, new2, tealet1, retarg=True)
        assert r == 5

        assert tealet2.state == _tealet.STATE_RUN;
        r = tealet2.switch(6)
        assert r == 7


class TestSetException:
    def test_throw_to_self_raises_remote_and_clears_on_next_switch(self):
        current = _tealet.current()

        assert _tealet.error_was_remote() is False
        with pytest.raises(RuntimeError, match="boom-self-throw"):
            current.throw(RuntimeError("boom-self-throw"))
        assert _tealet.error_was_remote() is True

        # Any subsequent switching API call clears the remote-error marker.
        assert current.switch() is None
        assert _tealet.error_was_remote() is False

    def test_throw_switches_and_uses_current_as_fallback(self):
        def victim(current, _arg):
            current.main().switch("victim-paused")
            return current.main()

        result = []
        seen = []
        original_hook = sys.unraisablehook

        def killer(current, target):
            result.append(target.throw(RuntimeError("boom-throw")))
            return current.main()

        def capture_unraisable(unraisable):
            seen.append(unraisable)

        sys.unraisablehook = capture_unraisable
        try:
            target = _tealet.tealet()
            assert target.run(victim, None) == "victim-paused"

            killer_t = _tealet.tealet()
            assert killer_t.run(killer, target) is None

            assert killer_t.state == _tealet.STATE_EXIT
            assert target.state == _tealet.STATE_EXIT
        finally:
            sys.unraisablehook = original_hook

        assert result == [None]
        assert seen, "expected unraisable error for uncaught injected exception"
        assert any(
            isinstance(u.exc_value, RuntimeError) and str(u.exc_value) == "boom-throw"
            for u in seen
        )

    def test_throw_on_new_target_injects_at_run_entry(self):
        seen = []
        original_hook = sys.unraisablehook

        def capture_unraisable(unraisable):
            seen.append(unraisable)

        sys.unraisablehook = capture_unraisable
        try:
            t = _tealet.tealet()
            assert t.throw(RuntimeError("boom-throw-run")) is None
            assert t.state == _tealet.STATE_EXIT
        finally:
            sys.unraisablehook = original_hook

        assert seen, "expected unraisable error for uncaught thrown exception"
        assert any(
            isinstance(u.exc_value, RuntimeError) and str(u.exc_value) == "boom-throw-run"
            for u in seen
        )

    def test_throw_on_prepared_target_behaves_like_set_exception_plus_run(self):
        seen = []
        called = []
        original_hook = sys.unraisablehook

        def worker(current, arg):
            called.append(arg)
            return current.main()

        def capture_unraisable(unraisable):
            seen.append(unraisable)

        sys.unraisablehook = capture_unraisable
        try:
            for make_stub in (False, True):
                t = _tealet.tealet()
                if make_stub:
                    t.stub()
                t.prepare(worker)
                assert t.throw(RuntimeError("boom-prepared-throw")) is None
                assert t.state == _tealet.STATE_EXIT
        finally:
            sys.unraisablehook = original_hook

        # Delivery happens at run entry, so worker is not entered.
        assert called == []
        assert seen, "expected unraisable error for uncaught thrown exception"
        assert any(
            isinstance(u.exc_value, RuntimeError) and str(u.exc_value) == "boom-prepared-throw"
            for u in seen
        )

    def test_run_allows_dummy_args_when_pending_exception_exists(self):
        seen = []
        original_hook = sys.unraisablehook

        def capture_unraisable(unraisable):
            seen.append(unraisable)

        sys.unraisablehook = capture_unraisable
        try:
            t = _tealet.tealet()
            t.set_exception(RuntimeError("boom-dummy-run"))
            assert t.run("dummy-func", {"dummy": True}) is None
            assert t.state == _tealet.STATE_EXIT
        finally:
            sys.unraisablehook = original_hook

        assert seen, "expected unraisable error for uncaught injected exception"
        assert any(
            isinstance(u.exc_value, RuntimeError) and str(u.exc_value) == "boom-dummy-run"
            for u in seen
        )

    def test_set_exception_before_run_injects_at_run_entry(self):
        entered = []
        seen = []
        original_hook = sys.unraisablehook

        def worker(current, _arg):
            entered.append(True)
            current.main().switch("paused")
            return current.main()

        def capture_unraisable(unraisable):
            seen.append(unraisable)

        sys.unraisablehook = capture_unraisable
        try:
            t = _tealet.tealet()
            t.set_exception(RuntimeError("boom-before-run"))
            assert t.run(worker, None) is None
            assert t.state == _tealet.STATE_EXIT
        finally:
            sys.unraisablehook = original_hook

        assert entered == []
        assert seen, "expected unraisable error for uncaught injected exception"
        assert any(
            isinstance(u.exc_value, RuntimeError) and str(u.exc_value) == "boom-before-run"
            for u in seen
        )

    def test_set_exception_delivers_on_next_switch(self):
        seen = []

        def worker(current, _arg):
            try:
                current.main().switch("paused")
            except RuntimeError as exc:
                seen.append(str(exc))
            current.main().switch("done")
            return current.main()

        t = _tealet.tealet()
        assert t.run(worker, None) == "paused"

        t.set_exception(RuntimeError("boom"))
        assert t.switch() == "done"
        assert t.switch() is None
        assert t.state == _tealet.STATE_EXIT
        assert seen == ["boom"]

    def test_error_was_remote_true_for_delivered_pending_exception(self):
        t = _tealet.tealet()
        assert _tealet.error_was_remote() is False

        t.set_exception(RuntimeError("boom-remote-flag"))
        with pytest.raises(RuntimeError):
            _tealet.current().switch("resume")
        assert _tealet.error_was_remote() is True

        # Any new switching API call clears the flag on entry.
        assert _tealet.current().switch() is None
        assert _tealet.error_was_remote() is False

    def test_error_was_remote_false_for_panic_with_pending_exception(self):
        t = _tealet.tealet()
        pending = RuntimeError("boom-panic-origin")
        t.set_exception(pending)

        with pytest.raises(_tealet.PanicError) as exc:
            _tealet.current().switch("panic", panic=True)

        assert exc.value.exception() is pending
        assert _tealet.error_was_remote() is False

    def test_set_exception_with_fallback_redirects_uncaught_unwind(self):
        def worker(current, _arg):
            current.main().switch("paused")
            return current.main()

        seen = []
        original_hook = sys.unraisablehook

        def capture_unraisable(unraisable):
            seen.append(unraisable)

        sys.unraisablehook = capture_unraisable
        try:
            t = _tealet.tealet()
            assert t.run(worker, None) == "paused"

            t.set_exception(ValueError("route"), fallback=_tealet.main())
            assert t.switch() is None
            assert t.state == _tealet.STATE_EXIT
        finally:
            sys.unraisablehook = original_hook

        assert seen, "expected unraisable error for uncaught injected exception"
        assert any(
            isinstance(u.exc_value, ValueError) and str(u.exc_value) == "route"
            for u in seen
        )

    def test_set_exception_overwrites_inflight_token_after_catch(self):
        seen = []

        def worker(current, _arg):
            for idx in range(2):
                try:
                    current.main().switch(f"paused-{idx}")
                except RuntimeError as exc:
                    seen.append(str(exc))
            current.main().switch("done")
            return current.main()

        t = _tealet.tealet()
        assert t.run(worker, None) == "paused-0"

        t.set_exception(RuntimeError("boom-1"), fallback=_tealet.main())
        assert t.switch() == "paused-1"

        # First injected exception was caught inside worker; next call should
        # overwrite prior inflight metadata rather than erroring.
        t.set_exception(RuntimeError("boom-2"), fallback=_tealet.main())
        assert t.switch() == "done"
        assert t.switch() is None
        assert t.state == _tealet.STATE_EXIT
        assert seen == ["boom-1", "boom-2"]

    def test_top_level_tealet_exit_is_swallowed(self):
        def worker(current, _arg):
            current.main().switch("paused")
            raise _tealet.TealetExit()

        seen = []
        original_hook = sys.unraisablehook

        def capture_unraisable(unraisable):
            seen.append(unraisable)

        sys.unraisablehook = capture_unraisable
        try:
            t = _tealet.tealet()
            assert t.run(worker, None) == "paused"
            assert t.switch() is None
            assert t.state == _tealet.STATE_EXIT
        finally:
            sys.unraisablehook = original_hook

        assert seen == []

    @pytest.mark.parametrize("exc", [SystemExit("bye"), KeyboardInterrupt("stop")])
    def test_top_level_fatal_baseexceptions_are_reraised_after_switch(self, exc):
        def worker(current, _arg):
            current.main().switch("paused")
            raise exc

        t = _tealet.tealet()
        assert t.run(worker, None) == "paused"
        with pytest.raises(type(exc)) as raised:
            t.switch()
        assert str(raised.value) == str(exc)
        assert t.state == _tealet.STATE_EXIT


class TestFrameIntrospection:
    def test_dormant_frame_is_none_when_introspection_disabled(self):
        has_pending_frame_introspection = sys.version_info >= (3, 11)
        compiled = bool(getattr(_tealet, "PYTEALET_WITH_PENDING_FRAME_INTROSPECTION", 1))
        original_enabled = _tealet.frame_introspection()

        def suspend(current, arg):
            current.main().switch("paused")
            return current.main()

        try:
            if compiled:
                assert _tealet.frame_introspection(True) is True
            else:
                assert _tealet.frame_introspection(False) is False

            t = _tealet.tealet()
            assert t.run(suspend, None) == "paused"

            if compiled and has_pending_frame_introspection:
                assert t.frame is not None
                assert _tealet.frame_introspection(False) is False

            if has_pending_frame_introspection:
                assert t.frame is None
            t.switch()
            assert t.state == _tealet.STATE_EXIT
        finally:
            _tealet.frame_introspection(original_enabled)

    def test_suspended_frame_traceback_after_parent_stack_change(self):
        original_enabled = _tealet.frame_introspection()
        if not original_enabled:
            try:
                _tealet.frame_introspection(True)
            except RuntimeError:
                pytest.skip("pending frame introspection is compile-time disabled")

        def recurse(depth, fn):
            if depth == 0:
                return fn()
            return recurse(depth - 1, fn)

        def suspend_with_nested_frames(current, arg):
            def outer():
                def inner():
                    return current.main().switch("paused")

                return inner()

            outer()
            return current.main()

        def spawner_function():
            return t.run(suspend_with_nested_frames, None)

        try:
            t = _tealet.tealet()
            assert recurse(5, spawner_function) == "paused"

            def query_traceback():
                frame = t.frame
                assert frame is not None

                names = []
                cursor = frame
                for _ in range(32):
                    if cursor is None:
                        break
                    names.append(cursor.f_code.co_name)
                    cursor = cursor.f_back

                # Keep a concrete shape expectation for stable versions.
                # If 3.12+ frame exposure internals change, this can be adjusted.
                assert names[0:3] == ["inner", "outer", "suspend_with_nested_frames"]

                tb = None
                cursor = frame
                for _ in range(32):
                    if cursor is None:
                        break
                    tb = types.TracebackType(tb, cursor, max(cursor.f_lasti, 0), cursor.f_lineno)
                    cursor = cursor.f_back

                rendered = "".join(traceback.format_tb(tb))
                return names, rendered

            names, rendered = recurse(5, query_traceback)
            assert "in inner" in rendered
            assert "in outer" in rendered
            assert "in suspend_with_nested_frames" in rendered
            if sys.version_info >= (3, 12):
                assert "spawner_function" not in names
                assert "in spawner_function" not in rendered

            t.switch()
            assert t.state == _tealet.STATE_EXIT
        finally:
            _tealet.frame_introspection(original_enabled)


class TestRandom1:
    max_status = 10000

    def randomRun(self, index):
        cur = _tealet.current()
        while True:
            i = random.randint(0, len(self.tealets))
            self.status += 1;
            if i == len(self.tealets):
                break
            prevstatus = self.status
            self.got_index = i
            if not self.tealets[i]:
                if self.status >= self.max_status:
                    break
                #print "new", i
                get_new()(self.randomTealet, i)
            else:
                #print 'switch', i
                d = self.tealets[i].switch()
                #assert d == math.sqrt(2344.2)
            assert self.status >= prevstatus
            assert _tealet.current() == cur
            assert self.tealets[index] == cur
            assert self.got_index == index
            if self.status >= self.max_status:
                break

    def randomTealet(self, current, index):
        i = self.got_index;
        assert _tealet.current() == current
        assert i == index
        assert i > 0 and i < len(self.tealets)
        assert self.tealets[i] == None
        self.tealets[i] = current
        self.randomRun(i)
        self.tealets[i] = None

        i = random.randint(0, len(self.tealets)-1)
        if not self.tealets[i]:
            assert self.tealets[0];
            i = 0
        self.got_index = i
        #print "ret", i
        return self.tealets[i]

    def test_random(self):
        self.tealets = [None]*127
        self.status = 0
        self.tealets[0] = _tealet.current()
        while self.status < self.max_status:
            self.randomRun(0)

        assert _tealet.current() == self.tealets[0]
        for i in range(1, len(self.tealets)):
            while self.tealets[i]:
                self.randomRun(0)


class TestRandom2:
    MAX_STATUS = 10000
    N_RUNS = 10
    MAX_DESCEND = 20
    ARRAYSIZE = 127

    def randomTealet(self, cur, index):
        assert _tealet.current() == cur
        assert index > 0 and index < len(self.tealets)
        assert self.tealets[index] == None
        self.tealets[index] = cur
        self.randomRun(index)
        self.tealets[index] = None
        return self.tealets[0] # switch to main

    def randomRun(self, index):
        assert self.tealets[index] == None or self.tealets[index] == _tealet.current()
        self.tealets[index] = _tealet.current()
        for i in range(self.N_RUNS):
            if self.randomDescend(index, random.randint(0, self.MAX_DESCEND+1)) == 0:
                break;
        self.tealets[index] = None

    def randomDescend(self, index, level):
        if level > 0:
            return self.randomDescend(index, level-1)
        # find target
        target = random.randint(0, len(self.tealets)-1)
        if self.status < self.MAX_STATUS:
            self.status += 1;
            if not self.tealets[target]:
                get_new()(self.randomTealet, target)
            else:
                self.tealets[target].switch()
            return 1
        else:
            # find a telet other than us to flush
            for j in range(len(self.tealets)):
                k = (j + target) % len(self.tealets)
                if k != index and self.tealets[k]:
                    self.status += 1;
                    self.tealets[k].switch()
                    return 1
            return 0

    def test_random(self):
        self.tealets = [None] * self.ARRAYSIZE
        self.status = 0
        self.tealets[0] = _tealet.current()

        while self.status < self.MAX_STATUS:
            self.randomRun(0);

        # drain the system
        self.tealets[0] = _tealet.current()
        while True:
            found = False
            for i, t in enumerate(self.tealets[1:]):
                if t:
                    self.status += 1
                    t.switch()
                    found = True
                    break
            if not found:
                break
        self.tealets[0] = None



# Tests can be run with: pytest tests/test_tealet.py
