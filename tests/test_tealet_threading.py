import gc
import queue
import threading
import weakref

import pytest

import _tealet

from ._tealet_test_helpers import join_thread_or_fail
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
        _tealet.tealet()  # create a new lineage for this thread
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



class TestThreadOwnership:
    def test_new_tealet_has_owner_tid_and_not_foreign(self):
        t = _tealet.tealet()
        assert t.thread_id == _tealet.current().thread_id
        assert t.is_foreign() is False

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
            assert dup.is_foreign() is True
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


