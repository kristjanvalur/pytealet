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
    dup = klass(stub)
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
    dup = klass(the_stub[0])
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


class TestThreadCleanup:
    """Tests for thread cleanup semantics and edge cases."""

    def test_thread_cleanup_returns_only_run_tealets(self):
        """Cleanup only returns RUN tealets with ACTIVE status, not STUB or main."""
        thread_main = _tealet.main()
        stub = _tealet.tealet()
        stub.stub()
        
        # Create a tealet that switches back to main and stays suspended in RUN.
        def switch_back(current, arg):
            # Switch back to main and return
            thread_main.switch()
        
        t = _tealet.tealet()
        t.run(switch_back, None)  # t runs and switches back, stays suspended
        
        nerfed = _tealet.thread_cleanup()
        nerfed_ids = {id(x) for x in nerfed}

        # t switched back to main and remains suspended in RUN state,
        # so cleanup should include it in nerfed.
        # STUB tealets are not returned (can be safely collected).
        # Main is cleanly deleted, not forcibly invalidated.
        assert id(thread_main) not in nerfed_ids
        assert id(stub) not in nerfed_ids  # STUB not in nerfed
        assert id(t) in nerfed_ids  # suspended RUN tealet is in nerfed

        # Recreate main for this thread so subsequent tests keep the usual baseline.
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_thread_cleanup_requires_main_tealet_context(self):
        def run(current, arg):
            with pytest.raises(_tealet.StateError):
                _tealet.thread_cleanup()
            return current.main()

        _tealet.tealet().run(run, None)

    def test_cleanup_nerfed_suspended_tealet_cannot_switch(self):
        """A suspended RUN tealet returned by cleanup cannot be switched to again."""
        def parked(current, arg):
            current.main().switch("paused")
            return current.main()

        t = _tealet.tealet()
        assert t.run(parked, None) == "paused"

        nerfed = _tealet.thread_cleanup()
        assert any(id(x) == id(t) for x in nerfed)

        with pytest.raises(_tealet.StateError):
            t.switch()

    def test_cleanup_empty_lineage(self):
        """Cleanup with only main tealet (no non-main tealets) returns empty list."""
        _tealet.main()  # ensure main exists
        nerfed = _tealet.thread_cleanup()
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
        
        nerfed = _tealet.thread_cleanup()
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
        nerfed = _tealet.thread_cleanup()
        assert nerfed == []

        # New lineage in same thread
        main2 = _tealet.main()
        assert main2.state == _tealet.STATE_RUN
        stub2 = _tealet.tealet()
        stub2.stub()
        assert stub2.state == _tealet.STATE_STUB
        nerfed2 = _tealet.thread_cleanup()
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
        
        nerfed = _tealet.thread_cleanup()
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
        nerfed = _tealet.thread_cleanup()
        assert isinstance(nerfed, list)
        # The stub was GC'd and wasn't RUN anyway, so won't be in nerfed
        
        # Recreate main for subsequent tests
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_cleanup_main_never_in_nerfed(self):
        """Main tealet is never in nerfed, only non-main RUN tealets."""
        main = _tealet.main()
        stub = _tealet.tealet()
        stub.stub()
        
        nerfed = _tealet.thread_cleanup()
        nerfed_ids = {id(x) for x in nerfed}
        
        # Main is never in nerfed (cleanly deleted)
        assert id(main) not in nerfed_ids
        # STUB is not in nerfed (not RUN)
        assert id(stub) not in nerfed_ids
        
        # Recreate main for subsequent tests
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_cleanup_idempotent_on_empty_lineage(self):
        """Calling cleanup multiple times on empty lineage is idempotent."""
        _tealet.main()
        nerfed1 = _tealet.thread_cleanup()
        assert nerfed1 == []
        
        # Second cleanup should be idempotent (no error, no main)
        nerfed2 = _tealet.thread_cleanup()
        assert nerfed2 == []
        
        # Recreate main for subsequent tests
        assert _tealet.main().state == _tealet.STATE_RUN


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
            with pytest.raises(_tealet.InvalidError):
                data["t"].run(lambda current, arg: current.main(), None)
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
            with pytest.raises(_tealet.InvalidError):
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
            dup = _tealet.tealet(data["stub"])
            assert dup.thread_id == data["owner_tid"]
            assert dup.belongs_to_current() is False
            with pytest.raises(_tealet.InvalidError):
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

class TestSwitch:
    def test_switch_panic_keyword(self):
        assert _tealet.current().switch(panic=False) is None
        with pytest.raises(_tealet.PanicError) as exc:
            _tealet.current().switch("panic-value", panic=True)
        assert exc.value.value == "panic-value"

        with pytest.raises(_tealet.PanicError) as exc2:
            _tealet.current().switch(panic=True)
        assert exc2.value.value is None

    def test_switch_panic_payload_identity_from_tealet(self):
        payload = {"kind": "panic", "n": 7}

        def worker(current, main):
            main.switch(payload, panic=True)
            return _tealet.main()

        with pytest.raises(_tealet.PanicError) as exc:
            _tealet.tealet().run(worker, _tealet.main())
        assert exc.value.value is payload

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
