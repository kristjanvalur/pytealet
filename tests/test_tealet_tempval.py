import gc
import threading
import weakref

import pytest

import _tealet

from ._tealet_test_helpers import join_thread_or_fail


class TestTempval:
    def test_new_tealet_tempval_is_none(self):
        t = _tealet.tealet()
        assert t.tempval is None
        t.tempval = None
        assert t.tempval is None

    def test_assign_and_delete(self):
        t = _tealet.tealet()
        payload = {"n": 1}
        t.tempval = payload
        assert t.tempval is payload
        t.tempval = None
        assert t.tempval is None
        t.tempval = payload
        del t.tempval
        assert t.tempval is None

    def test_clear_drops_the_reference(self):
        class Payload:
            pass

        t = _tealet.tealet()
        payload = Payload()
        ref = weakref.ref(payload)
        t.tempval = payload
        del payload
        assert ref() is not None
        t.tempval = None
        gc.collect()
        assert ref() is None

    def test_cycle_is_collected(self):
        t = _tealet.tealet()
        t.tempval = t
        ref = weakref.ref(t)
        del t
        gc.collect()
        assert ref() is None

    def test_stub_keeps_tempval_on_the_same_wrapper(self):
        t = _tealet.tealet()
        t.tempval = "stays"
        t.stub()
        assert t.tempval == "stays"
        t.tempval = None

    def test_duplicate_starts_empty(self):
        t = _tealet.tealet()
        t.tempval = "keep"
        dup = t.duplicate()
        assert dup.tempval is None
        assert t.tempval == "keep"
        t.stub()
        t.tempval = "stub"
        copied = t.duplicate()
        assert copied.tempval is None
        assert t.tempval == "stub"
        t.tempval = None

    def test_set_stub_leaves_both_slots(self):
        src = _tealet.tealet()
        src.stub()
        src.tempval = "source"
        dst = _tealet.tealet()
        dst.tempval = "dest"
        dst.set_stub(src)
        assert dst.tempval == "dest"
        assert src.tempval == "source"
        dst.tempval = None
        src.tempval = None

    def test_switch_leaves_tempval_on_the_tealet(self):
        child = _tealet.tealet()
        child.tempval = "before"
        main = _tealet.main()
        main.tempval = None

        def worker(current, _arg):
            assert current is child
            assert current.tempval == "before"
            current.tempval = "inside"
            main.tempval = "on-main"
            main.switch("parked")
            assert current.tempval == "inside"
            assert main.tempval == "on-main"
            main.tempval = None
            return main

        try:
            assert child.run(worker, None) == "parked"
            assert child.tempval == "inside"
            assert main.tempval == "on-main"
            child.switch()
            assert child.state == _tealet.STATE_EXIT
            assert child.tempval == "inside"
            assert main.tempval is None
        finally:
            child.tempval = None
            main.tempval = None

    def test_foreign_thread_cannot_read_or_write(self):
        data = {}
        ready = threading.Event()
        release = threading.Event()

        def worker():
            t = _tealet.tealet()
            t.tempval = "owned"
            data["t"] = t
            ready.set()
            release.wait(timeout=2.0)
            data["still"] = t.tempval
            t.tempval = None

        th = threading.Thread(target=worker)
        th.start()
        assert ready.wait(timeout=2.0)
        try:
            with pytest.raises(_tealet.ThreadMismatchError):
                data["t"].tempval
            with pytest.raises(_tealet.ThreadMismatchError):
                data["t"].tempval = "nope"
        finally:
            release.set()
            join_thread_or_fail(th)
        assert data["still"] == "owned"
