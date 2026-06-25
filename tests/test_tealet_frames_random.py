import sys
import traceback
import types

import _tealet
import pytest

from ._tealet_test_helpers import get_new, random
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
            self.status += 1
            if i == len(self.tealets):
                break
            prevstatus = self.status
            self.got_index = i
            if not self.tealets[i]:
                if self.status >= self.max_status:
                    break
                # print "new", i
                get_new()(self.randomTealet, i)
            else:
                # print 'switch', i
                d = self.tealets[i].switch()
                # assert d == math.sqrt(2344.2)
            assert self.status >= prevstatus
            assert _tealet.current() is cur
            assert self.tealets[index] is cur
            assert self.got_index == index
            if self.status >= self.max_status:
                break

    def randomTealet(self, current, index):
        i = self.got_index
        assert _tealet.current() is current
        assert i == index
        assert i > 0 and i < len(self.tealets)
        assert self.tealets[i] is None
        self.tealets[i] = current
        self.randomRun(i)
        self.tealets[i] = None

        i = random.randint(0, len(self.tealets) - 1)
        if not self.tealets[i]:
            assert self.tealets[0]
            i = 0
        self.got_index = i
        # print "ret", i
        return self.tealets[i]

    def test_random(self):
        self.tealets = [None] * 127
        self.status = 0
        self.tealets[0] = _tealet.current()
        while self.status < self.max_status:
            self.randomRun(0)

        assert _tealet.current() is self.tealets[0]
        for i in range(1, len(self.tealets)):
            while self.tealets[i]:
                self.randomRun(0)


class TestRandom2:
    MAX_STATUS = 10000
    N_RUNS = 10
    MAX_DESCEND = 20
    ARRAYSIZE = 127

    def randomTealet(self, cur, index):
        assert _tealet.current() is cur
        assert index > 0 and index < len(self.tealets)
        assert self.tealets[index] is None
        self.tealets[index] = cur
        self.randomRun(index)
        self.tealets[index] = None
        return self.tealets[0]  # switch to main

    def randomRun(self, index):
        assert self.tealets[index] is None or self.tealets[index] is _tealet.current()
        self.tealets[index] = _tealet.current()
        for i in range(self.N_RUNS):
            if self.randomDescend(index, random.randint(0, self.MAX_DESCEND + 1)) == 0:
                break
        self.tealets[index] = None

    def randomDescend(self, index, level):
        if level > 0:
            return self.randomDescend(index, level - 1)
        # find target
        target = random.randint(0, len(self.tealets) - 1)
        if self.status < self.MAX_STATUS:
            self.status += 1
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
                    self.status += 1
                    self.tealets[k].switch()
                    return 1
            return 0

    def test_random(self):
        self.tealets = [None] * self.ARRAYSIZE
        self.status = 0
        self.tealets[0] = _tealet.current()

        while self.status < self.MAX_STATUS:
            self.randomRun(0)

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
