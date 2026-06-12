import gc
import sys
import traceback
import weakref

import pytest

import _tealet

from ._tealet_test_helpers import get_new
class TestModule:
    def test_main(self):
        assert _tealet.main() == _tealet.current()

    def test_main2(self):
        assert _tealet.main() == _tealet.current().main()

    def test_main3(self):
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_previous_matches_method_previous_inside_running_tealet(self):
        def run(current, arg):
            return current.main(), (_tealet.previous(), current.previous())

        module_prev, method_prev = _tealet.tealet().run(run, None)

        assert module_prev == _tealet.main()
        assert method_prev == _tealet.main()

    def test_previous_on_main_after_switch_is_last_switcher(self):
        def parked(current, arg):
            current.main().switch("paused")
            return current.main()

        t = _tealet.tealet()
        assert t.run(parked, None) == "paused"
        assert _tealet.previous() == t

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

    def test_hide_frame_without_kwargs_uses_empty_mapping(self):
        def inner(*args, **kwargs):
            return args, kwargs

        result = _tealet.hide_frame(inner, (1, 2))
        assert result == ((1, 2), {})

    def test_hide_frame_rejects_none_kwargs(self):
        with pytest.raises(TypeError, match="dict"):
            _tealet.hide_frame(lambda: None, (), None)

    def test_hide_frame_rejects_non_dict_kwargs(self):
        with pytest.raises(TypeError, match="dict"):
            _tealet.hide_frame(lambda: None, (), 42)

    def test_module_keyword_argument_forms(self):
        original = _tealet.frame_introspection()

        try:
            assert _tealet.frame_introspection(enabled=False) is False
        finally:
            _tealet.frame_introspection(original)

        assert _tealet.thread_kill(cleanup_passes=2, kill_exc=None) == []
        assert _tealet.thread_reap(cleanup_passes=2, kill_exc=None) == []

        def inner(*args, **kwargs):
            return args, kwargs

        assert _tealet.hide_frame(callable=inner, args=(1,), kwargs={"x": 2}) == ((1,), {"x": 2})



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
            seen["self_is_current"] = current.current() == current
            seen["main"] = current.main()
            seen["previous"] = current.previous()
            return _tealet.main()

        _tealet.tealet().run(run, None)

        assert seen["self_is_current"] is True
        assert seen["main"] == _tealet.main()
        assert seen["previous"] == _tealet.main()

    @pytest.mark.skip(
        reason="Deferred-delete post-exit behavior is experimental; re-enable when PYTEALET_DEFER_DELETE is being exercised"
    )
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
        assert any(isinstance(u.exc_value, TypeError) and "tealet object expected" in str(u.exc_value) for u in seen)


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
            return "<myrepr %r>" % super(TestSubclass.sc, self).__repr__()

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

    def test_subclass_init_then_set_stub(self):
        payload = {"kind": "demo"}
        source = _tealet.tealet()
        source.stub()

        t = self.scinit(payload, label="attached")
        assert t.state == _tealet.STATE_NEW

        out = t.set_stub(source)

        assert out is t
        assert isinstance(t, self.scinit)
        assert t.payload is payload
        assert t.label == "attached"
        assert t.state == _tealet.STATE_STUB

    def test_set_stub_requires_new_target_and_stub_source(self):
        source = _tealet.tealet()
        source.stub()
        target = _tealet.tealet()

        target.set_stub(source, duplicate=True)
        assert target.state == _tealet.STATE_STUB

        with pytest.raises(_tealet.StateError, match="target must be new"):
            target.set_stub(source)

        with pytest.raises(_tealet.StateError, match="source must be stub"):
            _tealet.tealet().set_stub(_tealet.tealet())

    def test_set_stub_duplicate_false_rejected(self):
        source = _tealet.tealet()
        source.stub()

        with pytest.raises(ValueError, match="duplicate=False"):
            _tealet.tealet().set_stub(source, duplicate=False)

    def test_set_stub_duplicate_accepts_truthy_value(self):
        source = _tealet.tealet()
        source.stub()
        target = _tealet.tealet()

        out = target.set_stub(source, duplicate=1)

        assert out is target
        assert target.state == _tealet.STATE_STUB

    def test_exact_tealet_constructor_stays_no_args(self):
        with pytest.raises(TypeError, match=r"tealet\(\) takes no arguments"):
            _tealet.tealet(123)


