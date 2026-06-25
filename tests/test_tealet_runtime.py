import gc
import sys
import traceback
import weakref

import pytest

import _tealet

from ._tealet_test_helpers import get_new
class TestModule:
    def test_main(self):
        assert _tealet.main() is _tealet.current()

    def test_main2(self):
        assert _tealet.main() is _tealet.current().main()

    def test_main3(self):
        assert _tealet.main().state == _tealet.STATE_RUN

    def test_tealet_class_configures_main_wrapper(self):
        class CustomMain(_tealet.tealet):
            pass

        original = _tealet.get_tealet_class()
        old_main = _tealet.main()

        try:
            new_main = _tealet.set_tealet_class(CustomMain)
            assert isinstance(new_main, CustomMain)
            assert _tealet.get_tealet_class() is CustomMain
            assert _tealet.main() is new_main
            assert _tealet.current() is new_main
            assert old_main is not new_main
            assert old_main.is_main() is False
            assert new_main.is_main() is True
        finally:
            _tealet.set_tealet_class(original)

    def test_tealet_class_none_resets_to_base_type(self):
        class CustomMain(_tealet.tealet):
            pass

        original = _tealet.get_tealet_class()

        try:
            assert isinstance(_tealet.set_tealet_class(CustomMain), CustomMain)
            reset_main = _tealet.set_tealet_class(None)
            assert _tealet.get_tealet_class() is _tealet.tealet
            assert type(reset_main) is _tealet.tealet
        finally:
            _tealet.set_tealet_class(original)

    def test_set_tealet_class_rejects_non_tealet_type(self):
        with pytest.raises(TypeError, match="tealet class"):
            _tealet.set_tealet_class(object)

        with pytest.raises(TypeError):
            _tealet.set_tealet_class(42)

    def test_is_main_reports_underlying_main_handle(self):
        assert _tealet.main().is_main() is True
        assert _tealet.tealet().is_main() is False

    def test_tealet_hash_is_stable_across_run(self):
        def worker(current, _arg):
            return current.main()

        t = _tealet.tealet()
        original_hash = hash(t)

        t.run(worker, None)

        assert hash(t) == original_hash

    def test_replaced_main_wrapper_becomes_detached(self):
        class CustomMain(_tealet.tealet):
            pass

        original = _tealet.get_tealet_class()
        old_main = _tealet.main()

        try:
            new_main = _tealet.set_tealet_class(CustomMain)
            assert old_main is not new_main
            assert old_main.is_main() is False
        finally:
            _tealet.set_tealet_class(original)

    def test_base_tealet_duplicate_uses_configured_class(self):
        class CustomDuplicate(_tealet.tealet):
            pass

        original = _tealet.get_tealet_class()

        try:
            _tealet.set_tealet_class(CustomDuplicate)
            duplicate = _tealet.tealet().duplicate()
            assert isinstance(duplicate, CustomDuplicate)
        finally:
            _tealet.set_tealet_class(original)

    def test_subclass_duplicate_preserves_source_class(self):
        class Configured(_tealet.tealet):
            pass

        class Explicit(_tealet.tealet):
            pass

        original = _tealet.get_tealet_class()

        try:
            _tealet.set_tealet_class(Configured)
            duplicate = Explicit().duplicate()
            assert type(duplicate) is Explicit
        finally:
            _tealet.set_tealet_class(original)

    def test_previous_matches_method_previous_inside_running_tealet(self):
        def run(current, arg):
            return current.main(), (_tealet.previous(), current.previous())

        module_prev, method_prev = _tealet.tealet().run(run, None)

        assert module_prev is _tealet.main()
        assert method_prev is _tealet.main()

    def test_previous_on_main_after_switch_is_last_switcher(self):
        def parked(current, arg):
            current.main().switch("paused")
            return current.main()

        t = _tealet.tealet()
        assert t.run(parked, None) == "paused"
        assert _tealet.previous() is t

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
            seen["self_is_current"] = current.current() is current
            seen["main"] = current.main()
            seen["previous"] = current.previous()
            return _tealet.main()

        _tealet.tealet().run(run, None)

        assert seen["self_is_current"] is True
        assert seen["main"] is _tealet.main()
        assert seen["previous"] is _tealet.main()

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

    def test_return_current_is_invalid_exit_target(self):
        def run(current, arg):
            return current

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

        assert seen, "expected unraisable error for current return target"
        assert any("current tealet" in str(u.exc_value) for u in seen)


class TestResolveTargetHook:
    def test_resolve_target_can_override_callable_result_semantics(self):
        class RawResultTealet(_tealet.tealet):
            def resolve_target(self, result, exc, exc_target):
                assert result == 123
                assert exc is None
                assert exc_target is None
                return _tealet.main(), "handled-raw"

        seen = []
        original_hook = sys.unraisablehook

        def capture_unraisable(unraisable):
            seen.append(unraisable)

        sys.unraisablehook = capture_unraisable
        try:
            t = RawResultTealet()
            assert t.run(lambda current, arg: 123, None) == "handled-raw"
            assert t.state == _tealet.STATE_EXIT
        finally:
            sys.unraisablehook = original_hook

        assert seen == []

    def test_resolve_target_routes_none_return_when_suppressing_error(self):
        class RoutedTealet(_tealet.tealet):
            def resolve_target(self, result, exc, exc_target):
                assert result is None
                assert exc is None
                assert exc_target is None
                return _tealet.main(), "routed", True

        seen = []
        original_hook = sys.unraisablehook

        def capture_unraisable(unraisable):
            seen.append(unraisable)

        sys.unraisablehook = capture_unraisable
        try:
            t = RoutedTealet()
            assert t.run(lambda current, arg: None, None) == "routed"
            assert t.state == _tealet.STATE_EXIT
        finally:
            sys.unraisablehook = original_hook

        assert seen == []

    def test_resolve_target_can_suppress_worker_exception(self):
        class RoutedTealet(_tealet.tealet):
            def resolve_target(self, result, exc, exc_target):
                assert result is None
                assert isinstance(exc, ValueError)
                assert exc_target is None
                return _tealet.main(), "suppressed", True

        seen = []
        original_hook = sys.unraisablehook

        def capture_unraisable(unraisable):
            seen.append(unraisable)

        def run(current, arg):
            raise ValueError("worker exploded")

        sys.unraisablehook = capture_unraisable
        try:
            t = RoutedTealet()
            assert t.run(run, None) == "suppressed"
            assert t.state == _tealet.STATE_EXIT
        finally:
            sys.unraisablehook = original_hook

        assert seen == []

    def test_resolve_target_hook_failure_is_unraisable_and_falls_back(self):
        class BrokenTealet(_tealet.tealet):
            def resolve_target(self, result, exc, exc_target):
                raise RuntimeError("hook failed")

        seen = []
        original_hook = sys.unraisablehook

        def capture_unraisable(unraisable):
            seen.append(unraisable)

        sys.unraisablehook = capture_unraisable
        try:
            t = BrokenTealet()
            assert t.run(lambda current, arg: None, None) is None
            assert t.state == _tealet.STATE_EXIT
        finally:
            sys.unraisablehook = original_hook

        assert any(isinstance(u.exc_value, RuntimeError) and "hook failed" in str(u.exc_value) for u in seen)

    def test_custom_resolver_target_is_not_overridden_by_uncaught_redirect(self):
        class RoutedTealet(_tealet.tealet):
            def __init__(self, target):
                super().__init__()
                self._target = target

            def resolve_target(self, result, exc, exc_target):
                if exc is not None:
                    assert exc_target is _tealet.main()
                    return self._target, "via-custom-resolver", True
                return super().resolve_target(result, exc, exc_target)

        def parked(current, _arg):
            msg = current.main().switch("ready")
            return current.main(), ("received", msg)

        def worker(current, _arg):
            current.main().switch("paused")
            return current.main()

        redirect = _tealet.tealet()
        assert redirect.run(parked, None) == "ready"

        t = RoutedTealet(redirect)
        assert t.run(worker, None) == "paused"
        t.set_pending_exception(ValueError("route"), fallback=_tealet.main())

        assert t.switch() == ("received", "via-custom-resolver")
        assert t.state == _tealet.STATE_EXIT

    def test_custom_resolver_receives_uncaught_redirect_target(self):
        class QueryTealet(_tealet.tealet):
            def resolve_target(self, result, exc, exc_target):
                if exc is not None:
                    if exc_target is not None:
                        return exc_target, "via-query", True
                return super().resolve_target(result, exc, exc_target)

        def parked(current, _arg):
            msg = current.main().switch("ready")
            return current.main(), ("received", msg)

        def worker(current, _arg):
            current.main().switch("paused")
            return current.main()

        redirect = _tealet.tealet()
        assert redirect.run(parked, None) == "ready"

        t = QueryTealet()
        assert t.run(worker, None) == "paused"
        t.set_pending_exception(ValueError("route"), fallback=redirect)

        assert t.switch() == ("received", "via-query")
        assert t.state == _tealet.STATE_EXIT

    def test_throw_with_return_target_none_omits_uncaught_redirect_target(self):
        class QueryTealet(_tealet.tealet):
            def resolve_target(self, result, exc, exc_target):
                if exc is not None:
                    assert exc_target is None
                    return _tealet.main(), "via-none", True
                return super().resolve_target(result, exc, exc_target)

        def worker(current, _arg):
            current.main().switch("paused")
            return current.main()

        t = QueryTealet()
        assert t.run(worker, None) == "paused"
        assert t.throw(ValueError("route-none"), return_target=None) == "via-none"
        assert t.state == _tealet.STATE_EXIT

    def test_throw_with_explicit_return_target_sets_uncaught_redirect_target(self):
        class QueryTealet(_tealet.tealet):
            def resolve_target(self, result, exc, exc_target):
                if exc is not None:
                    if exc_target is not None:
                        return exc_target, "via-explicit", True
                return super().resolve_target(result, exc, exc_target)

        def parked(current, _arg):
            msg = current.main().switch("ready")
            return current.main(), ("received", msg)

        def worker(current, _arg):
            current.main().switch("paused")
            return current.main()

        redirect = _tealet.tealet()
        assert redirect.run(parked, None) == "ready"

        t = QueryTealet()
        assert t.run(worker, None) == "paused"
        assert t.throw(ValueError("route-explicit"), return_target=redirect) == ("received", "via-explicit")
        assert t.state == _tealet.STATE_EXIT


class TestPrepare:
    def test_prepare_returns_self_for_chaining(self):
        seen = []

        def worker(current, arg):
            seen.append(arg)
            return current.main(), "done-chain"

        t = _tealet.tealet().prepare(worker)
        assert isinstance(t, _tealet.tealet)
        assert t.state == _tealet.STATE_RUN
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
        assert t.state == _tealet.STATE_RUN
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
        assert t.state == _tealet.STATE_RUN
        assert seen == []
        assert t.switch(123) == "done-stub"
        assert seen == [123]
        assert t.state == _tealet.STATE_EXIT

    def test_prepare_target_can_be_exit_target(self):
        for state_name, make_target in [
            ("new-prepared", _tealet.tealet),
            ("stub-prepared", lambda: _tealet.tealet().stub()),
        ]:
            target = make_target()

            def target_func(current, arg, state_name=state_name):
                current.main().switch((state_name, arg))
                return current.main()

            target.prepare(target_func)

            class Routed(_tealet.tealet):
                def resolve_target(self, result, exc, exc_target):
                    assert result == "done"
                    assert exc is None
                    assert exc_target is None
                    return target, state_name, True

            source = Routed()
            assert source.run(lambda current, arg: "done", None) == (state_name, state_name)
            assert target.state == _tealet.STATE_RUN
            assert source.state == _tealet.STATE_EXIT

            assert target.switch("finish") is None
            assert target.state == _tealet.STATE_EXIT

    def test_prepare_first_throw_on_prepared_reports_unraisable_and_skips_worker(self):
        called = []
        seen = []
        original_hook = sys.unraisablehook

        def worker(current, arg):
            called.append(arg)
            return current.main(), "done-prepared"

        def capture_unraisable(unraisable):
            seen.append(unraisable)

        t = _tealet.tealet()
        t.prepare(worker)

        sys.unraisablehook = capture_unraisable
        try:
            assert t.throw(RuntimeError("boom-prepared-first-throw")) is None
        finally:
            sys.unraisablehook = original_hook

        assert called == []
        assert t.state == _tealet.STATE_EXIT
        assert seen, "expected unraisable error for uncaught thrown exception"
        assert any(
            isinstance(u.exc_value, RuntimeError) and str(u.exc_value) == "boom-prepared-first-throw"
            for u in seen
        )

    def test_prepare_requires_callable(self):
        t = _tealet.tealet()
        with pytest.raises(TypeError, match="must be callable"):
            t.prepare(42)

    def test_prepare_twice_requires_new_or_stub(self):
        seen = []

        def first(current, arg):
            seen.append(("first", arg))
            return current.main(), "first"

        def second(current, arg):
            seen.append(("second", arg))
            return current.main(), "second"

        t = _tealet.tealet()
        t.prepare(first)
        with pytest.raises(_tealet.StateError, match="must be new or stub"):
            t.prepare(second)

        assert t.switch("x") == "first"
        assert seen == [("first", "x")]
        assert t.state == _tealet.STATE_EXIT

    def test_explicit_run_after_prepare_requires_new_or_stub(self):
        seen = []

        def prepared(current, arg):
            seen.append(("prepared", arg))
            return current.main(), "prepared"

        def direct(current, arg):
            seen.append(("direct", arg))
            return current.main(), "direct"

        t = _tealet.tealet()
        t.prepare(prepared)

        with pytest.raises(_tealet.StateError, match="must be new or stub"):
            t.run(direct, "r")

        assert t.switch("p") == "prepared"
        assert seen == [("prepared", "p")]
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
        assert t.main() is _tealet.main()
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


