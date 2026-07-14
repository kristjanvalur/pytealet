import sys

import pytest

import _tealet

from ._tealet_test_helpers import get_new, tealet_new_descend
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
        t.set_pending_exception(pending)

        with pytest.raises(_tealet.PanicError) as exc:
            _tealet.current().switch("panic", panic=True)

        assert exc.value.exception() is pending
        with pytest.raises(RuntimeError, match="boom-pending"):
            exc.value.result()

    def test_switch(self):
        status = [0]
        t = [None, None]

        def t2(current, arg):
            assert current is not _tealet.main()
            assert current is not t[0]
            t[1] = current
            assert status[0] == 1
            status[0] = 2
            assert _tealet.current() is current
            t[0].switch()
            assert status[0] == 3
            status[0] = 4
            assert _tealet.current() is current
            t[0].switch()
            assert status[0] == 5
            status[0] = 6
            assert current is t[1]
            assert _tealet.current() is current
            t[1].switch()  # noop
            assert status[0] == 6
            status[0] = 7
            assert _tealet.current() is current
            return _tealet.main()

        def t1(current, arg):
            assert current is not _tealet.main()
            t[0] = current
            assert status[0] == 0
            status[0] = 1
            assert current is _tealet.current()
            get_new()(t2)
            assert status[0] == 2
            status[0] = 3
            assert current is _tealet.current()
            t[1].switch()
            assert status[0] == 4
            status[0] = 5
            assert current is _tealet.current()
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
            arg.switch()
            # back to main
            return _tealet.main()

        tealet1 = get_new()(new1, _tealet.current())
        # the tealet is now running
        tealet2 = tealet_new_descend(4, new2, tealet1)

        assert tealet2.state == _tealet.STATE_RUN
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
            r = arg.switch(4)
            assert r == 6
            # back to main
            return _tealet.main(), 7

        tealet1, r = get_new()(new1, _tealet.current(), retarg=True)
        assert r == 2
        # the tealet is now running
        tealet2, r = tealet_new_descend(4, new2, tealet1, retarg=True)
        assert r == 5

        assert tealet2.state == _tealet.STATE_RUN
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
        assert any(isinstance(u.exc_value, RuntimeError) and str(u.exc_value) == "boom-throw" for u in seen)

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
        assert any(isinstance(u.exc_value, RuntimeError) and str(u.exc_value) == "boom-throw-run" for u in seen)

    def test_throw_on_primed_target_behaves_like_set_pending_exception_plus_run(self):
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
                t.prime(worker)
                assert t.throw(RuntimeError("boom-primed-throw")) is None
                assert t.state == _tealet.STATE_EXIT
        finally:
            sys.unraisablehook = original_hook

        # Delivery happens at run entry, so worker is not entered.
        assert called == []
        assert seen, "expected unraisable error for uncaught thrown exception"
        assert any(isinstance(u.exc_value, RuntimeError) and str(u.exc_value) == "boom-primed-throw" for u in seen)

    def test_run_allows_dummy_args_when_pending_exception_exists(self):
        seen = []
        original_hook = sys.unraisablehook

        def capture_unraisable(unraisable):
            seen.append(unraisable)

        sys.unraisablehook = capture_unraisable
        try:
            t = _tealet.tealet()
            t.set_pending_exception(RuntimeError("boom-dummy-run"))
            assert t.run("dummy-func", {"dummy": True}) is None
            assert t.state == _tealet.STATE_EXIT
        finally:
            sys.unraisablehook = original_hook

        assert seen, "expected unraisable error for uncaught injected exception"
        assert any(isinstance(u.exc_value, RuntimeError) and str(u.exc_value) == "boom-dummy-run" for u in seen)

    def test_set_pending_exception_before_run_injects_at_run_entry(self):
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
            t.set_pending_exception(RuntimeError("boom-before-run"))
            assert t.run(worker, None) is None
            assert t.state == _tealet.STATE_EXIT
        finally:
            sys.unraisablehook = original_hook

        assert entered == []
        assert seen, "expected unraisable error for uncaught injected exception"
        assert any(isinstance(u.exc_value, RuntimeError) and str(u.exc_value) == "boom-before-run" for u in seen)

    def test_set_pending_exception_delivers_on_next_switch(self):
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

        t.set_pending_exception(RuntimeError("boom"))
        assert t.switch() == "done"
        assert t.switch() is None
        assert t.state == _tealet.STATE_EXIT
        assert seen == ["boom"]

    def test_error_was_remote_true_for_delivered_pending_exception(self):
        t = _tealet.tealet()
        assert _tealet.error_was_remote() is False

        t.set_pending_exception(RuntimeError("boom-remote-flag"))
        with pytest.raises(RuntimeError):
            _tealet.current().switch("resume")
        assert _tealet.error_was_remote() is True

        # Any new switching API call clears the flag on entry.
        assert _tealet.current().switch() is None
        assert _tealet.error_was_remote() is False

    def test_error_was_remote_false_for_panic_with_pending_exception(self):
        t = _tealet.tealet()
        pending = RuntimeError("boom-panic-origin")
        t.set_pending_exception(pending)

        with pytest.raises(_tealet.PanicError) as exc:
            _tealet.current().switch("panic", panic=True)

        assert exc.value.exception() is pending
        assert _tealet.error_was_remote() is False

    def test_set_pending_exception_with_fallback_redirects_uncaught_unwind(self):
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

            t.set_pending_exception(ValueError("route"), fallback=_tealet.main())
            assert t.switch() is None
            assert t.state == _tealet.STATE_EXIT
        finally:
            sys.unraisablehook = original_hook

        assert seen, "expected unraisable error for uncaught injected exception"
        assert any(isinstance(u.exc_value, ValueError) and str(u.exc_value) == "route" for u in seen)

    def test_set_pending_exception_overwrites_inflight_token_after_catch(self):
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

        t.set_pending_exception(RuntimeError("boom-1"), fallback=_tealet.main())
        assert t.switch() == "paused-1"

        # First injected exception was caught inside worker; next call should
        # overwrite prior inflight metadata rather than erroring.
        t.set_pending_exception(RuntimeError("boom-2"), fallback=_tealet.main())
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


