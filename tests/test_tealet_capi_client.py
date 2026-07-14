import _tealet
import _tealet_capi_client
import pytest


def test_capi_client_api_info():
    info = _tealet_capi_client.api_info()

    assert info["abi_version"] == _tealet.C_API_ABI_VERSION
    assert info["struct_size"] >= 0
    assert info["feature_flags"] >= 0
    assert info["has_base"] is True
    assert info["has_create"] is True
    assert info["has_stub"] is True
    assert info["has_set_stub"] is True
    assert info["has_prepare"] is True
    assert info["has_duplicate"] is True
    assert info["has_run"] is True
    assert info["has_switch"] is True
    assert info["has_throw"] is True
    assert info["has_set_pending_exception"] is True
    assert info["has_thread_reap"] is True
    assert info["has_thread_sweep"] is True
    assert info["has_thread_active"] is True
    assert info["has_thread_kill"] is True
    assert info["has_error_was_remote"] is True
    assert info["has_previous"] is True
    assert info["has_frame_introspection_get"] is True
    assert info["has_frame_introspection_set"] is True
    assert info["has_is_foreign"] is True
    assert info["has_state_get"] is True
    assert info["has_thread_id_get"] is True


def test_capi_client_current_is_main():
    assert _tealet_capi_client.current_is_main() is True


def test_capi_client_check_tealet():
    assert _tealet_capi_client.check_tealet(_tealet.current()) is True
    assert _tealet_capi_client.check_tealet(object()) is False


def test_capi_client_switch_roundtrip():
    def parked(current, _arg):
        resumed = current.main().switch("paused")
        return current.main(), resumed

    t = _tealet.tealet()
    assert t.run(parked, None) == "paused"
    assert _tealet_capi_client.capi_switch(t, "resumed") == "resumed"


def test_capi_client_switch_flags_panic():
    with pytest.raises(_tealet.PanicError) as exc:
        _tealet_capi_client.capi_switch_flags(_tealet.current(), _tealet_capi_client.SWITCH_PANIC, "panic-value")

    assert exc.value.result() == "panic-value"
    assert exc.value.exception() is None


def test_capi_client_switch_flags_unknown_bit_rejected():
    with pytest.raises(ValueError, match="unsupported switch flags"):
        _tealet_capi_client.capi_switch_flags(_tealet.current(), 0x80, "x")


def test_capi_client_run_forwarding():
    def worker(current, arg):
        return current.main(), ("via-capi-run", arg)

    t = _tealet.tealet()
    assert _tealet_capi_client.capi_run(t, worker, 123) == ("via-capi-run", 123)


def test_capi_client_prepare_forwarding():
    def worker(current, arg):
        return current.main(), ("via-capi-prepare", arg)

    t = _tealet.tealet()
    assert _tealet_capi_client.capi_prepare(t, worker) is None
    assert _tealet_capi_client.capi_state(t) == _tealet.STATE_PRIMED
    assert _tealet_capi_client.capi_switch(t, 321) == ("via-capi-prepare", 321)


def test_capi_client_throw_roundtrip():
    def parked(current, _arg):
        try:
            current.main().switch("ready")
        except RuntimeError as exc:
            return current.main(), ("caught", str(exc))
        return current.main(), "no-exception"

    t = _tealet.tealet()
    assert t.run(parked, None) == "ready"
    assert _tealet_capi_client.capi_throw(t, RuntimeError("boom-throw")) == (
        "caught",
        "boom-throw",
    )


def test_capi_client_throw_panic_flag():
    def parked(current, _arg):
        try:
            current.main().switch("ready")
        except BaseException as exc:
            return current.main(), ("caught", type(exc).__name__, str(exc))
        return current.main(), "no-exception"

    t = _tealet.tealet()
    assert t.run(parked, None) == "ready"
    assert _tealet_capi_client.capi_throw(t, RuntimeError("boom-panic"), _tealet_capi_client.THROW_PANIC) == (
        "caught",
        "PanicError",
        "tealet switch failed",
    )


def test_capi_client_throw_flags_unknown_bit_rejected():
    with pytest.raises(ValueError, match="unsupported throw flags"):
        _tealet_capi_client.capi_throw(_tealet.current(), RuntimeError("boom"), 0x80)


def test_capi_client_throw_with_return_target_none_omits_redirect_target():
    class QueryTealet(_tealet.tealet):
        def resolve_target(self, result, exc, exc_target):
            if exc is not None:
                assert exc_target is None
                return _tealet.main(), "via-capi-none", True
            return super().resolve_target(result, exc, exc_target)

    def worker(current, _arg):
        current.main().switch("paused")
        return current.main()

    t = QueryTealet()
    assert t.run(worker, None) == "paused"
    assert _tealet_capi_client.capi_throw(t, RuntimeError("boom-none"), 0, None) == "via-capi-none"


def test_capi_client_throw_with_explicit_return_target_sets_redirect_target():
    class QueryTealet(_tealet.tealet):
        def resolve_target(self, result, exc, exc_target):
            if exc is not None and exc_target is not None:
                return exc_target, "via-capi-explicit", True
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
    assert _tealet_capi_client.capi_throw(t, RuntimeError("boom-explicit"), 0, redirect) == (
        "received",
        "via-capi-explicit",
    )


def test_capi_client_set_pending_exception_then_switch():
    def parked(current, _arg):
        try:
            current.main().switch("ready")
        except RuntimeError as exc:
            return current.main(), ("caught", str(exc))
        return current.main(), "no-exception"

    t = _tealet.tealet()
    assert t.run(parked, None) == "ready"

    assert _tealet_capi_client.capi_set_pending_exception(t, RuntimeError("boom-set")) is None
    assert _tealet_capi_client.capi_switch(t) == ("caught", "boom-set")


def test_capi_client_thread_active_lists_live_tealet():
    def parked(current, _arg):
        resumed = current.main().switch("ready")
        return current.main(), resumed

    t = _tealet.tealet()
    assert t.run(parked, None) == "ready"

    active = _tealet_capi_client.capi_thread_active()
    assert t in active

    assert _tealet_capi_client.capi_switch(t, "done") == "done"


def test_capi_client_thread_kill_empty_when_no_active():
    assert _tealet_capi_client.capi_thread_kill() == []


def test_capi_client_thread_reap_empty_idempotent():
    assert _tealet_capi_client.capi_thread_reap() == []


def test_capi_client_thread_sweep_is_cross_interface_idempotent():
    # thread_sweep() is destructive: whichever interface calls it first may
    # consume pending dead-thread wrappers.
    capi_first = _tealet_capi_client.capi_thread_sweep()
    module_after = _tealet.thread_sweep()

    assert isinstance(capi_first, list)
    assert isinstance(module_after, list)
    assert module_after == []

    module_first = _tealet.thread_sweep()
    capi_after = _tealet_capi_client.capi_thread_sweep()

    assert isinstance(module_first, list)
    assert isinstance(capi_after, list)
    assert capi_after == []


def test_capi_client_error_was_remote_matches_module_flag():
    assert _tealet_capi_client.capi_error_was_remote() is _tealet.error_was_remote()


def test_capi_client_previous_matches_python_api():
    def parked(current, _arg):
        return current.main(), (
            _tealet.previous(),
            _tealet_capi_client.capi_previous(),
            current.previous(),
        )

    module_prev, capi_prev, method_prev = _tealet.tealet().run(parked, None)

    assert module_prev is _tealet.main()
    assert capi_prev is _tealet.main()
    assert method_prev is _tealet.main()


def test_capi_client_frame_introspection_toggle():
    compiled = bool(getattr(_tealet, "PYTEALET_WITH_PENDING_FRAME_INTROSPECTION", 1))
    original = _tealet_capi_client.capi_frame_introspection()

    assert isinstance(original, bool)

    try:
        assert _tealet_capi_client.capi_frame_introspection_set(False) is False
        assert _tealet.frame_introspection() is False
        if compiled:
            assert _tealet_capi_client.capi_frame_introspection_set(True) is True
            assert _tealet.frame_introspection() is True
        else:
            with pytest.raises(RuntimeError):
                _tealet_capi_client.capi_frame_introspection_set(True)
    finally:
        _tealet_capi_client.capi_frame_introspection_set(original)


def test_capi_client_metadata_helpers_match_python_properties():
    t = _tealet.tealet()

    assert _tealet_capi_client.capi_is_foreign(t) is False
    assert _tealet_capi_client.capi_state(t) == t.state
    assert _tealet_capi_client.capi_thread_id(t) == t.thread_id


def test_capi_client_metadata_helpers_reject_non_tealet():
    with pytest.raises(TypeError):
        _tealet_capi_client.capi_is_foreign(object())
    with pytest.raises(TypeError):
        _tealet_capi_client.capi_state(object())
    with pytest.raises(TypeError):
        _tealet_capi_client.capi_thread_id(object())


def test_capi_client_run_c_forwarding():
    t = _tealet.tealet()
    assert _tealet_capi_client.capi_run_c(t, 456) == ("via-capi-run-c", 456)


def test_capi_client_prepare_c_forwarding():
    t = _tealet.tealet()
    assert _tealet_capi_client.capi_prepare_c(t) is None
    assert _tealet_capi_client.capi_switch(t, 654) == ("via-capi-run-c", 654)


def test_capi_client_stub_creation():
    t = _tealet.tealet()
    out = _tealet_capi_client.capi_stub(t)

    assert out is None
    assert t.state == _tealet.STATE_STUB


def test_capi_client_create_new():
    t = _tealet_capi_client.capi_create()

    assert _tealet_capi_client.check_tealet(t) is True
    assert t.state == _tealet.STATE_NEW
    assert t.thread_id == _tealet.current().thread_id


def test_capi_client_duplicate_stub():
    t = _tealet.tealet()
    t.stub()

    dup = _tealet_capi_client.capi_duplicate(t)

    assert dup is not t
    assert dup.state == _tealet.STATE_STUB
    assert dup.thread_id == t.thread_id


def test_capi_client_set_stub():
    source = _tealet.tealet()
    source.stub()
    target = _tealet.tealet()

    out = _tealet_capi_client.capi_set_stub(target, source, True)

    assert out is None
    assert target.state == _tealet.STATE_STUB


def test_capi_client_set_stub_duplicate_false_rejected():
    source = _tealet.tealet()
    source.stub()

    with pytest.raises(ValueError, match="duplicate=False"):
        _tealet_capi_client.capi_set_stub(_tealet.tealet(), source, False)


def test_capi_client_set_stub_duplicate_accepts_truthy_value():
    source = _tealet.tealet()
    source.stub()
    target = _tealet.tealet()

    out = _tealet_capi_client.capi_set_stub(target, source, 1)

    assert out is None
    assert target.state == _tealet.STATE_STUB
