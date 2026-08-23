import pytest

import _tealet


class _TraceLog:
    def __init__(self, error_on_trace=False):
        self.actions = []
        self.error_on_trace = error_on_trace

    def __call__(self, event, args):
        self.actions.append((event, args))
        if self.error_on_trace:
            raise RuntimeError("trace boom")


def test_settrace_roundtrip_and_none():
    def cb(event, args):
        del event, args

    old = _tealet.gettrace()
    try:
        _tealet.settrace(None)
        assert _tealet.gettrace() is None
        assert _tealet.settrace(cb) is None
        assert _tealet.gettrace() is cb
        assert _tealet.settrace(None) is cb
        assert _tealet.gettrace() is None
    finally:
        _tealet.settrace(old)


def test_settrace_requires_callable():
    with pytest.raises(TypeError, match="callable"):
        _tealet.settrace(1)


def test_switch_and_return_are_traced():
    main = _tealet.current()
    log = _TraceLog()
    old = _tealet.settrace(log)
    try:

        def worker(current, _arg):
            main.switch("from-child")
            return main

        child = _tealet.tealet()
        assert child.run(worker, None) == "from-child"
        child.switch("back")
        pairs = [(event, origin, target) for event, (origin, target) in log.actions]
        assert pairs[0] == ("switch", main, child)
        assert ("switch", child, main) in pairs
        assert all(event in ("switch", "throw") for event, _origin, _target in pairs)
    finally:
        _tealet.settrace(old)


def test_exit_switch_is_traced():
    main = _tealet.current()
    log = _TraceLog()
    old = _tealet.settrace(log)
    try:

        def worker(_current, _arg):
            return main

        child = _tealet.tealet()
        child.run(worker, None)
        pairs = [(event, origin, target) for event, (origin, target) in log.actions]
        assert ("switch", main, child) in pairs
        assert ("switch", child, main) in pairs
    finally:
        _tealet.settrace(old)


def test_queued_exception_on_exit_is_throw_event():
    main = _tealet.current()
    log = _TraceLog()
    old = _tealet.settrace(log)
    try:

        def worker(_current, _arg):
            main.set_pending_exception(RuntimeError("exit-throw"))
            return main

        child = _tealet.tealet()
        with pytest.raises(RuntimeError, match="exit-throw"):
            child.run(worker, None)
        throw_pairs = [args for event, args in log.actions if event == "throw"]
        assert throw_pairs
        origin, target = throw_pairs[-1]
        assert origin is child
        assert target is main
    finally:
        _tealet.settrace(old)


def test_throw_event_on_pending_exception():
    main = _tealet.current()
    log = _TraceLog()
    old = _tealet.settrace(log)
    try:

        def worker(_current, _arg):
            try:
                main.switch("ready")
            except RuntimeError as exc:
                assert str(exc) == "traced-throw"
                return main
            raise AssertionError("expected injected throw")

        child = _tealet.tealet()
        assert child.run(worker, None) == "ready"
        child.throw(RuntimeError("traced-throw"))
        throw_pairs = [args for event, args in log.actions if event == "throw"]
        assert throw_pairs
        origin, target = throw_pairs[0]
        assert origin is main
        assert target is child
    finally:
        _tealet.settrace(old)


def test_trace_exception_disables_hook():
    log = _TraceLog()
    n = {"n": 0}

    def boom(event, args):
        log(event, args)
        n["n"] += 1
        if n["n"] >= 2:
            raise RuntimeError("trace boom")

    old = _tealet.settrace(boom)
    try:

        def worker(current, main):
            main.switch("from-child")
            return main

        child = _tealet.tealet()
        with pytest.raises(RuntimeError, match="trace boom"):
            child.run(worker, _tealet.main())
        assert _tealet.gettrace() is None
        assert len(log.actions) >= 1
    finally:
        _tealet.settrace(old)
