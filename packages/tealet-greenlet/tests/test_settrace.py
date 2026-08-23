import _tealet

import tealet_greenlet as greenlet


class SomeError(Exception):
    pass


def test_throw_unstarted_is_throw_event():
    main = greenlet.getcurrent()
    seen: list[object] = []

    def log(event, args):
        seen.append((event, args))

    def boom():
        raise SomeError()

    old = greenlet.settrace(log)
    try:
        g = greenlet.greenlet(boom)
        raised = None
        try:
            g.throw(SomeError)
        except SomeError as exc:
            raised = exc
        assert raised is not None
        assert seen
        assert seen[0] == ("throw", (main, g))
    finally:
        greenlet.settrace(old)


def test_settrace_none_restores_previous_core_hook():
    seen_core: list[object] = []

    def core_cb(event, args):
        del args
        seen_core.append(event)

    old = _tealet.settrace(core_cb)
    try:

        def dummy():
            return 1

        greenlet.settrace(lambda *a: None)
        assert _tealet.gettrace() is greenlet._adapt_trace
        greenlet.settrace(None)
        assert _tealet.gettrace() is core_cb

        g = greenlet.greenlet(dummy)
        g.switch()
        assert seen_core
    finally:
        _tealet.settrace(old)
        greenlet.settrace(None)


def test_settrace_none_does_not_clear_foreign_hook():
    def core_cb(event, args):
        del event, args

    old = _tealet.settrace(core_cb)
    try:
        greenlet.settrace(None)
        assert _tealet.gettrace() is core_cb
    finally:
        _tealet.settrace(old)
