import sys

import pytest

import _tealet
from tealet.profile import Profile


def _func_names(prof: Profile) -> set[str]:
    prof.create_stats()
    return {name for (_file, _line, name) in prof.stats}


def test_plain_runcall_without_switch():
    def f():
        return 42

    prof = Profile()
    assert prof.runcall(f) == 42
    assert "f" in _func_names(prof)


def test_enable_disable_restores_hooks():
    old_profile = sys.getprofile()
    old_trace = _tealet.gettrace()
    prof = Profile()
    prof.enable()
    try:
        assert sys.getprofile() is prof.dispatcher
        assert _tealet.gettrace() is not None
        assert _tealet.gettrace() is not old_trace
    finally:
        prof.disable()
    assert sys.getprofile() is old_profile
    assert _tealet.gettrace() is old_trace


def test_runcall_restores_hooks_on_exception():
    old_profile = sys.getprofile()
    old_trace = _tealet.gettrace()
    prof = Profile()

    def boom():
        raise RuntimeError("profile-boom")

    with pytest.raises(RuntimeError, match="profile-boom"):
        prof.runcall(boom)
    assert sys.getprofile() is old_profile
    assert _tealet.gettrace() is old_trace


def test_switch_records_both_stacks():
    def in_child():
        return sum(range(100))

    def worker(current, main):
        in_child()
        parked = main.switch("parked")
        in_child()
        return main, parked

    def in_main():
        return sum(range(50))

    def driver():
        in_main()
        child = _tealet.tealet()
        assert child.run(worker, _tealet.main()) == "parked"
        in_main()
        child.switch("back")

    prof = Profile()
    prof.runcall(driver)
    names = _func_names(prof)
    assert "driver" in names
    assert "worker" in names
    assert "in_child" in names
    assert "in_main" in names


def test_throw_records_target_work():
    def in_child():
        return sum(range(80))

    def worker(_current, main):
        try:
            main.switch("ready")
        except RuntimeError:
            in_child()
            return main
        raise AssertionError("expected throw")

    def driver():
        child = _tealet.tealet()
        assert child.run(worker, _tealet.main()) == "ready"
        child.throw(RuntimeError("profile-throw"))

    prof = Profile()
    prof.runcall(driver)
    names = _func_names(prof)
    assert "worker" in names
    assert "in_child" in names


def test_child_exit_does_not_break_main_stats():
    def worker(_current, _arg):
        return _tealet.main()

    def driver():
        child = _tealet.tealet()
        child.run(worker, None)
        return 1

    prof = Profile()
    assert prof.runcall(driver) == 1
    names = _func_names(prof)
    assert "driver" in names
    assert "worker" in names
