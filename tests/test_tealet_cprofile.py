import sys
import threading

import pytest

import _tealet

pytestmark = pytest.mark.skipif(sys.version_info < (3, 12), reason="tealet.cprofile requires Python 3.12+")

from tealet.cprofile import Profile


def _func_names(prof: Profile) -> set[str]:
    prof.create_stats()
    return {name for (_file, _line, name) in prof.stats}


def test_plain_runcall_without_switch():
    def f():
        return 42

    prof = Profile()
    assert prof.runcall(f) == 42
    assert "f" in _func_names(prof)


def test_switch_records_worker():
    def in_child():
        return sum(range(100))

    def worker(current, main):
        in_child()
        parked = main.switch("parked")
        in_child()
        return main, parked

    def driver():
        child = _tealet.tealet()
        assert child.run(worker, _tealet.main()) == "parked"
        child.switch("back")

    prof = Profile()
    prof.runcall(driver)
    names = _func_names(prof)
    assert "driver" in names
    assert "worker" in names
    assert "in_child" in names


def test_same_worker_is_one_family():
    def worker(_current, _arg):
        return _tealet.main()

    def driver():
        for _ in range(3):
            child = _tealet.tealet()
            child.run(worker, None)

    prof = Profile()
    prof.runcall(driver)
    names = {view.family[-1]: view.nstacks for view in prof.stack_families()}
    assert names.get("worker") == 3
    tid = threading.get_ident()
    for view in prof.stacks():
        assert view.thread_id == tid


def test_enable_disable_roundtrip():
    prof = Profile()
    prof.enable()
    prof.disable()

    def f():
        return 1

    assert prof.runcall(f) == 1


def test_profiler_id_conflicts_with_cprofile():
    import cProfile

    other = cProfile.Profile()
    other.enable()
    try:
        with pytest.raises(ValueError, match="profiling tool"):
            Profile().enable()
    finally:
        other.disable()
