import sys
import threading
import time

import pytest

import _tealet

if sys.version_info < (3, 12):
    pytest.skip("tealet.cprofile requires Python 3.12+", allow_module_level=True)

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
    hits = [prof.combined().stats[k] for k in prof.combined().stats if k[2] == "f"]
    assert hits
    cc, nc, _tt, _ct, _callers = hits[0]
    assert cc >= 1
    assert nc >= cc


def test_pstats_can_reload_stack_families():
    import pstats

    def f():
        return 1

    prof = Profile()
    prof.runcall(f)
    views = prof.stack_families()
    pstats.Stats(*views)
    pstats.Stats(*views)
    views[0].print_stats()


def test_print_stats_on_empty_profile():
    Profile().print_stats()
    Profile().combined().print_stats()


def test_print_stats_accepts_tuple_sort():
    def f():
        return 1

    prof = Profile()
    prof.runcall(f)
    prof.print_stats(("time", "name"))
    prof.combined().print_stats(("tottime",))


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
    assert not any(view.family[-1] == "worker" for view in prof.stacks())
    tid = threading.get_ident()
    for view in prof.stacks():
        assert view.thread_id == tid


def test_fold_on_exit_attribute_updates_c_profiler():
    def worker(_current, _arg):
        return _tealet.main()

    def driver():
        _tealet.tealet().run(worker, None)

    prof = Profile()
    assert prof.fold_on_exit is True
    prof.fold_on_exit = False
    assert prof.fold_on_exit is False
    prof.runcall(driver)
    assert any(view.family[-1] == "worker" for view in prof.stacks())


def test_timer_attribute_updates_c_profiler():
    prof = Profile()
    assert prof.timer == "wall"
    prof.timer = "thread"
    assert prof.timer == "thread"
    with pytest.raises(ValueError, match="timer"):
        prof.timer = "cpu"


def test_dump_calls_is_a_copy():
    def inner():
        return 1

    def outer():
        return inner()

    prof = Profile()
    prof.runcall(outer)
    first = prof._p.dump_stacks()
    calls = None
    for stack in first:
        for entry in stack["entries"]:
            if entry["calls"]:
                calls = entry["calls"]
                break
        if calls is not None:
            break
    assert calls is not None
    calls.clear()
    second = prof._p.dump_stacks()
    assert any(entry["calls"] for stack in second for entry in stack["entries"])


def test_fold_on_exit_off_keeps_individual_stacks():
    def worker(_current, _arg):
        return _tealet.main()

    def driver():
        for _ in range(3):
            child = _tealet.tealet()
            child.run(worker, None)

    prof = Profile(fold_on_exit=False)
    prof.runcall(driver)
    workers = [view for view in prof.stacks() if view.family[-1] == "worker"]
    assert len(workers) == 3
    names = {view.family[-1]: view.nstacks for view in prof.stack_families()}
    assert names.get("worker") == 3


def test_enable_disable_roundtrip():
    prof = Profile()
    prof.enable()
    prof.disable()

    def f():
        return 1

    assert prof.runcall(f) == 1


def test_enable_preserves_python_settrace():
    seen: list[object] = []

    def py_cb(event, args):
        seen.append((event, args[0], args[1]))

    old = _tealet.settrace(py_cb)
    try:
        prof = Profile()
        prof.enable()
        try:

            def parked(current, _arg):
                resumed = current.main().switch("paused")
                return current.main(), resumed

            t = _tealet.tealet()
            assert t.run(parked, None) == "paused"
        finally:
            prof.disable()
        assert _tealet.gettrace() is py_cb

        def parked_again(current, _arg):
            resumed = current.main().switch("paused")
            return current.main(), resumed

        t2 = _tealet.tealet()
        assert t2.run(parked_again, None) == "paused"
        assert seen
    finally:
        _tealet.settrace(old)


def test_builtins_records_c_call():
    def f():
        return len([1, 2, 3])

    prof = Profile()
    assert prof.runcall(f) == 3
    names = _func_names(prof)
    assert "f" in names
    assert any("len" in name for name in names)

    off = Profile(builtins=False)
    off.runcall(f)
    assert not any("len" in name for name in _func_names(off))


def test_c_callable_labels_match_lsprof():
    import contextvars
    import _lsprof

    def worker(current, main):
        return main.switch("parked")

    def driver():
        child = _tealet.tealet()
        assert child.run(worker, _tealet.main()) == "parked"
        child.switch("back")
        len([1, 2, 3])
        contextvars.Context().run(lambda: 1)

    expected = {
        repr(_tealet.tealet.switch),
        repr(_tealet.tealet.run),
        repr(contextvars.Context.run),
    }

    ls = _lsprof.Profiler()
    ls.enable()
    try:
        driver()
    finally:
        ls.disable()
    ls_names = {entry.code for entry in ls.getstats() if isinstance(entry.code, str)}

    prof = Profile()
    prof.runcall(driver)
    ours = _func_names(prof)
    assert expected <= ls_names
    assert expected <= ours
    assert any("len" in name for name in ours)
    assert any("_tealet.main" in name for name in ours)


def test_timer_must_be_wall_or_thread():
    with pytest.raises(ValueError, match="timer"):
        Profile(timer="cpu")
    prof = Profile(timer="thread")
    assert prof.runcall(lambda: 1) == 1


def _gil_enabled() -> bool:
    fn = getattr(sys, "_is_gil_enabled", None)
    if fn is None:
        return True
    return bool(fn())


def spin_work(min_cpu: float = 0.04) -> float:
    t0 = time.thread_time()
    n = 0
    i = 0
    while time.thread_time() - t0 < min_cpu:
        n += (i * i) & 0xFFFFFFFF
        i += 1
        if i % 32 == 0:
            time.sleep(0)
    return time.thread_time() - t0


def _cumtime(stats: dict, name: str) -> float:
    total = 0.0
    for (_file, _line, func), (_cc, _nc, _tt, ct, _callers) in stats.items():
        if func == name:
            total += ct
    return total


def _two_spinners(timer: str) -> tuple[float, float, float]:
    barrier = threading.Barrier(3)
    own: dict[int, float] = {}
    errors: list[BaseException] = []

    def thread_main() -> None:
        barrier.wait()
        try:
            own[threading.get_ident()] = spin_work()
        except BaseException as exc:
            errors.append(exc)

    prof = Profile(timer=timer)
    prof.enable()
    threads = [threading.Thread(target=thread_main) for _ in range(2)]
    for thread in threads:
        thread.start()
    t0 = time.perf_counter()
    barrier.wait()
    for thread in threads:
        thread.join()
    elapsed = time.perf_counter() - t0
    prof.disable()
    assert errors == []
    assert len(own) == 2
    profile_sum = sum(_cumtime(view.stats, "spin_work") for view in prof.stacks())
    return profile_sum, sum(own.values()), elapsed


def test_wall_timer_gil_slice_does_not_double_count():
    if not _gil_enabled():
        pytest.skip("wall GIL slicing is only on when the GIL is enabled")
    profile_sum, own_sum, elapsed = _two_spinners("wall")
    assert own_sum > 0.02
    assert profile_sum > 0.02
    # one interpreter: billed wall is about elapsed wall, not two overlapping clocks.
    assert profile_sum == pytest.approx(elapsed, rel=0.6)
    assert profile_sum < elapsed * 1.6


def test_wall_timer_free_threaded_counts_each_thread():
    if _gil_enabled():
        pytest.skip("without a GIL, wall time is per-thread elapsed")
    profile_sum, own_sum, elapsed = _two_spinners("wall")
    assert own_sum > 0.02
    assert profile_sum > 0.02
    assert profile_sum > elapsed * 1.2


def test_thread_timer_matches_thread_cpu():
    profile_sum, own_sum, _elapsed = _two_spinners("thread")
    assert own_sum > 0.02
    assert profile_sum > 0.02
    assert profile_sum == pytest.approx(own_sum, rel=0.6)


def test_profiler_id_conflicts_with_cprofile():
    import cProfile

    other = cProfile.Profile()
    other.enable()
    try:
        with pytest.raises(ValueError, match="profiling tool"):
            Profile().enable()
    finally:
        other.disable()
