import sys
import threading
import time

import pytest

import _tealet
from tealet import profile as tealet_profile
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
        assert _tealet.gettrace() is tealet_profile._on_thread_switch
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


def test_two_threads_use_separate_profilers():
    barrier = threading.Barrier(2)
    results: dict[str, set[str]] = {}
    errors: list[BaseException] = []

    def thread_main(label: str) -> None:
        def inner():
            return sum(range(40))

        def worker(current, main):
            inner()
            parked = main.switch(label)
            inner()
            return main, parked

        def driver():
            inner()
            child = _tealet.tealet()
            assert child.run(worker, _tealet.main()) == label
            child.switch("back")

        prof = Profile()
        barrier.wait()
        try:
            prof.runcall(driver)
            results[label] = _func_names(prof)
        except BaseException as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=thread_main, args=("a",)),
        threading.Thread(target=thread_main, args=("b",)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    assert results["a"] >= {"driver", "worker", "inner"}
    assert results["b"] >= {"driver", "worker", "inner"}


def _family_names(prof: Profile) -> dict[str, int]:
    return {view.family[-1]: view.nstacks for view in prof.stack_families()}


def test_same_worker_is_one_stack_family():
    def worker(_current, _arg):
        return _tealet.main()

    def driver():
        for _ in range(3):
            child = _tealet.tealet()
            child.run(worker, None)

    prof = Profile()
    prof.runcall(driver)
    names = _family_names(prof)
    assert names["worker"] == 3
    assert names["driver"] == 1
    assert not any(view.family[-1] == "worker" for view in prof.stacks())
    tid = threading.get_ident()
    for view in prof.stacks():
        assert view.thread_id == tid
        assert view.thread_ids == frozenset({tid})
    worker_family = next(v for v in prof.stack_families() if v.family[-1] == "worker")
    assert worker_family.thread_id == tid
    assert worker_family.thread_ids == frozenset({tid})


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
    assert _family_names(prof)["worker"] == 3


def test_different_workers_are_separate_families():
    def worker_a(_current, _arg):
        return _tealet.main()

    def worker_b(_current, _arg):
        return _tealet.main()

    def driver():
        _tealet.tealet().run(worker_a, None)
        _tealet.tealet().run(worker_b, None)

    prof = Profile()
    prof.runcall(driver)
    names = _family_names(prof)
    assert names["worker_a"] == 1
    assert names["worker_b"] == 1


def test_lambda_same_line_is_one_family():
    def start(child):
        child.run(lambda current, _arg: current.main(), None)

    def driver():
        start(_tealet.tealet())
        start(_tealet.tealet())

    prof = Profile()
    prof.runcall(driver)
    lambdas = [view for view in prof.stack_families() if view.family[-1] == "<lambda>"]
    assert len(lambdas) == 1
    assert lambdas[0].nstacks == 2


def test_combined_and_pstats_families():
    import pstats

    def worker(_current, _arg):
        return _tealet.main()

    def driver():
        _tealet.tealet().run(worker, None)

    prof = Profile()
    prof.runcall(driver)
    combined = prof.combined()
    assert combined.nstacks >= 2
    assert combined.thread_id == threading.get_ident()
    assert combined.thread_ids == frozenset({threading.get_ident()})
    names = {name for (_file, _line, name) in combined.stats}
    assert "driver" in names
    assert "worker" in names
    stats = pstats.Stats(*prof.stack_families())
    assert stats.total_calls > 0


def test_two_threads_same_job_are_one_family():
    def job():
        return sum(range(25))

    prof = Profile()
    errors: list[BaseException] = []

    def thread_main():
        prof.enable()
        try:
            job()
        except BaseException as exc:
            errors.append(exc)
        finally:
            prof.disable()

    threads = [threading.Thread(target=thread_main) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    names = _family_names(prof)
    assert names.get("job") == 2
    job_family = next(v for v in prof.stack_families() if v.family[-1] == "job")
    assert job_family.thread_id is None
    assert len(job_family.thread_ids) == 2


def spin_work(min_cpu: float = 0.04) -> float:
    """Burn thread CPU, yielding often so two workers interleave."""
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


def test_thread_time_matches_two_spinners():
    # default clock is thread_time: each stack's billed time is that thread's
    # CPU, with or without a GIL. one Profile per thread so self.cur is not
    # shared across concurrent setprofile callbacks.
    barrier = threading.Barrier(3)
    own: dict[int, float] = {}
    errors: list[BaseException] = []
    profiles: list[Profile] = []
    lock = threading.Lock()

    def thread_main() -> None:
        barrier.wait()
        prof = Profile()
        prof.enable()
        try:
            own[threading.get_ident()] = spin_work()
        except BaseException as exc:
            errors.append(exc)
        finally:
            prof.disable()
        with lock:
            profiles.append(prof)

    threads = [threading.Thread(target=thread_main) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join()
    assert errors == []
    assert len(own) == 2
    assert len(profiles) == 2
    profile_sum = sum(_cumtime(view.stats, "spin_work") for prof in profiles for view in prof.stacks())
    own_sum = sum(own.values())
    assert own_sum > 0.02
    assert profile_sum > 0.02
    assert profile_sum == pytest.approx(own_sum, rel=0.55)
