# packages/tealetio/examples/qt

Local Qt host for this example. `tealet_qt` lives in this directory. It is
not a workspace member. Do not give it a `pyproject.toml`, and do not put
PySide6 or PyQt6 in `uv.lock`. Promote it to its own package only when it
becomes a standalone project.

`_qt.py` imports PySide6 or PyQt6 when `tealet_qt` is imported. Do not add
either binding to tealetio.

`QtScheduler` is the host. `run_forever` and `run_until_complete` hold
`scheduler.running()` and park in `QApplication.exec`. `QtHost` calls
`turn` from a timer or a queued wakeup. A normal `Runner` drives the
scheduler: `runner.run` and `close` both call `run_until_complete`.
`run(main)` is that session for one callable and passes it the
`QtHost`. The example's `application` is that tealet. `QtHost`
requires a threaded proactor. `tealetSlot` only spawns. The wait
helpers park the tealet that the slot started. A dialog uses `show`
and `finished`. `open` forces window modality, and setting application
modality afterwards hides the box, so `wait_dialog` does not call it.
`running()` stays held for the whole `exec` loop. That is the same
driver context as `run()`: `turn()` requires it, and
`get_running_scheduler()` works between turns. A signal handler on
this thread runs `call_on_scheduler` inline. The slice is not on the
stack, so the wake still posts `set_driver_wakeup`. `wait_call` is
the only path that ends in a nested `exec`, and it runs that call
from a timer after `turn` has returned.

Tests need a Qt binding. `tests/conftest.py` puts this directory on
`sys.path` and sets `QT_QPA_PLATFORM=offscreen` before Qt is imported.
Run them with `uv run --no-sync`, because a sync prunes a `uv pip` install
of PySide6. `ty` does not check this tree.
