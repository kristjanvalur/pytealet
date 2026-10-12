# packages/tealetio/examples/qt

Local Qt host for this example. `tealet_qt` lives in this directory. It is
not a workspace member. Do not give it a `pyproject.toml`, and do not put
PySide6 or PyQt6 in `uv.lock`. Promote it to its own package only when it
becomes a standalone project.

`_qt.py` imports PySide6 or PyQt6 when `tealet_qt` is imported. Do not add
either binding to tealetio.

`run(main)` is the boilerplate host: `QApplication`, scheduler, `QtHost`,
spawn `main` as one tealet, `exec`. The example's `application` is that
tealet. `QtHost` is the driver (`turn` plus `set_driver_wakeup`). `tealetSlot` only
spawns. The wait helpers park the tealet that the slot started. A dialog
uses `show` and `finished`. `open` forces window modality, and setting
application modality afterwards hides the box, so `wait_dialog` does not
call it. A signal handler between turns cannot call `Event.set`; the wake
goes through `call_on_scheduler`. `wait_call` is the only path that ends
in `exec`, and it runs that call from a timer after `turn` has returned.

Tests need a Qt binding. `tests/conftest.py` puts this directory on
`sys.path` and sets `QT_QPA_PLATFORM=offscreen` before Qt is imported.
Run them with `uv run --no-sync`, because a sync prunes a `uv pip` install
of PySide6. `ty` does not check this tree.
