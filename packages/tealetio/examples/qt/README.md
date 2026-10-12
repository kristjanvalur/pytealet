# Qt on tealetio

A PySide6 window whose thread is `QApplication.exec`. The scheduler does not
call `run()`. `QtHost`, from the local `tealet_qt` package in this directory,
calls `scheduler.turn()` when a queued signal or a timer fires.

The idea comes from [qasync](https://github.com/CabbageDevelopment/qasync):
Qt keeps the thread, and a slot starts work that can wait.

A Qt slot is usually an event handler. It returns at once, and the next step
is another slot. `tealetSlot` returns at once too, and the body is a task.
That task can park, so the job is ordinary straight-line code.

```python
@tealetSlot()
def _on_run(self) -> None:
    if self.job_running:
        return
    run_job(self, _FLOW_PAUSE)
```

`run_job` asks whether to start, sleeps while the label says "working", asks
whether to keep the result, sleeps while it says "saving", then says the job
is done. Each question is `message_box`, which parks until `QDialog.finished`.
The box is application-modal, and the thread stays in the host loop, so the
other tasks keep running. Between questions the click counter and the text
field take input again.

The window shows three loops at once:

- a clock, updated once a second, including the title of an open question
- a fetch of one JSON todo from `jsonplaceholder.typicode.com`
- the job started by **Run job**

`tealet_qt` is not a workspace member. PySide6 is not a tealetio dependency,
and it is not in the lock. A later `uv sync` removes a `uv pip` install, so
the commands below pass `--no-sync`.

From this directory:

```bash
uv pip install PySide6
uv run --no-sync python app.py
```

From the repository root, use `python packages/tealetio/examples/qt/app.py`
in that same `uv run`.

`--self-test` uses the offscreen Qt platform, skips the network fetch, clicks
Ok, Yes, Ok through the job, and checks that the label ends at `job: done`.
It also checks that the first turn painted the clock.

```bash
uv run --no-sync python app.py --self-test
```

The helper tests use the offscreen platform as well:

```bash
uv run --no-sync python -m pytest tests/ -q
```
