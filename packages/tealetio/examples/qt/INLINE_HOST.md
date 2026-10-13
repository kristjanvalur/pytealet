# Future Qt host notes

This example is not a production package. Two later shapes are recorded
here. Neither is part of `QtHost` today.

## `QScheduler` in the shape of `QEventLoop`

A production `QScheduler.run_until_complete(main)` can follow
`qasync.QEventLoop.run_until_complete` (v0.28.0). Create the task, call
`QApplication.exec`, and `exit` when the task's done callback runs.
`main` waits on the window. `run()` in the sense "return when the
scheduler is idle" is the wrong lifetime: the process stays up because
the window is open. `tealet_qt.run` is this host already, under a
different name.

`exec` and `run_until_complete` live on the main tealet. Qt waits
there. The deadline timer and the queued kick call `turn()`. `turn()`
polls and, when a task is runnable, switches onto that task's stack.
A park or the end of the batch switches back to the main tealet,
`turn()` returns to the slot, and the slot returns to `exec`. The wait
is Qt's poll on the main tealet. User tasks are the other stacks. The
callback drain at the start of a slice also runs on the main tealet,
before that switch. This path does not call `_wait_thread`, and it does
not keep the scheduler's `while` loop on the stack across the wait.

That is the same shape as `QEventLoop`. qasync's `exec` is the loop,
and a timer or a ready socket runs an asyncio callback and returns to
`exec`. On Windows one `QThread` waits on the IOCP and posts the
completion back as a signal; `exec` is still the loop. Here the pump
is `turn()` instead of that callback. The scheduler and its proactor
stay in place underneath.

After `exec`, qasync runs one `processEvents` and raises if the future
is still pending. The future is usually already done, because its
callback ran inside `exec` and that callback is what called `exit`.
Closing our last window quits `exec` before the tealet parked on
`finished` has been turned, and `aboutToQuit` has already stopped the
host, so `tealet_qt.run` calls `turn` once more after `exec`. If the
task finishes in the slice before `exec`, skip `exec`: `exit` has
already been called.

## Inline proactor

The example keeps a threaded proactor. A completion thread posts
`set_driver_wakeup`, and that queued call is what brings Qt back. The
rest of this note is a later option for an inline proactor, where the
Qt thread would be the only reaper.

`SelectorProactor` and `UringProactor(completion_threads=0)` reap inside
`wait()` on the calling thread. Under `QApplication.exec` that thread is
parked in Qt's poll. A completion wakes Qt when the fd Qt is blocked on
is the one that becomes ready.

## Inline uring

`UringProactor.ring.fd` is the io_uring file descriptor. On an inline
proactor the Qt thread is the only reaper, and that fd is readable when
the completion queue has events. A `QSocketNotifier` for read puts the
ring in Qt's poll. The slot disables the notifier, calls `wait(0)`
(`ring.wait(0)`, which reaps and flushes submissions this thread may
publish), then enables the notifier again while the proactor is open.

That is the same wake qasync uses for a socket: readiness is a Qt event.

Keep this on `inline_completions`. The default `UringProactor` already
has service threads as the reapers; a second waiter on `ring.fd` races
with them. The default ring is interrupt-driven. An `IOPOLL` ring does
not signal poll on `ring.fd`.

`QSocketNotifier` is level-triggered. Leave it disabled until `wait(0)`
has reaped, or a queue that is still ready spins the dispatcher. qasync
does the same around its notifier callback.

`turn` still publishes submissions and harvests anything already queued.
The notifier covers a completion that arrives after that slice.

## Selector

A selector has no single fd that stands for every socket. `ring.fd` has
no equivalent there.

`SelectorProactor` already accepts a `selectors.BaseSelector`, and
`wait` calls `selector.select()` before `_step_fd_operation`. A Qt
wrapper can implement `register`, `modify`, and `unregister` with one
`QSocketNotifier` per interest. It still needs a different wait
contract, because a blocking `select()` would park the Qt thread inside
the proactor. qasync's selector raises from `select` and reports
readiness from the notifier slot.

Two contracts fit the current `_poll` loop:

- `select(0)` returns events the notifiers have latched since the last
  call, and a positive timeout is refused. The host blocks in Qt.
  `turn` then `wait(0)` steps the latched fds through `_poll`.
- Or `ready(fd, mask)` runs that same step without `select`.

The wrapper owns the notifiers. On activation it latches the fd,
disables the notifier, and asks the host to `turn`. After the step it
enables the notifier again while the fd stays registered. The
proactor's socketpair wakeup is a notifier too, so `wake_wait` wakes Qt
through that fd.

`ThreadedSelectorProactor` already blocks in `select` on a worker and
posts the driver wakeup. It has no need of this wrapper. Its `close`
sets the stop flag and writes the socketpair, so the worker's `select`
returns and the thread can exit.
