# Channel timeout hand-off

A channel rendezvous commits the transfer before the parked peer resumes. A `RawTimeoutError` delivered into that peer splits the two sides: one call has already returned success, and the other would raise. The value cannot be put back.

## What is committed

`receive`, finding a parked sender, pops the packet out of the channel and returns it. The sender is only queued. If a timeout then resumes the sender, the packet is no longer in the channel. It is in the receiver's frame, or already with the receiver's caller.

`send`, finding a parked receiver, stores the packet and returns. That return is the sender's success. If a timeout then resumes the receiver, failing `receive` does not fail `send`. Dropping the packet loses the message.

Rolling the transfer back is not available once the other call can observe it.

`Event`, `PulseEvent`, and `CrossThreadEvent` do not have this gap on their own. A timeout that ties with `set()` propagates. Nothing in `set()` has been handed to another caller. A transfer built on an event opts into the atomic flag itself.

## Atomic flag

`Task` keeps a depth count. `atomic_inc` adds one, including on a different task. `atomic_dec` removes one. `in_atomic` is true while the count is nonzero. `atomic_restore` does not add a level. On exit the count is whatever it was on entry, so several `set` calls can increment it across several waits inside one block, and leaving the block drops all of those levels. A level held before entry stays. The count must not fall below the entry value. An exception inside the block propagates, and the count is still restored on the way out.

The count does not catch exceptions and does not postpone them. `Task.throw` and `cancel` land immediately. `timeout()` is a control-flow policy that reads `in_atomic` and, while it is true, arms the same timer again with `call_later(0)` instead of delivering the throw. The timer has still fired: `_expired` is set, and `reschedule` will not move it.

`AtomicEvent` applies that pair. `set` calls `atomic_inc` on each parked task before it wakes them, and rolls those levels back if `set` raises. `swait` is `atomic_restore` around `Event.swait`. A task that arrives after `set` finds the flag already set, so it is not marked. `Event.set` does not suspend the caller, and neither does `AtomicEvent.set`, so the caller holds no atomic level of its own. The synchronous wait is `swait`. `Event.wait` is the asyncio future and is not this park. The task methods stay public for a transfer that parks on something else.

The channel uses the same pair. It does not mark the wait itself. After the packet has moved, the wake calls `atomic_inc` on the parked task, which may already be inside some other section. That task restores the entry count with `atomic_restore` when its park returns. Preference decides whether the committer then runs the peer immediately. That switch suspends the committer, so the same wake holds the committer in `atomic()` until `run()` returns. Negative preference does this on send. Positive preference does it on receive. The atomic operations do not swap. Only which side remains suspended does. `Event.set` does not need `atomic()`, because the caller of `set` keeps running. `atomic()` adds one level and removes it on exit. Levels a peer adds during the block must already be gone, which `atomic_restore` does for a wait inside it. The marker covers the gap in which one side has committed and the other has not yet resumed past the rendezvous.

The re-armed timer is not in the list this drain already popped, so it waits for a later drain. When the task is still parked and not runnable, that drain is the start of a following batch, before ready tasks. The attempt can still see the marker and arm itself once more. The parked task runs after that drain and clears the marker. The drain after that resume delivers the throw. Both sides have observed the transfer by then.

`Timeout.__exit__` cancels the current handle. That is the re-armed timer when the deadline has already fired, so a block that ends before the later drain does not throw. A `timeout()` wrapped only around the rendezvous therefore still succeeds when the timer ties with the commit. `expired()` stays true, because the timer did fire. If the hand-off finished before the timer ran, the original handle is cancelled and `expired()` stays false. An outer `timeout()` that continues past the rendezvous is still open, so the throw lands on the next park inside that block. `reschedule` does not extend a timeout once it has fired.

`Task.throw` does not consult the marker. `cancel()` must still abort a wait immediately. The parked `send` and `receive` still suppress a `RawTimeoutError` that has already landed once the peer has committed. On `send`, the throw unlinks the waiter before the `except` runs, and that unlink drops the packet, so a missing packet counts as the peer's commit only while the atomic mark is set. A timeout during the wait still propagates. Suppressing a throw that has already landed can hide a broader timeout that was not scoped around the rendezvous. `timeout()` no longer depends on that suppression for this gap.
