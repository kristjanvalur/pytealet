# Channel timeout hand-off

A channel rendezvous commits the transfer before the parked peer resumes. A `RawTimeoutError` delivered into that peer splits the two sides: one call has already returned success, and the other would raise. The value cannot be put back.

## What is committed

`receive`, finding a parked sender, pops the packet out of the channel and returns it. The sender is only queued. If a timeout then resumes the sender, the packet is no longer in the channel. It is in the receiver's frame, or already with the receiver's caller.

`send`, finding a parked receiver, stores the packet and returns. That return is the sender's success. If a timeout then resumes the receiver, failing `receive` does not fail `send`. Dropping the packet loses the message.

Rolling the transfer back is not available once the other call can observe it.

`Event`, `PulseEvent`, and `CrossThreadEvent` do not have this gap. A timeout that ties with `set()` propagates. Nothing has been handed to another caller.

## Atomic section

`Task._atomic` is a depth count. `Timeout._timeout` does not throw into a task while the count is nonzero. The timer has still fired: `_expired` is set, and `reschedule` will not move it. The throw is queued with `call_soon` for a later callback drain.

`Task._atomic_section` enters on entry and exits on exit. It marks a stretch of that task's own code, and sections nest. On exit the depth is one higher than on entry, and that level is released.

The channel does not wrap the wait in `_atomic_section`. After the packet has moved it calls `_enter_atomic` on the parked task, which may already be inside some other section. `Task._atomic_exit` records the depth at entry. On exit the count is that depth, or one higher. Unchanged means the peer never committed, so a timeout during the wait still throws. One higher means the peer committed, and the exit drops that one level, leaving the outer depth in place. Any other change is an enter/exit bug. The marker covers the gap in which one side has committed and the other has not yet resumed past the rendezvous. It does not cover the wait itself.

`call_soon` from a drain does not run in that same drain. The next drain is the start of the following batch, and that drain runs before ready tasks. The deferred attempt can therefore still see the marker and queue itself once more. The parked task runs after that drain and clears the marker. The drain after that resume delivers the throw. Both sides have observed the transfer by then.

`Timeout.__exit__` sets a closed flag before it cancels the timer handle. `call_soon` is not cancellable, so a deferred callback after the `with` block has left sees the flag and does nothing. A `timeout()` wrapped only around the rendezvous therefore still succeeds when the timer ties with the commit: the block ends and the deferred throw is dropped. `expired()` stays true, because the timer did fire. If the hand-off finished before the timer ran, the handle is cancelled and `expired()` stays false. An outer `timeout()` that continues past the rendezvous is still open, so the throw lands on the next park inside that block. `reschedule` does not extend a timeout once it has fired.

`Task.throw` does not consult the marker. `cancel()` must still abort a wait immediately. The parked `send` and `receive` still suppress a `RawTimeoutError` that has already landed once the peer has committed. On `send`, the throw unlinks the waiter before the `except` runs, and that unlink drops the packet, so a missing packet counts as the peer's commit only while the atomic mark is set. A timeout during the wait still propagates. Suppressing a throw that has already landed can hide a broader timeout that was not scoped around the rendezvous. `timeout()` no longer depends on that suppression for this gap.
