# Channel timeout hand-off

A channel rendezvous commits the transfer before the parked peer resumes. A `RawTimeoutError` delivered into that peer splits the two sides: one call has already returned success, and the other would raise. The value cannot be put back.

## What is committed

`receive`, finding a parked sender, pops the packet out of the channel and returns it. The sender is only queued. If a timeout then resumes the sender, the packet is no longer in the channel. It is in the receiver's frame, or already with the receiver's caller.

`send`, finding a parked receiver, stores the packet and returns. That return is the sender's success. If a timeout then resumes the receiver, failing `receive` does not fail `send`. Dropping the packet loses the message.

The parked side therefore suppresses that `RawTimeoutError`, so both sides see success. The suppression can hide a broader timeout that was not scoped around the rendezvous.

`Event`, `PulseEvent`, and `CrossThreadEvent` do not do this. A timeout that ties with `set()` propagates. Nothing has been handed to another caller, so letting the timeout win only changes that one turn.

## Suggested approach

Mark the tealet atomic across the hand-off. When a timeout is about to throw into a tealet that is atomic, it does not deliver the exception. It reschedules itself for one turn.

The marker covers the gap in which one side has committed and the other has not resumed past the rendezvous. The hand-off clears the marker before that next turn, so the throw lands once both sides have observed the transfer. The deadline still fires. It does not land in the middle of the commit.

Rolling the transfer back is not available at the point where the timeout would be delivered. The other side has already observed success.
