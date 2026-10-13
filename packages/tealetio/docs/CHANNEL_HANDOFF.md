# Channel timeout hand-off

A channel rendezvous commits the transfer before the parked peer resumes. An exception delivered into that peer splits the two sides: one call has already returned success, and the other raises. The value cannot be put back.

## What is committed

`receive`, finding a parked sender, pops the packet out of the channel and returns it. The sender is only queued. A throw into that sender cannot put the packet back. It is in the receiver's frame, or already with the receiver's caller.

`send`, finding a parked receiver, stores the packet and returns. That return is the sender's success. A throw into the receiver then fails `receive` without failing `send`. Dropping the packet loses the message.

Rolling the transfer back is not available once the other call can observe it.

`timeout()` is delivered when its timer runs, including during the hand-off. `cancel()` and any other `Task.throw` land in the call when they are delivered. An exception injected into `send` or `receive` by `timeout()`, `cancel()`, or another `Task.throw` carries `operation_result`, a `(success, value, exception)` triple. It did not originate with the sender. A `send_exception` payload raised by a normal `receive` does not carry the attribute. It originated with the sender. `success` is true once the packet has moved. On `receive`, `value` is the object and `exception` is `None`, or `exception` is the instance passed to `send_exception` and `value` is `None`. That payload still has no attribute of its own. On `send` both are `None`. The channel has dropped its own copy. A caller that still needs the object calls `Channel.operation_result` and re-raises that exception. A sender puts the object back on its own queue only when `success` is false. `tealetio.examples.send_or_requeue` is that handler, and the Channel section of `PYTHON_API.md` shows the call. After the packet has moved, cancelling the sender leaves the value with the receiver and fails `send`, so a retry delivers a second copy. Cancelling the receiver puts the object on that exception. A sender cancelled before anyone takes the packet reports `success` false, and the offer is dropped. A receiver cancelled before a packet arrives reports `success` false, and both payload fields are `None`. The channel stays usable afterwards.

`Event`, `PulseEvent`, and `CrossThreadEvent` do not have this gap on their own. A timeout that ties with `set()` propagates. Nothing in `set()` has been handed to another caller.

## Timeout

`expired()` stays true once the timer has fired, and `reschedule` will not move it. `Timeout.__exit__` cancels the current handle. A block that ends before the timer runs does not throw, and `expired()` stays false. When `send` or `receive` returns before the timer runs, a `timeout()` that is still open lands on the next park inside that block. When the timer runs while the call is still inside `send` or `receive`, the call raises and `operation_result` says whether the packet had moved.

An immediate wake suspends the committer inside the peer's `run()`. If that switch returns to the driver before the committer resumes, a due timer is delivered there. The peer has already taken the packet, so the call raises with `success` true.

The channel does not catch `RawTimeoutError`. The handler sits on the call itself, inside `with timeout()` when a timeout is in use, so it sees the original exception. `Channel.operation_result(exc)` returns the triple from `exc`, then `__cause__`, then `__context__`, or `None` when it is not there. Leaving the `with` raises a new `TimeoutError`, and the triple stays on `__cause__`. On Python 3.10, asyncio can replace `CancelledError` and leave the annotated instance as `__context__`.

A batch drains ready callbacks, then due timers, once, before its tasks. A `call_soon` that completes the other side therefore runs before a timer that is already due. The packet can move, and the timeout can then land in the call that is still parked.

## Rendezvous

`Rendezvous` in `tealetio.examples.misc` illustrates a one-slot exchange. It is not a library primitive. The sender treats the object as delivered only after the receiver has accepted it. The side that arrives first creates the hand-off and parks on a plain `Event`. The sender, if it arrives second, writes the object into that hand-off and parks until the receiver answers. A normal wake is acceptance: the receiver records that answer and returns the object. If the receiver is resumed by an exception after the object was handed over, it answers with rejection and the sender raises `RuntimeError`. If the sender is resumed by an exception before it sees an answer, it raises that exception and does not report delivery. The receiver may still take the object. A timeout while one side is still the only waiter withdraws the offer. Postponing `timeout()` or `cancel()` is not part of this protocol. The slot is cleared before the arriving sender waits for the answer, so the next exchange can start once the receiver has left the call. On the success path, `Event.set` only queues the waiter, so the receiver observes the object before `send` returns. A `timeout(0)` around `send` can fire in that answer wait: the driver is often ahead of the queued receiver, the throw lands before the receiver runs, and `send` raises without reporting delivery. The receiver then still takes the object.
