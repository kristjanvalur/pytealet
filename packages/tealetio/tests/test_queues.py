import asyncio

import pytest

from tealetio import (
    Event,
    LifoQueue,
    PriorityQueue,
    Queue,
    QueueEmpty,
    QueueFull,
    QueueShutDown,
)

from helpers import new_scheduler as _new_scheduler


class TestQueueExamples:
    def test_queue_fifo_order(self):
        q: Queue[int] = Queue()
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)

        assert q.get_nowait() == 1
        assert q.get_nowait() == 2
        assert q.get_nowait() == 3

    def test_queue_nowait_errors(self):
        q: Queue[int] = Queue(maxsize=1)
        with pytest.raises(QueueEmpty):
            q.get_nowait()

        q.put_nowait(1)
        with pytest.raises(QueueFull):
            q.put_nowait(2)

    def test_queue_put_get_with_scheduler_blocking(self):
        s = _new_scheduler()
        q: Queue[int] = Queue(maxsize=1)
        seen: list[str] = []

        def producer() -> None:
            q.sput(1)
            seen.append("put:1")
            q.sput(2)
            seen.append("put:2")

        def consumer() -> None:
            s.yield_()
            seen.append(f"get:{q.sget()}")
            s.yield_()
            seen.append(f"get:{q.sget()}")

        s.spawn(producer)
        s.spawn(consumer)
        s.run()

        assert seen == ["put:1", "get:1", "put:2", "get:2"]

    def test_queue_join_and_task_done(self):
        s = _new_scheduler()
        q: Queue[int] = Queue()
        produced_evt = Event()
        seen: list[str] = []

        def producer() -> None:
            # Let other spawned tasks start so producer exit does not try
            # to hand off directly to an unstarted tealet.
            s.yield_()
            q.sput(1)
            q.sput(2)
            seen.append("produced")
            produced_evt.set()

        def consumer() -> None:
            s.yield_()
            q.sget()
            q.task_done()
            seen.append("done:1")
            q.sget()
            q.task_done()
            seen.append("done:2")

        def waiter() -> None:
            produced_evt.swait()
            q.sjoin()
            seen.append("joined")

        s.spawn(producer)
        s.spawn(consumer)
        s.spawn(waiter)
        s.run()

        assert seen == ["produced", "done:1", "done:2", "joined"]

    def test_queue_task_done_underflow_raises(self):
        q: Queue[int] = Queue()
        with pytest.raises(ValueError, match=r"task_done\(\) called too many times"):
            q.task_done()

    def test_queue_shutdown_graceful_drains_existing_items(self):
        q: Queue[int] = Queue()
        q.put_nowait(1)
        q.put_nowait(2)

        q.shutdown()

        with pytest.raises(QueueShutDown):
            q.put_nowait(3)
        assert q.get_nowait() == 1
        q.task_done()
        assert q.get_nowait() == 2
        q.task_done()
        q.sjoin()
        with pytest.raises(QueueShutDown):
            q.get_nowait()

    def test_queue_shutdown_immediate_drains_and_unblocks_join(self):
        q: Queue[int] = Queue()
        q.put_nowait(1)
        q.put_nowait(2)

        q.shutdown(immediate=True)

        assert q.empty()
        q.sjoin()
        with pytest.raises(QueueShutDown):
            q.get_nowait()
        with pytest.raises(QueueShutDown):
            q.put_nowait(3)
        with pytest.raises(ValueError, match=r"task_done\(\) called too many times"):
            q.task_done()

    def test_queue_shutdown_immediate_wakes_blocked_sync_joiner(self):
        s = _new_scheduler()
        q: Queue[int] = Queue()
        q.put_nowait(1)
        q.put_nowait(2)
        seen: list[str] = []

        def waiter() -> None:
            q.sjoin()
            seen.append("joined")

        def closer() -> None:
            s.yield_()
            q.shutdown(immediate=True)
            seen.append("shutdown")

        s.spawn(waiter)
        s.spawn(closer)
        s.run()

        assert seen == ["shutdown", "joined"]

    def test_queue_shutdown_wakes_blocked_sync_getter(self):
        s = _new_scheduler()
        q: Queue[int] = Queue()
        seen: list[str] = []

        def consumer() -> None:
            try:
                q.sget()
            except QueueShutDown:
                seen.append("getter:shutdown")

        def closer() -> None:
            s.yield_()
            q.shutdown()
            seen.append("shutdown")

        s.spawn(consumer)
        s.spawn(closer)
        s.run()

        assert seen == ["shutdown", "getter:shutdown"]

    def test_queue_shutdown_wakes_blocked_sync_putter(self):
        s = _new_scheduler()
        q: Queue[int] = Queue(maxsize=1)
        q.put_nowait(1)
        seen: list[str] = []

        def producer() -> None:
            try:
                q.sput(2)
            except QueueShutDown:
                seen.append("putter:shutdown")

        def closer() -> None:
            s.yield_()
            q.shutdown()
            seen.append("shutdown")

        s.spawn(producer)
        s.spawn(closer)
        s.run()

        assert seen == ["shutdown", "putter:shutdown"]

    def test_queue_asyncio_put_get(self):
        q: Queue[int] = Queue(maxsize=1)
        seen: list[str] = []

        async def producer() -> None:
            await q.put(1)
            seen.append("put:1")
            await q.put(2)
            seen.append("put:2")

        async def consumer() -> None:
            await asyncio.sleep(0)
            seen.append(f"get:{await q.get()}")
            await asyncio.sleep(0)
            seen.append(f"get:{await q.get()}")

        async def run() -> None:
            await asyncio.gather(producer(), consumer())

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))
        assert seen == ["put:1", "get:1", "put:2", "get:2"]

    def test_queue_asyncio_join(self):
        q: Queue[int] = Queue()
        seen: list[str] = []

        async def producer() -> None:
            await q.put(1)
            await q.put(2)
            seen.append("produced")

        async def consumer() -> None:
            await asyncio.sleep(0)
            await q.get()
            q.task_done()
            seen.append("done:1")
            await asyncio.sleep(0)
            await q.get()
            q.task_done()
            seen.append("done:2")

        async def waiter() -> None:
            await q.join()
            seen.append("joined")

        async def run() -> None:
            await asyncio.gather(producer(), consumer(), waiter())

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))
        assert seen == ["produced", "done:1", "done:2", "joined"]

    def test_queue_shutdown_wakes_blocked_asyncio_getter(self):
        q: Queue[int] = Queue()
        seen: list[str] = []

        async def consumer() -> None:
            try:
                await q.get()
            except QueueShutDown:
                seen.append("getter:shutdown")

        async def run() -> None:
            task = asyncio.create_task(consumer())
            await asyncio.sleep(0)
            q.shutdown()
            seen.append("shutdown")
            await task

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))
        assert seen == ["shutdown", "getter:shutdown"]

    def test_queue_shutdown_wakes_blocked_asyncio_putter(self):
        q: Queue[int] = Queue(maxsize=1)
        q.put_nowait(1)
        seen: list[str] = []

        async def producer() -> None:
            try:
                await q.put(2)
            except QueueShutDown:
                seen.append("putter:shutdown")

        async def run() -> None:
            task = asyncio.create_task(producer())
            await asyncio.sleep(0)
            q.shutdown()
            seen.append("shutdown")
            await task

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))
        assert seen == ["shutdown", "putter:shutdown"]

    def test_queue_shutdown_immediate_wakes_blocked_asyncio_joiner(self):
        q: Queue[int] = Queue()
        q.put_nowait(1)
        q.put_nowait(2)
        seen: list[str] = []

        async def waiter() -> None:
            await q.join()
            seen.append("joined")

        async def run() -> None:
            task = asyncio.create_task(waiter())
            await asyncio.sleep(0)
            q.shutdown(immediate=True)
            seen.append("shutdown")
            await task

        asyncio.run(asyncio.wait_for(run(), timeout=1.0))
        assert seen == ["shutdown", "joined"]

    def test_priority_queue_order(self):
        q: PriorityQueue[tuple[int, str]] = PriorityQueue()
        q.put_nowait((2, "b"))
        q.put_nowait((1, "a"))
        q.put_nowait((3, "c"))

        assert q.get_nowait() == (1, "a")
        assert q.get_nowait() == (2, "b")
        assert q.get_nowait() == (3, "c")

    def test_lifo_queue_order(self):
        q: LifoQueue[int] = LifoQueue()
        q.put_nowait(1)
        q.put_nowait(2)
        q.put_nowait(3)

        assert q.get_nowait() == 3
        assert q.get_nowait() == 2
        assert q.get_nowait() == 1
