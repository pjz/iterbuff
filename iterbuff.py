"""
Inspired by https://github.com/michalc/asyncio-buffered-pipeline

the goal of iterbuf is to allow buffering of individual async iterators

"""

import asyncio
from functools import wraps
from collections import namedtuple

EOQ = object()  # sentinel object since asyncio.Queues aren't .close()-able

BufferedException = namedtuple('BufferedException', 'exception')  # simple wrapper object

class bufferable:

    def __init__(self, maxsize=1):
        """
        Configure the buffer size
        """
        self.maxsize = maxsize

    def __call__(self, decorated):
        """wrap the decorated function"""

        @wraps(decorated)
        async def wrapped(*a, **kw):
            # set up the queue here so it's in the same run-loop as the task
            self.q = asyncio.Queue(maxsize=self.maxsize)
            self.not_full = asyncio.Event()
            task = asyncio.create_task(self._producer(decorated(*a, **kw)))
            try:
                while True:
                    item = await self.q.get()
                    self.not_full.set()
                    if item == EOQ:
                        return
                    if isinstance(item, BufferedException):
                        raise item.exception from None
                    yield item
                    self.q.task_done()
            finally:
                # kill the task (a no-op if it's dead already)
                task.cancel()
                # wait for it to die
                await task

        return wrapped

    async def _producer(self, coro_iter):
        """
        producer task
        - runs the passed-in iterator, putting results on self.q
        - sends raised exceptions wrapped in a BufferedException
        - sends EOQ when done
        """
        try:
            async for item in coro_iter:
                await self.q.put(item)
                if self.q.full():
                    self.not_full.clear()
                    await self.not_full.wait()
        except BaseException as e:
            await self.q.put(BufferedException(e))
        else:
            await self.q.put(EOQ)



def fbufferable(maxsize=1):
    """
    bufferable, but written as a function instead of an object
    """

    def wrap(decorated):

        @wraps(decorated)
        async def wrapped(*a, **kw):

            q = asyncio.Queue(maxsize=maxsize)

            async def producer(coro_iter):
                ''' feed the queue '''
                try:
                    async for item in coro_iter:
                        await q.put(item)
                except BaseException as e:
                    await q.put(BufferedException(e))
                else:
                    await q.put(EOQ)

            # start the producer
            task = asyncio.create_task(producer(decorated(*a, **kw)))
            # now wait for it to produce
            try:
                while True:
                    item = await q.get()
                    if item == EOQ:
                        return
                    if isinstance(item, BufferedException):
                        raise item.exception from None
                    yield item
                    q.task_done()
            finally:
                # kill the task (a no-op if it's dead already)
                task.cancel()
                # wait for it to die
                await task

        return wrapped
    return wrap

