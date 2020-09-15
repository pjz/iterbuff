import time
import asyncio

from iterbuff import bufferable, fbufferable

#@bufferable(1)
async def gen_1():
    for value in range(0, 10):
        await asyncio.sleep(1)  # Could be a slow HTTP request
        yield value

#@bufferable(1)
async def gen_2(it):
    async for value in it:
        await asyncio.sleep(1)  # Could be a slow HTTP request
        yield value * 2

#@bufferable(1)
async def gen_3(it):
    async for value in it:
        await asyncio.sleep(1)  # Could be a slow HTTP request
        yield value + 3

async def main():
    it_1 = gen_1()
    it_2 = gen_2(it_1)
    it_3 = gen_3(it_2)

    async for val in it_3:
        print(val)

async def roughtime(coro):
    start = time.time()
    await coro
    end = time.time()
    diff = end-start
    print(f'Took about {diff} secs')


asyncio.run(roughtime(main()))


