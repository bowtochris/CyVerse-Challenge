#based off of https://github.com/nats-io/nats.py

import time
import asyncio
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

async def run(loop):
    try:
        nc = NATS()

        await nc.connect("nats://localhost:4222", loop=loop)

        last = 0.0

        async def message_handler(msg):
            subject = msg.subject
            reply = msg.reply
            data = msg.data.decode()
            last = time.time()
            print("Received a message on '{subject} {reply}': {data}".format(
                subject=subject, reply=reply, data=data))

        # Simple publisher and async subscriber via coroutine.
        sid = await nc.subscribe("norris", cb=message_handler)

        last = time.time()

        while(True):
            if((time.time() - last) > 5.0):
                break
            try:
                # Flush connection to server, returns when all messages have been processed.
                # It raises a timeout if roundtrip takes longer than 1 second.
                await nc.flush(1)
            except ErrTimeout:
                print("Flush timeout")
                break
            await asyncio.sleep(1)

        finished = False
        while(not finished):
            finished = True
            for task in asyncio.all_tasks():
                if ((not (task == (asyncio.current_task())) and (not task.done()))):
                    finished = False
                    task.cancel()
            await asyncio.sleep(1)
    finally:
            print("Done")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))