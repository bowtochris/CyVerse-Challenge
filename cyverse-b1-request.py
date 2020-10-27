#based off of https://github.com/nats-io/nats.py
#https://realpython.com/python-sql-libraries/

import os
import time
import asyncio
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers
import sqlite3

from sqlite3 import Error

def create_connection(path):
    connection = None
    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection
    
async def run(loop):
    nc = NATS()

    await nc.connect("nats://localhost:4222", loop=loop)

    msg = await nc.request("queue_b", str.encode(str("3")), time.time())
    print(bytes.decode(msg.data))
   
    finished = False
    while(not finished):
        finished = True
        for task in asyncio.all_tasks():
            if ((not (task == (asyncio.current_task())) and (not task.done()))):
                finished = False
                task.cancel()
        await asyncio.sleep(1)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))