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
    connection = None
    try:
        init = not os.path.exists("norris.db")
        connection = create_connection("norris.db")
        if init:
            connection.execute("create table Norris (subject varchar(255), reply varchar(255), data varchar(255), last float(53));")
            connection.commit()
            
        nc = NATS()

        await nc.connect("nats://localhost:4222", loop=loop)

        last = 0.0

        async def message_handler_norris(msg):
            subject = msg.subject
            reply = msg.reply
            data = msg.data.decode()
            last = time.time()
            print("Received a message on '{subject} {reply}': {data}".format(
                subject=subject, reply=reply, data=data))
            cmd = "insert into Norris values (\"{subject}\", \"{reply}\", \"{data}\", {last})".format(
                subject=subject, reply=reply, data=data, last=last)
            connection.execute(cmd)
            connection.commit()

        async def message_handler_b(msg):
            subject = msg.subject
            reply = msg.reply
            data = msg.data.decode()
            last = time.time()
            print("Received a message on '{subject} {reply}': {data}".format(
                subject=subject, reply=reply, data=data))
            try:
                num_responses = int(data)
                cmd = "select data from Norris order by last desc limit " + str(num_responses)
                val = connection.execute(cmd)
                connection.commit()
                resp = ""
                for line in val.fetchall():
                   resp = resp + "\n" + line[0]
                await nc.publish(reply, str.encode(str(resp.strip())))
            except Exception as e:
                resp = ""
                resp = resp + "\n" + "Malfored Request!"
                resp = resp + "\n" + str(e)
                resp = resp + "\n" + str(msg)
                await nc.publish(reply, str.encode(str(resp.strip())))

        # Simple publisher and async subscriber via coroutine.
        sid_norris = await nc.subscribe("norris", cb=message_handler_norris)
        sid_reply = await nc.subscribe("queue_b", cb=message_handler_b)

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
        if not connection == None:
            connection.close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))