#based off of https://github.com/nats-io/nats.py#basic-usage
#Also uses info from
#https://realpython.com/python-requests/

import json
import time
import requests
import asyncio
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

async def run(loop):
    nc = NATS()

    await nc.connect("nats://localhost:4222", loop=loop)

    async def message_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("Received a message on '{subject} {reply}': {data}".format(
            subject=subject, reply=reply, data=data))

    response = requests.get('https://api.icndb.com/jokes/random/3')

    if response and ('value' in response.json()):
        norris = response.json()['value']
        for i in norris:
            await nc.publish("norris", str.encode(str(i['joke'])))
            try:
                # Flush connection to server, returns when all messages have been processed.
                # It raises a timeout if roundtrip takes longer than 1 second.
                await nc.flush(1)
            except ErrTimeout:
                print("Flush timeout")
                break
    else:
         print(response.text)
        
    # Terminate connection to NATS.
    await nc.close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.close()