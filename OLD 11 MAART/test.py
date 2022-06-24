import datetime
import zmq
import asyncio
import zmq.asyncio
import json
from timeit import default_timer as timer
# import logging
# logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)    # dit heb ik toegevoegd om te loggen wat hij doet of zo

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # python-3.8.0a4  --> to prevent an asyncio zmq error

ctx = zmq.asyncio.Context.instance()


async def sender1():
    socket = ctx.socket(zmq.PUSH)
    socket.connect("tcp://127.0.0.1:5557")
    while 1:
        FTX_START = timer()
        message = f"Hello World. {timer()}"
        message = json.dumps(message)
        await socket.send_string(message)


async def sender2():
    socket = ctx.socket(zmq.PUSH)
    socket.connect("tcp://127.0.0.1:5557")
    while 1:
        message = f"HALLO. {timer()}"
        message = json.dumps(message)
        await socket.send_string(message)


async def receiver():
    socket = ctx.socket(zmq.PULL)
    socket.bind("tcp://127.0.0.1:5557")
    while 1:
        message = await socket.recv_json()
        print(f"RECEIVED MESSAGE: {message}")
        await asyncio.sleep(0.001)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(asyncio.wait(
        [sender1(), sender2(), receiver()]
    ))