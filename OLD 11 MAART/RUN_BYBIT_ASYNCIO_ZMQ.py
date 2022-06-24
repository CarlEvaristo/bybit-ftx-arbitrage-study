from pybit import HTTP, WebSocket
import os
import json
import traceback
import zmq
import asyncio
import zmq.asyncio

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # python-3.8.0a4

api_key = os.environ.get("API")
api_secret = os.environ.get("SECRET")
symbol = "BTCUSD"
ctx = zmq.asyncio.Context.instance()

async def sender() -> None:
    # Perps endpoints:
    endpoint_public = 'wss://stream.bybit.com/realtime_public'
    endpoint_private = 'wss://stream.bybit.com/realtime_private'

    # Connect to the Bybit auth spot websocket (subscriptions are not required)
    try:
        private_feed = WebSocket(endpoint=endpoint_private,
                                subscriptions=['order'],
                                api_key=api_key,
                                api_secret=api_secret,
                                ping_interval=25,
                                ping_timeout=15)
    except Exception as e:
        print(e)

    # context = zmq.Context()
    socket = ctx.socket(zmq.PUSH)
    socket.bind("tcp://127.0.0.1:5557")
    print("Connecting to server...")
    ORDERSTATUS = {}

    while 1:
        try:
            message = private_feed.fetch('order')
            if message != [] and message != {}:
                if message[-1] != ORDERSTATUS:
                    ORDERSTATUS = message[-1]
                    message = json.dumps(message[-1]["order_status"])
                    await socket.send_string(message)
                    print(f"SEND MESSAGE: {message}")

        except Exception as e:
            print("Something wrong with the WS feed!: " + str(e))
            traceback.print_exc()
            await asyncio.sleep(1)

        await asyncio.sleep(0.001)


async def receiver() -> None:
    # context = zmq.Context()
    print("Connecting to server...")
    socket = ctx.socket(zmq.PULL)
    socket.connect("tcp://127.0.0.1:5557")

    while 1:
        message = await socket.recv_json()
        # message = json.dumps(message)
        print(f"RECEIVED MESSAGE: {message}")
        await asyncio.sleep(0.001)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(asyncio.wait(
        [receiver(),sender()]
    ))


# # Define a main async method (our program)
# async def main():
#     # Run both print method and wait for them to complete (passing in asyncState)
#     await asyncio.gather(sender(), receiver())
#
# if __name__ == "__main__":
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(main())
#     loop.run_forever()

