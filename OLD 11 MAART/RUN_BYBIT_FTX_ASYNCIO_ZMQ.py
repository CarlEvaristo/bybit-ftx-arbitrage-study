from pybit import HTTP, WebSocket  # for bybit
import time
from timeit import default_timer as timer
import os
import json
import traceback
import hmac
import numpy as np
import websocket_code  # for ftx
import websockets  # for ftx
import zmq
import asyncio
import zmq.asyncio
import logging

logging.basicConfig(format='%(asctime)s - %(message)s',
                    level=logging.DEBUG)  # dit heb ik toegevoegd om te loggen wat hij doet of zo

asyncio.set_event_loop_policy(
    asyncio.WindowsSelectorEventLoopPolicy())  # python-3.8.0a4  --> to prevent an asyncio zmq error

# ------------ BYBIT ------------------------------
api_key_bybit = os.environ.get("API_BYBIT")
api_secret_bybit = os.environ.get("SECRET_BYBIT")
# ------------ FTX --------------------------------
api_key_ftx = os.environ.get("API_FTX")
api_secret_ftx = os.environ.get("SECRET_FTX")
subaccount_name = "1"
# -------------------------------------------------

future_market = "BTCUSDT"  # PERP ON BYBIT
spot_market = "BTC/USD"  # SPOT ON FTX

ctx = zmq.asyncio.Context.instance()


async def websocket_bybit():
    # Perps endpoints:
    endpoint_public = 'wss://stream.bybit.com/realtime_public'
    endpoint_private = 'wss://stream.bybit.com/realtime_private'

    # Connect to Bybit auth PRIVATE websocket (for order status)
    try:
        ws_private = WebSocket(endpoint=endpoint_private,
                               subscriptions=['order'],
                               api_key=api_key_bybit,
                               api_secret=api_secret_bybit,
                               ping_interval=25,
                               ping_timeout=15)
    except Exception as e:
        print(e)
        await websocket_bybit()

    # Connect to Bybit auth PUBLIC websocket (for ticker info)
    try:
        ws_public = WebSocket(endpoint=endpoint_public,
                              subscriptions=[f"instrument_info.100ms.{future_market}"],
                              ping_interval=25,
                              ping_timeout=15)
    except Exception as e:
        print(e)
        await websocket_bybit()

    # CONNECT TO ZMQ CACHE SERVER - ORDERSTATUS
    # ctx = zmq.Context()
    socket = ctx.socket(zmq.PUSH)
    socket.connect("tcp://127.0.0.1:5557")

    # Receive data from Bybit websockets
    while 1:
        try:
            msg_ticker = ws_public.fetch(f"instrument_info.100ms.{future_market}")
            if msg_ticker != {}:

                print(f"SEND BYBIT MESSAGE: {msg_ticker}")
                msg_ticker = json.dumps(msg_ticker)
                await socket.send_string(msg_ticker)
                await asyncio.sleep(0.001)

        except Exception as e:
            print("Something wrong with the WS feed!: " + str(e))
            traceback.print_exc()
            await asyncio.sleep(1)



async def websocket_ftx():
    # CONNECT TO ZMQ CACHE SERVER - ORDERSTATUS
    # context = zmq.Context()
    socket = ctx.socket(zmq.PUSH)
    socket.connect("tcp://127.0.0.1:5557")

    ts = int(time.time() * 1000)
    signature = hmac.new(api_secret_ftx.encode(), f'{ts}websocket_login'.encode(), 'sha256').hexdigest()
    msg = \
        {
            'op': 'login',
            'args': {
                'key': api_key_ftx,
                'sign': signature,
                'time': ts,
                'subaccount': subaccount_name
            }
        }
    msg = json.dumps(msg)

    async with websockets.connect('wss://ftx.com/ws/') as websocket:
        await websocket.send(msg)

        msg2 = {'op': 'subscribe', 'channel': 'ticker', 'market': spot_market}
        msg3 = {'op': 'subscribe', 'channel': 'orders'}
        await websocket.send(json.dumps(msg2))
        await websocket.send(json.dumps(msg3))

        # orderstatus = {}
        while websocket.open:

            message = json.loads(await websocket.recv())

            message_type = message['type']
            if message_type in {'subscribed', 'unsubscribed'}:
                continue
            elif message_type == 'info':
                if message['code'] == 20001:
                    await websocket_ftx()
            elif message_type == 'error':
                await websocket_ftx()
                # raise Exception(message)

            if message['channel'] == 'ticker':
                spot = message['data']
                spot_bid = spot["bid"]
                spot_ask = spot["ask"]
                # if message['channel'] == 'orders':
                #     orderstatus = message['data']
                # dict_ftx = {"exchange": "FTX", "bid": spot_bid, "ask": spot_ask, "orderstatus": orderstatus}

                dict_ftx = {"exchange": "FTX", "bid": spot_bid, "ask": spot_ask}

                print(f"SEND FTX MESSAGE: {dict_ftx}")
                dict_ftx = json.dumps(dict_ftx)
                await socket.send_string(dict_ftx)
                await asyncio.sleep(0.00001)

        await websocket_ftx()


async def receiver():
    # CONNECT TO BYBIT ZMQ CACHE SERVER
    # context = zmq.Context()
    print("Connecting to receive server...")
    socket = ctx.socket(zmq.PULL)
    socket.bind("tcp://127.0.0.1:5557")

    while 1:
        # message = json.dumps(message)
        message = await socket.recv_json()
        print(f"RECEIVED MESSAGE: {message}")
        await asyncio.sleep(0.00001)


# async def handler():
#     await asyncio.wait([receiver(), websocket_bybit()])   #, websocket_ftx()
#
# if __name__ == "__main__":
#     asyncio.get_event_loop().run_until_complete(handler())


# if __name__ == "__main__":
#     asyncio.get_event_loop().run_until_complete(
#         asyncio.wait(
#         [receiver(), websocket_bybit(), websocket_ftx()]
#     ))


# Define a main async method (our program)
async def main():
    # Run both print method and wait for them to complete (passing in asyncState)
    await asyncio.gather(receiver(), websocket_bybit(), websocket_ftx())

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()



