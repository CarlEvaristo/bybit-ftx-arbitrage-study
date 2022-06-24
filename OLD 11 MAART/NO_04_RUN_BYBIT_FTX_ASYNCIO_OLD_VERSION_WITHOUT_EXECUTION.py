import asyncio
import websockets
import json
import zmq
import asyncio
import zmq.asyncio
import os
import time
import hmac

asyncio.set_event_loop_policy(
    asyncio.WindowsSelectorEventLoopPolicy())  # python-3.8.0a4  --> to prevent an asyncio zmq error


# ------------ BYBIT ------------------------------
api_key_bybit = os.environ.get("API_BYBIT")
api_secret_bybit = os.environ.get("SECRET_BYBIT")

ws_url = "wss://stream.bybit.com/realtime_private"

# Generate expires.
expires = int((time.time() + 1) * 1000)

# Generate signature.
signature = str(hmac.new(bytes(api_secret_bybit, "utf-8"), bytes(f"GET/realtime{expires}", "utf-8"), digestmod="sha256").hexdigest())

param = "api_key={api_key}&expires={expires}&signature={signature}".format(
    api_key=api_key_bybit,
    expires=expires,
    signature=signature
)

url_bybit = ws_url + "?" + param

# ------------ FTX --------------------------------
api_key_ftx = os.environ.get("API_FTX")
api_secret_ftx = os.environ.get("SECRET_FTX")
subaccount_name = "1"

ts = int(time.time() * 1000)
signature = hmac.new(api_secret_ftx.encode(), f'{ts}websocket_login'.encode(), 'sha256').hexdigest()
msg_ftx = \
    {
        'op': 'login',
        'args': {
            'key': api_key_ftx,
            'sign': signature,
            'time': ts,
            'subaccount': subaccount_name
        }
    }
msg_ftx = json.dumps(msg_ftx)

# -------------------------------------------------

future_market = "BTCUSDT"  # PERP ON BYBIT
spot_market = "BTC/USD"  # SPOT ON FTX

ctx = zmq.asyncio.Context.instance()


async def websocket_bybit_public():
    socket = ctx.socket(zmq.PUSH)
    socket.connect("tcp://127.0.0.1:5557")
    async with websockets.connect("wss://stream.bytick.com/realtime_public") as websocket:
        await websocket.send(f'{{"op": "subscribe", "args": ["instrument_info.100ms.{future_market}"]}}')
        # ^^ let op ik moest text accolades dubbel gebruiken anders werden ze geinterpreteerd als fstring accolades
        async for message in websocket:
            try:
                message = json.loads(message)
                bybit_bid = str(message["data"]["update"][0]["bid1_price"])
                bybit_ask = str(message["data"]["update"][0]["ask1_price"])
                message = {"Exchange": "BYBIT", "Bid": bybit_bid, "Ask": bybit_ask}
                print(f"SEND MESSAGE:     {message}")
                message = json.dumps(message)
                await socket.send_string(message)
            except:
                pass


async def websocket_bybit_private():
    socket = ctx.socket(zmq.PUSH)
    socket.connect("tcp://127.0.0.1:5557")
    async with websockets.connect(url_bybit) as websocket:
        await websocket.send('{"op": "subscribe", "args": ["order"]}')
        async for message in websocket:
            try:
                message = json.loads(message)
                order_status = str(message["data"][0])
                message = {"Exchange": "BYBIT", "Orderstatus": order_status}
                print(f"SEND MESSAGE:     {message}")
                message = json.dumps(message)
                await socket.send_string(message)
            except:
                pass


async def websocket_ftx():
    socket = ctx.socket(zmq.PUSH)
    socket.connect("tcp://127.0.0.1:5557")
    ftx_bid = ""
    ftx_ask = ""
    order_status = ""
    async with websockets.connect("wss://ftx.com/ws/") as websocket:
        await websocket.send(msg_ftx)
        msg1 = {'op': 'subscribe', 'channel': 'ticker', 'market': spot_market}
        msg1 = json.dumps(msg1)
        await websocket.send(msg1)
        msg2 = {'op': 'subscribe', 'channel': 'orders'}
        msg2 = json.dumps(msg2)
        await websocket.send(msg2)
        async for message in websocket:
            try:
                message = json.loads(message)
                if message['channel'] == 'ticker':
                    if (str(message["data"]["bid"]) != ftx_bid) or (str(message["data"]["ask"]) != ftx_ask):
                        ftx_bid = str(message["data"]["bid"])
                        ftx_ask = str(message["data"]["ask"])
                        message = {"Exchange": "FTX", "Bid": ftx_bid, "Ask": ftx_ask}
                        print(f"SEND MESSAGE:     {message}")
                        message = json.dumps(message)
                        await socket.send_string(message)

                if message['channel'] == 'orders':
                    if str(message["data"]) != order_status:
                        order_status = str(message["data"])
                        message = {"Exchange": "FTX", "Orderstatus": order_status}
                        print(f"SEND MESSAGE:     {message}")
                        message = json.dumps(message)
                        await socket.send_string(message)
            except:
                pass


async def receiver():
    print("Connecting to ZMQ receive server...")
    socket = ctx.socket(zmq.PULL)
    socket.bind("tcp://127.0.0.1:5557")

    while 1:
        received_message = await socket.recv_json()
        print(f"RECEIVED MESSAGE: {received_message}")
        await asyncio.sleep(0.00001)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(asyncio.wait(
        [websocket_bybit_public(), websocket_bybit_private(), websocket_ftx(), receiver()]
    ))
