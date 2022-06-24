# from pybit import WebSocket
# import os
# import time
#
# api_key = os.environ.get("API")
# api_secret = os.environ.get("SECRET")
#
# ws = WebSocket(
#     "wss://stream.bybit.com/spot/ws",
#     api_key=api_key, api_secret=api_secret
# )

# while True:
#     data = ws.fetch("executionReport")
#     if data:
#         print(data)
#     time.sleep(0.001)









import time
import json
import hmac
import asyncio
import websockets
import os

api_key = os.environ.get("API")
api_secret = os.environ.get("SECRET")

expires = int((time.time() + 1) * 1000)
signature = str(hmac.new(bytes(api_secret, "utf-8"), bytes(f"GET/realtime{expires}", "utf-8"),
                         digestmod="sha256").hexdigest())


websocket =  websockets.connect("wss://stream.bybit.com/spot/ws")
websocket.send(msg)

        global MY_BID, TICKER_SPOT, TICKER_PERP, ORDERSTATUS
        counter = 0

        while websocket.open:
            counter = counter + 1
            if counter == 1:
                msg1 = {'op': 'subscribe', 'channel': 'ticker', 'market': future_market}
                msg2 = {'op': 'subscribe', 'channel': 'ticker', 'market': spot_market}
                msg3 = {'op': 'subscribe', 'channel': 'orders'}
                await websocket.send(json.dumps(msg1))
                await websocket.send(json.dumps(msg2))
                await websocket.send(json.dumps(msg3))
            else:
                message = json.loads(await websocket.recv())

                message_type = message['type']
                if message_type in {'subscribed', 'unsubscribed'}:
                    continue
                elif message_type == 'info':
                    if message['code'] == 20001:
                        websocket(msg)
                elif message_type == 'error':
                    websocket(msg)
                    # raise Exception(message)


                if message['channel'] == 'ticker' and message['market'] == spot_market:
                    spot = message['data']
                    TICKER_SPOT = spot
                if message['channel'] == 'ticker' and message['market'] == future_market:
                    perp = message['data']
                    TICKER_PERP = perp
                if message['channel'] == 'orders':
                    ORDERSTATUS = message['data']
                    print(message['data'])

            await asyncio.sleep(0.00001)


if __name__ == "__main__":
    print('Opening websocket.')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(websocket(msg))
    loop.run_forever()
