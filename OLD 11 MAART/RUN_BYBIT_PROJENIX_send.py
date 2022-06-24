from pybit import HTTP, WebSocket
import time
import os
import json
import traceback
import zmq

context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.bind("tcp://127.0.0.1:5557")

api_key = os.environ.get("API")
api_secret = os.environ.get("SECRET")
symbol = "BTCUSD"
ORDERSTATUS = {}

# Perps endpoints:
endpoint_public = 'wss://stream.bybit.com/realtime_public'
endpoint_private = 'wss://stream.bybit.com/realtime_private'

# public_subs = ['orderBookL2_25.'+symbol, 'orderBook_200.100ms.'+symbol, 'trade.'+symbol, 'instrument_info.100ms.'+symbol]
private_subs = ['order']

# Connect to the Bybit auth spot websocket (subscriptions are not required)
try:
    private_feed = WebSocket(endpoint=endpoint_private,
                            subscriptions=private_subs,
                            api_key=api_key,
                            api_secret=api_secret,
                            ping_interval=25,
                            ping_timeout=15)
except Exception as e:
    print(e)

while 1:
    try:
        # for public_sub in public_subs:
        #     message = public_feed.fetch(public_sub)
        #     if message != [] and message != {}:
        #         if 'orderBook' in public_sub:
        #             message = bybit.lob_convert(message)
        #         message = public_sub + ' ' + json.dumps(message)
        #         #print(message)

        for private_sub in private_subs:
            message = private_feed.fetch('order')
            if message != [] and message != {}:
                if message[-1] != ORDERSTATUS:
                    ORDERSTATUS = message[-1]
                    message = json.dumps(message[-1])
                    socket.send_string(message)
                    print(message)


    except Exception as e:
        print("Something wrong with the WS feed!: " + str(e))
        traceback.print_exc()
        time.sleep(5)

    time.sleep(0.001)

