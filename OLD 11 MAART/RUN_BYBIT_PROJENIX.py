from pybit import HTTP, WebSocket

import time
from time import sleep
import json
import redis
import sys
import traceback
import zmq

from inc import keys
from inc import loblib
from inc import config
from inc import bybit
from inc import streams

ctx = zmq.Context()
sock = ctx.socket(zmq.PUB)
sock.bind("tcp://*:1234")

# symbol = sys.argv[1]
# instrument = sys.argv[2]
# sub = sys.argv[3]

# Inverse:
endpoint = 'wss://stream.bybit.com/realtime'

# Linear (unused):
endpoint_public = 'wss://stream.bybit.com/realtime_public'
endpoint_private = 'wss://stream.bybit.com/realtime_private'

public_subs = streams.get_public_subs()
private_subs = streams.get_private_subs()
print(public_subs)
# print(private_subs)
print('Streamer is running ...')

# ['orderBookL2_25.'+symbol, 'orderBook_200.100ms.'+symbol, 'trade.'+symbol, 'instrument_info.100ms.'+symbol]

try:
    public_feed = WebSocket(
        endpoint=endpoint,
        subscriptions=public_subs,
        ping_interval=25,
        ping_timeout=15
    )
except Exception as e:
    print(e)

try:
    private_feed = WebSocket(
        endpoint=endpoint,
        subscriptions=private_subs,
        # subscriptions = subs,
        api_key=keys.api_key,
        api_secret=keys.api_secret,
        ping_interval=25,
        ping_timeout=15
    )
except Exception as e:
    print(e)

while (1):
    try:
        for public_sub in public_subs:
            message = public_feed.fetch(public_sub)
            if message != [] and message != {}:
                if 'orderBook' in public_sub:
                    message = bybit.lob_convert(message)
                message = public_sub + ' ' + json.dumps(message)
                # print(message)
                sock.send_string(message)
                # print()

        for private_sub in private_subs:
            message = private_feed.fetch(private_sub)
            if message != [] and message != {}:
                message = public_sub + ' ' + json.dumps(message)
                sock.send_string(message)

        # sleep(1)

    except Exception as e:
        print("Something wrong with the WS feed!: " + str(e))
        traceback.print_exc()
        sleep(5)

