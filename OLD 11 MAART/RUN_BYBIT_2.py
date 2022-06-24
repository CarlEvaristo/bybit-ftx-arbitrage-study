from pybit import WebSocket
subs = [
    "instrument_info.100ms.BTCUSDT"
]
ws = WebSocket(
    "wss://stream-testnet.bybit.com/realtime_public",
    subscriptions=subs
)
while True:
    data = ws.fetch(subs[0])
    if data:
        print("BID", data["bid1_price"])
        print("ASK", data["ask1_price"])