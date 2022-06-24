import time
import json
import hmac
import requests
from datetime import datetime
import asyncio
import websockets
import os
import numpy as np
from ftx_client_class import FtxClient

api_key = os.environ.get("API_1")
api_secret = os.environ.get("SECRET_1")
subaccount_name = "1"

coin = "LINK"
spot_market = f"{coin}/USD"
future_market = f"{coin}-PERP"

spread = 0.6 / 100
ContractSize = 0.0002

MY_BID = 0
MY_ASK = 0
MY_EXIT_BID = 0
MY_EXIT_ASK = 0
TICKER_SPOT = {}
TICKER_PERP = {}
ORDERSTATUS = {}

ts = int(time.time() * 1000)
signature = hmac.new(
    api_secret.encode(), f'{ts}websocket_login'.encode(), 'sha256').hexdigest()
msg = \
    {
        'op': 'login',
        'args': {
            'key': api_key,
            'sign': signature,
            'time': ts,
            'subaccount': subaccount_name
        }
    }


async def websocket(msg):
    async with websockets.connect('wss://ftx.com/ws/') as websocket:
        await websocket.send(msg)
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
                    raise Exception(message)

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


async def my_bid():
    global MY_BID, TICKER_PERP, TICKER_SPOT
    spread_array = np.array([])
    maximum_items = 1000
    minimum_items = 999
    stdev_num = 2

    while TICKER_PERP == {} or TICKER_SPOT == {}:
        await asyncio.sleep(0.00001)
    while True:
        spread = TICKER_PERP["ask"] - TICKER_SPOT["bid"]
        spread_array = np.insert(spread_array, 0, [spread], axis=0)  # add spread data to np array
        spread_array = np.delete(spread_array, np.s_[maximum_items:])  # remove old items from np array

        if spread_array.size > minimum_items:
            spread_mean = spread_array.mean()
            spread_std = spread_array.std()
            mean_plus_st_dev = abs(spread_mean + (spread_std * stdev_num))  # klopt dit wel !!!!!!!!!!????????? (abs)
            my_bid_price = TICKER_PERP["ask"] - mean_plus_st_dev
            if my_bid_price > (TICKER_PERP["ask"] * 0.999):  # 10 BASISPOINTS  0.1% (0.1% / 100 = 0.001 --> 1 - 0.001 = 0.999)
                my_bid_price = TICKER_PERP["ask"] * 0.999
            if my_bid_price >= TICKER_SPOT["bid"]:
                my_bid_price = TICKER_SPOT["bid"]
            MY_BID = my_bid_price

        await asyncio.sleep(0.00001)


async def my_ask():
    global MY_ASK, TICKER_PERP, TICKER_SPOT
    perp_data = requests.get(f'https://ftx.com/api/markets/{future_market}').json()
    price_increment_perp = perp_data["result"]["priceIncrement"]
    while TICKER_PERP == {}:
        await asyncio.sleep(0.00001)
    while True:
        # REMOVE "ASK JUMPING" TO SEE IF THIS PREVENTS SELLING TOO LOW
        # (IF THIS MEANS I'M SLOWER THAN THE REST I COULD STILL SELL TOO LOW...!!!!!!!!!!!!!!)
        if TICKER_PERP["bid"] < (TICKER_PERP["ask"] - price_increment_perp):
            my_ask_price = (TICKER_PERP["ask"] - price_increment_perp)
        else:
            my_ask_price = TICKER_PERP["ask"]

        MY_ASK = my_ask_price
        await asyncio.sleep(0.00001)


async def my_spot_exit_ask():
    global MY_EXIT_ASK, TICKER_PERP, TICKER_SPOT

    while TICKER_PERP == {} or TICKER_SPOT == {}:
        await asyncio.sleep(0.00001)
    while True:
        # TRYING TO SELL SPOT ABOVE PERP BEST BID
        perp_bid_plus_bps = TICKER_PERP["bid"] * 1.0005  # 5 bps (0.05%) try to sell spot above perp best bid (to have some room for perp volatility inbetween spot close and perp close)
        if perp_bid_plus_bps > TICKER_SPOT["ask"]:
            my_spot_ask = perp_bid_plus_bps
        else:
            my_spot_ask = TICKER_SPOT["ask"]
        MY_EXIT_ASK = my_spot_ask
        await asyncio.sleep(0.00001)

        # old version without bps margin
        # if perp_bid > spot_ask:
        #     my_spot_ask = perp_bid
        # else:
        #     my_spot_ask = spot_ask
        # MY_EXIT_ASK = my_spot_ask
        # await asyncio.sleep(0.00001)


async def my_perp_exit_bid():
    global MY_EXIT_BID, TICKER_PERP
    perp_data = requests.get(f'https://ftx.com/api/markets/{future_market}').json()
    price_increment_perp = perp_data["result"]["priceIncrement"]
    while TICKER_PERP == {} or TICKER_SPOT == {}:
        await asyncio.sleep(0.00001)
    while True:
        # REMOVE "BID JUMPING" TO SEE IF THIS PREVENTS BUYING TOO HIGH
        # (IF THIS MEANS I'M SLOWER THAN THE REST I COULD STILL BUY TOO HIGH...!!!!!!!!!!!!!!)
        if TICKER_PERP["ask"] > (TICKER_PERP["bid"] + price_increment_perp):
            my_perp_bid = (TICKER_PERP["bid"] + price_increment_perp)
        else:
            my_perp_bid = TICKER_PERP["bid"]

        MY_EXIT_BID = my_perp_bid
        await asyncio.sleep(0.00001)


async def order_execution():
    global MY_BID, MY_ASK, TICKER_SPOT, TICKER_PERP, ORDERSTATUS

    print("Start new order execution. Opening positions starts.")
    # get static data
    spot_data = requests.get(f'https://ftx.com/api/markets/{spot_market}').json()
    price_increment_spot = spot_data["result"]["priceIncrement"]
    size_increment = spot_data["result"]["sizeIncrement"]
    spot_minProvideSize = spot_data["result"]["minProvideSize"]

    while MY_BID == 0 or MY_ASK == 0:
        print("Waiting for sufficient data to calculate my bid/ask prices")
        await asyncio.sleep(0.00001)

    # determine position size
    balance = client.get_balances()
    available_USD = [item["free"] for item in balance if item["coin"] == "USD"][0]
    available_USD = (available_USD * 0.95) / 2  # takes 95% of subaccount and divides it 50/50 over spot/perp

    price_per_incr = MY_BID * size_increment  # ---> PRICE PER INCREMENT!!!!!
    total_incr = int(available_USD / price_per_incr)

    spot_size = total_incr * size_increment
    print(f"{coin}: SPOT ORDER SIZE: {spot_size}")
    if spot_size < spot_minProvideSize:
        print(f"{coin}: INITIAL ORDER FAILED: BALANCE TOO LOW FOR MIN REQUIRED SIZE INCREMENT")

    # PLACE INITIAL SPOT ORDER ----------------------------------------------------------------------------------
    try:
        client.place_order(market=f"{spot_market}", side="buy", price=MY_BID,
                           type="limit", size=spot_size, post_only=True, reduce_only=False)
    except:
        print("INITIAL SPOT ORDER FAILED --> MANIER BEDENKEN VOOR RETRY!!!!!")
        pass

    while ORDERSTATUS == {}:  # BELANGRIJK!!!!!!! EVEN WACHTEN TOT WEBSOCKET NEW ORDER BEVESTIGT
        await asyncio.sleep(0.00001)

    while ORDERSTATUS["status"] != "new":  # BELANGRIJK!!!!!!! EVEN WACHTEN TOT WEBSOCKET NEW ORDER BEVESTIGT
        await asyncio.sleep(0.00001)

    print("INITIAL SPOT ORDER SUCCESS")

    # UPDATE LOOP UNTIL SPOT ORDER FILLED
    while float(ORDERSTATUS["filledSize"] == 0):
        await asyncio.sleep(0.00001)
        if ((MY_BID > (ORDERSTATUS["price"] + price_increment_spot)) or (MY_BID < (ORDERSTATUS["price"] - price_increment_spot))) \
                and (ORDERSTATUS["status"] != "closed") and (ORDERSTATUS["filledSize"] != ORDERSTATUS["size"]):
            print(f"TIME SPOT ENTRY 0 = {datetime.now()}")
            try:
                client.cancel_order(order_id=ORDERSTATUS["id"])
            except:
                pass
            else:
                while ORDERSTATUS["status"] != "closed":  # BELANGRIJK! W8 OP ORDER BEVESTIGING
                    await asyncio.sleep(0.00001)
                print("spot order canceled")

            print(f"TIME SPOT ENTRY 1 = {datetime.now()}")

        await asyncio.sleep(0.00001)
        if (ORDERSTATUS["status"] == "closed") and (ORDERSTATUS["filledSize"] != ORDERSTATUS["size"]):
            try:
                client.place_order(market=f"{spot_market}",
                                   side="buy",
                                   price=MY_BID,
                                   type="limit",
                                   size=(ORDERSTATUS["size"] - ORDERSTATUS["filledSize"]),
                                   post_only=True,
                                   reduce_only=False)
            except:
                pass
            else:
                while ORDERSTATUS["status"] != "new" and (
                        ORDERSTATUS["filledSize"] != ORDERSTATUS["size"]):  # BELANGRIJK! W8 OP ORDER BEVESTIGING
                    await asyncio.sleep(0.00001)
                print("new spot order placed")
            print(f"TIME SPOT ENTRY 2 = {datetime.now()}")

    spot_size = ORDERSTATUS["filledSize"]  # determine spot size because of partial fills!!!!!!!!!!!!!!!!!!!!!

    # PLACE INITIAL PERP ORDER -------------------------------------------------------------------------------
    try:
        client.place_order(market=f"{future_market}", side="sell", price=MY_ASK,
                           type="limit", size=spot_size, post_only=True, reduce_only=False)
    except:
        print("INITIAL PERP ORDER FAILED --> MANIER BEDENKEN VOOR RETRY!!!!!")
        pass

    while ORDERSTATUS["status"] != "new":  # BELANGRIJK!!!!!!! EVEN WACHTEN TOT WEBSOCKET NEW ORDER BEVESTIGT
        await asyncio.sleep(0.00001)

    print("INITIAL PERP ORDER SUCCESS")

    # UPDATE LOOP UNTIL PERP ORDER FILLED

    while (ORDERSTATUS["filledSize"] != ORDERSTATUS["size"]):
        await asyncio.sleep(0.00001)
        if (ORDERSTATUS["price"] != TICKER_PERP["ask"]) and (ORDERSTATUS["status"] != "closed") and (
                ORDERSTATUS["filledSize"] != ORDERSTATUS["size"]):
            print(f"TIME PERP ENTRY 0 = {datetime.now()}")
            try:
                client.cancel_order(order_id=ORDERSTATUS["id"])
            except:
                pass
            else:
                while ORDERSTATUS["status"] != "closed":  # BELANGRIJK! W8 OP ORDER BEVESTIGING
                    await asyncio.sleep(0.00001)
                print("perp order canceled")

            print(f"TIME PERP ENTRY 1 = {datetime.now()}")
        await asyncio.sleep(0.00001)
        if (ORDERSTATUS["status"] == "closed") and (ORDERSTATUS["filledSize"] != ORDERSTATUS["size"]):
            try:
                client.place_order(market=f"{future_market}", side="sell", price=MY_ASK, type="limit",
                                   size=(ORDERSTATUS["size"] - ORDERSTATUS["filledSize"]), post_only=True,
                                   reduce_only=False)
            except:
                pass
            else:
                while ORDERSTATUS["status"] != "new" and (ORDERSTATUS["filledSize"] != ORDERSTATUS[
                    "size"]):  # BELANGRIJK!!!!!!! EVEN WACHTEN TOT WEBSOCKET NEW ORDER BEVESTIGT
                    await asyncio.sleep(0.00001)
                print("new perp order placed")
            print(f"TIME PERP ENTRY 2 = {datetime.now()}")

    print("You're fully hedged, entered exit code.")

    # EXIT CODE STARTS HERE ------------------------------------------------------------------------------------------
    # PLACE INITIAL SPOT EXIT ORDER
    try:
        client.place_order(market=f"{spot_market}", side="sell", price=MY_EXIT_ASK,
                           type="limit", size=spot_size, post_only=True, reduce_only=True)
    except:
        print("INITIAL SPOT EXIT ORDER FAILED --> MANIER BEDENKEN VOOR RETRY!!!!!")
        pass

    while ORDERSTATUS["status"] != "new":  # BELANGRIJK!!!!!!! EVEN WACHTEN TOT WEBSOCKET NEW ORDER BEVESTIGT
        await asyncio.sleep(0.00001)

    print("INITIAL SPOT EXIT ORDER SUCCESS")

    while (ORDERSTATUS["filledSize"] != ORDERSTATUS["size"]):
        await asyncio.sleep(0.00001)
        if ((MY_EXIT_ASK > (ORDERSTATUS["price"] + price_increment_spot)) or (MY_EXIT_ASK < (ORDERSTATUS["price"] - price_increment_spot))) \
                and (ORDERSTATUS["status"] != "closed") and (ORDERSTATUS["filledSize"] != ORDERSTATUS["size"]):
            print(f"TIME SPOT EXIT 0 = {datetime.now()}")
            try:
                client.cancel_order(order_id=ORDERSTATUS["id"])
            except:
                pass
            else:
                while ORDERSTATUS["status"] != "closed":  # BELANGRIJK! W8 OP ORDER BEVESTIGING
                    await asyncio.sleep(0.00001)
                print("spot order canceled")

            print(f"TIME SPOT EXIT 1 = {datetime.now()}")
        await asyncio.sleep(0.00001)

        if (ORDERSTATUS["status"] == "closed") and (ORDERSTATUS["filledSize"] != ORDERSTATUS["size"]):
            try:
                client.place_order(market=f"{spot_market}", side="sell", price=MY_EXIT_ASK, type="limit",
                                   size=(ORDERSTATUS["size"] - ORDERSTATUS["filledSize"]), post_only=True,
                                   reduce_only=True)
            except:
                pass
            else:
                while ORDERSTATUS["status"] != "new" and (ORDERSTATUS["filledSize"] != ORDERSTATUS["size"]):
                    await asyncio.sleep(0.00001)
                print("new spot exit order placed")
            print(f"TIME SPOT EXIT 2 = {datetime.now()}")
        await asyncio.sleep(0.00001)

    # PERP LIMIT EXIT -------------------------------------------------------------------------------------------
    try:
        client.place_order(market=f"{future_market}", side="buy", price=MY_EXIT_BID,
                           type="limit", size=spot_size, post_only=True, reduce_only=True)
    except:
        print("INITIAL PERP EXIT ORDER FAILED --> MANIER BEDENKEN VOOR RETRY!!!!!")
        pass

    while ORDERSTATUS["status"] != "new":  # BELANGRIJK!!!!!!! EVEN WACHTEN TOT WEBSOCKET NEW ORDER BEVESTIGT
        await asyncio.sleep(0.00001)

    print("INITIAL PERP EXIT ORDER SUCCESS")

    # UPDATE LOOP UNTIL PERP ORDER FILLED

    while (ORDERSTATUS["filledSize"] != ORDERSTATUS["size"]):
        await asyncio.sleep(0.00001)
        if (ORDERSTATUS["price"] != TICKER_PERP["bid"]) and (ORDERSTATUS["status"] != "closed") \
                and (ORDERSTATUS["filledSize"] != ORDERSTATUS["size"]):
            print(f"TIME PERP EXIT 0 = {datetime.now()}")
            try:
                client.cancel_order(order_id=ORDERSTATUS["id"])
            except:
                pass
            else:
                while ORDERSTATUS["status"] != "closed":  # BELANGRIJK! W8 OP ORDER BEVESTIGING
                    await asyncio.sleep(0.00001)
                print("perp exit order canceled")
            print(f"TIME PERP EXIT 1 = {datetime.now()}")
        await asyncio.sleep(0.00001)

        if (ORDERSTATUS["status"] == "closed") and (ORDERSTATUS["filledSize"] != ORDERSTATUS["size"]):
            try:
                client.place_order(market=f"{future_market}", side="buy", price=MY_EXIT_BID, type="limit",
                                   size=(ORDERSTATUS["size"] - ORDERSTATUS["filledSize"]), post_only=True,
                                   reduce_only=True)
            except:
                pass
            else:
                while ORDERSTATUS["status"] != "new" and (ORDERSTATUS["filledSize"] != ORDERSTATUS[
                    "size"]):  # BELANGRIJK!!!!!!! EVEN WACHTEN TOT WEBSOCKET NEW ORDER BEVESTIGT
                    await asyncio.sleep(0.00001)
                print("new perp exit order placed")
            print(f"TIME PERP EXIT 2 = {datetime.now()}")

    print("End of order execution. The positions are closed. Restart.")
    await order_execution()


# Define a main async method (our program)
async def main():
    # Run both print method and wait for them to complete (passing in asyncState)
    await asyncio.gather(websocket(json.dumps(msg)), my_ask(), my_bid(), order_execution(), my_spot_exit_ask(),
                         my_perp_exit_bid())


client = FtxClient(api_key=api_key, api_secret=api_secret, subaccount_name=subaccount_name)

if __name__ == "__main__":
    print('Opening websocket. Waiting for order fills')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()

# # Run our program until it is complete
# loop = asyncio.get_event_loop()
# loop.run_until_complete(main())
# loop.close()
