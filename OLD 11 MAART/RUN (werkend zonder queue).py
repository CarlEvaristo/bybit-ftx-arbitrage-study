import requests
import time
import datetime
import numpy as np
import os
import websocket_code
from ftx_client_class import FtxClient
import threading


# ------------ VARIABLES ----------------------------------
coin = "LINK"
API_1 = os.environ.get("API_1")
SECRET_1 = os.environ.get("SECRET_1")
sub = "1"

orderstatus = {}
ticker = (1,2)
spread_array = np.array([])
# ----------------------------------------------------------


def get_order_status(ws):
    global orderstatus
    while True:
        data = ws.get_orders()
        if data != {}:
            orderstatus = list(data.items())[-1][1]
        time.sleep(0.001)


def fill_spread_array(ws, coin):
    global spread_array
    global ticker
    maximum_items = 1000
    # infinite loop: fill np array with spread data / delete old data
    while True:
        # calculate spread data
        spot = ws.get_ticker(market=f"{coin.upper()}/USD")
        perp = ws.get_ticker(market=f"{coin.upper()}-PERP")
        if (spot != {}) and (perp != {}):
            spread = perp["ask"] - spot["bid"]
            spread_array = np.insert(spread_array, 0, [spread], axis=0)        # add spread data to np array
            spread_array = np.delete(spread_array, np.s_[maximum_items:])    # remove old items from np array
            ticker = (perp["bid"], perp["ask"], spot["bid"], spot["ask"])
        time.sleep(0.001)


def get_my_bid_price(ws, coin):
    global spread_array
    global ticker
    minimum_items = 1000
    stdev_num = 2

    while spread_array.size < minimum_items:
        print("Waiting for sufficient spread data")
        time.sleep(0.001)

    # calculate spread's mean and std
    spread_mean = spread_array.mean()
    spread_std = spread_array.std()
    mean_plus_st_dev = abs(spread_mean + (spread_std * stdev_num))  # klopt dit wel !!!!!!!!!!???????????????????????????? (abs)

    # get latest ticker
    # (perp["bid"], perp["ask"], spot["bid"], spot["ask"])
    spot_bid = ticker[2]
    perp_ask = ticker[1]

    my_bid = perp_ask - mean_plus_st_dev
    #                                         ik heb de standaard basispoints van 2% even uitgezet
    # if my_bid > (perp_ask * 0.998):  # BASISPOINTS  0.2% --> was 1.005  --> 0.995  --> nu alleem fees (0.14% afgerond naar 0.2%) als bps  --> 1.002  --> 0.998
    #     my_bid = perp_ask * 0.998
    if my_bid >= spot_bid:
        my_bid = spot_bid
    return my_bid


def get_my_ask_price(price_increment_perp):
    global ticker
    # get latest ticker
    # (perp["bid"], perp["ask"], spot["bid"], spot["ask"])
    perp_bid = ticker[0]
    perp_ask = ticker[1]

    if perp_bid < (perp_ask - price_increment_perp):
        my_ask_price = perp_ask - price_increment_perp
    else:
        my_ask_price = perp_ask
    return my_ask_price

def order_execution(coin, client, ws):
    time.sleep(2)
    global orderstatus
    global ticker

    coin = coin.upper()
    coin_spot = f"{coin}/USD"
    coin_perp = f"{coin}-PERP"

    # determine position size
    balance = client.get_balances()

    available_USD = [item["free"] for item in balance if item["coin"] == "USD"][0]
    available_USD = (available_USD * 0.95) / 2  # takes 95% of subaccount and divides it 50/50 over spot/perp
    # num_batches = num_batches   # OLD CODE FOR BATCHES
    # available_USD_per_batch = (available_USD / num_batches)  # OLD CODE FOR BATCHES

    # determine SPOT's possible position size  --> WE MUST DETERMINE SPOT'S SIZE FIRST, AS SPOT SIZE IS BOUNDED BY SIZE INCREMENTS
    # OLD CODE FOR BATCHES
    # size = available_USD_per_batch / my_bid_price
    # size = available_USD / my_bid_price
    # "size-increment" of spot coin: the nr of decimals, etc.
    # minimum required SPOT size

    # get SPOT data
    spot_data = requests.get(f'https://ftx.com/api/markets/{coin_spot}').json()
    price_increment_spot = spot_data["result"]["priceIncrement"]
    size_increment = spot_data["result"]["sizeIncrement"]
    spot_minProvideSize = spot_data["result"]["minProvideSize"]

    # get price increment PERP
    price_increment_perp = requests.get(f'https://ftx.com/api/markets/{coin_perp}').json()["result"]["priceIncrement"]

    # get my_bid_price
    my_bid_price = get_my_bid_price(ws, coin)

    price_per_incr = my_bid_price * size_increment  # ---> PRICE PER INCREMENT!!!!!
    total_incr = int(available_USD / price_per_incr)

    spot_size = total_incr * size_increment
    print(f"{coin}: SPOT ORDER SIZE: {spot_size}")
    if spot_size < spot_minProvideSize:
        print(f"{coin}: INITIAL ORDER FAILED: BALANCE TOO LOW FOR MIN REQUIRED SIZE INCREMENT")
        return "FAILED"

    # PLACE INITIAL SPOT ORDER
    try:
        client.place_order(market=f"{coin_spot}", side="buy", price=my_bid_price,
                               type="limit", size=spot_size, post_only=True, reduce_only=False)
        while orderstatus == {}:
            time.sleep(0.001)

        while orderstatus["status"] != "new":  # BELANGRIJK!!!!!!! EVEN WACHTEN TOT WEBSOCKET NEW ORDER BEVESTIGT
            time.sleep(0.001)
            print("waiting for first spot order")
        print("first spot order placed")

    except Exception as e:
        print(e)
        order_execution(coin, client, ws)

    while (orderstatus["filledSize"] != orderstatus["size"]):
        print("spot entry while loop")
        if (get_my_bid_price(ws, coin) > (orderstatus["price"] + price_increment_spot)) or (
                get_my_bid_price(ws, coin) < (orderstatus["price"] - price_increment_spot)) and \
                (orderstatus["status"] != "closed") and (orderstatus["filledSize"] != orderstatus["size"]):
            print(f"TIME SPOT ENTRY 0 = {datetime.datetime.now()}")
            if orderstatus["status"] != "closed":  # BELANGRIJK!!!!!!! EVEN WACHTEN TOT WEBSOCKET CANCEL BEVESTIGT
                try:
                    client.cancel_order(order_id=orderstatus["id"])
                    while orderstatus["status"] != "closed": # BELANGRIJK! W8 OP ORDER BEVESTIGING
                        print(orderstatus)
                        time.sleep(0.001)
                        print("waiting for spot order cancel")
                    print("spot order canceled")
                except Exception as e:
                    print(e)

            print(f"TIME SPOT ENTRY 1 = {datetime.datetime.now()}")
        if (orderstatus["status"] == "closed") and (orderstatus["filledSize"] != orderstatus["size"]):
            try:
                client.place_order(market=f"{coin_spot}",
                                       side="buy",
                                       price=get_my_bid_price(ws, coin),
                                       type="limit",
                                       size=(orderstatus["size"] - orderstatus["filledSize"]),
                                       post_only=True,
                                       reduce_only=False)
                while orderstatus["status"] != "new":  # BELANGRIJK! W8 OP ORDER BEVESTIGING
                    print(orderstatus)
                    time.sleep(0.001)
                    print("waiting for spot order update")
                print("new spot order placed")
            except Exception as e:
                print(e)
            print(f"TIME SPOT ENTRY 2 = {datetime.datetime.now()}")  # !!!!!!!!!!!!!!!!!!!!!!!!!!!!


    try:
        client.place_order(market=f"{coin_perp}", side="sell", price=get_my_ask_price(price_increment_perp),
                                   type="limit", size=spot_size, post_only=True, reduce_only=False)
        while orderstatus["status"] != "new":  # BELANGRIJK!!!!!!! EVEN WACHTEN TOT WEBSOCKET NEW ORDER BEVESTIGT
            print(orderstatus)
            time.sleep(0.001)
            print("waiting for first perp order")
        print("first perp order placed")
    except Exception as e:
        print(e)

    while (orderstatus["filledSize"] != orderstatus["size"]):
        print("perp entry while loop")
        if (orderstatus["price"] != ticker[1]) and (orderstatus["status"] != "closed") and (orderstatus["filledSize"] != orderstatus["size"]):
            print(f"TIME PERP ENTRY 0 = {datetime.datetime.now()}")
            if orderstatus["status"] != "closed":
                try:
                    client.cancel_order(order_id=orderstatus["id"])
                    while orderstatus["status"] != "closed":  # BELANGRIJK!!!!!!! W8 TOT WEBSOCKET CANCEL BEVESTIGT
                        time.sleep(0.001)
                        print("waiting for perp order cancel")
                    print("perp order canceled")
                except Exception as e:
                    print(e)
            print(f"TIME PERP ENTRY 1 = {datetime.datetime.now()}")
            print(orderstatus)
        if (orderstatus["status"] == "closed") and (orderstatus["filledSize"] != orderstatus["size"]):
            try:
                client.place_order(market=f"{coin_perp}",
                                       side="sell",
                                       price=get_my_ask_price(price_increment_perp),
                                       type="limit",
                                       size=(orderstatus["size"] - orderstatus["filledSize"]),
                                       post_only=True,
                                       reduce_only=False)
                while orderstatus["status"] != "new":  # BELANGRIJK!!!!!!! EVEN WACHTEN TOT WEBSOCKET NEW ORDER BEVESTIGT
                    time.sleep(0.001)
                    print("waiting for perp order update")
                print("new perp order placed")
            except Exception as e:
                print(e)
            print(f"TIME PERP ENTRY 2 = {datetime.datetime.now()}")

    # return "SUCCESS"  # DIT GAAN NAAR SAMS EXIT ---> ONDERSTAANDE CODE IS MIJN EXIT
    exit_order_execution(coin, client, spot_size, price_increment_spot, ws)


def my_spot_exit_price():
    global ticker
    # get latest ticker
    # (perp["bid"], perp["ask"], spot["bid"], spot["ask"])
    perp_bid = ticker[0]
    perp_ask = ticker[1]  # WHEN PERP MARKET EXIT (SPOT BASED ON PERP_ASK INSTEAD OF PERP_BID) !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    spot_ask = ticker[3]

    if perp_bid > spot_ask:
        my_spot_ask = perp_bid
    else:
        my_spot_ask = spot_ask
    return my_spot_ask


def my_perp_exit_price(price_increment_perp):
    global ticker
    # get latest ticker
    # (perp["bid"], perp["ask"], spot["bid"], spot["ask"])
    perp_bid = ticker[0]
    perp_ask = ticker[1]

    # if perp_ask > (perp_bid + price_increment_perp):
    #     my_bid = perp_bid + price_increment_perp
    # else:
    #     my_bid = perp_bid
    # return my_bid

    return perp_bid

def exit_order_execution(coin, client, spot_size, price_increment_spot, ws):
    global orderstatus
    global ticker    # (perp["bid"], perp["ask"], spot["bid"], spot["ask"])
    print("TEST ENTERED EXIT FUNCTION")
    coin = coin.upper()
    coin_spot = f"{coin}/USD"
    coin_perp = f"{coin}-PERP"

    # get price increment PERP
    price_increment_perp = requests.get(f'https://ftx.com/api/markets/{coin_perp}').json()["result"]["priceIncrement"]

    # SPOT EXIT ORDER
    try:
        client.place_order(market=f"{coin_spot}", side="sell", price=my_spot_exit_price(), type="limit", size=spot_size, post_only=True, reduce_only=True)
        print("TEST PLACED SPOT EXIT ORDER")

        while orderstatus["status"] != "new":  # BELANGRIJK!!!!!!! EVEN WACHTEN TOT WEBSOCKET NEW ORDER BEVESTIGT
            time.sleep(0.001)
            print("TEST WAIT 4 CONFIRMATION OF FIRST SPOT EXIT ORDER")
        print("first spot exit order placed")

    except Exception as e:
        print(e)
        exit_order_execution(coin, client, spot_size, price_increment_spot, ws)

    while (orderstatus["filledSize"] != orderstatus["size"]):
        print("spot exit while loop")
        if (my_spot_exit_price() > (orderstatus["price"] + price_increment_spot)) or (my_spot_exit_price() < (orderstatus["price"] - price_increment_spot)) and \
                (orderstatus["status"] != "closed") and (orderstatus["filledSize"] != orderstatus["size"]):
            print(f"TIME SPOT EXIT 0 = {datetime.datetime.now()}")
            if orderstatus["status"] != "closed":
                try:
                    client.cancel_order(order_id=orderstatus["id"])
                    while orderstatus["status"] != "closed":
                        time.sleep(0.001)
                        print("waiting for SPOT EXIT order cancel")
                    print("spot exit order canceled")
                except Exception as e:
                    print(e)

            print(f"TIME SPOT EXIT 1 = {datetime.datetime.now()}")
        if (orderstatus["status"] == "closed") and (orderstatus["filledSize"] != orderstatus["size"]):
            try:
                client.place_order(market=f"{coin_spot}", side="sell", price=my_spot_exit_price(), type="limit", size=(orderstatus["size"] - orderstatus["filledSize"]), post_only=True, reduce_only=True)
                while orderstatus["status"] != "new":
                    time.sleep(0.001)
                    print("waiting for SPOT EXIT order update")
                print("new spot exit order placed")
            except Exception as e:
                print(e)
            print(f"TIME SPOT EXIT 2 = {datetime.datetime.now()}")

    # # PERP MARKET EXIT ORDER
    # perp_order = client.place_order(market=f"{coin_perp}", side="buy", price=0, type="market", size=spot_size, reduce_only=True)
    # time.sleep(2)
    # print(f"{coin}: PERP MARKET EXIT ORDER: {perp_order}")

    # PERP LIMIT EXIT
    try:
        client.place_order(market=f"{coin_perp}", side="buy", price=my_perp_exit_price(price_increment_perp), type="limit", size=spot_size, post_only=True, reduce_only=True)
        while orderstatus["status"] != "new":
            time.sleep(0.001)
            print("waiting for first PERP EXIT order")
        print("first perp exit order placed")
    except Exception as e:
        print(e)

    while (orderstatus["filledSize"] != orderstatus["size"]):
        print("perp exit while loop")
        if (orderstatus["price"] != ticker[1]) and (orderstatus["status"] != "closed") and (orderstatus["filledSize"] != orderstatus["size"]):
            print(f"TIME PERP EXIT 0 = {datetime.datetime.now()}")
            if orderstatus["status"] != "closed":
                try:
                    client.cancel_order(order_id=orderstatus["id"])
                    while orderstatus["status"] != "closed":
                        time.sleep(0.001)
                        print("waiting for PERP EXIT cancel")
                    print("perp exit order canceled")
                except Exception as e:
                    print(e)
            print(f"TIME PERP EXIT 1 = {datetime.datetime.now()}")
        if (orderstatus["status"] == "closed") and (orderstatus["filledSize"] != orderstatus["size"]):
            try:
                client.place_order(market=f"{coin_perp}", side="buy",price=my_perp_exit_price(price_increment_perp),type="limit", size=(orderstatus["size"] - orderstatus["filledSize"]),post_only=True,reduce_only=True)
                while orderstatus["status"] != "new":
                    time.sleep(0.001)
                    print("waiting for PERP EXIT update")
                print("new perp exit order placed")
            except Exception as e:
                print(e)
            print(f"TIME PERP EXIT 2 = {datetime.datetime.now()}")

    order_execution(coin, client, ws)


if __name__ == "__main__":
    client = FtxClient(api_key=API_1, api_secret=SECRET_1, subaccount_name=sub)

    ws = websocket_code.FtxWebsocketClient(api=API_1, secret=SECRET_1, subaccount=sub)
    try:
        ws.connect()
        print("Connected to FTX websocket")
    except:
        print(f"WEBSOCKET ERROR. STARTING RETRY WEBSOCKET CONNECT.")

    thread1 = threading.Thread(target=order_execution, args=(coin, client, ws))
    thread2 = threading.Thread(target=get_order_status, args=(ws,))
    thread3 = threading.Thread(target=fill_spread_array, args=(ws, coin))

    thread1.start()
    thread2.start()
    thread3.start()

    thread1.join()
    thread2.join()
    thread3.join()
