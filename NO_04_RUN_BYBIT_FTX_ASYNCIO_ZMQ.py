import websockets
from datetime import datetime
import sys
import json
import zmq
import asyncio
import zmq.asyncio
import os
import time
import math
from pybit import HTTP
from ftx_client_class import FtxClient  # nodig voor ftx api, ivm balance size etc.
import numpy as np
import aiohttp
import hashlib
import hmac
import logging
# logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)    # dit heb ik toegevoegd om te loggen wat hij doet of zo
import warnings
warnings.filterwarnings(action="ignore", category=DeprecationWarning)

asyncio.set_event_loop_policy(
    asyncio.WindowsSelectorEventLoopPolicy())  # python-3.8.0a4  --> to prevent an asyncio zmq error

coin = "AVAX"
future_market = f"{coin.upper()}USDT"  # PERP ON BYBIT
spot_market = f"{coin.upper()}/USD"  # SPOT ON FTX

# ------------ BYBIT ------------------------------
api_key_bybit = os.environ.get("API_BYBIT")
api_secret_bybit = os.environ.get("SECRET_BYBIT")

ws_url = "wss://stream.bybit.com/realtime_private"

# Generate expires.
expires = int((time.time() + 1) * 1000)
# Generate signature.
signature = str(hmac.new(bytes(api_secret_bybit, "utf-8"), bytes(f"GET/realtime{expires}", "utf-8"),
                         digestmod="sha256").hexdigest())

param = f"api_key={api_key_bybit}&expires={expires}&signature={signature}"
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


ctx = zmq.asyncio.Context.instance()


async def websocket_bybit_public():
    socket = ctx.socket(zmq.PUSH)
    socket.connect("tcp://127.0.0.1:5557")
    print("Bybit public websocket connected.")
    async with websockets.connect("wss://stream.bytick.com/realtime_public") as websocket:
        await websocket.send(f'{{"op": "subscribe", "args": ["instrument_info.100ms.{future_market}"]}}')
        # ^^ let op ik moest text accolades dubbel gebruiken anders werden ze geinterpreteerd als fstring accolades
        async for message in websocket:
            try:
                message = json.loads(message)
                bybit_bid = str(message["data"]["update"][0]["bid1_price"])
                bybit_ask = str(message["data"]["update"][0]["ask1_price"])
                message = {"Type": "Ticker", "Exchange": "BYBIT", "Bid": bybit_bid, "Ask": bybit_ask}
                # print(f"SEND MESSAGE:     {message}")
                message = json.dumps(message)
                await socket.send_string(message)
            except:
                pass


async def websocket_bybit_private():
    socket = ctx.socket(zmq.PUSH)
    socket.connect("tcp://127.0.0.1:5557")
    print("Bybit private websocket connected.")
    async with websockets.connect(url_bybit) as websocket:
        await websocket.send('{"op": "subscribe", "args": ["order"]}')
        async for message in websocket:
            try:
                message = json.loads(message)
                order_status = str(message["data"][0])
                message = {"Type": "Orderstatus", "Exchange": "BYBIT", "Orderstatus": order_status}
                # print(f"SEND MESSAGE:     {message}")
                message = json.dumps(message)
                await socket.send_string(message)
            except:
                pass


async def websocket_ftx():
    socket = ctx.socket(zmq.PUSH)
    socket.connect("tcp://127.0.0.1:5557")
    print("Ftx websocket connected.")
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
                        message = {"Type": "Ticker", "Exchange": "FTX", "Bid": ftx_bid, "Ask": ftx_ask}
                        # print(f"SEND MESSAGE:     {message}")
                        message = json.dumps(message)
                        await socket.send_string(message)

                if message['channel'] == 'orders':
                    if message["data"] != order_status:
                        order_status = message["data"]
                        message = {"Type": "Orderstatus", "Exchange": "FTX", "Orderstatus": order_status}
                        print(f"SEND MESSAGE:     {message}")
                        message = json.dumps(message)
                        await socket.send_string(message)
            except:
                pass


async def data_handler():
    # first get static api data via aiohttp
    bybit_data = await bybit_api()  # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    price_incr_perp = bybit_data[0]

    spot_data = await ftx_api()
    price_incr_spot = spot_data[0]

    # ALWAYS USE PRICE INCREMENT OF EXCHANGE WITH LARGEST INCREMENT VALUES !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    if price_incr_spot > price_incr_perp:
        price_incr_perp = price_incr_spot

    num_decimals = str(price_incr_perp)[::-1].find('.')

    spread_array = np.array([])
    exit_spread_array = np.array([])
    maximum_items = 100  # 10000
    minimum_items = 50  # 9900
    stdev_num = 1

    socket = ctx.socket(zmq.PULL)
    socket.bind("tcp://127.0.0.1:5557")

    socket2 = ctx.socket(zmq.PUSH)
    socket2.connect("tcp://127.0.0.1:5559")

    print("Data handler initiated.")

    perp_ask = 0
    spot_ask = 0
    perp_bid = 0
    spot_bid = 0
    ftx_orderstatus = ""
    bybit_orderstatus = ""

    while True:
        received_message = await socket.recv_json()
        await asyncio.sleep(0.00001)

        if (received_message['Type'] == 'Ticker') and (received_message['Exchange'] == 'BYBIT'):
            perp_bid = float(received_message['Bid'])
        if (received_message['Type'] == 'Ticker') and (received_message['Exchange'] == 'BYBIT'):
            perp_ask = float(received_message['Ask'])
        if (received_message['Type'] == 'Ticker') and (received_message['Exchange'] == 'FTX'):
            spot_bid = float(received_message['Bid'])
        if (received_message['Type'] == 'Ticker') and (received_message['Exchange'] == 'FTX'):
            spot_ask = float(received_message['Ask'])
        if (received_message['Type'] == 'Orderstatus') and (received_message['Exchange'] == 'FTX'):
            ftx_orderstatus = received_message['Orderstatus']
        if (received_message['Type'] == 'Orderstatus') and (received_message['Exchange'] == 'BYBIT'):
            bybit_orderstatus = received_message['Orderstatus']
            # IMPORTANT: TURN JSON INTO PYTHON DICT --> BUT 1ST TURN JSON INTO THIS FORMAT: '{"KEY":"VALUE"}' AND REMOVE EMPTY STRINGS
            bybit_orderstatus = (repr(bybit_orderstatus))[1:-1]
            bybit_orderstatus = bybit_orderstatus.replace("'", '"')
            bybit_orderstatus = bybit_orderstatus.replace('""', '"None"')
            bybit_orderstatus = bybit_orderstatus.replace('False', '"False"')
            bybit_orderstatus = json.loads(bybit_orderstatus)
            await asyncio.sleep(0.00001)

        if (perp_ask != 0) and (spot_ask != 0) and (perp_bid != 0) and (spot_bid != 0):
            spread = perp_ask - spot_ask
            spread_array = np.insert(spread_array, 0, [spread], axis=0)  # add spread data to np array
            spread_array = np.delete(spread_array, np.s_[maximum_items:])  # remove old items from np array

            if spread_array.size > minimum_items:
                mean_plus_st_dev = spread_array.mean() + (spread_array.std() * stdev_num)  # klopt dit wel !!!!???? moet niet: abs(spread_mean)

                # PERP ENTRY PRICE
                # KOSTEN TOTALE TRADE = 2 * 0.07% = 0.14% - REBATE (2 * 0.025% = 0.05) = 0.09%
                # VERDEELD OVER ENTRY/EXIT = 0.045% KOSTEN => AFGEROND 0.05% KOSTEN
                perp_entry_ask = spot_ask + mean_plus_st_dev
                if perp_entry_ask < (spot_ask * 1.0005):  # 5 BASISPOINTS  0.05% (0.05% / 100 = 0.0005 --> 1 + 0.0005 = 1.0005)
                    perp_entry_ask = (spot_ask * 1.0005)
                if perp_entry_ask <= perp_ask:
                    perp_entry_ask = perp_ask

                # # FINETUNE PERP ASK PRICE (IVM MIN PRICE INCREMENTS PERPS BYBIT) !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                if perp_entry_ask % price_incr_perp != 0:
                    perp_entry_ask = (math.floor(perp_entry_ask / price_incr_perp)) * price_incr_perp
                perp_entry_ask = round(perp_entry_ask, num_decimals)


            # EXIT PERP BID VARIANT
            exit_spread = perp_bid - spot_bid
            exit_spread_array = np.insert(exit_spread_array, 0, [exit_spread], axis=0)  # add spread data to np array
            exit_spread_array = np.delete(exit_spread_array, np.s_[maximum_items:])  # remove old items from np array

            if exit_spread_array.size > minimum_items:

                # PERP EXIT BID PRICE
                # TRYING TO BUY BACK PERP BELOW SPOT BEST BID
                # IF SPOT BEST BID IS CLOSER THAN X BPS
                # (MARKET CLOSE SELL SPOT -> CROSS SPREAD -> SPOT BEST BID)
                # KOSTEN TOTALE TRADE = 2 * 0.07% = 0.14% - REBATE (2 * 0.025% = 0.05) = 0.09%
                # VERDEELD OVER ENTRY/EXIT = 0.045% KOSTEN  => AFGEROND 0.05% KOSTEN
                # 5 BASISPOINTS 0.05% / 100 = 0.0005  -> 1 - 0.0005 = 0.9995
                perp_bid_minus_bps = perp_bid * 0.9995  # 4,5 BASISPOINTS 0.045% / 100 = 0.00045  -> 1 - 0.00045 = 0.9991
                if perp_bid_minus_bps < spot_bid:
                    perp_exit_bid = perp_bid_minus_bps
                else:
                    perp_exit_bid = spot_bid

                # # FINETUNE PERP BID PRICE (IVM MIN PRICE INCREMENTS PERPS BYBIT) !!!!!!!!!!!!!!!!!!!!!!!!!!!!
                if perp_exit_bid % price_incr_perp != 0:
                    perp_exit_bid = (math.floor(perp_exit_bid / price_incr_perp)) * price_incr_perp
                perp_exit_bid = round(perp_exit_bid, num_decimals)

                message = {"Perp entry ask": perp_entry_ask, "Perp exit bid": perp_exit_bid, "ftx_spot_bid": spot_bid,
                           "ftx_spot_ask": spot_ask, "bybit_perp_bid": perp_bid, "bybit_perp_ask": perp_ask,
                           "ftx_orderstatus": ftx_orderstatus, "bybit_orderstatus": bybit_orderstatus}
                message = json.dumps(message)
                await socket2.send_string(message)
            else:
                print(f"Data-handler: Spread array not yet sufficiently filled, size: {spread_array.size}")
        else:
            print("Data-handler: Waiting for websocket data...")
        await asyncio.sleep(0.00001)


async def order_execution():
    socket2 = ctx.socket(zmq.PULL)
    socket2.bind("tcp://127.0.0.1:5559")

    # IMPORTANT: WAIT FOR ZQN DATA TO COME IN
    while (await socket2.recv_json()) == "":
        await asyncio.sleep(0.00001)

    print("ORDER EXECUTION INITIATION.")

    # BYBIT spot STATIC DATA : AIOHTTP ASYNC API REQUEST!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    bybit_data = await bybit_api()
    price_incr_perp = bybit_data[0]
    size_incr_perp = bybit_data[1]
    min_size_perp = bybit_data[2]

    # FTX spot STATIC DATA : AIOHTTP ASYNC API REQUEST  --> FTX SPOT determines position size!!!!!!!!!!!!!!!!!!
    spot_data = await ftx_api()
    price_incr_spot = spot_data[0]
    size_incr_spot = spot_data[1]
    min_size_spot = spot_data[2]

    # ALWAYS USE SIZE INCREMENT OF EXCHANGE WITH LARGEST INCREMENT VALUES
    if size_incr_spot > size_incr_perp:
        size_incr_perp = size_incr_spot

    # FTX BALANCE
    ftx_available_USD = await ftx_api_balance()
    print(f"FTX AVAILABLE USD BALANCE: {ftx_available_USD}")

    # BYBIT BALANCE
    bybit_available_USD = await bybit_api_balance()
    print(f"BYBIT AVAILABLE USD BALANCE: {bybit_available_USD}")

    # CHECK IF FTX BALANCE IS BIG ENOUGH (TO HEDGE BYBIT POSITION)
    if bybit_available_USD > ftx_available_USD:
        print("ERROR, BYBIT BALANCE TOO LOW. \nExit program.")
        sys.exit()

    ask_price_new = (await socket2.recv_json())['Perp entry ask']

    # DETERMINE ORDER SIZE
    price_per_incr = ask_price_new * size_incr_perp  # ---> PRICE PER INCREMENT!!!!!
    total_incr = int(bybit_available_USD / price_per_incr)
    order_size = total_incr * size_incr_perp
    print(f"ORDER SIZE: {order_size}")
    if (order_size < min_size_spot) or (order_size < min_size_perp):
        print(f"{spot_market}: INITIAL ORDER FAILED: SIZE TOO LOW FOR MIN REQUIRED SIZE")
    await asyncio.sleep(0.00001)

    # PLACE INITIAL BYBIT PERP LIMIT ORDER ------------------------------------------------------------------------
    try:
        client_bybit.place_active_order(symbol=future_market, side="Sell", order_type="Limit", qty=order_size,
                                        price=(await socket2.recv_json())['Perp entry ask'], time_in_force="PostOnly",
                                        reduce_only=False, close_on_trigger=False)
    except Exception as e:
        print(f"INITIAL ORDER ERROR: {e}")
        sys.exit()

    bybit_orderstatus = (await socket2.recv_json())['bybit_orderstatus']
    while bybit_orderstatus == "":  # BELANGRIJK!!!!!!! WAIT FOR ORDER CONFIRMATION
        print("WAITING FOR 1ST ORDER STATUS CONFIRMATION")
        bybit_orderstatus = (await socket2.recv_json())['bybit_orderstatus']

    bybit_orderstatus = (await socket2.recv_json())['bybit_orderstatus']
    # BELANGRIJK!!!!!!! W8 TOT WS 'NEW' ORDER BEVESTIGT
    while bybit_orderstatus["order_status"] != 'New' and bybit_orderstatus["order_status"] != "Filled" \
            and bybit_orderstatus["side"] == "Sell":  # checken of we niet naar de "New" of "Filled" status kijken van de vorige (buy) order
        print("WAITING FOR 'NEW' ORDER STATUS CONFIRMATION")
        bybit_orderstatus = (await socket2.recv_json())['bybit_orderstatus']

    print(f"INITIAL PERP ORDER SUCCESS: {bybit_orderstatus}")

    # ----- WHILE LOOP -----
    # UPDATE LOOP UNTIL SPOT ORDER FILLED -> na partial fill al overgaan tot hedgen
    # (dus niet wachten tot remaining size == 0)
    data = (await socket2.recv_json())
    perp_entry_ask = data['Perp entry ask']
    cum_exec_qty = data["bybit_orderstatus"]["cum_exec_qty"]
    order_price = data["bybit_orderstatus"]["price"]
    order_id = data["bybit_orderstatus"]["order_id"]
    order_status = data["bybit_orderstatus"]["order_status"]

    while (cum_exec_qty == 0):  # let op gaat bij partial fill niet verder tot 100% fill: gaat daarentegen meteen hedgen (anders kan je door gemiddelde perp entry prijs (meerdere fills) niet goed hedgen)
        # LET OP cum_exec_qty = Number of filled contracts from the order's size
        await asyncio.sleep(0.00001)
        print(f"order price {order_price} + increment {price_incr_perp} = {order_price + price_incr_perp}")
        print(perp_entry_ask)
        if (perp_entry_ask > (order_price + price_incr_perp) or \
            perp_entry_ask < (order_price - price_incr_perp)) and \
                (order_status != "Cancelled") and (cum_exec_qty == 0):
            print(f"TIME PERP ENTRY 0 = {datetime.now()}")
            try:
                client_bybit.replace_active_order(symbol=future_market, order_id=order_id, p_r_price=perp_entry_ask)
            except Exception as e:
                print(f"EXCEPT 1: {e}")
                pass
            else:
                bybit_orderstatus = (await socket2.recv_json())['bybit_orderstatus']
                while bybit_orderstatus["price"] != perp_entry_ask:
                    # BELANGRIJK! W8 OP ORDER BEVESTIGING
                    await asyncio.sleep(0.00001)
                    print("orderstatus wait 1")
                    bybit_orderstatus = (await socket2.recv_json())['bybit_orderstatus']
                print("perp order replaced")

            print(f"TIME PERP ENTRY 1 = {datetime.now()}")
            await asyncio.sleep(0.00001)

        if (order_status == "Cancelled") and (cum_exec_qty == 0):
            bybit_orderstatus = (await socket2.recv_json())['bybit_orderstatus']
            order_id = bybit_orderstatus["order_id"]
            try:
                client_bybit.place_active_order(symbol=future_market, side="Sell", order_type="Limit", qty=order_size,
                                                price=perp_entry_ask,
                                                time_in_force="PostOnly",
                                                reduce_only=False, close_on_trigger=False)
            except Exception as e:
                print(f"EXCEPT 2: {e}")
                pass
            else:
                # BELANGRIJK! W8 OP ORDER BEVESTIGING
                bybit_orderstatus = (await socket2.recv_json())['bybit_orderstatus']
                while bybit_orderstatus["order_status"] != "New" and bybit_orderstatus["order_status"] != "Filled" \
                        and bybit_orderstatus["order_id"] != order_id:  # checken of we niet naar de "New" of "Filled" status kijken van de vorige (buy) order
                    await asyncio.sleep(0.00001)
                    print("orderstatus wait 2")
                    bybit_orderstatus = (await socket2.recv_json())['bybit_orderstatus']
                print("new perp order placed")
            print(f"TIME PERP ENTRY 2 = {datetime.now()}")

        print("WHILE LOOP END OF ITERATION")
        data = (await socket2.recv_json())
        print(data["bybit_orderstatus"])
        perp_entry_ask = data['Perp entry ask']
        cum_exec_qty = data["bybit_orderstatus"]["cum_exec_qty"]
        order_price = data["bybit_orderstatus"]["price"]
        order_id = data["bybit_orderstatus"]["order_id"]
        order_status = data["bybit_orderstatus"]["order_status"]

    order_size = (await socket2.recv_json())["bybit_orderstatus"]["cum_exec_qty"]  # determine PERP FILLED SIZE because of partial fills!!!!!!!!!!!!!!!!!!!!!
    print("BYBIT PERP LIMIT ORDER FILLED. SENDING FTX SPOT MARKET BUY ORDER.")
    # PLACE INITIAL FTX SPOT MARKET ORDER ------------------------------------------------------------------------------
    spot_order = client_ftx.place_order(market=f"{spot_market}", side="buy", price=0, type="market", size=order_size,
                                        reduce_only=False)
    time.sleep(2)
    print(f"FTX SPOT MARKET ORDER: {spot_order}")
    print("You're fully hedged")


    # EXIT CODE!!!!!!
    print("Entered exit code.")
    # PLACE INITIAL BYBIT PERP LIMIT EXIT ORDER ------------------------------------------------------------------------
    try:
        client_bybit.place_active_order(symbol=future_market, side="Buy", order_type="Limit", qty=order_size,
                                        price=(await socket2.recv_json())['Perp exit bid'], time_in_force="PostOnly",
                                        reduce_only=True, close_on_trigger=False)
    except Exception as e:
        print(f"INITIAL EXIT ORDER ERROR: {e}")
        sys.exit()

    bybit_orderstatus = (await socket2.recv_json())['bybit_orderstatus']
    # BELANGRIJK!!!!!!! W8 TOT WS 'NEW' ORDER BEVESTIGT
    while bybit_orderstatus["order_status"] != 'New' and bybit_orderstatus["order_status"] != "Filled"\
            and bybit_orderstatus["side"] == "Buy":  # checken of we niet naar de "New" of "Filled" status kijken van de vorige (buy) order
        print("WAITING FOR 'NEW' EXIT ORDER STATUS CONFIRMATION")
        print("BYBIT ORDERSTATUS", bybit_orderstatus)
        bybit_orderstatus = (await socket2.recv_json())['bybit_orderstatus']

    print(f"INITIAL PERP EXIT ORDER SUCCESS: {bybit_orderstatus}")

    # ----- WHILE LOOP -----
    # UPDATE LOOP UNTIL PERP EXIT ORDER 100% FILLED -> GEEN PARTIAL FILL (ZOALS BY ENTRY) MAAR WACHTEN TOT ALLES OP IS.
    # (dus niet wachten tot remaining size == 0)
    data = (await socket2.recv_json())
    perp_exit_bid = data['Perp exit bid']
    cum_exec_qty = data["bybit_orderstatus"]["cum_exec_qty"]
    order_price = data["bybit_orderstatus"]["price"]
    order_id = data["bybit_orderstatus"]["order_id"]
    order_status = data["bybit_orderstatus"]["order_status"]

    while (cum_exec_qty != order_size):  # LET OP WACHT OP GEHELE FILL (GEEN PARTIAL FILL ZOALS BIJ ENTRY)
        # LET OP cum_exec_qty = Number of filled contracts from the order's size
        await asyncio.sleep(0.00001)
        print(f"order price {order_price} + increment {price_incr_perp} = {order_price + price_incr_perp}")
        print(perp_exit_bid)
        if (perp_exit_bid > (order_price + price_incr_perp) or \
            perp_exit_bid < (order_price - price_incr_perp)) and \
                (order_status != "Cancelled") and (cum_exec_qty != order_size):
            print(f"TIME PERP EXIT 0 = {datetime.now()}")
            try:
                client_bybit.replace_active_order(symbol=future_market, order_id=order_id, p_r_price=perp_exit_bid)
            except Exception as e:
                print(f"EXCEPT 1: {e}")
                pass
            else:
                bybit_orderstatus = (await socket2.recv_json())['bybit_orderstatus']
                while bybit_orderstatus["price"] != perp_exit_bid:
                    await asyncio.sleep(0.00001)
                    print("orderstatus wait 1")
                    bybit_orderstatus = (await socket2.recv_json())['bybit_orderstatus']
                print("perp exit order replaced")

            print(f"TIME PERP EXIT 1 = {datetime.now()}")
            await asyncio.sleep(0.00001)

        if (order_status == "Cancelled") and (cum_exec_qty != order_size):
            bybit_orderstatus = (await socket2.recv_json())['bybit_orderstatus']
            order_id = bybit_orderstatus["order_id"]
            try:
                remaining_size = (order_size - cum_exec_qty)
                client_bybit.place_active_order(symbol=future_market, side="Buy", order_type="Limit",
                                                qty=remaining_size,
                                                price=perp_exit_bid,
                                                time_in_force="PostOnly",
                                                reduce_only=True, close_on_trigger=False)
            except Exception as e:
                print(f"EXCEPT 2: {e}")
                pass
            else:
                bybit_orderstatus = (await socket2.recv_json())['bybit_orderstatus']
                while bybit_orderstatus["order_status"] != "New" and bybit_orderstatus["order_status"] != "Filled" \
                        and bybit_orderstatus["order_id"] != order_id:  # checken of we niet naar de "New" of "Filled" status kijken van de vorige (buy) ord
                    await asyncio.sleep(0.00001)
                    print("orderstatus wait 2")
                    bybit_orderstatus = (await socket2.recv_json())['bybit_orderstatus']
                print("new perp exit order placed")
            print(f"TIME PERP EXIT 2 = {datetime.now()}")

        print("WHILE LOOP END OF ITERATION")
        data = (await socket2.recv_json())
        print(data["bybit_orderstatus"])
        perp_exit_bid = data['Perp exit bid']
        cum_exec_qty = data["bybit_orderstatus"]["cum_exec_qty"]
        order_price = data["bybit_orderstatus"]["price"]
        order_id = data["bybit_orderstatus"]["order_id"]
        order_status = data["bybit_orderstatus"]["order_status"]

    order_size = (await socket2.recv_json())["bybit_orderstatus"]["cum_exec_qty"]  # determine PERP FILLED SIZE
    print("BYBIT PERP LIMIT EXIT ORDER FILLED. SENDING FTX SPOT MARKET SELL ORDER.")
    # PLACE INITIAL FTX SPOT MARKET ORDER ------------------------------------------------------------------------------
    spot_order = client_ftx.place_order(market=f"{spot_market}", side="sell", price=0, type="market", size=order_size,
                                        reduce_only=True)
    await asyncio.sleep(2)
    print(f"FTX SPOT MARKET EXIT ORDER: {spot_order}")
    print("End of order execution. All positions are closed. Restart.")
    await order_execution()


# BELOW THE AIOHTTP BYBIT AND FTX COROUTINES:
async def bybit_api():
    async with aiohttp.ClientSession() as bybit_session1:
        async with bybit_session1.get('https://api.bybit.com/v2/public/symbols') as resp_bybit1:
            data = await resp_bybit1.text()
            data = json.loads(data)["result"]
            data = [item for item in data if item['name'] == future_market][0]
            price_incr_perp = float(data['price_filter']['tick_size'])
            size_incr_perp = float(data['lot_size_filter']['qty_step'])
            min_size_perp = float(data['lot_size_filter']['min_trading_qty'])
    await bybit_session1.close()
    return [price_incr_perp, size_incr_perp, min_size_perp]


async def ftx_api():
    async with aiohttp.ClientSession() as ftx_session1:
        async with ftx_session1.get(f'https://ftx.com/api/markets/{spot_market}') as resp_ftx1:
            ftx_data = await resp_ftx1.text()
            ftx_data = json.loads(ftx_data)["result"]
            price_incr_spot = ftx_data["priceIncrement"]
            size_incr_spot = ftx_data["sizeIncrement"]
            min_size_spot = ftx_data["minProvideSize"]
    await ftx_session1.close()
    return [price_incr_spot, size_incr_spot, min_size_spot]


# BELOW THE AIOHTTP BYBIT PRIVATE COROUTINES
async def bybit_api_balance():
    balance_bybit_coin = "USDT"
    param_str = f"api_key={api_key_bybit}&coin={balance_bybit_coin}&recv_window={10000}&timestamp={round(time.time() * 1000)}"
    hash = (hmac.new(bytes(api_secret_bybit, "utf-8"), param_str.encode("utf-8"), hashlib.sha256)).hexdigest()
    signature = {"sign": hash}
    param_str = f"{param_str}&sign={signature['sign']}"

    api_url = "https://api.bybit.com/v2/private/wallet/balance"

    async with aiohttp.ClientSession() as bybit_session:
        async with bybit_session.get(url=api_url, params=param_str) as resp_bybit:
            try:
                bybit_available_USD = await resp_bybit.text()
                # print(bybit_available_USD)
            except Exception as e:
                print(f"BYBIT API BALANCE REQUEST ERROR: {e}")
                pass
            else:
                bybit_available_USD = json.loads(bybit_available_USD)
                bybit_available_USD = bybit_available_USD['result'][balance_bybit_coin]['available_balance']
                bybit_available_USD = (bybit_available_USD * 0.90)
    await bybit_session.close()
    return bybit_available_USD


async def ftx_api_balance():
    balance_ftx_coin = "USD"
    API = os.environ.get("API_FTX")
    API_secret = os.environ.get("SECRET_FTX")
    url = "https://ftx.com/api/wallet/balances"

    ts = str(int(time.time() * 1000))
    signature_payload = f'{ts}GET/api/wallet/balances'.encode()
    signature = hmac.new(API_secret.encode(), signature_payload, 'sha256').hexdigest()
    headers = {
        "FTX-KEY": API,
        "FTX-SIGN": signature,
        "FTX-TS": (ts),
        'FTX-SUBACCOUNT': "1"
    }
    async with aiohttp.ClientSession() as ftx_session:
        async with ftx_session.get(url, headers=headers) as resp_ftx:
            try:
                ftx_available_USD = await resp_ftx.text()
            except Exception as e:
                print(f"FTX API BALANCE REQUEST ERROR: {e}")
                pass
            else:
                ftx_available_USD = json.loads(ftx_available_USD)
                ftx_available_USD = ftx_available_USD["result"]
                ftx_available_USD = [item["free"] for item in ftx_available_USD if item["coin"] == balance_ftx_coin][0]
                ftx_available_USD = (ftx_available_USD * 0.90) # 90% of account: omdat we niet exakt weten voor welke prijs de order wordt uitgevoerd.
    await ftx_session.close()
    return ftx_available_USD



# LET OP, LAATSTE TO DO: FTX BALANCE IS NOG NIET AIOHTTP!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# DAARNA CYTHON IMPLEMENTEREN !!!!!!!!!!!!!!!!!!!!!!!!!!
# DAARNA OP AWS ZETTEN

if __name__ == "__main__":
    client_ftx = FtxClient(api_key=api_key_ftx, api_secret=api_secret_ftx, subaccount_name=subaccount_name)
    client_bybit = HTTP("https://api.bybit.com", api_key=api_key_bybit, api_secret=api_secret_bybit)

    asyncio.get_event_loop().run_until_complete(asyncio.wait(
        [websocket_bybit_public(), websocket_bybit_private(), websocket_ftx(), data_handler(), order_execution()]
    ))
