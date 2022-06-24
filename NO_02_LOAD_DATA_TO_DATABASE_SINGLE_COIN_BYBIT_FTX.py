from pybit import WebSocket
import sqlite3
import websocket_code
import datetime as datetime
import sys
import time

coin = "XRP"
future_market = f"{coin.upper()}USDT"  # PERP ON BYBIT
spot_market = f"{coin.upper()}/USD"  # SPOT ON FTX

# CONNECT TO FTX WS
ws_ftx = websocket_code.FtxWebsocketClient()
try:
    ws_ftx.connect()
    print("Connected to FTX websocket")
except Exception as e:
    print(f"Websocket Error Message: {e}")
    sys.exit()

# CONNECT TO BYBIT WS
subs_bybit = [f"instrument_info.100ms.{future_market}"]
ws_bybit = WebSocket("wss://stream.bybit.com/realtime_public", subscriptions=subs_bybit)

#create db
try:
    # connect dbase for each coin
    connection = sqlite3.connect(f'database/{coin}.db')
    cursor = connection.cursor()

    # create database for this coin if necessary   -> "COIN_" BEFORE TABLE-NAME, BECAUSE TABLES CAN'T START WITH A NUMBER, AND COINS SOMETIMES DO
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS coin_{coin} (
            time TEXT NOT NULL PRIMARY KEY DESC,
            spot_bid TEXT, 
            spot_ask TEXT,
            perp_bid TEXT, 
            perp_ask TEXT
        ) WITHOUT ROWID
    """)
    connection.commit()
except Exception as e:
    print(f"Error, while creating databases: {e}")

#connect to db
connection = sqlite3.connect(f'database/{coin}.db')
cursor = connection.cursor()


while True:
    try:
        now = datetime.datetime.now().timestamp()
        spot = (ws_ftx.get_ticker(market=f"{spot_market}"))
        perp = ws_bybit.fetch(subs_bybit[0])

        print(f"{now}, {spot['bid']}, {spot['ask']}, {perp['bid1_price']}, {perp['ask1_price']}")

        cursor.execute(
            f"INSERT or REPLACE INTO coin_{coin} (time, spot_bid, spot_ask, perp_bid, perp_ask) VALUES "
            f"({now}, {spot['bid']}, {spot['ask']}, {perp['bid1_price']}, {perp['ask1_price']})"),
        connection.commit()

        # REMOVE OLDER ROWS   !!!!!!!  DELETE WERKT NOG NIET !!!!!!!
        now = int(datetime.datetime.now().timestamp())
        time_threshold = int(now) - 2000
        cursor.execute(f"DELETE FROM coin_{coin} WHERE time < {time_threshold}")
        # cursor.execute(f"DELETE FROM coin_{coin} WHERE rowid > {2000}")
        connection.commit()


    except Exception as e:
        print(f"Waiting for databases to be filled...{e}")

    time.sleep(0.2)






