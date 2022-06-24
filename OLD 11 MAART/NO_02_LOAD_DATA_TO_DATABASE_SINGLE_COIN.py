import sqlite3
import websocket_code
import datetime as datetime
import sys
import time
# logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)    # dit heb ik toegevoegd om te loggen wat hij doet of zo

coin = "SOL"


ws = websocket_code.FtxWebsocketClient()
try:
    ws.connect()
    print("Connected to FTX websocket")
except Exception as e:
    print(f"Websocket Error Message: {e}")
    sys.exit()

with open('coins.txt', 'w') as file:
    file.write(coin)

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
            spot_last TEXT,
            perp_bid TEXT, 
            perp_ask TEXT,
            perp_last TEXT,
            Price TEXT,
            Size TEXT,
            Side TEXT
        ) WITHOUT ROWID
    """)
    connection.commit()
except Exception as e:
    print(f"Error, while creating databases: {e}")

#connect to db
connection = sqlite3.connect(f'database/{coin}.db')
cursor = connection.cursor()

id_list = []

while True:
    try:
        now = datetime.datetime.now().timestamp()
        spot = (ws.get_ticker(market=f"{coin.upper()}/USD"))
        perp = (ws.get_ticker(market=f"{coin.upper()}-PERP"))

        trades = ws.get_trades(market=f"{coin.upper()}/USD")
        if trades != []:
            for trade in trades[-1]:
                if trade["id"] not in id_list:
                    # timestamp = trade["time"][:-6]
                    # timestamp = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f').timestamp()
                    id_list.append(trade["id"])

                    if trade["side"] == "buy":
                        side = 'g'
                    else:
                        side = 'r'

                    print(f"{now}, {spot['bid']}, {spot['ask']}, {spot['last']}, {perp['bid']}, {perp['ask']}, {perp['last']}, {trade['price']}, {trade['size']}, {side}")

                    cursor.execute(
                        f"INSERT or REPLACE INTO coin_{coin} (time, spot_bid, spot_ask, spot_last, perp_bid, perp_ask, perp_last, Price, Size, Side) VALUES "
                        f"({now}, {spot['bid']}, {spot['ask']}, {spot['last']}, {perp['bid']}, {perp['ask']}, {perp['last']}, {trade['price']}, {trade['size']}, '{side}')"),
                    connection.commit()

                else:
                    print(f"{now}, {spot['bid']}, {spot['ask']}, {spot['last']}, {perp['bid']}, {perp['ask']}, {perp['last']}")

                    cursor.execute(
                        f"INSERT or REPLACE INTO coin_{coin} (time, spot_bid, spot_ask, spot_last, perp_bid, perp_ask, perp_last) VALUES "
                        f"({now}, {spot['bid']}, {spot['ask']}, {spot['last']}, {perp['bid']}, {perp['ask']}, {perp['last']})"),
                    connection.commit()

        # # REMOVE OLDER ROWS   !!!!!!!  DELETE WERKT NOG NIET !!!!!!!
        # now = int(datetime.datetime.now().timestamp())
        # time_threshold = int(now) - 200
        # cursor.execute(f"DELETE FROM coin_{coin} WHERE time > {time_threshold}")
        # # self.cursor.execute(f"DELETE FROM coin_{self.coin} WHERE rowid > {self.num_rows}")
        # connection.commit()

        print(f"Next database populate iteration starts.")

    except Exception as e:
        print(f"Waiting for databases to be filled...")

    time.sleep(0.01)






