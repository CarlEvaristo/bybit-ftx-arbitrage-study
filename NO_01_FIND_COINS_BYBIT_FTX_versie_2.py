import time
from pybit import HTTP
import requests
import pandas as pd
from pybit import WebSocket
import websocket_code
import sys

# console setting (making space for printing df's)
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.width', 320)
# REMOVE THE SCIENTIFIC NOTATION FROM
pd.options.display.float_format = '{:.10f}'.format

# CONNECT TO BYBIT API
session = HTTP("https://api.bytick.com")

bybit_pairs_unfiltered = ["BTCUSDT", "ETHUSDT","SANDUSDT", "SHIB1000USDT", "ADAUSDT", "XRPUSDT", "MANAUSDT", "BNBUSDT", "SOLUSDT", "BITUSDT", "DOTUSDT", "DOGEUSDT", "SFPUSDT", "UNIUSDT", "CROUSDT", "LUNAUSDT", "AVAXUSDT", "LINKUSDT", "LTCUSDT", "ALGOUSDT", "BCHUSDT", "ATOMUSDT", "MATICUSDT", "FILUSDT", "ICPUSDT", "ETCUSDT", "XLMUSDT", "VETUSDT", "AXSUSDT", "TRXUSDT", "FTTUSDT", "XTZUSDT", "THETAUSDT", "HBARUSDT", "EGLDUSDT", "EOSUSDT", "ZENUSDT", "AAVEUSDT", "FLOWUSDT", "NEARUSDT", "FTMUSDT", "GALAUSDT", "REQUSDT", "KSMUSDT", "RUNEUSDT", "IMXUSDT", "OMGUSDT", "SCUSDT", "IOTXUSDT", "BATUSDT", "DASHUSDT", "COMPUSDT", "ONEUSDT", "CHZUSDT", "LRCUSDT", "STXUSDT", "ZECUSDT", "ENJUSDT", "XEMUSDT", "SUSHIUSDT", "ANKRUSDT", "GRTUSDT", "RENUSDT", "DYDXUSDT", "RSRUSDT", "SRMUSDT", "CRVUSDT", "IOSTUSDT", "CELRUSDT", "1INCHUSDT", "STORJUSDT", "AUDIOUSDT", "COTIUSDT", "CHRUSDT", "CVCUSDT", "WOOUSDT", "ALICEUSDT", "ENSUSDT", "C98USDT", "YGGUSDT", "ILVUSDT", "RNDRUSDT", "MASKUSDT", "TLMUSDT", "SLPUSDT", "GTCUSDT", "LITUSDT", "CTKUSDT", "BICOUSDT", "YFIUSDT", "SXPUSDT", "BSVUSDT", "KLAYUSDT", "QTUMUSDT", "SNXUSDT", "LPTUSDT", "SPELLUSDT", "ANTUSDT", "DUSKUSDT", "ARUSDT", "PEOPLEUSDT", "IOTAUSDT", "CELOUSDT", "WAVESUSDT", "RVNUSDT", "KNCUSDT", "KAVAUSDT", "DENTUSDT", "XMRUSDT", "ROSEUSDT", "CREAMUSDT", "JASMYUSDT", "LOOKSUSDT", "HNTUSDT", "10000NFTUSDT", "NEOUSDT", "ZILUSDT", "CKBUSDT", "RAYUSDT", "MKRUSDT", "REEFUSDT", "BANDUSDT", "RSS3USDT", "OCEANUSDT", "1000BTTUSDT", "SUNUSDT", "JSTUSDT", "API3USDT"]
df = pd.DataFrame(columns=['Bybit_Symbol', 'Bybit_Volume', 'FTX_Symbol', 'FTX_Volume'])

for item in bybit_pairs_unfiltered:
    try:
        # ----------  get bybit data  --------------
        data_perp = session.latest_information_for_symbol(symbol=item)
        bybit_24volume = float(data_perp["result"][0]['turnover_24h'])
        bybit_data = [item, bybit_24volume]

        # ----------  get ftx data  --------------
        symbol = item.replace("USDT", "/USD")
        data_spot = requests.get(f"https://ftx.com/api/markets/{symbol}").json()
        ftx_name = data_spot["result"]['name']
        ftx_24volume = data_spot["result"]['volumeUsd24h']
        ftx_data = [ftx_name, ftx_24volume]

        # ----------  add data to lists  --------------
        combined_data = bybit_data + ftx_data
        combined_series = pd.Series(combined_data, index=df.columns)
        df = df.append(combined_series, ignore_index=True)

    except Exception as e:
        pass


# verwijder coins met te laag 24h volume
df = df.loc[df.FTX_Volume > 1000000]
df = df.loc[df.Bybit_Volume > 1000000]

# # Sorteer obv spread
# df.sort_values("spread_%", ascending=False, inplace=True)

coins = df["Bybit_Symbol"].tolist()

# CONNECT TO FTX WS
ws_ftx = websocket_code.FtxWebsocketClient()
try:
    ws_ftx.connect()
    print("Connected to FTX websocket")
except Exception as e:
    print(f"Websocket Error Message: {e}")
    sys.exit()

# CONNECT TO BYBIT WS
subs_text = "instrument_info.100ms."
bybit_subs = []
for item in coins:
    bybit_subs.append(subs_text + item)
ws_bybit = WebSocket("wss://stream.bybit.com/realtime_public", subscriptions=bybit_subs)

df_allcoins = pd.DataFrame(columns=coins)

while df_allcoins.shape[0] < 60:  # 1 minute of data
    time.sleep(1)
    new_row = {}

    for count, value in enumerate(coins):
        # BYBIT
        perp = ws_bybit.fetch(bybit_subs[count])
        # FTX
        coin = value.replace("USDT", "/USD")
        spot = (ws_ftx.get_ticker(market=f"{coin}"))

        if perp != {} and spot != {}:
            perp_ask = float(perp['ask1_price'])
            spot_ask = spot['ask']
            # Bereken cross-exchange basis spread
            spread = ((perp_ask - spot_ask) / spot_ask )*100
            new_row[value] = spread
    df_allcoins = df_allcoins.append(new_row, ignore_index=True)
    df_allcoins = df_allcoins.dropna()
    print(df_allcoins)

means = df_allcoins.mean()
means.sort_values(ascending=False, inplace=True)
print(means)
print(means[0])
