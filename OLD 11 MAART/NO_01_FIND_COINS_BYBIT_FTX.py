from pybit import HTTP
import requests
import pandas as pd

# console setting (making space for printing df's)
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.width', 320)
# REMOVE THE SCIENTIFIC NOTATION FROM
pd.options.display.float_format = '{:.10f}'.format

session = HTTP("https://api-testnet.bybit.com")

bybit_pairs_unfiltered = ["BTCUSDT", "ETHUSDT","SANDUSDT", "SHIB1000USDT", "ADAUSDT", "XRPUSDT", "MANAUSDT", "BNBUSDT", "SOLUSDT", "BITUSDT", "DOTUSDT", "DOGEUSDT", "SFPUSDT", "UNIUSDT", "CROUSDT", "LUNAUSDT", "AVAXUSDT", "LINKUSDT", "LTCUSDT", "ALGOUSDT", "BCHUSDT", "ATOMUSDT", "MATICUSDT", "FILUSDT", "ICPUSDT", "ETCUSDT", "XLMUSDT", "VETUSDT", "AXSUSDT", "TRXUSDT", "FTTUSDT", "XTZUSDT", "THETAUSDT", "HBARUSDT", "EGLDUSDT", "EOSUSDT", "ZENUSDT", "AAVEUSDT", "FLOWUSDT", "NEARUSDT", "FTMUSDT", "GALAUSDT", "REQUSDT", "KSMUSDT", "RUNEUSDT", "IMXUSDT", "OMGUSDT", "SCUSDT", "IOTXUSDT", "BATUSDT", "DASHUSDT", "COMPUSDT", "ONEUSDT", "CHZUSDT", "LRCUSDT", "STXUSDT", "ZECUSDT", "ENJUSDT", "XEMUSDT", "SUSHIUSDT", "ANKRUSDT", "GRTUSDT", "RENUSDT", "DYDXUSDT", "RSRUSDT", "SRMUSDT", "CRVUSDT", "IOSTUSDT", "CELRUSDT", "1INCHUSDT", "STORJUSDT", "AUDIOUSDT", "COTIUSDT", "CHRUSDT", "CVCUSDT", "WOOUSDT", "ALICEUSDT", "ENSUSDT", "C98USDT", "YGGUSDT", "ILVUSDT", "RNDRUSDT", "MASKUSDT", "TLMUSDT", "SLPUSDT", "GTCUSDT", "LITUSDT", "CTKUSDT", "BICOUSDT", "YFIUSDT", "SXPUSDT", "BSVUSDT", "KLAYUSDT", "QTUMUSDT", "SNXUSDT", "LPTUSDT", "SPELLUSDT", "ANTUSDT", "DUSKUSDT", "ARUSDT", "PEOPLEUSDT", "IOTAUSDT", "CELOUSDT", "WAVESUSDT", "RVNUSDT", "KNCUSDT", "KAVAUSDT", "DENTUSDT", "XMRUSDT", "ROSEUSDT", "CREAMUSDT", "JASMYUSDT", "LOOKSUSDT", "HNTUSDT", "10000NFTUSDT", "NEOUSDT", "ZILUSDT", "CKBUSDT", "RAYUSDT", "MKRUSDT", "REEFUSDT", "BANDUSDT", "RSS3USDT", "OCEANUSDT", "1000BTTUSDT", "SUNUSDT", "JSTUSDT", "API3USDT"]

bybit_pairs = []
ftx_pairs = []

df = pd.DataFrame(columns=['Bybit_Symbol', 'Bybit_Volume', 'Bybit_Ask', 'FTX_Symbol', 'FTX_Volume', 'FTX_Ask'])

for item in bybit_pairs_unfiltered:

    try:
        # ----------  get data bybit  --------------
        data_perp = session.latest_information_for_symbol(symbol=item)
        bybit_24volume = float(data_perp["result"][0]['turnover_24h'])
        bybit_ask = float(data_perp["result"][0]['ask_price'])
        bybit_data = [item, bybit_24volume, bybit_ask]

        # ----------  get data ftx  --------------
        symbol = item.replace("USDT", "/USD")
        data_spot = requests.get(f"https://ftx.com/api/markets/{symbol}").json()
        ftx_name = data_spot["result"]['name']
        ftx_24volume = data_spot["result"]['volumeUsd24h']
        ftx_ask = data_spot["result"]['ask']
        ftx_data = [ftx_name,ftx_24volume,ftx_ask]
        # ----------  add data to lists  --------------
        combined_data = bybit_data + ftx_data
        combined_series = pd.Series(combined_data, index=df.columns)
        df = df.append(combined_series, ignore_index=True)

    except Exception as e:
        pass


# verwijder coins met te laag 24h volume
df = df.loc[df.FTX_Volume > 1000000]
df = df.loc[df.Bybit_Volume > 1000000]

# Bereken cross-exchange basis spread
df["spread_%"] = ((df["Bybit_Ask"] - df["FTX_Ask"]) / df["FTX_Ask"])*100
# Sorteer obv spread
df.sort_values("spread_%", ascending=False, inplace=True)

print(df)