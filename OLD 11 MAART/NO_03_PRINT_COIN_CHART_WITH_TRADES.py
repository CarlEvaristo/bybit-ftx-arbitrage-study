import sqlite3
import pandas as pd
import pandas_ta as ta
import matplotlib.pyplot as plt

coin="SOL"
stdev_num=2
stdev_lookback=4000
num_rows=50000

coin_spot = f"{coin.upper()}/USD"
coin_perp = f"{coin.upper()}-PERP"

# IMPORTANT: HERE I READ X NR OF ROWS (1 ROW == +/-1 SECOND) FROM THE DATABASE
connection = sqlite3.connect(f'database/{coin}.db')
sql_query = pd.read_sql_query(f''' SELECT * FROM coin_{coin} ORDER BY time DESC LIMIT {num_rows} ''', connection)

# read database
df = pd.DataFrame(sql_query, columns = ['time', 'spot_bid', 'spot_ask', 'spot_last', 'perp_bid', 'perp_ask', 'perp_last'])
# df = df.astype(float)
df.time = df.time.astype(float)
df.spot_bid = df.spot_bid.astype(float)
df.spot_ask = df.spot_ask.astype(float)
df.spot_last = df.spot_last.astype(float)
df.perp_bid = df.perp_bid.astype(float)
df.perp_ask = df.perp_ask.astype(float)
df.perp_last = df.perp_last.astype(float)


# convert unix timestamp to datetime and set as index
df['time'] = pd.to_datetime(df['time'],unit='s')
df = df.set_index('time')
df.index = df.index.tz_localize('GMT')
df.index = df.index.tz_convert('Europe/Amsterdam')
df.sort_index(ascending=True, inplace=True)


# relative statistics -----------------------------------------------------------------------------------------
df["spread_%"] = ((df["perp_bid"] - df["spot_bid"]) / df["spot_bid"])*100  # WHOLE SPREAD for ratio do nothing, for % do *100
# (perp bid - spot bid) --> (niet perp ask) want perp is market sell order (cross spread towards bid)

df["mean_%"] = ta.sma(close=df["spread_%"], length=stdev_lookback)
df["st_dev_%"] = ta.stdev(close=df["spread_%"], length=stdev_lookback)
df["mean_plus_st_dev_%"] = df["mean_%"] + (df["st_dev_%"] * stdev_num)
df["mean_minus_st_dev_%"] = df["mean_%"] - (df["st_dev_%"] * stdev_num)
# -------------------------------------------------------------------------------------------------------------

df["mid_price"] = (df["perp_bid"] + df["spot_bid"]) / 2  # HALF SPREAD !!!!!!!!!!

# LET OP LET OP MARKET ORDER AANPASSING / MARKET ORDER AANPASSING / MARKET ORDER AANPASSING / MARKET ORDER AANPASSING 1/2
# IPV PERP BEST ASK DOE IK PERP BEST BID OM SPREAD TE BEREKENEN    (MARKET ORDER STEEKT SPOT SPREAD OVER!!!!!!!!!!!!!!!!!!)
df["spread"] = (df["perp_bid"] - df["spot_bid"])  # WHOLE "spread" -> NOT HALF SPREAD !!!!!!!!!!
# (perp bid - spot bid) --> (niet perp ask) want perp is market sell order (cross spread towards bid)

df["spread_mean"] = ta.sma(close=df["spread"], length=stdev_lookback)
df["st_dev"] = ta.stdev(close=df["spread"], length=stdev_lookback)
df["mean_plus_st_dev"] = df["spread_mean"] + (df["st_dev"] * stdev_num)

# # ASK PRICE: PERP EERST
# # MARKET ORDER SPOT: SPOT BEST ASK (IPV SPOT BEST BID) OM SPREAD TE BEREKENEN  (MO STEEKT SPOT SPREAD OVER!!!!!!!!!!!!!!!!!!)
# df["my_ask_price"] = df["spot_ask"] + df["mean_plus_st_dev"]  # market order versie

# BID PRIJS: SPOT EERST
# MARKET ORDER PERP: PERP BEST BID (IPV PERP BEST ASK) OM SPREAD TE BEREKENEN  (MO STEEKT SPOT SPREAD OVER!!!!!!!!!!!!!!!!!!)
df["my_bid_price"] = df["perp_bid"] - df["mean_plus_st_dev"]

df2 = df[["perp_bid", "spot_bid", "my_bid_price"]]  # , "my_ask_price"
df = df[["spread_%", "mean_plus_st_dev_%", "mean_%", "mean_minus_st_dev_%"]]



df3 = pd.DataFrame(sql_query, columns = ['time', 'Price', 'Size', 'Side', 'Market_Buys', 'Market_Sells'])
df3.time = df3.time.astype(float)
df3.Market_Buys = df3.Size.loc[df3.Side == 'g']
df3.Market_Sells = df3.Size.loc[df3.Side == 'r']
df3.Price = df3.Price.astype(float)
df3.Size = df3.Size.astype(float)
df3.Market_Buys = df3.Market_Buys.astype(float)
df3.Market_Sells = df3.Market_Sells.astype(float)
df3.Market_Buys.dropna()
df3.Market_Sells.dropna()
df3 = df3[['time', 'Market_Buys', 'Market_Sells']]

# convert unix timestamp to datetime and set as index
df3['time'] = pd.to_datetime(df3['time'],unit='s')
df3 = df3.set_index('time')
df3.index = df3.index.tz_localize('GMT')
df3.index = df3.index.tz_convert('Europe/Amsterdam')
df3.sort_index(ascending=True, inplace=True)

print(df3)

fig, axes = plt.subplots(nrows=3, ncols=1, sharex="all")
df.plot(ax=axes[0], grid=False, color={'spread_%': '#D3D3D3', 'mean_plus_st_dev_%': 'b', 'mean_%': 'r', "mean_minus_st_dev_%": 'b'})
df2.plot(ax=axes[1], grid=False, color={'perp_bid': 'r', 'spot_bid': 'y', "my_bid_price": 'black'})
df3.plot(ax=axes[2], grid=False, color={'Market_Buys': 'g', 'Market_Sells': 'r'})

plt.xticks(rotation=15, )
plt.show()


# # dStart = "2021-10-26 17:40:00+02:00"
# # dEnd = "2021-10-26 17:50:00+02:00"
# # xlim = (dStart,dEnd)
#
# df2 = df[["perp_bid", "spot_bid", "my_bid_price"]]  # , "my_ask_price"
# df2.plot(color={'perp_bid': 'r', 'spot_bid': 'y', "my_bid_price": 'black'})  # "my_ask_price": 'b',
# plt.show()
#
#
# df = df[["spread_%", "mean_plus_st_dev_%", "mean_%", "mean_minus_st_dev_%"]]
# df.plot(color={'spread_%': '#D3D3D3', 'mean_plus_st_dev_%': 'b', 'mean_%': 'r', "mean_minus_st_dev_%": 'b'})
# plt.axhline(y=0, color='black', linestyle='-')
# plt.show()