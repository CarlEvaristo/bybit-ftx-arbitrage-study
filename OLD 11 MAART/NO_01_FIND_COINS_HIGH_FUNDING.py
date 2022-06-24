import pandas as pd
import requests
import datetime as datetime
import time

def highest_predicted_funding_coin():
    # console setting (making space for printing df's)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.max_rows', 10000000)
    pd.set_option('display.width', 320)

    # REMOVE THE SCIENTIFIC NOTATION FROM
    pd.options.display.float_format = '{:.10f}'.format

    ## DOWNLOAD FUNDING RATE DATA

    funding = requests.get('https://ftx.com/api/funding_rates').json()
    funding = pd.DataFrame(funding['result'])
    funding["rate"] = funding["rate"] * 100

    funding.sort_values(["future", "time"], ascending=[True, False], inplace=True)
    funding.drop_duplicates(subset="future", inplace=True)

    perps = requests.get('https://ftx.com/api/futures').json()
    df = pd.DataFrame(perps['result'])
    df = df.loc[df.perpetual == True]
    df.rename(columns={"name": "future"}, inplace=True)
    df.rename(columns={"volumeUsd24h": "volume24perp"}, inplace=True)
    df = df.loc[:, ["future", "underlying", "volume24perp"]]

    combined = df.merge(funding, how="left", on="future").dropna(how="any")
    combined.sort_values("rate", ascending=False, inplace=True)
    combined = combined.loc[combined.volume24perp > 2000000]
    combined = combined.loc[combined.rate > 0.0015]

    for index, row in combined.iterrows():
        predicted = requests.get(f"https://ftx.com/api/futures/{row['future']}/stats").json()
        predicted = pd.DataFrame(predicted["result"], index=[0])
        predicted = (predicted.iloc[0, 1] * 100)
        combined.loc[index, "predicted"] = predicted

    combined = combined.loc[combined.predicted > 0.0015]
    combined.sort_values("predicted", ascending=False, inplace=True)
    combined.drop(columns=['time'], inplace=True)

    now = int(datetime.datetime.now().timestamp())
    hr24_back = (now - 86400)

    for index, row in combined.iterrows():
        funding_24hr = requests.get('https://ftx.com/api/funding_rates',
                                    params={'start_time': hr24_back, 'end_time': now, "future": row['future']}).json()
        funding_24hr = pd.DataFrame(funding_24hr['result'])
        funding_24hr = funding_24hr["rate"].mean() * 100
        combined.loc[index, "daily"] = funding_24hr

    # combined = combined.loc[combined.daily > 0.001]

    for index, row in combined.iterrows():
        spot_market = requests.get(f"https://ftx.com/api/markets/{row['underlying']}/USD").json()
        spot_market = pd.DataFrame(spot_market, index=[0])
        spot_market = spot_market.iloc[0, 0]
        combined.loc[index, "spot"] = spot_market

    combined = combined.loc[combined.spot == True]

    for index, row in combined.iterrows():
        spot_volume24 = requests.get(f"https://ftx.com/api/markets/{row['underlying']}/USD").json()
        spot_volume24 = pd.DataFrame(spot_volume24['result'], index=[0])
        spot_volume24 = spot_volume24.iloc[0, 20]
        combined.loc[index, "volume24spot"] = spot_volume24

    combined = combined.loc[combined.volume24spot > 2000000]

    combined.volume24spot = pd.to_numeric(combined.volume24spot, downcast="float")
    combined.volume24perp = pd.to_numeric(combined.volume24perp, downcast="float")

    combined.volume24perp = combined.volume24perp.map('{:.0f}'.format)
    combined.volume24spot = combined.volume24spot.map('{:.0f}'.format)

    if combined.size == 0:
        print("Currently no coins with sufficient funding and volume. Retry starts after 1 min pause.")
        time.sleep(60)
        highest_predicted_funding_coin()
    else:
        print("Available coins: ")
        print(combined)
        coin = combined.iloc[0, 1]
        print("Best coin: ", coin)
        return coin

highest_predicted_funding_coin()