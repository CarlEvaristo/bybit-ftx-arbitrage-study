import pandas as pd
import requests


class GetData:

    def __init__(self, volume_threshold):
        # download all perps data (this is without funding data)
        self.perps = requests.get('https://ftx.com/api/futures').json()
        self.volume_threshold = volume_threshold
        self.perps = pd.DataFrame(self.perps['result'])
        self.perps = self.perps.loc[self.perps.perpetual == True]
        self.perps.rename(columns={"name": "perpetual"}, inplace=True)
        self.perps = self.perps.loc[:,["perpetual", "underlying", "volumeUsd24h"]]
        self.perps.rename(columns={"volumeUsd24h": "volume24perp"}, inplace=True)

        # check per coin if it has an underlying spot pair
        for index, row in self.perps.iterrows():
            self.spot_market = requests.get(f"https://ftx.com/api/markets/{row['underlying']}/USD").json()
            self.spot_market = pd.DataFrame(self.spot_market, index=[0])
            self.spot_market = self.spot_market.iloc[0, 0]
            self.perps.loc[index, "spot"] = self.spot_market

        # only keep perps that have spot pairs
        self.perps = self.perps.loc[self.perps.spot == True]

        # download spot 24 hrs volume and add "volume24spot" column
        for index, row in self.perps.iterrows():
            self.spot_volume24 = requests.get(f"https://ftx.com/api/markets/{row['underlying']}/USD").json()
            self.spot_volume24 = pd.DataFrame(self.spot_volume24['result'], index=[0])
            self.spot_volume24 = self.spot_volume24.iloc[0, 20]
            self.perps.loc[index, "volume24spot"] = self.spot_volume24

        # make spot and perp volume columns int type
        self.perps = self.perps.astype({"volume24perp": float, "volume24spot": float})

        # filter for perp coins with high volume
        self.perps = self.perps.loc[self.perps.volume24perp > self.volume_threshold]

        # filter for spot coins with high volume
        self.perps = self.perps.loc[self.perps.volume24spot > self.volume_threshold]


        # sorteer alles op volume spot en perp columns
        self.perps.sort_values(["volume24spot", "volume24perp"], ascending=False, inplace=True)


    def get_available_coins_list(self):
        available_coins = self.perps["underlying"].tolist()
        return(available_coins)
