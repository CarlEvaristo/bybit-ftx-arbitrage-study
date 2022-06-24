import requests

response = requests.post(f'https://ftx.com/api/markets/BTC/USD')
print(response.elapsed.total_seconds())

