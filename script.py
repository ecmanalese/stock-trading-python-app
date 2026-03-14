import requests
import os
import csv
from dotenv import load_dotenv

load_dotenv()

POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
LIMIT = 20
url = f"https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
response = requests.get(url)
tickers = []

data = response.json()

for ticker in data['results']:
    tickers.append(ticker)

while 'next_url' in data:
    print('next page')
    response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
    data = response.json()
    print(data)
    for ticker in data['results']:
        tickers.append(ticker)

sample_ticker = {
    'ticker': 'HBB', 
    'name': 'Hamilton Beach Brands Holding Company Class A Common Stock', 
    'market': 'stocks', 
    'locale': 'us', 
    'primary_exchange': 'XNYS', 
    'type': 'CS', 
    'active': True, 
    'currency_name': 'usd', 
    'cik': '0001709164', 
    'composite_figi': 'BBG00HJ4P620', 
    'share_class_figi': 'BBG00HJ4P6S2', 
    'last_updated_utc': '2026-03-13T06:07:59.481403843Z'
    }

headers = sample_ticker.keys()
output_csv = 'tickers.csv'
with open(output_csv, 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=headers)
    writer.writeheader()
    for ticker in tickers:
        row = {key: ticker[key] for key in headers}
        writer.writerow(row)