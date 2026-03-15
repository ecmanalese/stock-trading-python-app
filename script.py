import requests
import os
import csv
from dotenv import load_dotenv
import snowflake.connector
from datetime import datetime

load_dotenv()

POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
LIMIT = 1000
API_CALLS_PER_MINUTE = 5

def load_stock_tickers_job():
    datestamp = datetime.now().strftime('%Y-%m-%d')
    url = f"https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
    response = requests.get(url)
    tickers = []

    if response.status_code >= 400:
        print(f"Initial request failed ({response.status_code}): {response.text}")
        return []

    data = response.json()
    for ticker in data['results']:
        ticker['datestamp'] = datestamp
        tickers.append(ticker)
    print(f"Loaded {len(tickers)} tickers")

    while 'next_url' in data:
        response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        if response.status_code >= 400:
            print(f"Stopping after status {response.status_code}: {response.text}")
            break
        data = response.json()
        for ticker in data['results']:
            ticker['datestamp'] = datestamp
            tickers.append(ticker)
        print(f"Loaded {len(tickers)} tickers")
        print('Loading next page')

    print(f"{len(tickers)} tickers loaded")
    return tickers

def dump_to_csv(data):
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
        for ticker in data:
            row = {key: ticker.get(key, '') for key in headers}
            writer.writerow(row)
    print(f"{len(data)} loaded to csv")

def load_to_snowflake(data):
    connect_kwargs = {
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PW'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA'),
    }
    conn = snowflake.connector.connect(**connect_kwargs)
    cursor = conn.cursor()

    create_table_query = """
        CREATE TABLE IF NOT EXISTS stock_tickers (
            ticker VARCHAR, 
            name VARCHAR,
            market VARCHAR,
            locale VARCHAR,
            primary_exchange VARCHAR,
            type VARCHAR, 
            active BOOLEAN,
            currency_name VARCHAR,
            cik VARCHAR,
            composite_figi VARCHAR,
            share_class_figi VARCHAR,
            last_updated_utc TIMESTAMP,
            datestamp DATE);
        """
    
    cursor.execute(create_table_query)
    
    # Ensure all required keys are present in each ticker dict
    required_keys = ['ticker', 'name', 'market', 'locale', 'primary_exchange', 'type', 'active', 'currency_name', 'cik', 'composite_figi', 'share_class_figi', 'last_updated_utc', 'datestamp']
    for ticker in data:
        for key in required_keys:
            ticker.setdefault(key, '')
    
    insert_query = """
        INSERT INTO stock_tickers (
            ticker, 
            name, 
            market, 
            locale, 
            primary_exchange, 
            type, 
            active, 
            currency_name, 
            cik, 
            composite_figi, 
            share_class_figi, 
            last_updated_utc,
            datestamp
        )
        VALUES (
            %(ticker)s, 
            %(name)s, 
            %(market)s, 
            %(locale)s, 
            %(primary_exchange)s, 
            %(type)s, 
            %(active)s, 
            %(currency_name)s, 
            %(cik)s, 
            %(composite_figi)s, 
            %(share_class_figi)s, 
            %(last_updated_utc)s,
            %(datestamp)s
            )
    """
    cursor.executemany(insert_query, data)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"{len(data)} loaded to snowflake")

if __name__ == "__main__":
    data = load_stock_tickers_job()
    load_to_snowflake(data)