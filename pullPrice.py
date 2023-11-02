import yfinance as yf
import pandas as pd
from SP500Wiki import *
from methodsNEW import *

conn = establish_database_connection()
cur = conn.cursor()

# Ticker Extraction
get_tickers_query = "SELECT symbol FROM sp500wiki;"
cur.execute(get_tickers_query)
tickers = [record[0] for record in cur.fetchall()]
tickers = tickers[0:10]

# User date range input
startdate = input("Enter the start date YYYY-MM-DD: ")
enddate = input("Enter the end date YYYY-MM-DD: ")


for ticker_symbol in tickers:
    ticker = yf.Ticker(ticker_symbol)
    
    # Get daily data
    data = ticker.history(start=startdate, end=enddate)
    data2 = yf.download(ticker_symbol, period="1d", interval="1m", start=startdate, end=enddate)  #  1m is lowest denominator

    if not data.empty:
        data['Ticker'] = ticker_symbol
        data.reset_index(inplace=True)
        data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')
        data_list = data.to_dict('records')
        update_or_insert_yf_daily_data(cur, ticker_symbol, data_list)

    # Get intraday data
    if not data2.empty:
        data2.reset_index(inplace=True)
        data2['Date'] = data2['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        data2['Ticker'] = ticker_symbol
        data2['Delta'] = data2['Close'] - data2['Open']
        data2['Hourly_RoC'] = data2['Close'].diff(periods=60)
        data2_list = data2.to_dict('records')
        update_or_insert_yf_intraday_data(cur, ticker_symbol, data2_list)

# Commit changes to the database and close the cursor
conn.commit()
cur.close()
