# import required packages and method from other files
import yfinance as yf
from SP500Wiki import *
from methodsNEW import *


conn = establish_database_connection()  # Establish a database connection
cur = conn.cursor()  # # Create a database cursor


# Ticker Extraction
get_tickers_query = "SELECT symbol FROM sp500wiki;"  # Define the SQL query to fetch tickers from the 'sp500wiki' table
cur.execute(get_tickers_query)  # Execute the SQL query to retrieve tickers
tickers = [record[0] for record in cur.fetchall()]  # Fetch the tickers and store them in a list
tickers = tickers[0:4]  # Select the first four tickers for time and space sake

# Financial dataframes
general = pd.DataFrame()  # Create an empty DataFrame for general data
income_statement_Q = pd.DataFrame()  # Create an empty DataFrame for quarterly income statements
income_statement_Y = pd.DataFrame()  # Create an empty DataFrame for yearly income statements
balance_sheet_Q = pd.DataFrame()  # Create an empty DataFrame for quarterly balance sheets
balance_sheet_Y = pd.DataFrame()  # Create an empty DataFrame for yearly balance sheets
cashflow_Q = pd.DataFrame()  # Create an empty DataFrame for quarterly cash flow statements
cashflow_Y = pd.DataFrame()  # Create an empty DataFrame for yearly cash flow statements

data_frames = []

# Iterate through the tickers
for ticker in tickers:
    try:
        # Download the ticker's data
        ticker_data = yf.Ticker(ticker)
        info = ticker_data.info

        # Create a DataFrame for the ticker's information
        general = pd.DataFrame({
            'Ticker': [ticker],
            'StockExchange': [info.get('exchange', None)],
            'Name': [info.get('longName', None)],
            'Industry': [info.get('industry', None)],
            'Sector': [info.get('sector', None)],
            'LongDescription': [info.get('longBusinessSummary', None)],
            'Website': [info.get('website', None)],
            # 'CIK' : [info.get('address1', None)],# cik
            'HeadQuarter' : [info.get('address1', None)],# hq
            'Founded': [info.get('startDate', None)] # foundation
        })
        data_frames.append(general)
        # print(ticker)
    except Exception as e:
        print(f"Error retrieving data for {ticker}: {str(e)}")

    # Concatenate the list of DataFrames into one DataFrame
print(data_frames)

# Income Statement quarterly 
for ticker_symbol in tickers:
    ticker = yf.Ticker(ticker_symbol)
    df = ticker.quarterly_income_stmt.T
    df['Ticker'] = ticker_symbol
    df['Date'] = df.index.strftime('%Y-%m-%d')
    if not df.empty:
        cur = conn.cursor()
        for _, row in df.iterrows():  # Iterate through all rows of data
            update_or_insert_yf_income_statement_data(cur, ticker_symbol, row)
        conn.commit()  # Commit changes to the database
        cur.close()
    income_statement_Q = pd.concat([income_statement_Q, df])

# Balance Sheet quarterly
for ticker_symbol in tickers:
    ticker = yf.Ticker(ticker_symbol)
    df = ticker.quarterly_balance_sheet.T
    df['Ticker'] = ticker_symbol
    df['Date'] = df.index.strftime('%Y-%m-%d')
    if not df.empty:
        cur = conn.cursor()
        for _, row in df.iterrows():  # Iterate through all rows of data
            update_or_insert_yf_balance_sheet_data(cur, ticker_symbol, row)
        conn.commit()  # Commit changes to the database
        cur.close()
    balance_sheet_Q = pd.concat([balance_sheet_Q, df])

# CF quarterly
for ticker_symbol in tickers:
    ticker = yf.Ticker(ticker_symbol)
    df = ticker.quarterly_cashflow.T
    df['Ticker'] = ticker_symbol
    df['Date'] = df.index.strftime('%Y-%m-%d')
    if not df.empty:
        cur = conn.cursor()
        for _, row in df.iterrows():  # Iterate through all rows of data
            update_or_insert_yf_cash_flow_data(cur, ticker_symbol, row)
        conn.commit()  # Commit changes to the database
        cur.close()
    cashflow_Q = pd.concat([cashflow_Q, df])