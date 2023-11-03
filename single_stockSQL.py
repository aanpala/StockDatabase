import datetime
import backtrader as bt
import backtrader.analyzers as btanalyzers
import sqlalchemy
import pandas as pd
from strategies import *
from Methods import *

# Define your database connection string
database_url = "postgresql://postgres:bobwashere@localhost:5432/MyDatabase"
engine = sqlalchemy.create_engine(database_url)

class MyData(bt.feeds.PandasData):
    params = (
        ('ticker', 'ticker'),
        ('datetime', 'date'),
        ('open', 'open'),
        ('close', 'close'),
        ('volume', 'volume'),
    )

if __name__ == "__main__":
    # Define your ticker, start date, and end date
    ticker = "A"
    start_date = "2022-07-01"
    end_date = "2023-10-31"

    # Fetch data from the SQL table using a query
    query = f"SELECT * FROM yfDailyPrice WHERE ticker = '{ticker}' AND date BETWEEN '{start_date}' AND '{end_date}'"
    df = pd.read_sql(query, engine)

    # Convert the 'date' column to datetime
    df['date'] = pd.to_datetime(df['date'])

    # Create a Backtrader data feed
    data = MyData(dataname=df, name=ticker)

    cerebro = bt.Cerebro()
    cerebro.adddata(data, name=ticker)
    cerebro.addstrategy(Strat2RSIBollinger)
    cerebro.broker.setcash(1000000.0)
    cerebro.addsizer(bt.sizers.PercentSizer, percents=10)

    print(f'Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    back = cerebro.run()
    print(f'Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

    conn = establish_database_connection()  # Establish a database connection
    cur = conn.cursor()  # Create a database cursor
    tradelogdf = Strat2RSIBollinger.get_trade_history_df()
    holdings = Strat2RSIBollinger.get_holdings_df()
    print(tradelogdf)
    print(holdings)

    if not tradelogdf.empty:
        for _, row in tradelogdf.iterrows():
            update_or_insert_tradelog_data(cur, ticker, row)
        conn.commit()

    if not holdings.empty:
        for _, row in holdings.iterrows():
            update_or_insert_holding_data(cur, ticker, row)
        conn.commit()
        cur.close()

    cerebro.plot()[0]
