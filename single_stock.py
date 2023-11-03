import datetime
import backtrader as bt
import backtrader.analyzers as btanalyzers
import yfinance as yf
import pandas as pd
from strategies import *
from Methods import *

if __name__ == "__main__":
    portfolio = ["A", "AAPL", "AAL"]  # List of tickers to process
    portfolio = ["A"]
    strategy_class = Strat0TEST
    dataframes = []  

    for ticker in portfolio:
        data = yf.download(ticker, start="2022-07-01", end="2023-10-31")
        data = bt.feeds.PandasData(dataname=data)
        cerebro = bt.Cerebro()
        cerebro.adddata(data, name=ticker)

        # Use the strategy class defined earlier
        cerebro.addstrategy(strategy_class)

        cerebro.broker.setcash(1000000.0)
        cerebro.addsizer(bt.sizers.PercentSizer, percents=10)

        print(f'Starting Portfolio Value for {ticker}: %.2f' % cerebro.broker.getvalue())
        back = cerebro.run()
        print(f'Final Portfolio Value for {ticker}: %.2f' % cerebro.broker.getvalue())

        conn = establish_database_connection()  # Establish a database connection
        cur = conn.cursor()  # Create a database cursor
        tradelogdf = strategy_class.get_trade_history_df()
        holdings = strategy_class.get_holdings_df()

        if not tradelogdf.empty:  # Trade log
            for _, row in tradelogdf.iterrows():
                update_or_insert_tradelog_data(cur, ticker, row)
            conn.commit()

        if not holdings.empty:  # Holdings
            for _, row in holdings.iterrows():
                update_or_insert_holding_data(cur, ticker, row)
            conn.commit()
            cur.close()

        dataframes.append((ticker, tradelogdf, holdings))  # Store dataframes for this ticker

    tradelogdf = tradelogdf.sort_values(by='Date', ascending=True)
    for ticker, tradelogdf, holdings in dataframes:
        print(f"Trade Log for {ticker}:")
        print(tradelogdf)
        print(f"Holdings for {ticker}:")
        print(holdings)
