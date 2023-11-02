The set of calls

1. Ensure method and strategies files is in same folder as respective code.
2. run CreateTable.py
3. run SP500wiki.py to extract all stock tickers in S&P500 according to Wikipedia.
4. run pulldata2.py to update SQL tables: General information, Income Statement, Balance Sheet and Cashflow data.
5. run pullPrice.py to get daily price and intraday price data for all stocks within a set timeperiod from Yahoo Finance.
6. Execute backtraderonSQL to establish connection between SQL files and python code. This is where we will test out traidng algorithms
7. Perfromance and holdings data will be saved in files here
