#libraries
import backtrader as bt
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, DateTime
from sqlalchemy.sql import text

import psycopg2
import json

conn = psycopg2.connect(
    dbname='MyDatabase',
    user='postgres',
    password='bobwashere',
    host='localhost',
    port=5432  
)
def create_strategy_table_if_not_exists(cur):
    create_strategy_table_query = """
    CREATE TABLE IF NOT EXISTS strategy_table (
                class_name TEXT,
                param TEXT,
                description TEXT,
                PRIMARY KEY (class_name)
        )
    """
    cur.execute(create_strategy_table_query)

def update_or_insert_strategy(cur, class_name, param, description):
    # Check if the entry already exists based on class_name
    cur.execute("SELECT * FROM strategy_table WHERE class_name = %s", (class_name,))
    existing_row = cur.fetchone()

    if existing_row:
        # If it exists, update the entry
        cur.execute("UPDATE strategy_table SET param = %s, description = %s", (param, description))
    else:
        # If it doesn't exist, insert a new entry
        cur.execute("INSERT INTO strategy_table (class_name, param, description) VALUES (%s, %s, %s)", (class_name, param, description))


cur = conn.cursor()

create_strategy_table_if_not_exists(cur)
strategyList = [
        {"class_name": "Strat0TEST", "param": "sma_period, stop_loss_ratio", "description": "DescriptionA"},
        {"class_name": "Strat0SMA", "param": "sma_period, stop_loss_ratio", "description":"This strategy class implements a basic moving average crossover strategy. It buys (goes long) when the closing price crosses above a simple moving average (SMA) and sells (exits the position) when the closing price crosses below the SMA. It also includes a stop loss mechanism, which sells the position if the price falls below a certain ratio of the entry price."},
        {"class_name": "Strat1RSIVWAP", "param": "rsi_period, vwap_period, overbought, oversold, entry_threshold, exit_threshold, overbought_threshold, oversold_threshold", "description": "This strategy class combines the Relative Strength Index (RSI) and the Volume Weighted Average Price (VWAP) to make trading decisions. It buys when RSI is oversold, and the price is above VWAP. It shorts when RSI is overbought, and the price is below VWAP. It also includes exit conditions based on RSI and VWAP."},
        {"class_name": "Strat2RSIBollinger", "param": "bollinger_period, rsi_period, rsi_overbought, rsi_oversold, std_dev", "description": "This strategy class uses Bollinger Bands and RSI to make trading decisions. It goes long when the price touches the lower Bollinger Band and RSI is oversold. It shorts when the price touches the upper Bollinger Band and RSI is overbought. It includes conditions for closing positions based on Bollinger Bands and RSI."},
        {"class_name": "Strat3SupportResistance", "param": "vwap_period,rsi_period,rsi_overbought,rsi_oversold", "description": "This strategy class checks RSI values and tracks the portfolio value. It can be used for trading decisions based on RSI and portfolio value."},
        {"class_name": "Strat4MACD", "param": "vwap_period,rsi_period,rsi_overbought,rsi_oversold,macd_short_period,macd_long_period,macd_signal_period", "description": "This strategy class combines RSI, MACD (Moving Average Convergence Divergence), and VWAP to make trading decisions. It buys when RSI is oversold, MACD is bullish, and the price is above VWAP. It includes an exit condition when RSI becomes overbought or MACD turns bearish."},
    ]

for row in strategyList:
    update_or_insert_strategy(cur, row["class_name"], row["param"], row["description"])

conn.commit()
cur.close()
conn.close()

# structure
'''
Function Structure

params: Parameters

Methods:
    def __init__(self): In the __init__ method, initialize your strategy, set up data feeds, indicators, and any other initializations required.

    def next(self): Implement the next method, where you define the trading logic. This method is called on each new data point, and you make trading decisions here. Define conditions for entering and exiting trades, setting stop losses, take profits, etc.

1. Set up any technical indicators that your strategy will use. You can use built-in indicators from Backtrader's bt.indicators module or create custom indicators.

2. Order Execution:
Use the buy, sell, close, or other relevant methods to execute orders.
Manage the order lifecycle in the notify_order method.

3. Position Sizing and Risk Management:
Implement position sizing and risk management rules, such as portfolio allocation, position sizing, stop-loss levels, and trailing stops.

4. Performance and Data Logging:
Log and keep track of performance metrics, trade data, and any other relevant information for analysis.

5. Notifications and Alerts:
Implement notification or alert mechanisms for critical events in your strategy.

6. Backtesting and Optimization:
Perform backtesting on historical data to evaluate the strategy's performance. Optimize the strategy parameters if needed to improve performance.

7. Portfolio Management:
Manage the portfolio by tracking positions, portfolio value, and capital allocation.

8. Risk Analysis:
Assess and manage risk, including drawdown analysis and risk-reward ratios.

9. Custom Features:
Add any custom features or conditions specific to your strategy's logic.
'''

class VolumeWeightedAveragePrice(bt.Indicator):
    
    params = (('period', 20), )

    alias = ('VWAP', 'VolumeWeightedAveragePrice',)
    lines = ('VWAP',)

    def __init__(self):
        cumvol = bt.ind.SumN(self.data.volume, period=self.params.period)
        typprice = ((self.data.close + self.data.high + self.data.low) / 3) * self.data.volume
        cumtypprice = bt.ind.SumN(typprice, period=self.params.period)
        self.lines.VWAP = cumtypprice / cumvol
        super(VolumeWeightedAveragePrice, self).__init__()
    
class Strat0TEST(bt.Strategy):
    params = (
        ('sma_period', 20),
        ('stop_loss_ratio', 2.0),
    )

    trade_history = []
    holdings = {}
    holdingsdf = []

    def __init__(self):
        self.strategy_name = self.__class__.__name__
        self.sma = bt.indicators.SimpleMovingAverage(self.data, period=self.params.sma_period)
        self.in_trade = False
        self.entry_price = 0.0

    def store_trade(self, action, price):
        date = self.data.datetime.datetime(0)
        ticker = self.data._name
        quantity = self.position.size
        trade_info = {
            'Strategy': self.strategy_name,
            'Ticker': ticker,
            'Date': date,
            'Action': action,
            'Price': price,
            'Quantity': quantity,
        }
        self.__class__.trade_history.append(trade_info)

    @classmethod
    def get_trade_history_df(cls):
        return pd.DataFrame(cls.trade_history)

    @classmethod
    def get_holdings_df(cls):
        return pd.DataFrame(cls.holdingsdf)

    def next(self):
        if not self.in_trade:
            if self.data.close[0] > self.sma[0]:
                self.in_trade = True
                self.entry_price = self.data.close[0]
                self.__class__.trade_history.append({
                    'Strategy': self.strategy_name,
                    'Ticker': self.data._name,
                    'Date': self.data.datetime.datetime(0),
                    'Action': "Buy",
                    'Price': self.data.close[0],
                    'Quantity': self.position.size,
                    'PnL': (self.data.close[0] - self.entry_price) * abs(self.position.size),
                })
                self.__class__.holdingsdf.append({
                    'Strategy': self.strategy_name,
                    'Ticker': self.data._name,
                    'Date': self.data.datetime.datetime(0),
                    'Price':  self.data.close[0],
                    'Quantity': self.position.size  #self.holdings[ticker]['Quantity']
                })     
                self.buy()

            elif self.data.close[0] < self.sma[0]:
                self.in_trade = True
                self.entry_price = self.data.close[0]
                trade_info = {
                    'Strategy': self.strategy_name,
                    'Ticker': self.data._name,
                    'Date': self.data.datetime.datetime(0),
                    'Action': "Sell",
                    'Price': self.data.close[0],
                    'Quantity': self.position.size,
                    'PnL': (self.data.close[0] - self.entry_price) * abs(self.position.size),
                } 
                self.__class__.holdingsdf.append({
                    'Strategy': self.strategy_name,
                    'Ticker': self.data._name,
                    'Date': self.data.datetime.datetime(0),
                    'Price':  self.data.close[0],
                    'Quantity': self.position.size  #self.holdings[ticker]['Quantity']
                })    
                self.sell()
                self.__class__.trade_history.append(trade_info)

            ticker = self.data._name
            current_quantity = self.position.size
            current_price = self.data.close[0]

            if current_quantity != 0:
                # if ticker in self.holdings:
                #     self.holdings[ticker]['Quantity'] += current_quantity
                # else:
                #     self.holdings[ticker] = {
                #         'Strategy': self.strategy_name,
                #         'Ticker': ticker,
                #         'Date': self.data.datetime.datetime(0),
                #         'Price': current_price,
                #         'Quantity': current_quantity,
                #     }
                self.__class__.holdingsdf.append({
                    'Strategy': self.strategy_name,
                    'Ticker': ticker,
                    'Date': self.data.datetime.datetime(0),
                    'Price': current_price,
                    'Quantity': current_quantity  #self.holdings[ticker]['Quantity']
                })       
                
        elif self.in_trade:
            if self.data.close[0] < self.sma[0] or self.data.close[0] <= (1 - self.params.stop_loss_ratio) * self.entry_price:
                self.in_trade = False
                self.close()  # Close the current position
                action = 'Sell' if self.position.size > 0 else 'Cover'
                trade_info = {
                    'Strategy': self.strategy_name,
                    'Ticker': self.data._name,
                    'Date': self.data.datetime.datetime(0),
                    'Action': action,
                    'Price': self.data.close[0],
                    'Quantity': self.position.size,
                    'PnL': (self.data.close[0] - self.entry_price) * abs(self.position.size),
                } 
                self.__class__.trade_history.append(trade_info)
                
                ticker = self.data._name
                current_quantity = self.position.size
                current_price = self.data.close[0]

                if current_quantity != 0:
                #     if ticker in self.holdings:
                #         if action == 'Sell':
                #             self.holdings[ticker]['Quantity'] -= current_quantity
                #         elif action == 'Cover':
                #             self.holdings[ticker]['Quantity'] += current_quantity
                #     else:
                #         self.holdings[ticker] = {
                #             'Strategy': self.strategy_name,
                #             'Ticker': ticker,
                #             'Date': self.data.datetime.datetime(0),
                #             'Price': current_price,
                #             'Quantity': current_quantity,
                #         }
                    self.__class__.holdingsdf.append({
                        'Strategy': self.strategy_name,
                        'Ticker': ticker,
                        'Date': self.data.datetime.datetime(0),
                        'Price': current_price,
                        'Quantity': current_quantity  #self.holdings[ticker]['Quantity']
                    })

class Strat0SMA(bt.Strategy):
    Description = "This strategy class implements a basic moving average crossover strategy. It buys (goes long) when the closing price crosses above a simple moving average (SMA) and sells (exits the position) when the closing price crosses below the SMA. It also includes a stop loss mechanism, which sells the position if the price falls below a certain ratio of the entry price."
    params = (
        ('sma_period', 20),  # Period for Simple Moving Average
        ('stop_loss_ratio', 2.0),  # Stop loss as a ratio of the entry price
    )
    trade_history = []
    holdings = []
   
    def __init__(self):
        self.strategy_name = self.__class__.__name__
        self.sma = bt.indicators.SimpleMovingAverage(self.data, period=self.params.sma_period)
        self.in_trade = False
        self.entry_price = 0.0

    def store_trade(self, action, price):
        date = self.data.datetime.datetime(0)
        ticker = self.data._name
        quantity = self.position.size
        trade_info = {
            'Strategy': self.strategy_name,
            'Ticker': ticker,
            'Date': date,
            'Action': action,
            'Price': price,
            'Quantity': quantity,
        }
        self.__class__.trade_history.append(trade_info)

    @classmethod
    def get_trade_history_df(cls):
        return pd.DataFrame(cls.trade_history)

    @classmethod
    def get_holdings_df(cls):
        return pd.DataFrame(cls.holdingsdf)

    def next(self):
        if not self.in_trade:
            if self.data.close[0] > self.sma[0]:
                self.in_trade = True
                self.entry_price = self.data.close[0]
                self.buy()
            elif self.data.close[0] < self.sma[0]:
                self.in_trade = True
                self.entry_price = self.data.close[0]
                self.sell()

            ticker = self.data._name
            current_quantity = self.position.size
            current_price = self.data.close[0]

            if current_quantity != 0:
                if ticker in self.holdings:
                    self.holdings[ticker]['Quantity'] += current_quantity
                else:
                    self.holdings[ticker] = {
                        'Strategy': self.strategy_name,
                        'Ticker': ticker,
                        'Date': self.data.datetime.datetime(0),
                        'Price': current_price,
                        'Quantity': current_quantity,
                    }
                self.__class__.holdingsdf.append({
                    'Strategy': self.strategy_name,
                    'Ticker': ticker,
                    'Date': self.data.datetime.datetime(0),
                    'Price': current_price,
                    'Quantity': self.holdings[ticker]['Quantity']
                })       
                
        elif self.in_trade:
            if self.data.close[0] < self.sma[0] or self.data.close[0] <= (1 - self.params.stop_loss_ratio) * self.entry_price:
                self.in_trade = False
                self.close()  # Close the current position
                action = 'Sell' if self.position.size > 0 else 'Cover'
                trade_info = {
                    'Strategy' : self.strategy_name,
                    'Ticker': self.data._name,
                    'Date': self.data.datetime.datetime(0),
                    'Action': action,
                    'Price': self.data.close[0],
                    'Quantity': self.position.size,
                    'PnL': (self.data.close[0] - self.entry_price) * abs(self.position.size),
                } 
                self.__class__.trade_history.append(trade_info)
                
                ticker = self.data._name
                current_quantity = self.position.size
                current_price = self.data.close[0]

                if current_quantity != 0:
                    if ticker in self.holdings:
                        if action == 'Sell':
                            self.holdings[ticker]['Quantity'] -= current_quantity
                        elif action == 'Cover':
                            self.holdings[ticker]['Quantity'] += current_quantity
                    else:
                        self.holdings[ticker] = {
                            'Strategy': self.strategy_name,
                            'Ticker': ticker,
                            'Date': self.data.datetime.datetime(0),
                            'Price': current_price,
                            'Quantity': current_quantity,
                        }
                    self.__class__.holdingsdf.append({
                        'Strategy': self.strategy_name,
                        'Ticker': ticker,
                        'Date': self.data.datetime.datetime(0),
                        'Price': current_price,
                        'Quantity': self.holdings[ticker]['Quantity']
                    })
                                      
class Strat1RSIVWAP(bt.Strategy):
    Description= "This strategy class combines the Relative Strength Index (RSI) and the Volume Weighted Average Price (VWAP) to make trading decisions. It buys when RSI is oversold, and the price is above VWAP. It shorts when RSI is overbought, and the price is below VWAP. It also includes exit conditions based on RSI and VWAP."
    
    params = (
        ('rsi_period', 14),  # Period for RSI
        ('vwap_period', 20),  # Period for VWAP
        ('overbought', 70),  # RSI overbought threshold
        ('oversold', 30),  # RSI oversold threshold
        ('entry_threshold', 0.001),  # VWAP deviation threshold for entry
        ('exit_threshold', 0.005),  # VWAP deviation threshold for exit
        ('overbought_threshold', 1.01),  # Adjust this threshold as needed
        ('oversold_threshold', 0.99),  # Adjust this threshold as needed
    )
    trade_history = []
    def __init__(self):
        self.strategy_name = self.__class__.__name__
        self.rsi = bt.indicators.RelativeStrengthIndex(period=self.params.rsi_period)
        self.vwap = VolumeWeightedAveragePrice(period=self.params.vwap_period)
        self.in_trade = False
        self.entry_price = 0.0
        self.trade_history = []  # List to store trade records
    
    def next(self):
            if not self.in_trade:
                if self.rsi < self.params.oversold and self.data.close[0] > self.vwap[0] * self.params.overbought_threshold:
                    # Long entry condition: RSI is oversold, and price is above VWAP
                    self.in_trade = True
                    self.entry_price = self.data.close[0]
                    self.buy()
                    self.store_trade('Buy', self.entry_price)
                    print("Buy")

                elif self.rsi > self.params.overbought and self.data.close[0] < self.vwap[0] * self.params.oversold_threshold:
                    # Short entry condition: RSI is overbought, and price is below VWAP
                    self.in_trade = True
                    self.entry_price = self.data.close[0]
                    self.sell()  # Enter a short position
                    self.store_trade('Short', self.entry_price)
                    print("Short")

            elif self.in_trade:
                if ((self.rsi > self.params.overbought and self.position.size > 0) or
                    (self.rsi < self.params.oversold and self.position.size < 0)) and \
                    ((self.vwap[0] - self.data.close[0]) > self.params.exit_threshold and self.position.size > 0):
                    # Close the long position when RSI overbought and price below VWAP
                    self.close()
                    self.store_trade('Sell', self.data.close[0])
                    action = 'Sell'
                    print("Sell")
                    trade_info = {
                        'Strategy' : self.strategy_name,
                        'Ticker': self.data._name,
                        'Date': self.data.datetime.datetime(0),
                        'Action': action,
                        'Price': self.data.close[0],
                        'Quantity': self.position.size,
                        'PnL': (self.data.close[0] - self.entry_price) * abs(self.position.size),
                    } 
                
                    self.__class__.trade_history.append(trade_info)

                elif ((self.rsi < self.params.oversold and self.position.size < 0) or
                    (self.rsi > self.params.overbought and self.position.size > 0)) and \
                    ((self.data.close[0] - self.vwap[0]) > self.params.exit_threshold and self.position.size < 0):
                    # Close the short position when RSI oversold and price above VWAP
                    self.close()
                    self.store_trade('Cover', self.data.close[0])
                    action = 'Cover'
                    print("Cover")
                    trade_info = {
                        'Strategy' : self.strategy_name,
                        'Ticker': self.data._name,
                        'Date': self.data.datetime.datetime(0),
                        'Action': action,
                        'Price': self.data.close[0],
                        'Quantity': self.position.size,
                        'PnL': (self.data.close[0] - self.entry_price) * abs(self.position.size),
                    } 
                    self.__class__.trade_history.append(trade_info)

    def store_trade(self, action, price):
        date = self.data.datetime.datetime(0)
        ticker = self.data._name
        quantity = self.position.size
        trade_info = {
            'Strategy' : self.strategy_name,
            'Ticker': ticker,
            'Date': date,
            'Action': action,
            'Price': price,
            'Quantity': quantity,
        }
        self.__class__.trade_history.append(trade_info)
    @classmethod
    def get_trade_history_df(cls):
        return pd.DataFrame(cls.trade_history)
               
class Strat2RSIBollinger(bt.Strategy):
    Description = "This strategy class uses Bollinger Bands and RSI to make trading decisions. It goes long when the price touches the lower Bollinger Band and RSI is oversold. It shorts when the price touches the upper Bollinger Band and RSI is overbought. It includes conditions for closing positions based on Bollinger Bands and RSI."
    params = (
        ("bollinger_period", 30),  # Bollinger Band period
        ("rsi_period", 13),  # RSI period
        ("rsi_overbought", 70),  # RSI overbought level
        ("rsi_oversold", 30),  # RSI oversold level
        ("std_dev", 2),  # Standard deviation for Bollinger Bands
    )
    trade_history = []
    trade_history = []
    holdings = {}
    holdingsdf = []
    
    def __init__(self):
        self.strategy_name = self.__class__.__name__
        self.bollinger = bt.indicators.BollingerBands(self.data, period=self.params.bollinger_period, devfactor=self.params.std_dev)
        self.rsi = bt.indicators.RelativeStrengthIndex(period=self.params.rsi_period)
        self.in_trade = False
        self.entry_price = 0.0
        self.trade_history = []  # List to store trade records

    def store_trade(self, action, price):
        date = self.data.datetime.datetime(0)
        ticker = self.data._name
        quantity = self.position.size
        trade_info = {
            'Strategy': self.strategy_name,
            'Ticker': ticker,
            'Date': date,
            'Action': action,
            'Price': price,
            'Quantity': quantity,
        }
        self.__class__.trade_history.append(trade_info)

    @classmethod
    def get_trade_history_df(cls):
        return pd.DataFrame(cls.trade_history)

    @classmethod
    def get_holdings_df(cls):
        return pd.DataFrame(cls.holdingsdf)


    def next(self):
        if not self.in_trade:
            # Bullish Divergence: Lower low in price but higher low in RSI
            if (self.data.close[0] <= self.bollinger.lines.bot and self.rsi < self.params.rsi_oversold and self.data.close[0] < self.data.close[-1] and self.rsi < self.rsi[-1]):
                self.buy()
                self.in_trade = True
                self.entry_price = self.data.close[0]
                # print("Buy - Bullish Divergence")
                self.__class__.trade_history.append({
                    'Strategy': self.strategy_name,
                    'Ticker': self.data._name,
                    'Date': self.data.datetime.datetime(0),
                    'Action': "Long",
                    'Price': self.data.close[0],
                    'Quantity': self.position.size,
                    'PnL': (self.data.close[0] - self.entry_price) * abs(self.position.size),
                })
                self.__class__.holdingsdf.append({
                    'Strategy': self.strategy_name,
                    'Ticker': self.data._name,
                    'Date': self.data.datetime.datetime(0),
                    'Price':  self.data.close[0],
                    'Quantity': self.position.size  #self.holdings[ticker]['Quantity']
                })     

            # Bearish Divergence: Higher high in price but lower high in RSI
            elif (self.data.close[0] >= self.bollinger.lines.top and self.rsi > self.params.rsi_overbought and self.data.close[0] > self.data.close[-1] and self.rsi > self.rsi[-1]):
                self.in_trade = True
                self.entry_price = self.data.close[0]
                self.sell()
                # print("Short - Bearish Divergence")
                self.__class__.holdingsdf.append({
                    'Strategy': self.strategy_name,
                    'Ticker': self.data._name,
                    'Date': self.data.datetime.datetime(0),
                    'Price':  self.data.close[0],
                    'Quantity': self.position.size  #self.holdings[ticker]['Quantity']
                })    
                self.__class__.trade_history.append({
                    'Strategy': self.strategy_name,
                    'Ticker': self.data._name,
                    'Date': self.data.datetime.datetime(0),
                    'Action': "Sell",
                    'Price': self.data.close[0],
                    'Quantity': self.position.size,
                    'PnL': (self.data.close[0] - self.entry_price) * abs(self.position.size),
                })

        elif self.in_trade:
            # Bullish Convergence: Lower low in price and lower low in RSI
            if (self.data.close[0] <= self.bollinger.lines.bot and self.rsi < self.params.rsi_oversold and self.data.close[0] > self.data.close[-1] and self.rsi >= self.rsi[-1]):
                self.close()
                self.__class__.trade_history.append({
                    'Strategy' : self.strategy_name,
                    'Ticker': self.data._name,
                    'Date': self.data.datetime.datetime(0),
                    'Action': 'Cover',
                    'Price': self.data.close[0],
                    'Quantity': self.position.size,
                    'PnL': (self.data.close[0] - self.entry_price) * abs(self.position.size),
                } )
                print("Close - Bullish Convergence")

            # Bearish Convergence: Higher high in price and higher high in RSI
            elif (self.data.close[0] >= self.bollinger.lines.top and self.rsi > self.params.rsi_overbought and self.data.close[0] < self.data.close[-1] and self.rsi <= self.rsi[-1]):
                self.close()
                self.__class__.trade_history.append({
                    'Strategy' : self.strategy_name,
                    'Ticker': self.data._name,
                    'Date': self.data.datetime.datetime(0),
                    'Action': 'Sell',
                    'Price': self.data.close[0],
                    'Quantity': self.position.size,
                    'PnL': (self.data.close[0] - self.entry_price) * abs(self.position.size),
                } )                
                print("Close - Bearish Convergence")

    
        # mean reversion
    # sideways movement into 
    # divergence: lower low in price but higher low in rsi. Leads to bullish

class Strat3SupportResistance(bt.Strategy):
    Description = "This strategy class checks RSI values and tracks the portfolio value. It can be used for trading decisions based on RSI and portfolio value."
    params = (
        ("vwap_period", 20),
        ("rsi_period", 14),  # RSI period
        ("rsi_overbought", 70),  # RSI overbought level
        ("rsi_oversold", 30)  # RSI oversold level
    )
    def __init__(self):
        self.portfolio_value = []  # List to store portfolio values

    def next(self):
        rsi_value = self.rsi[0]
        portfolio_value = self.broker.get_value()
    
    def store_trade(self, action, price):
        date = self.data.datetime.datetime(0)
        ticker = self.data._name
        quantity = self.position.size
        trade_info = {
            'Ticker': ticker,
            'Date': date,
            'Action': action,
            'Price': price,
            'Quantity': quantity,
        }
        self.trade_history.append(trade_info)
  
    def print_trade_log(self):
        print("Trade Log:")
        for trade in self.trade_history:
            print(f"Ticker: {trade['Ticker']}, Date: {trade['Date']}, Action: {trade['Action']}, Price: {trade['Price']}, Quantity: {trade['Quantity']}")
         
class Strat4MACD(bt.Strategy):
    Description =  "This strategy class combines RSI, MACD (Moving Average Convergence Divergence), and VWAP to make trading decisions. It buys when RSI is oversold, MACD is bullish, and the price is above VWAP. It includes an exit condition when RSI becomes overbought or MACD turns bearish." 
    params = (
        ("vwap_period", 20),    # VWAP calculation period
        ("rsi_period", 14),     # RSI period
        ("rsi_overbought", 70), # RSI overbought level
        ("rsi_oversold", 30),   # RSI oversold level
        ("macd_short_period", 12),  # MACD short-term period
        ("macd_long_period", 26),   # MACD long-term period
        ("macd_signal_period", 9),  # MACD signal period
    )

    def __init__(self):
        self.vwap = bt.indicators.WeightedMovingAverage(self.data.close, period=self.params.vwap_period)
        self.rsi = bt.ind.RelativeStrengthIndex(period=self.params.rsi_period)
        self.macd = bt.ind.MACD(
            period_me1=self.params.macd_short_period,
            period_me2=self.params.macd_long_period,
            period_signal=self.params.macd_signal_period
        )

    def store_trade(self, action, price):
        date = self.data.datetime.datetime(0)
        ticker = self.data._name
        quantity = self.position.size

        trade_info = {
            'Ticker': ticker,
            'Date': date,
            'Action': action,
            'Price': price,
            'Quantity': quantity        }
        self.trade_history.append(trade_info)
        
    def print_trade_log(self):
        print("Trade Log:")
        for trade in self.trade_history:
            print(f"Ticker: {trade['Ticker']}, Date: {trade['Date']}, Action: {trade['Action']}, Price: {trade['Price']}, Quantity: {trade['Quantity']}")
    
    def next(self):
        if self.order:
            return

        if not self.position:
            if self.rsi < self.params.rsi_oversold and self.data.close[0] > self.vwap[0] and self.macd.macd[0] > self.macd.signal[0]:
                self.buy()
        else:
            if self.rsi > self.params.rsi_overbought or self.macd.macd[0] < self.macd.signal[0]:
                self.close()    
        
        
             
class MyStrategy1(bt.Strategy):
    params =  (
        ('rsi_period', 14),
        ('rsi_overbought', 70),
        ('rsi_oversold', 30),
        ('vwap_period', 20))
    
    def __init__(self):
        self.portfolio_value = []  # List to store portfolio values


    def next(self):
        rsi_value = self.rsi[0]
        portfolio_value = self.broker.get_value()
        
        # Access stocks and their quantities in the portfolio
        for data in self.datas:
            stock_name = data._name  # Name of the stock
            stock_quantity = self.getposition(data).size
            print(f"{stock_name}: {stock_quantity}")
        print(f"Portfolio Value: {portfolio_value}")
        self.portfolio_value.append(portfolio_value)
        
        if rsi_value < 30 and not self.crossed_30:
                self.crossed_30 = True
                self.crossed_70 = False
                self.log("RSI crossed below 30 - Buy Signal")

        if rsi_value > 70 and not self.crossed_70:
            self.crossed_30 = False
            self.crossed_70 = True
            self.log("RSI crossed above 70 - Sell Signal")

        vwap_value = self.vwap[0]

        if self.data.close[0] > vwap_value and rsi_value < 30:
            print("BUY")
            self.buy()

        if self.data.close[0] < vwap_value and rsi_value > 70:
            print("SELL")

            self.sell()
                                  
class TestStrategy(bt.Strategy):
# trade after 5 days no matter
    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.order = None
        self.trade_history = []
        self.in_trade = False  # To track if we are in a trade
        self.entry_price = 0.0
        self.bar_executed = 0
        self.stop_loss = 0  # Initialize stop loss level
        self.take_profit = 0  # Initialize take profit level


    def store_trade(self, action, price):
        date = self.data.datetime.datetime(0)
        ticker = self.data._name
        quantity = self.position.size

        trade_info = {
            'Ticker': ticker,
            'Date': date,
            'Action': action,
            'Price': price,
            'Quantity': quantity        }
        self.trade_history.append(trade_info)

    def print_trade_log(self):
        print("Trade Log:")
        for trade in self.trade_history:
            print(f"Ticker: {trade['Ticker']}, Date: {trade['Date']}, Action: {trade['Action']}, Price: {trade['Price']}, Quantity: {trade['Quantity']}")
   
   
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                self.store_trade(action='Buy', price=self.entry_price)
                print('buy')
            elif order.issell():
                self.store_trade(action='Short', price=self.entry_price)
                print("Short")
            self.bar_executed = len(self)
        self.order = None

    def next(self):
        action = 'Sell' if self.position.size > 0 else 'Cover'
        self.store_trade(action=action, price=self.data.close[0])

        if self.order:
            return

        if not self.position:
            if self.dataclose[0] < self.dataclose[-1]:
                if self.dataclose[-1] < self.dataclose[-2]:
                    self.log('BUY CREATE, %.2f' % self.dataclose[0])
                    self.order = self.buy()
                    self.stop_loss = self.dataclose[0] - (2 * (self.dataclose[0] - self.dataclose[-1]))  # Set stop loss
                    self.take_profit = self.dataclose[0] + (2 * (self.dataclose[0] - self.dataclose[-1]))  # Set take profit
        else:
            if len(self) >= (self.bar_executed + 5) or self.dataclose[0] <= self.stop_loss:
                if self.dataclose[0] <= self.stop_loss:
                    self.log("STOPPED OUT")
                elif self.dataclose[0] >= self.take_profit:
                    self.log("TAKE PROFIT")
                else:
                    self.log('SELL CREATED at {}'.format(self.dataclose[0]))
                self.order = self.sell()
                trade_info = {
                    'Ticker': self.data._name,
                    'Date': self.data.datetime.datetime(0),
                    'Action': action,
                    'Price': self.data.close[0],
                    'Quantity': abs(self.position.size),
                    'PnL': (self.data.close[0] - self.entry_price) * abs(self.position.size)
                }
                print(action)
                print(f"Trade: {trade_info}")


        # Add a condition for a sell signal
        #elif self.dataclose[0] > self.dataclose[-1]:
         #   if self.dataclose[-1] > self.dataclose[-2]:

               # SELL, SELL, SELL!!! (with all possible default parameters)
#                self.log('SELL CREATE, %.2f' % self.dataclose[0])
#                self.sell()

class SupportResistanceStrategy(bt.Strategy):
    params = (
        ("support_offset_percentage", 1.0),  # 1% below the close for support
        ("resistance_offset_percentage", 1.0),  # 1% above the close for resistance
    )
    def __init__(self):
        pass

    def next(self):
        current_close = self.data.close[0]
        support_level = current_close * (1 - self.params.support_offset_percentage / 100)
        resistance_level = current_close * (1 + self.params.resistance_offset_percentage / 100)

        if self.data.close[0] > support_level and self.data.close[-1] < support_level:
            self.buy()

        if self.data.close[0] < resistance_level and self.data.close[-1] > resistance_level:
            self.sell()
            
class SmartCross(bt.Strategy):
    params = (
        ('rsi_period', 14),
        ('rsi_overbought', 70),
        ('rsi_oversold', 30),
        ('vwap_period', 20))

    def __init__(self):
        self.vwap = {}
        self.crossover = {}
        self.stop_loss = {}
        self.take_profit = {}
        self.rsi = {}
        self.trade_history = {}
        self.previous_portfolio_value = self.broker.getvalue()
        self.previous_position_count = {}
        self.purchase_price = {}  # Dictionary to store purchase prices


        for data in self.datas:
            self.vwap[data] = bt.indicators.WeightedMovingAverage(data.close, period=self.params.vwap_period)
            ma_fast = bt.ind.SMA(period=10)
            ma_slow = bt.ind.SMA(period=50)
            self.crossover[data] = bt.ind.CrossOver(ma_fast, self.vwap[data])
            self.stop_loss[data] = 0
            self.take_profit[data] = 0
            self.rsi[data] = bt.indicators.RelativeStrengthIndex(data.close, period=self.params.rsi_period)
            self.trade_history[data] = []
            self.previous_position_count[data] = 0

    def log_trade(self, trade, data):
        dt_open = trade.data.datetime.datetime()
        dt_close = self.datetime.datetime()
        price_open = trade.price
        price_close = trade.price
        psize = trade.size
        pnl = trade.pnl
        self.trade_history[data].append({
            'Date Open': dt_open,
            'Date Close': dt_close,
            'Price Open': price_open,
            'Price Close': price_close,
            'Size': psize,
            'PnL': pnl
        })

        trade_df = pd.DataFrame(self.trade_history[data])
        self.log(f"Trade Information for {data._name}:")
        self.log(trade_df)
        
    def print_portfolio(self):
        for data in self.datas:
            position = self.getposition(data)
            if position.size:
                current_price = data.close[0]
                purchase_price = self.purchase_price.get(data, None)  # Get purchase price
                if purchase_price is not None:
                    price_change = current_price - purchase_price
                    pnl = price_change * position.size
                    trade = {
                        'Ticker': data._name,
                        'Date': self.datetime.datetime(0),
                        'Action': 'Buy' if position.size > 0 else 'Sell',
                        'Price': current_price,
                        'Purchase Price': purchase_price,
                        'Quantity': abs(position.size),
                        'PnL': pnl,
                    }
                    print(f"Trade: {trade}")

    def next(self):
        for data in self.datas:
            close_price = data.close[0]
            open_price = data.open[0]

            if not self.position and self.crossover[data] > 0:
                print(f"BUY SIGNAL for {data._name}")
                self.buy(data=data)
                self.stop_loss[data] = data.close[0] - 2 * (data.close[0] - data.close[-1])
                self.take_profit[data] = data.close[0] + 2 * (data.close[0] - data.close[-1])
                self.purchase_price[data] = close_price  # Set purchase price

            elif self.position and (self.crossover[data] < 0 or close_price <= self.stop_loss[data] or close_price >= self.take_profit[data]):
                if close_price <= self.stop_loss[data]:
                    print(f"STOPPED OUT for {data._name}")
                    self.purchase_price.pop(data, None)  # Remove purchase price if sold
                elif close_price >= self.take_profit[data]:
                    print(f"TAKE PROFIT for {data._name}")
                    self.purchase_price.pop(data, None)  # Remove purchase price if sold
                else:
                    print(f"SELL SIGNAL for {data._name}")
                    self.purchase_price.pop(data, None)  # Remove purchase price if sold

        self.print_portfolio()
        #current_portfolio_value = self.broker.getvalue()
        #portfolio_change = current_portfolio_value - self.previous_portfolio_value
        #print(f"Portfolio Value: {current_portfolio_value:.2f}, Change: {portfolio_change:.2f}")
        #print self.trade_history = {}

        #self.previous_portfolio_value = current_portfolio_value


class VWAPRSIStrategy(bt.Strategy):
    params = (
        ("vwap_period", 20),
        ("rsi_period", 14),
        ("rsi_overbought", 70),
        ("rsi_oversold", 30)
    )

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.datavwap = VWAP(self.datas[0], period=self.params.vwap_period)
        self.rsi = bt.indicators.RelativeStrengthIndex(period=self.params.rsi_period)
        self.order = None

    def next(self):
        if self.order:
            return

        if self.rsi < self.params.rsi_oversold and self.dataclose[0] > self.datavwap[0]:
            self.order = self.buy()
        elif self.rsi > self.params.rsi_overbought and self.dataclose[0] < self.datavwap[0]:
            self.order = self.sell()

    def notify_order(self, order):
        if order.status in [order.Completed, order.Canceled, order.Margin, order.Rejected]:
            self.order = None


