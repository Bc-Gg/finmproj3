"""Sample output of btc
Calculating 50-day and 200-day moving averages...
Generating buy/sell signals based on moving average crossover...
                      Open time            Open            High  ...       SMA200 Signal Position
56596 2024-11-09 23:56:00+00:00  76617.31000000  76734.67000000  ...  76473.40065      1      0.0
56597 2024-11-09 23:57:00+00:00  76656.28000000  76656.34000000  ...  76475.05740      1      0.0
56598 2024-11-09 23:58:00+00:00  76801.66000000  76801.66000000  ...  76477.44090      1      0.0
56599 2024-11-09 23:59:00+00:00  76658.42000000  76658.42000000  ...  76479.25200      1      0.0
56600 2024-11-10 00:00:00+00:00  76760.53000000  76760.53000000  ...  76480.70280      1      0.0

[5 rows x 16 columns]
"""

from dataLake import CryptoDataLake
from dataWorkbench import DataWorkbench
import pandas as pd
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore")

class CryptoTechnicalAnalysis:
    def __init__(self):
        self.crypto_data_lake = CryptoDataLake()
        self.data_workbench = DataWorkbench()

    def fetch_crypto_data(self, symbol, start_date, end_date, interval="1d"):
        print(f"Fetching {symbol} data from {start_date} to {end_date}...")
        return self.crypto_data_lake.fetch_and_store_data(symbol, start_date, end_date, interval)

    def calculate_moving_averages(self, data, short_window=50, long_window=200):
        print(f"Calculating {short_window}-day and {long_window}-day moving averages...")
        data['SMA50'] = data['Close'].rolling(window=short_window, min_periods=1).mean()
        data['SMA200'] = data['Close'].rolling(window=long_window, min_periods=1).mean()
        return data

    def generate_signals(self, data):
        print("Generating buy/sell signals based on moving average crossover...")
        data['Signal'] = 0
        data['Signal'][data['SMA50'] > data['SMA200']] = 1  # Buy signal
        data['Signal'][data['SMA50'] < data['SMA200']] = -1  # Sell signal
        data['Position'] = data['Signal'].diff()  # Capture when the signal changes
        return data

    def plot_data(self, data):
        print("Plotting data...")
        plt.figure(figsize=(14, 8))
        plt.plot(data.index, data['Close'], label='Close Price', color='blue')
        plt.plot(data.index, data['SMA50'], label='50-Day SMA', color='green')
        plt.plot(data.index, data['SMA200'], label='200-Day SMA', color='red')

        # Plot buy signals
        buy_signals = data[data['Position'] == 1]
        plt.scatter(buy_signals.index, buy_signals['Close'], label='Buy Signal', marker='^', color='green', lw=3)

        # Plot sell signals
        sell_signals = data[data['Position'] == -1]
        plt.scatter(sell_signals.index, sell_signals['Close'], label='Sell Signal', marker='v', color='red', lw=3)

        plt.title(f"Moving Average Crossover for {data['Symbol'][0]}")
        plt.xlabel("Date")
        plt.ylabel("Price (USD)")
        plt.legend()
        plt.grid(True)
        plt.show()

    def run_analysis(self, symbol, start_date, end_date):
        crypto_data = self.fetch_crypto_data(symbol, start_date, end_date)
        crypto_data = self.calculate_moving_averages(crypto_data, short_window=50, long_window=200)
        crypto_data = self.generate_signals(crypto_data)


        return crypto_data


if __name__ == "__main__":
    analysis = CryptoTechnicalAnalysis()
    symbol = "BTCUSDT"  # Bitcoin
    start_date = "2024-10-01"
    end_date = "2024-11-10"
    results = analysis.run_analysis(symbol, start_date, end_date)
    print(results.tail())