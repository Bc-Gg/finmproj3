from datetime import datetime
import pandas as pd

class BaseDataModel:
    def __init__(self, timestamp, symbol=None):
        self.timestamp = timestamp if isinstance(timestamp, datetime) else datetime.now()
        self.symbol = symbol  # Optional identifier for the financial instrument

    def __repr__(self):
        return f"BaseDataModel(Timestamp: {self.timestamp}, Symbol: {self.symbol})"

    def is_recent(self, days=7):
        """
        Checks if the data point is within the last 'days' days.
        """
        delta = datetime.now() - self.timestamp
        return delta.days <= days

    def is_above_threshold(self, value, threshold):
        """
        Checks if a given value exceeds a specified threshold.
        """
        return value > threshold

class IntradayDataModel(BaseDataModel):
    def __init__(self, timestamp, symbol, open_price, high_price, low_price, close_price, volume):
        super().__init__(timestamp, symbol)
        self.open_price = float(open_price)
        self.high_price = float(high_price)
        self.low_price = float(low_price)
        self.close_price = float(close_price)
        self.volume = float(volume)

    def __repr__(self):
        return (f"IntradayDataModel(Symbol: {self.symbol}, Timestamp: {self.timestamp}, "
                f"Open: {self.open_price}, High: {self.high_price}, "
                f"Low: {self.low_price}, Close: {self.close_price}, Volume: {self.volume})")

    def aggregate_by_interval(self, data_list, interval='1T'):
        """
        Aggregates intraday data over specified time intervals.

        :param data_list: List of IntradayDataModel instances.
        :param interval: Time interval for aggregation (e.g., '1T' for 1 minute, '5T' for 5 minutes).
        :return: Aggregated data as a list of IntradayDataModel instances.
        """
        # Create a DataFrame from data_list
        df = pd.DataFrame([{
            'timestamp': data.timestamp,
            'open_price': data.open_price,
            'high_price': data.high_price,
            'low_price': data.low_price,
            'close_price': data.close_price,
            'volume': data.volume
        } for data in data_list])

        df.set_index('timestamp', inplace=True)

        # Resample 
        resampled_df = df.resample(interval).agg({
            'open_price': 'first',
            'high_price': 'max',
            'low_price': 'min',
            'close_price': 'last',
            'volume': 'sum'
        }).dropna()

        # Convert resampled DataFrame back to list of IntradayDataModel instances
        aggregated_data = []
        for idx, row in resampled_df.iterrows():
            aggregated_data.append(
                IntradayDataModel(
                    timestamp=idx,
                    symbol=self.symbol,
                    open_price=row['open_price'],
                    high_price=row['high_price'],
                    low_price=row['low_price'],
                    close_price=row['close_price'],
                    volume=row['volume']
                )
            )

        return aggregated_data