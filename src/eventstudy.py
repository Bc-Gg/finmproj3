"""A sample output of this module is:

Fetching data for AAPL from 2024-01-01 to 2024-01-10...
Data for AAPL already exists locally.
Aggregating data by 5T intervals...
Analyzing the impact of news sentiment on stock prices...
Event Study Results: [186.58231979370117]

"""

from dataLake import StockDataLake, CryptoDataLake
from dataCatalog import DataCatalog, DataCatagory
from dataWorkbench import DataWorkbench
from QuantDataModel import IntradayDataModel
from Config import *
import warnings
warnings.filterwarnings("ignore")
import pandas as pd

class EventStudy:
    def __init__(self):
        # Initialize components
        self.data_lake = StockDataLake()
        self.data_catalog = DataCatalog()
        self.data_workbench = DataWorkbench()

    def fetch_data(self, ticker, start_date, end_date, interval='1d'):
        print(f"Fetching data for {ticker} from {start_date} to {end_date}...")
        return self.data_lake.fetch_and_store_data(ticker, start_date, end_date, interval)

    def transform_data(self, dataset, interval='5T'):
        print(f"Aggregating data by {interval} intervals...")
        intraday_model = IntradayDataModel(
            timestamp=dataset.index[0],
            symbol="AAPL",
            open_price=dataset['Open'].iloc[0],
            high_price=dataset['High'].iloc[0],
            low_price=dataset['Low'].iloc[0],
            close_price=dataset['Close'].iloc[0],
            volume=dataset['Volume'].iloc[0]
        )
        aggregated_data = intraday_model.aggregate_by_interval([intraday_model], interval)
        return aggregated_data

    def analyze_data(self, intraday_data, news_sentiment):
        print("Analyzing the impact of news sentiment on stock prices...")
        sentiment_impact = []
        for data in intraday_data:
            if news_sentiment > 0:
                sentiment_impact.append(data.close_price * 1.01)  # Assume positive news increases price
            else:
                sentiment_impact.append(data.close_price * 0.99)  # Assume negative news decreases price
        return sentiment_impact

    def run_event_study(self, ticker, start_date, end_date, news_sentiment):
        # Fetch Data
        intraday_data = self.fetch_data(ticker, start_date, end_date)

        # Transform Data
        aggregated_data = self.transform_data(intraday_data)
        results = self.analyze_data(aggregated_data, news_sentiment)

        return results


if __name__ == "__main__":
    event_study = EventStudy()
    start_date = "2024-01-01"
    end_date = "2024-01-10"
    news_sentiment = 0.8  # Positive sentiment threshold
    results = event_study.run_event_study("AAPL", start_date, end_date, news_sentiment)
    print("Event Study Results:", results)