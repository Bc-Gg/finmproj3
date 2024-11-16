from Config import *

import yfinance as yf
import os
import pickle
from datetime import datetime, timezone
from Crypto_helper import *

class DataLake:
    def __init__(self, storage_path=dataLakeFolder):
        self.storage_path = storage_path
        if not os.path.exists(self.storage_path):
            os.makedirs(self.storage_path)

    def store_data(self, dataset_name, data):
        file_path = os.path.join(self.storage_path, f"{dataset_name}.pkl")
        with open(file_path, 'wb') as f:
            pickle.dump(data, f)

    def retrieve_data(self, dataset_name):
        file_path = os.path.join(self.storage_path, f"{dataset_name}.pkl")
        if os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                return pickle.load(f)
        return None

class StockDataLake(DataLake):
    def __init__(self, storage_path=dataLakeFolder + '/equity'):
        super().__init__(storage_path)

    def fetch_and_store_data(self, ticker_symbol, start_date, end_date, interval="1d"):

        dataset_name = f"{ticker_symbol}_{start_date}_{end_date}_{interval}"
        
        stored_data = self.retrieve_data(dataset_name)
        if stored_data is not None:
            print(f"Data for {ticker_symbol} already exists locally.")
            return stored_data

        ticker = yf.Ticker(ticker_symbol)
        data = ticker.history(start=start_date, end=end_date, interval=interval)
        
        self.store_data(dataset_name, data)
        print(f"Data for {ticker_symbol} stored successfully.")
        return data
    

class CryptoDataLake(DataLake):

    def __init__(self, storage_path=dataLakeFolder + '/crypto'):
        super().__init__(storage_path)


    def fetch_and_store_data(self, ticker_symbol, start_date, end_date, interval="1d"):

        dataset_name = f"{ticker_symbol}_{start_date}_{end_date}_{interval}"
        
        stored_data = self.retrieve_data(dataset_name)
        if stored_data is not None:
            print(f"Data for {ticker_symbol} already exists locally.")
            return stored_data

        start_timestamp = int(((pd.to_datetime(start_date)).tz_localize('UTC')).timestamp() * 1000)
        end_timestamp = int(((pd.to_datetime(end_date)).tz_localize('UTC')).timestamp() * 1000)
        full_data_list = obtain_full_spotdata(start_timestamp, end_timestamp, 
                                          binance_exchange, ticker_symbol)
        data = pd.DataFrame(full_data_list, columns = SPOT_COLUMNS)
        data['Open time'] = data['Open time'].apply(lambda x: transform_timestamp(int(x)))
        data.drop_duplicates('Open time', keep='first', inplace=True)
        
        self.store_data(dataset_name, data)
        print(f"Data for {ticker_symbol} stored successfully.")
        return data