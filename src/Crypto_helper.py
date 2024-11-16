import ccxt
import time
import pandas as pd
from datetime import datetime, timezone


SPOT_COLUMNS = ['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 
                'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore']

binance_exchange = ccxt.binanceus({
    'timeout': 15000,
    'enableRateLimit': True
    # 'options': {'defaultType': 'future'}
})

def get_spot(exchange, symbol, interval = '1m', 
                    startTime = None,
                    endTime = None, 
                    limit = 1000):
            
    if (startTime == None and endTime == None):
        return exchange.publicGetKlines({'symbol': symbol, 
                                        'interval': interval, 
                                        'limit': limit})
    elif (startTime == None and endTime != None):
        return exchange.publicGetKlines({'symbol': symbol, 
                                        'interval': interval,
                                        'endTime': endTime,
                                        'limit': limit})
    elif (startTime != None and endTime == None):
        return exchange.publicGetKlines({'symbol': symbol, 
                                        'interval': interval,
                                        'startTime': startTime,
                                        'limit': limit})
    else:
        return exchange.publicGetKlines({'symbol': symbol, 
                                        'interval': interval,
                                        'startTime': startTime,
                                        'endTime': endTime,
                                        'limit': limit})

def convert_to_seconds(time_input):
    number = int(time_input[:-1])
    unit = time_input[-1]

    if unit == 's':
        return number
    elif unit == 'm':
        return number * 60
    elif unit == 'h':
        return number * 3600
    elif unit == 'd':
        return number * 86400
    else:
        raise ValueError("Unsupported time unit")
    
def transform_timestamp(timestamp_integer):
    '''
    As data points involved milliseconds, we need to transform them by constant 1000.
    '''

    return pd.to_datetime(int(timestamp_integer / 1000), utc=True, unit='s')

def transform_to_timestamp_integer(datetime_object):
    '''
    As data points involved milliseconds, we need to transform them by constant 1000.
    '''
    
    return int(datetime_object.timestamp() * 1000)

def obtain_full_spotdata(start_timestamp, 
                         end_timestamp,
                         exchange, symbol, interval = '1m', 
                         limit = 1000):

    time_difference = int(convert_to_seconds(interval) * limit * 1000)

    full_data_list = []

    curr_time = start_timestamp + time_difference
    while (curr_time + time_difference < end_timestamp):
        data_list = get_spot(exchange = exchange, symbol = symbol, interval = interval, 
                             endTime = curr_time, 
                             limit = limit)
        full_data_list = full_data_list + data_list

        time.sleep(0.2)
        curr_time += time_difference

    data_list = get_spot(exchange = exchange, symbol = symbol, interval = interval, 
                        startTime = curr_time,
                        endTime = end_timestamp, 
                        limit = limit)

    full_data_list = full_data_list + data_list

    return full_data_list


if __name__ == "__main__":
    
    example_coin = 'BTCUSDT'
    start_timestamp = int(((pd.to_datetime('2024-10-01')).tz_localize('UTC')).timestamp() * 1000)
    end_timestamp = int(((pd.to_datetime('2024-10-10')).tz_localize('UTC')).timestamp() * 1000)

    full_data_list = obtain_full_spotdata(start_timestamp, end_timestamp, 
                                          binance_exchange, example_coin)

    spot_1m_kline = pd.DataFrame(full_data_list, columns = SPOT_COLUMNS)
    spot_1m_kline['Open time'] = spot_1m_kline['Open time'].apply(lambda x: transform_timestamp(int(x)))

    spot_1m_kline = spot_1m_kline.drop_duplicates('Open time', keep='first')
    spot_1m_kline[spot_1m_kline.duplicated('Open time')]['Open time']

    print(spot_1m_kline.head(10))