from dataLake import *
from tqdm import tqdm

# Equity data
stock_data_lake = StockDataLake()
nasdaq_100_tickers = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "GOOG", "META", "TSLA", "NVDA", "PYPL", "ADBE", "CSCO", "PEP", 
    "COST", "AVGO", "TXN", "QCOM", "INTC", "AMD", "NFLX", "SBUX", "INTU", "AMGN", "ISRG", "AMAT", 
    "MDLZ", "ADP", "BKNG", "TMUS", "CHTR", "MU", "GILD", "MRNA", "MDT", "FISV", "ATVI", "ADI", 
    "CSX", "LRCX", "KLAC", "NXPI", "CTSH", "EA", "VRTX", "MELI", "REGN", "ROST", "IDXX", "LULU", 
    "MAR", "KDP", "ORLY", "EXC", "CTAS", "AEP", "PANW", "XEL", "FTNT", "CDNS", "PAYX", "SNPS", 
    "MNST", "WBA", "EBAY", "SIRI", "FAST", "VRSK", "DXCM", "PCAR", "ODFL", "CPRT", "BIDU", "BIIB", 
    "WDC", "SGEN", "ANSS", "AZN", "ALGN", "CRWD", "ZM", "TEAM", "ZS", "DOCU", "OKTA", "DDOG", 
    "PDD", "NTES", "JD", "BKR", "VRSN", "LCID", "MRVL", "ABNB", "RIVN", "ASML"
]

start_date = "2020-01-01"
end_date = "2024-11-10"
interval = "1d"

for ticker in tqdm(nasdaq_100_tickers, desc="Fetching NASDAQ-100 data"):
    stock_data_lake.fetch_and_store_data(ticker, start_date, end_date, interval)

stored_data = stock_data_lake.retrieve_data(f"AAPL_{start_date}_{end_date}_{interval}")
if stored_data is not None:
    print(stored_data.head())


