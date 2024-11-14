from Config import *

class DataLake:
    def __init__(self, dataLakePath = dataLakeFolder) -> None:
        self.raw_datapath = os.listdir(os.path.join(dataLakePath, 'raw'))
        self.processed_datapath = os.listdir(os.path.join(dataLakePath, 'processed'))
        
    def store_data(self, dataset_name, data, processed=False):
        ...
        
    def retrieve_data(self, dataset_name, processed=False):
        if processed:
            ...
        else:
            ...