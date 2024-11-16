from Config import *
from collections import defaultdict

class DataCatagory:
    def __init__(self) -> None:
        self.datasets = defaultdict()
        
    def add_dataset(self, dataset_name: str, dataset):
        self.datasets[dataset_name] = dataset
        
    def search_by_keyword(self, keyword: str):
        for dataset in self.datasets:
            if keyword in dataset:
                return self.datasets[dataset]
        return None


class DataCatalog:
    def __init__(self) -> None:
        self.catagrories = {}
    
    def add_catagory(self, catagory_name: str, catagory:DataCatagory):
        self.catagrories[catagory_name] = catagory
    
    def add_dataset(self, catagory_name: str, dataset_name: str, dataset):
        self.catagrories[catagory_name].add_dataset(dataset_name, dataset)
    
    def list_datasets(self):
        for catagory in self.catagrories:
            print(catagory)
            for dataset in self.catagrories[catagory].datasets:
                print("\t", dataset)
    
    def search_dataset(self, dataset_name: str):
        res = []
        for catagory in self.catagrories:
            dataset = self.catagrories[catagory].search_by_keyword(dataset_name)
            if dataset: res.append(dataset)
        return res