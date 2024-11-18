class DataWorkbench:

    def __init__(self):
        self.data_storage = {}

    def store_data(self, dataset_name, data):
        self.data_storage[dataset_name] = data

    def retrieve_data(self, dataset_name):
        return self.data_storage.get(dataset_name, "Dataset not found")
    
    def clean_data(self, dataset_name, clean_func, *args, **kwargs):
        if dataset_name in self.data_storage:
            cleaned_data = clean_func(self.data_storage[dataset_name], *args, **kwargs)
            self.store_data(dataset_name, cleaned_data)
            return cleaned_data if cleaned_data else "Data cleaning failed"
        
        return "Dataset not found"
    
    def update_data(self, dataset_name, new_data):
        if dataset_name in self.data_storage:
            self.data_storage[dataset_name] = new_data
            return "Dataset updated"
        
        else:
            self.store_data(dataset_name, new_data)
            return "Dataset not found. New dataset created"
    
    def transform_data(self, dataset_name, transformation_func, *args, **kwargs):
        data = self.retrieve_data(dataset_name)
        return transformation_func(data, *args, **kwargs) if data else "Dataset not found"
