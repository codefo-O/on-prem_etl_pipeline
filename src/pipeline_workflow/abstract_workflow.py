from abc import abstractmethod, ABC

class AbstractWorkflow(ABC):
    def __init__(self, params, spark):
        self.params = params
        self.spark = spark
        self.steps = {
            'csvtoparquet' : ['pipeline_read_csv', 'pipeline_transform', 'pipeline_save_parquet'],
            'jsontoparquet' : ['pipeline_read_json', 'pipeline_transform', 'pipeline_save_parquet'],
            'csvtojson' : ['pipeline_read_csv', 'pipeline_transform', 'pipeline_save_json']
        }

    @abstractmethod
    def run(self):
        pass

    def get_config(self, file_type):
        return self.steps[file_type] 