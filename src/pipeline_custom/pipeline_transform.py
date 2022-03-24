from pyspark.sql import functions as f
from pipeline_step.pipeline_step import PipelineStep
import re

class PipelineTransform(PipelineStep):
    def __init__(self):
        super().__init__()
        print('Transforming Data')
    def run(self, spark, params, df):
        partition_column = params.args['partition_column']
        for each in df.schema.names:
            df = df.withColumnRenamed(each, re.sub(r'\s+([a-zA-Z_][a-zA-Z_0-9]*)\s*','',each.replace(' ', '_')))
        df.show()
        return df