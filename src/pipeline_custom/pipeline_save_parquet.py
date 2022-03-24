from pyspark.sql import functions as f
from pipeline_step.pipeline_step import PipelineStep


class PipelineSaveParquet(PipelineStep):
    def __init__(self):
        super().__init__()
        print('Saving data as Parquet')

    def run(self, spark, params, df):
        output_path = params.args['output_path']
        partition_column = params.args['partition_column']
        df.write.partitionBy(partition_column).mode('overwrite').parquet(output_path)
        return df 