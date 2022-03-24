from pipeline_step.pipeline_step import PipelineStep

class PipelineReadJson(PipelineStep):
    def __init__(self):
        super().__init__()
        print('Reading JSON File')

    def run(self, spark, params, df):
        path = params.args['input_path']
        spark.read.json(path, multiLine = "true").show()
        return spark.read.json(path, multiLine = "true")