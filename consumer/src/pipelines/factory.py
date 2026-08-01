from pipelines.clickhouse_pipeline import ClickHousePipeline
from pipelines.ozone_pipeline import OzonePipeline


class PipelineFactory:

    _PIPELINES = {
        "clickhouse": ClickHousePipeline,
        "ozone": OzonePipeline,
    }

    @classmethod
    def create(cls, pipeline_config):

        pipeline_type = pipeline_config.type

        pipeline_class = cls._PIPELINES.get(pipeline_type)

        if pipeline_class is None:
            raise ValueError(
                f"Unsupported pipeline: {pipeline_type}"
            )

        return pipeline_class(pipeline_config)