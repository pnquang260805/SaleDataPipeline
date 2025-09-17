import logging

from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, expr

from interface.transform_silver import TransformSilverService
from services.delta_services import DeltaService


@dataclass
class Level(TransformSilverService):
    delta_service: DeltaService
    ip_loc: str = "s3a://warehouse/default/dim_level"

    def __post_init__(self):
        self.logger = logging

    def transform(self, silver_df: DataFrame):
        level_df = silver_df.select("level")
        level_df = level_df.withColumn("id", expr("uuid()"))
        if not self.delta_service.is_delta_table(self.ip_loc):
            self.spark_service.write_file(
                dir=self.ip_loc, df=level_df, format="delta", mode="overwrite"
            )
            return
        delta_table = self.delta_service.get_delta_table(self.ip_loc)
        self.delta_service.scd_type2(delta_table, level_df, "level")
