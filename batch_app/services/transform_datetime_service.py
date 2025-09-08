import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from dataclasses import dataclass

from interface.transform_silver import TransformSilverService
from utils.logger import log
from services.delta_services import DeltaService


@dataclass
class TransformDate(TransformSilverService):
    delta_service: DeltaService
    dim_date_loc: str = "s3a://warehouse/default/dim_time"

    def __post_init__(self):
        self.logger = logging

    @log
    def transform(self, silver_df: DataFrame):
        local_col_name = "vietnam_time"

        time_df = silver_df.select("@timestamp")
        time_df = (
            time_df.withColumn("date", to_date(col("@timestamp")))
            .withColumn(
                local_col_name,
                date_format(
                    from_utc_timestamp("@timestamp", "Asia/Ho_Chi_Minh"), "yyyy-MM-dd"
                ),
            )
            .withColumn("year", year(local_col_name))
            .withColumn("month", month(local_col_name))
            .withColumn("day", day(local_col_name))
            .withColumn("quarter", quarter(local_col_name))
            .drop("@timestamp")
            .drop(local_col_name)
            .dropDuplicates()
        )
        if not self.delta_service.is_delta_table(self.dim_date_loc):
            self.spark_service.write_file(
                dir=self.dim_date_loc, df=time_df, format="delta", mode="overwrite"
            )
            return
        delta_table = self.delta_service.get_delta_table(self.dim_date_loc)
        self.delta_service.merge(delta_table, time_df, "date")
