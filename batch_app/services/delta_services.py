import logging

from dataclasses import dataclass
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from services.spark_service import SparkService
from utils.logger import log


@dataclass
class DeltaService:
    spark_service: SparkService

    def __post_init__(self):
        self.spark = self.spark_service.get_spark()
        self.logger = logging

    @log
    def scd_type2(
        self, target: DeltaTable, source: DataFrame, condition: str, table_path: str
    ) -> None:
        (
            target.alias("t")
            .merge(source.alias("s"), condition)
            .whenMatchedUpdate(
                set={
                    "expiration_date": col("s.effective_date"),
                    "is_current": lit(False),
                }
            )
            .execute()
        )
        self.spark_service.write_delta_table(source, table_path, mode="append")

    @log
    def get_delta_table(self, dir: str) -> DeltaTable:
        return DeltaTable.forPath(self.spark, dir)

    @log
    def is_delta_table(self, dir: str) -> bool:
        return DeltaTable.isDeltaTable(self.spark, dir)
