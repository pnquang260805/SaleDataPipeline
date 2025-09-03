import logging

from dataclasses import dataclass
from delta.tables import DeltaTable
from pyspark.sql import DataFrame

from services.spark_service import SparkService
from utils.logger import log


@dataclass
class DeltaService:
    spark_service: SparkService

    def __post_init__(self):
        self.spark = self.spark_service.get_spark()
        self.logger = logging

    def __build_conditions(self, df: DataFrame):
        cols = df.columns
        return " AND ".join([f"t.{col} = s.{col}" for col in cols])

    def __build_values(self, df: DataFrame):
        cols = df.columns
        res = {}
        for col in cols:
            res[col] = f"s.{col}"
        res["id"] = "uuid()"
        return res

    @log
    def merge(self, target: DeltaTable, source: DataFrame) -> None:
        (
            target.alias("t")
            .merge(source.alias("s"), f"{self.__build_conditions(source)}")
            .whenNotMatchedInsert(values=self.__build_values(source))
            .execute()
        )

    @log
    def get_delta_table(self, dir: str) -> DeltaTable:
        return DeltaTable.forPath(self.spark, dir)
