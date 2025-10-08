import re

from datetime import datetime
from dataclasses import dataclass
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

from utils.logger import log
from services.spark_service import SparkService


@dataclass
class TransformRawProduct:
    spark_service: SparkService

    def __nomalizer(self, inp):
        words = re.findall(r"[A-Z][a-z]*", inp)
        res = "_".join(word.lower() for word in words)
        return res

    def __post_init__(self):
        self.spark = self.spark_service.get_spark()

    @log
    def transform(
        self,
        raw_data_url: str,
        silver_dir: str,
        input_format: str = "json",
        saved_format: str = "parquet",
        *args,
        **kwargs,
    ) -> DataFrame:
        raw_df = self.spark_service.read_file(
            raw_data_url, input_format, multiline=True
        )
        df = raw_df.select(col("payload.*"))

        after_cols = df.schema["after"].dataType.fields
        before_struct = struct(*[lit(None).cast(f.dataType).alias(f.name) for f in after_cols])
        df = df.withColumn("before", before_struct)

        silver_df = df.select(
            col("after.ID").alias("id"),
            col("before.ProductName").alias(
                f"before_{self.__nomalizer('ProductName')}"
            ),
            col("before.Description").alias(
                f"before_{self.__nomalizer('Description')}"
            ),
            col("before.Price").alias(f"before_{self.__nomalizer('Price')}"),
            col("before.QuantityInStock").alias(
                f"before_{self.__nomalizer('QuantityInStock')}"
            ),
            col("after.ProductName").alias(f"after_{self.__nomalizer('ProductName')}"),
            col("after.Description").alias(f"after_{self.__nomalizer('Description')}"),
            col("after.Price").alias(f"after_{self.__nomalizer('Price')}"),
            col("after.QuantityInStock").alias(
                f"after_{self.__nomalizer('QuantityInStock')}"
            ),
            col("op"),
            col("source.db").alias("database"),
            col("source.table").alias("table"),
            col("ts_ms").cast("bigint").alias("timestamp"),
        )
        silver_df = silver_df.withColumn("timestamp", (col("timestamp") / 1000))
        precision = 2
        div = int(10**precision)
        silver_df = (
            silver_df.withColumn(
                "ts_utc",
                to_utc_timestamp(to_timestamp("timestamp"), "Asia/Ho_Chi_Minh"),
            )
            .withColumn("before_price", hex(unbase64("before_price")))
            .withColumn("before_price", (conv("before_price", 16, 10)) / div)
            .withColumn("after_price", hex(unbase64("after_price")))
            .withColumn("after_price", (conv("after_price", 16, 10)) / div)
        )
        self.spark_service.write_file(silver_dir, silver_df, saved_format)
        return silver_df