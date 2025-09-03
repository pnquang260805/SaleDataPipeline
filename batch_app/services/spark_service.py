from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from delta import *

from typing import List


class SparkService:
    def __init__(
        self,
        conf: SparkConf,
        extra_packages: List[str],
        master: str,
        app_name: str = "spark",
    ):
        builder = (
            SparkSession.builder.appName(app_name).master(master).config(conf=conf)
        )
        self.spark = configure_spark_with_delta_pip(
            builder, extra_packages
        ).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

    def get_spark(self):
        return self.spark

    def read_file(
        self, url: str, format: str, schema: StructType, *args, **kwargs
    ) -> DataFrame:
        df = self.spark.read.format(format).schema(schema).options(**kwargs).load(url)
        return df

    def write_file(
        self,
        dir: str,
        df: DataFrame,
        format: str,
        mode: str = "append",
        *args,
        **kwargs
    ) -> None:
        df.write.format(format).mode(mode).save(dir)
