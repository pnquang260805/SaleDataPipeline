from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


class SparkService:
    def __init__(self, conf: SparkConf, master: str, app_name: str = "spark"):
        self.spark = (
            SparkSession.builder.appName(app_name)
            .master(master)
            .config(conf=conf)
            .getOrCreate()
        )

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
