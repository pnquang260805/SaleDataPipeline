from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


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
