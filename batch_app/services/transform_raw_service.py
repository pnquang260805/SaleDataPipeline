import requests

from dataclasses import dataclass
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import DataFrame

from utils.logger import log
from services.common_service import CommonService
from services.spark_service import SparkService


@dataclass
class TransformRawService:
    lookup_url: str
    spark_service: SparkService

    def __post_init__(self):
        self.spark = self.spark_service.get_spark()

    @log
    def transform_bronze_to_silver(
        self,
        raw_data_url: str,
        silver_dir: str,
        input_format: str = "json",
        saved_format: str = "parquet",
        *args,
        **kwargs
    ) -> DataFrame:
        """
        Optional parameter:
            save_mode: overwrite, append, ignore
        """
        schema = StructType(
            [
                StructField("@timestamp", StringType(), True),
                StructField("@version", StringType(), True),
                StructField("bytes_sent", StringType(), True),
                StructField("client_ip", StringType(), True),
                StructField(
                    "event",
                    StructType([StructField("original", StringType(), True)]),
                    True,
                ),
                StructField("exception_message", StringType(), True),
                StructField("exception_type", StringType(), True),  # thêm vào
                StructField(
                    "host", StructType([StructField("name", StringType(), True)]), True
                ),
                StructField("http_version", StringType(), True),
                StructField("level", StringType(), True),
                StructField(
                    "log",
                    StructType(
                        [  # thêm vào
                            StructField(
                                "file",
                                StructType([StructField("path", StringType(), True)]),
                                True,
                            )
                        ]
                    ),
                    True,
                ),
                StructField("message", StringType(), True),
                StructField("method", StringType(), True),
                StructField("request", StringType(), True),
                StructField("server_ip", StringType(), True),
                StructField("service", StringType(), True),
                StructField("status", StringType(), True),
                StructField("timestamp", StringType(), True),  # để StringType
                StructField("type", StringType(), True),
                StructField("user_agent", StringType(), True),
            ]
        )

        df = self.spark_service.read_file(
            raw_data_url, "json", schema, multiline="true"
        )
        df = df.dropDuplicates()

        fill_dict = {
            "exception_message": "No exception",
            "exception_type": "No Exception",
        }

        df = df.na.fill(fill_dict)
        self.spark_service.write_file(silver_dir, df, "parquet")
        return df
