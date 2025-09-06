import os
import logging

from dotenv import load_dotenv
from pyspark.conf import SparkConf

from utils.containers import Containers
from utils.logger import log
from config.app_config import AppConfig
from typing import *
from dataclasses import dataclass

load_dotenv()
container = Containers()
logger = logging
cfg = AppConfig()


def spark_config():
    conf = SparkConf()
    (
        conf.set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.databricks.delta.metastore.type", "filesystem")
        .set("spark.hadoop.fs.s3a.access.key", cfg.ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", cfg.SECRET_KEY)
        .set("spark.hadoop.fs.s3a.endpoint", cfg.s3_endpoint)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.sql.warehouse.dir", "s3a://warehouse")
        .set("spark.dynamicAllocation.shuffleTracking.enabled", "true")
    )
    return conf


@log
def main() -> None:
    master = "spark://spark-master:7077"

    spark_service = container.spark_service(
        conf=spark_config(), master=master, extra_packages=cfg.extra_packages()
    )
    datetime_service = container.datetime_service()
    common_service = container.common_service()
    transform_datetime_service = container.transform_datetime_service()
    transform_raw_service = container.transform_raw_service()

    # print(spark_service.get_spark().sparkContext.master)

    yesterday = datetime_service.get_yesterday()

    last_log = common_service.check_lookup(
        f"http://redis-api:80/api/redis/get-key?key=last_log", key="value"
    )
    if last_log != yesterday:
        return

    url = f"s3a://bronze/log/{yesterday}/*.json"

    df = transform_raw_service.transform(url, f"s3a://silver/{yesterday}")
    # print(df.show(5, truncate=False))
    transform_datetime_service.transform(df)


if __name__ == "__main__":
    main()
