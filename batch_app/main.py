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
        .set("spark.executor.cores", "1")
        .set("spark.cores.max", "1")
    )
    return conf


@log
def main() -> None:
    master = "spark://spark-master:7077"

    spark_service = container.spark_service(
        conf=spark_config(), master=master, extra_packages=cfg.extra_packages()
    )

    datetime_service = container.datetime_service()
    yesterday = datetime_service.get_yesterday()

    logger.info("Starting transform customer")
    # transform customer to warehouse
    transform_raw_customer = container.transform_raw_customer()
    transform_customer_service = container.transform_customer_service()

    customer_raw_url = f"s3a://bronze/customers/{yesterday}/*.json"
    customer_df = transform_raw_customer.transform(
        customer_raw_url, f"s3a://silver/customers/{yesterday}/"
    )

    transform_customer_service.transform(customer_df)

    # Transform products
    transform_raw_product = container.transform_raw_products()
    transform_product_service = container.transform_product_service()
    product_raw_url = f"s3a://bronze/products/{yesterday}/*.json"
    product_df = transform_raw_product.transform(
        product_raw_url, f"s3a://silver/products/{yesterday}/"
    )
    transform_product_service.transform(product_df)

    # Transform datetime
    logger.info("Starting transform datetime")
    transform_datetime_service = container.transform_datetime_service()
    transform_datetime_service.transform()


if __name__ == "__main__":
    main()
