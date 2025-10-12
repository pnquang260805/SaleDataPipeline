import os
import logging

from dotenv import load_dotenv

from utils.containers import Containers
from utils.logger import log
from config.app_config import AppConfig
from typing import *
from dataclasses import dataclass
from config.spark_config import spark_config

load_dotenv()
container = Containers()
logger = logging

cfg = AppConfig()
@log
def main() -> None:
    master = "spark://spark-master:7077"

    spark_service = container.spark_service(
        conf=spark_config(), master=master, extra_packages=cfg.extra_packages()
    )

    datetime_service = container.datetime_service()
    yesterday = datetime_service.get_yesterday()

    # Transform products
    transform_raw_product = container.transform_raw_products()
    transform_product_service = container.transform_product_service()
    product_raw_url = f"s3a://bronze/products/{yesterday}/*.json"
    product_df = transform_raw_product.transform(
        product_raw_url, f"s3a://silver/products/{yesterday}/"
    )
    transform_product_service.transform(product_df)


if __name__ == "__main__":
    main()
