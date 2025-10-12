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

    logger.info("Starting transform customer")
    # transform customer to warehouse
    transform_raw_customer = container.transform_raw_customer()
    transform_customer_service = container.transform_customer_service()

    customer_raw_url = f"s3a://bronze/customers/{yesterday}/*.json"
    customer_df = transform_raw_customer.transform(
        customer_raw_url, f"s3a://silver/customers/{yesterday}/"
    )

    transform_customer_service.transform(customer_df)


if __name__ == "__main__":
    main()
