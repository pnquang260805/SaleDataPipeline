import os
import logging

from dotenv import load_dotenv
from datetime import datetime, timedelta

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
    logger.info("Starting transform datetime")
    transform_datetime_service = container.transform_datetime_service()
    # O(n)
    for i in range(365):
        today = datetime.now() + timedelta(days=i)
        transform_datetime_service.transform(today)


if __name__ == "__main__":
    main()
