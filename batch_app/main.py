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


@dataclass
class Services:
    spark_service: object
    dt_service: object
    common_service: object


def init_services(
    container: Containers, conf: SparkConf, master: str, extra_packages: List[str]
) -> Services:
    spark_service = container.spark_service(
        conf=conf, master=master, extra_packages=extra_packages
    )
    dt_service = container.datetime_service()
    common_service = container.common_service()
    return Services(
        spark_service=spark_service,
        dt_service=dt_service,
        common_service=common_service,
    )


def build_config(cfg: AppConfig):
    conf = SparkConf()
    # conf.set("spark.jars.packages", ",".join(cfg.extra_packages()))

    (
        conf.set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.hadoop.fs.s3a.access.key", cfg.ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", cfg.SECRET_KEY)
        .set("spark.hadoop.fs.s3a.endpoint", cfg.s3_endpoint)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.sql.warehouse.dir", "s3a://warehouse")
    )

    return conf


def prepare_paths(dt_service, cfg: AppConfig) -> Tuple[str, str, str]:
    yesterday = dt_service.get_yesterday()
    raw_url = f"{cfg.bronze_base}/log/{yesterday}/*.json"
    silver_url = f"{cfg.silver_base}/{yesterday}"
    return yesterday, raw_url, silver_url


def check_last_log_and_maybe_exit(
    common_service, cfg: AppConfig, yesterday: str
) -> bool:
    key = "last_log"
    lookup_url = f"{cfg.redis_lookup_base}/get-key?key={key}"
    last_log_day = common_service.check_lookup(lookup_url=lookup_url, key="value")
    if yesterday != last_log_day:
        logger.info("%s has no log (last_log=%s)", yesterday, last_log_day)
        return False
    return True


@log
def run_pipeline(container: Containers, services: Services, cfg: AppConfig):
    yesterday, raw_url, silver_url = prepare_paths(services.dt_service, cfg)
    ok = check_last_log_and_maybe_exit(services.common_service, cfg, yesterday)
    if not ok:
        return
    lookup_url = f"{cfg.redis_lookup_base}last_log"
    log_transform_service = container.transform_raw_service(lookup_url)
    df = log_transform_service.transform_bronze_to_silver(raw_url, silver_url)
    # print(df.show(5, truncate=False))

    transform_date_service = container.transform_datetime_service()
    transform_date_service.transform(df)


@log
def main() -> None:
    cfg = AppConfig()
    conf = build_config(cfg)

    container = Containers()  # consider injecting this (for tests)
    services = init_services(container, conf, cfg.master, cfg.extra_packages())

    try:
        run_pipeline(container, services, cfg)
    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        raise


if __name__ == "__main__":
    main()
