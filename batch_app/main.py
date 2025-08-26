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


def init_services(container: Containers, conf: SparkConf, master: str) -> Services:
    spark_service = container.spark_service(conf=conf, master=master)
    dt_service = container.datetime_service()
    common_service = container.common_service()
    return Services(
        spark_service=spark_service,
        dt_service=dt_service,
        common_service=common_service,
    )


def build_config(cfg: AppConfig):
    conf = SparkConf()
    conf.set("spark.jars", ",".join(cfg.full_lib_path()))

    # Config iceberg
    (
        conf.set(
            "spark.sql.catalog.rest_catalog.io-impl",
            "org.apache.iceberg.hadoop.HadoopFileIO",
        )
        .set(
            f"spark.sql.catalog.{cfg.catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .set(f"spark.sql.catalog.{cfg.catalog_name}.type", "rest")
        .set(f"spark.sql.catalog.{cfg.catalog_name}.uri", cfg.rest_iceberg_uri)
        .set(f"spark.sql.catalog.{cfg.catalog_name}.warehouse", cfg.warehouse_dir)
    )

    # Config S3
    (
        conf.set("spark.hadoop.fs.s3a.access.key", cfg.ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", cfg.SECRET_KEY)
        .set("spark.hadoop.fs.s3a.endpoint", cfg.s3_endpoint)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
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
    log_transform_service = container.log_transform(lookup_url)
    df = log_transform_service.transform_raw(raw_url, silver_url)
    print(df.show(5, truncate=False))


@log
def main() -> None:
    cfg = AppConfig()
    conf = build_config(cfg)

    container = Containers()  # consider injecting this (for tests)
    services = init_services(container, conf, cfg.master)

    try:
        run_pipeline(container, services, cfg)
    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        raise


if __name__ == "__main__":
    main()
