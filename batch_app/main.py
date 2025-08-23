import os
import logging

from dotenv import load_dotenv
from pyspark.conf import SparkConf

from utils.containers import Container
from utils.logger import log

load_dotenv()
container = Container()
logger = logging


@log
def main() -> None:
    dt_service = container.datetime_service()
    common_service = container.common_service()

    awssdk_version = "2.32.19"
    catalog_name = "iceberg_s3_catalog"
    rest_iceberg_uri = "http://rest:8181"
    warehouse_dir = "s3a://warehouse/"
    db_name = "iceberg_db"
    aws_bundle_version = "1.12.262"

    hadoop_version = "3.3.4"  # phù hợp với spark 3.5.0

    ACCESS_KEY = "admin"
    SECRET_KEY = "password"
    s3_endpoint = "http://minio:9000"

    conf = SparkConf()
    lib_dir = "/batch_lib/"
    jars = [
        "iceberg-spark-runtime-3.5_2.12-1.9.2",
        f"hadoop-aws-{hadoop_version}",
        f"sts-{awssdk_version}",
        f"s3-{awssdk_version}",
        f"url-connection-client-{awssdk_version}",
        f"hadoop-common-{hadoop_version}",
        f"aws-java-sdk-bundle-{aws_bundle_version}",
    ]

    full_path_lib = [lib_dir + jar + ".jar" for jar in jars]

    conf.set("spark.jars", ",".join(full_path_lib)).set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    # Config iceberg
    (
        conf.set(
            "spark.sql.catalog.rest_catalog.io-impl",
            "org.apache.iceberg.hadoop.HadoopFileIO",
        )
        .set(
            f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog"
        )
        .set(f"spark.sql.catalog.{catalog_name}.type", "rest")
        .set(f"spark.sql.catalog.{catalog_name}.uri", rest_iceberg_uri)
        .set(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_dir)
    )

    # Config S3
    (
        conf.set("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .set("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

    master = "spark://spark-master:7077"
    spark_service = container.spark_service(conf=conf, master=master)

    key = "last_log"
    lookup_url = f"http://redis-api/api/redis/get-key?key={key}"
    yesterday = dt_service.get_yesterday()
    raw_url = f"s3a://bronze/log/{yesterday}/*.json"
    silver_url = f"s3a://silver/log/{yesterday}"

    if common_service.check_lookup(lookup_url=lookup_url, key="value") is None:
        logger.info(f"{yesterday} has no log")
        return

    log_transform_service = container.log_transform(lookup_url)
    df = log_transform_service.transform_raw(raw_url, silver_url)


if __name__ == "__main__":
    main()
