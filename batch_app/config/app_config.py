import os
import logging

from typing import *
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


@dataclass
class AppConfig:
    # version
    awssdk_version: str = "2.32.19"
    hadoop_version: str = "3.3.4"  # phù hợp với spark 3.5.0
    aws_bundle_version: str = "1.12.262"
    iceberg_spark_version: str = "3.5_2.12-1.9.2"

    # libs
    lib_dir = "./lib/"

    # iceberg and s3
    catalog_name: str = "iceberg_s3_catalog"
    rest_iceberg_uri: str = "http://rest:8181"
    warehouse_dir: str = "s3a://warehouse/"
    db_name: str = "iceberg_db"
    ACCESS_KEY: str = os.getenv("ACCESS_KEY")
    SECRET_KEY: str = os.getenv("SECRET_KEY")
    s3_endpoint: str = os.getenv("ENDPOINT")

    redis_lookup_base: str = "http://redis-api:80/api/redis"
    master: str = "spark://spark-master:7077"
    bronze_base: str = "s3a://bronze"
    silver_base: str = "s3a://silver"

    def full_lib_path(self) -> List[str]:
        jars = [
            f"iceberg-spark-runtime-{self.iceberg_spark_version}",
            f"hadoop-aws-{self.hadoop_version}",
            f"sts-{self.awssdk_version}",
            f"s3-{self.awssdk_version}",
            f"url-connection-client-{self.awssdk_version}",
            f"hadoop-common-{self.hadoop_version}",
            f"aws-java-sdk-bundle-{self.aws_bundle_version}",
        ]
        lib_dir = os.getenv("LIB_DIR")
        return [lib_dir + jar_name + ".jar" for jar_name in jars]
