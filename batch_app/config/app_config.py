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
    hadoop_version: str = "3.3.4"
    bundle_version: str = "1.12.262"
    delta_version: str = "2.4.0"

    ACCESS_KEY: str = os.getenv("ACCESS_KEY")
    SECRET_KEY: str = os.getenv("SECRET_KEY")
    s3_endpoint: str = os.getenv("ENDPOINT")

    redis_lookup_base: str = "http://redis-api:80/api/redis"
    master: str = "spark://spark-master:7077"
    bronze_base: str = "s3a://bronze"
    silver_base: str = "s3a://silver"

    def extra_packages(self) -> List[str]:
        jars = [
            f"org.apache.hadoop:hadoop-aws:{self.hadoop_version}",
            f"org.apache.hadoop:hadoop-common:{self.hadoop_version}",
            f"com.amazonaws:aws-java-sdk-bundle:{self.bundle_version}",
            # Delta
            # f"io.delta:delta-core_2.12:{self.delta_version}",
        ]
        return jars
