import logging

from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from dataclasses import dataclass

from interface.transform_silver import TransformSilverService
from utils.logger import log
from services.delta_services import DeltaService
from services.spark_service import SparkService

@dataclass
class TransformCustomer(TransformSilverService):
    delta_service: DeltaService
    dim_date_loc: str = "s3a://warehouse/default/dim_customer"
    sql_dim_loc: str = "default.dim_customer"
    spark_service: SparkService

    def __post_init__(self):
        self.logger = logging
        self.spark = self.spark_service.get_spark()

    @log
    def transform(self, raw_df : DataFrame):
        pass