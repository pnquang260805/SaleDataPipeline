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
class TransformDate(TransformSilverService):
    delta_service: DeltaService
    dim_date_loc: str = "s3a://warehouse/default/dim_date"
    sql_dim_loc: str = "sales.dim_date"
    spark_service: SparkService

    def __post_init__(self):
        self.logger = logging
        self.spark = self.spark_service.get_spark()

    @log
    def transform(self):
        yesterday = datetime.now() - timedelta(days=1)
        month_dict = {
            1: "January",
            2: "February",
            3: "March",
            4: "April",
            5: "May",
            6: "June",
            7: "July",
            8: "August",
            9: "September",
            10: "October",
            11: "November",
            12: "December",
        }
        alternate_key_col = "full_date_alternate_key"
        data = {
            "date_key": yesterday.strftime("%Y%m%d"),
            "full_date_alternate_key": yesterday.strftime("%Y-%m-%d"),
        }
        df = self.spark.createDataFrame([data])
        df = df.withColumn("full_date_alternate_key", to_date(col("full_date_alternate_key"), "yyyy-MM-dd"))
        dim_date_df = (
            df.withColumn("day_number_of_week", dayofweek(alternate_key_col))
            .withColumn("day_name_of_week", date_format(alternate_key_col, "EEEE"))
            .withColumn("day_number_of_month", dayofmonth(alternate_key_col))
            .withColumn("day_number_of_year", dayofyear(alternate_key_col))
            .withColumn("week_number_of_year", weekofyear(alternate_key_col))
            .withColumn("month_number", month(alternate_key_col))
            .withColumn("month_name", lit(month_dict[int(yesterday.strftime("%m"))]))
            .withColumn("calendar_quarter", quarter(alternate_key_col))
            .withColumn("calendar_year", year(alternate_key_col))
        )
        dim_date_df = dim_date_df.dropDuplicates()
        self.spark_service.write_delta_table(
            dim_date_df, self.dim_date_loc, mode="append"
        )
