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
    sql_dim_loc : str = "default.dim_date"
    spark_service : SparkService

    def __post_init__(self):
        self.logger = logging
        self.spark = self.spark_service.get_spark()

    @log
    def transform(self):
        yesterday = (datetime.now() - timedelta(days=1))
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
            12: "December"
        }
        data = {
            "DateKey": yesterday.strftime("%Y%m%d"),
            "FullDateAlternateKey": yesterday.strftime("%Y-%m-%d"),
        }
        df = self.spark.createDataFrame([data])
        alternate_key_col = "FullDateAlternateKey"
        dim_date_df = (df.withColumn("DayNumberOfWeek", dayofweek(alternate_key_col))
                       .withColumn("DayNameOfWeek", date_format(alternate_key_col, "EEEE"))
                       .withColumn("DayNumberOfMonth", dayofmonth(alternate_key_col))
                       .withColumn("DayNumberOfYear", dayofyear(alternate_key_col))
                       .withColumn("WeekNumberOfYear", weekofyear(alternate_key_col))
                       .withColumn("MonthNumber", month(alternate_key_col))
                       .withColumn("MonthName", lit(month_dict[int(yesterday.strftime("%m"))]))
                       .withColumn("CalendarQuarter", quarter(alternate_key_col))
                       .withColumn("CalenderYear", year(alternate_key_col))
                       )
        delta_table = self.delta_service.is_delta_table(self.dim_date_loc)
        if not delta_table:
            self.spark_service.write_delta_table(dim_date_df, self.dim_date_loc)
        else:
            self.delta_service.scd_type2(delta_table, dim_date_df, "DateKey")