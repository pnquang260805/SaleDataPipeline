from datetime import datetime
from dataclasses import dataclass
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from utils.logger import log
from services.spark_service import SparkService


@dataclass
class TransformRawCustomer:
    spark_service: SparkService

    def __post_init__(self):
        self.spark = self.spark_service.get_spark()

    @log
    def transform(
        self,
        raw_data_url: str,
        silver_dir: str,
        input_format: str = "json",
        saved_format: str = "parquet",
        *args,
        **kwargs
    ) -> DataFrame:
        """
        Optional parameter:
            save_mode: overwrite, append, ignore
        """

        raw_df = self.spark_service.read_file(
            raw_data_url, format="json", multiLine="true"
        )
        raw_df = raw_df.select(col("payload.*"))
        # raw_df.show(truncate=False)
        silver_df = raw_df.select(
            col("after.ID").alias("id"),
            col("before.FirstName").alias("first_name_before"),
            col("before.MiddleName").alias("middle_name_before"),
            col("before.LastName").alias("last_name_before"),
            col("before.Email").alias("email_before"),
            col("before.AddressLine1").alias("address_line_1_before"),
            col("before.AddressLine2").alias("address_line_2_before"),
            col("before.PhoneNumber").alias("phone_number_before"),
            col("before.Gender").alias("gender_before"),
            col("after.FirstName").alias("first_name_after"),
            col("after.MiddleName").alias("middle_name_after"),
            col("after.LastName").alias("last_name_after"),
            col("after.Email").alias("email_after"),
            col("after.AddressLine1").alias("address_line_1_after"),
            col("after.AddressLine2").alias("address_line_2_after"),
            col("after.PhoneNumber").alias("phone_number_after"),
            col("after.Gender").alias("gender_after"),
            col("after.DateOfBirth").alias("date_of_birth"),
            col("op"),
            col("source.db").alias("database"),
            col("source.table").alias("table"),
            col("ts_ms").alias("timestamp"),
        )
        silver_df = silver_df.withColumn(
            "update_date", lit(datetime.now().strftime("%Y-%m-%d"))
        )
        self.spark_service.write_file(silver_dir, silver_df, saved_format)
        return silver_df
