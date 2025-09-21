import logging

from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from dataclasses import dataclass
from delta import DeltaTable
from pyspark.sql.window import Window

from interface.transform_silver import TransformSilverService
from utils.logger import log
from services.delta_services import DeltaService
from services.spark_service import SparkService


datetime_format = "%Y-%m-%d"


@dataclass
class TransformCustomer(TransformSilverService):
    delta_service: DeltaService
    dim_customer_loc: str = "s3a://warehouse/default/dim_customer"
    spark_service: SparkService

    def __post_init__(self):
        self.logger = logging
        self.spark = self.spark_service.get_spark()

    @log
    def __transform_uc(
        self, silver_df: DataFrame, existed_customers_df: DeltaTable, max_key: int
    ):
        w = Window.orderBy(monotonically_increasing_id())
        df = silver_df.select(
            col("id").alias("customer_alternate_key"),
            col("first_name_after").alias("first_name"),
            col("middle_name_after").alias("middle_name"),
            col("last_name_after").alias("last_name"),
            col("email_after").alias("email"),
            col("address_line_1_after").alias("address_line_1"),
            col("address_line_2_after").alias("address_line_2"),
            col("phone_number_after").alias("phone_number"),
            col("gender_after").alias("gender"),
            col("date_of_birth"),
        ).where((col("op") == "u") | (col("op") == "c"))
        df = df.dropDuplicates()
        df = (
            df.withColumn(
                "effective_date",
                (lit(datetime.now().strftime(datetime_format)).cast("date")),
            )
            .withColumn("is_current", lit(True))
            .withColumn("dob", to_date(col("date_of_birth")))
            .drop("date_of_birth")
        )

        df = df.withColumn(
            "customer_key", (row_number().over(w) + max_key).cast("bigint")
        )
        self.logger.warn(df.schema.simpleString())

        conditions = "t.customer_alternate_key = s.customer_alternate_key AND t.is_current = true"

        if existed_customers_df is None:
            self.spark_service.write_file(
                dir=self.dim_customer_loc,
                df=df,
                format="delta",
                mode="append",
                mergeSchema="true",
            )
            return
        self.delta_service.scd_type2(
            target=existed_customers_df,
            source=df,
            condition=conditions,
            table_path=self.dim_customer_loc,
        )

    @log
    def __transform_delete(
        self, silver_df: DataFrame, existed_customers_df: DeltaTable
    ):
        df = silver_df.select(col("id")).where(col("op") == "d")
        df = df.withColumn(
            "expiration_date", lit(datetime.now().strftime(datetime_format))
        )

        # Mark the last customer is deleted
        (
            existed_customers_df.alias("t")
            .merge(
                df.alias("s"), "t.customer_alternate_key = s.id AND t.is_current = true"
            )
            .whenMatchedUpdate(
                set={
                    "expiration_date": col("s.expiration_date"),
                    "is_current": lit(False),
                }
            )
        )

    @log
    def transform(self, silver_df: DataFrame):
        existed_customers_df = self.delta_service.get_delta_table(self.dim_customer_loc)
        if not existed_customers_df:
            max_key = 0
        else:
            max_key = (
                existed_customers_df.toDF().agg(max("customer_key")).collect()[0][0]
            )
        self.logger.info("Start transform UC")
        self.__transform_uc(silver_df, existed_customers_df, max_key)
        self.logger.info("Start transform D")
        self.__transform_delete(silver_df, existed_customers_df)
