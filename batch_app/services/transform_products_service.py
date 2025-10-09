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
class TransformProduct(TransformSilverService):
    delta_service: DeltaService
    s3_loc: str = "s3a://warehouse/default/dim_product"
    spark_service: SparkService

    @log
    def __transform_uc(
        self, silver_df: DataFrame, existed_product_df: DeltaTable, max_key: int
    ):
        w = Window.orderBy(monotonically_increasing_id())
        df = silver_df.select(
            col("id").alias("product_alternate_key"),
            col("after_product_name").alias("product_name"),
            col("after_description").alias("description"),
            col("after_price").alias("price"),
            col("after_quantity_in_stock").alias("quantity_in_stock"),
        ).where((col("op") == "u") | (col("op") == "c"))
        df = df.dropDuplicates()
        df = (
            df.withColumn(
                "effective_date",
                (lit(datetime.now().strftime(datetime_format)).cast("date")),
            )
            .withColumn("is_current", lit(True))
        )

        df = df.withColumn(
            "product_key", (row_number().over(w) + max_key).cast("bigint")
        )
        self.logger.warn(df.schema.simpleString())

        conditions = "t.product_alternate_key = s.product_alternate_key AND t.is_current = true"

        if existed_product_df is None:
            self.spark_service.write_file(
                dir=self.s3_loc,
                df=df,
                format="delta",
                mode="append",
                mergeSchema="true",
            )
            return
        self.delta_service.scd_type2(
            target=existed_product_df,
            source=df,
            condition=conditions,
            table_path=self.dim_customer_loc,
        )

    @log
    def __transform_delete(
        self, silver_df: DataFrame, existed_product_df: DeltaTable
    ):
        df = silver_df.select(col("id")).where(col("op") == "d")
        df = df.withColumn(
            "expiration_date", lit(datetime.now().strftime(datetime_format))
        )

        # Mark the last customer is deleted
        (
            existed_product_df.alias("t")
            .merge(
                df.alias("s"), "t.product_alternate_key = s.id AND t.is_current = true"
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
        existed_product_df = self.delta_service.get_delta_table(self.s3_loc)
        max_key = 0
        if existed_product_df:
            max_key = (
                existed_product_df.toDF().agg(max("product_key")).collect()[0][0]
            )
        self.__transform_uc(silver_df, existed_product_df)
        self.__transform_delete(silver_df, existed_product_df)
        