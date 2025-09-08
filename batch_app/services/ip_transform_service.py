import logging

from dataclasses import dataclass
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from interface.transform_silver import TransformSilverService
from services.delta_services import DeltaService


@dataclass
class IpTransformService(TransformSilverService):
    delta_service: DeltaService
    ip_loc: str = "s3a://warehouse/default/dim_ip"

    def __post_init__(self):
        self.logger = logging

    def transform(self, silver_df: DataFrame):
        ip_client_df = silver_df.selectExpr("client_ip as ip").withColumn(
            "type", lit("client")
        )
        ip_server_df = silver_df.selectExpr("server_ip as ip").withColumn(
            "type", lit("server")
        )
        ip_df = ip_client_df.union(ip_server_df)  # Gộp 2 bảng cùng trường
        ip_df = ip_df.dropDuplicates()
        if not self.delta_service.is_delta_table(self.ip_loc):
            self.spark_service.write_file(
                dir=self.ip_loc, df=ip_df, format="delta", mode="overwrite"
            )
            return
        delta_table = self.delta_service.get_delta_table(self.ip_loc)
        self.delta_service.merge(delta_table, ip_df, "ip")
