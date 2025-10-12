from pyspark.conf import SparkConf

from config.app_config import AppConfig

cfg = AppConfig()
def spark_config():
    conf = SparkConf()
    (
        conf.set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.databricks.delta.metastore.type", "filesystem")
        .set("spark.hadoop.fs.s3a.access.key", cfg.ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", cfg.SECRET_KEY)
        .set("spark.hadoop.fs.s3a.endpoint", cfg.s3_endpoint)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.sql.warehouse.dir", "s3a://warehouse")
        .set("spark.dynamicAllocation.shuffleTracking.enabled", "true")
        .set("spark.executor.cores", "1")
        .set("spark.cores.max", "1")
    )
    return conf