from dependency_injector import containers, providers

from services.datetime_service import DatetimeService
from services.log_transform import LogTransform
from services.spark_service import SparkService
from services.common_service import CommonService
from services.datetime_service import DatetimeService


class Container(containers.DeclarativeContainer):
    config = providers.Configuration()
    spark_service = providers.Singleton(SparkService)
    common_service = providers.Factory(CommonService)
    datetime_service = providers.Singleton(DatetimeService)
    log_transform = providers.Factory(LogTransform, spark_service=spark_service)
