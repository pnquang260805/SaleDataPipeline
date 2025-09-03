from dependency_injector import containers, providers

from services.datetime_service import DatetimeService
from services.transform_raw_service import TransformRawService
from services.spark_service import SparkService
from services.common_service import CommonService
from services.datetime_service import DatetimeService


class Containers(containers.DeclarativeContainer):
    config = providers.Configuration()
    spark_service = providers.Singleton(SparkService)
    common_service = providers.Factory(CommonService)
    datetime_service = providers.Singleton(DatetimeService)
    transform_raw_service = providers.Factory(
        TransformRawService, spark_service=spark_service
    )
