from dependency_injector import containers, providers

from services.datetime_service import DatetimeService
from services.transform_raw_customer import TransformRawCustomer
from services.spark_service import SparkService
from services.common_service import CommonService
from services.datetime_service import DatetimeService
from services.delta_services import DeltaService
from services.transform_datetime_service import TransformDate
from services.transform_customer_service import TransformCustomer


class Containers(containers.DeclarativeContainer):
    config = providers.Configuration()
    spark_service = providers.Singleton(SparkService)
    common_service = providers.Factory(CommonService)
    datetime_service = providers.Singleton(DatetimeService)
    transform_raw_customer = providers.Factory(
        TransformRawCustomer, spark_service=spark_service
    )
    delta_service = providers.Factory(DeltaService, spark_service=spark_service)
    transform_datetime_service = providers.Factory(
        TransformDate, spark_service=spark_service, delta_service=delta_service
    )
    transform_customer_service = providers.Factory(
        TransformCustomer, delta_service=delta_service, spark_service=spark_service
    )
