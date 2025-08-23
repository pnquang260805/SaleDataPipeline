from dependency_injector import containers, providers

from service.consumer_kafka import KafkaConsumer
from service.producer_kafka import KafkaProducer
from service.redis_service import RedisService


class Container(containers.DeclarativeContainer):
    config = providers.Configuration()
    kafka_producer_service = providers.Factory(
        KafkaProducer, bootstrap_servers=config.bootstrap_servers
    )
    kafka_consumer_service = providers.Factory(
        KafkaConsumer,
        bootstrap_servers=config.bootstrap_servers,
    )
