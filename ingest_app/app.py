from dependency_injector.wiring import Provide, inject

from utils.container import Container

if __name__ == "__main__":
    bootstrap_servers = "kafka:9092"

    container = Container()
    container.config.bootstrap_servers.override("kafka:9092")

    consumer = container.kafka_consumer_service()
    consumer.consume_message(["log_batch"])
