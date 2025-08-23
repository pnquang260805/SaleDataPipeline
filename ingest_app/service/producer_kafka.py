import logging

from confluent_kafka import Producer

from interface.producer import ProducerMessage
from utils.logger import log


class KafkaProducer(ProducerMessage):
    def __init__(self, bootstrap_servers: str):
        conf = {"bootstrap.servers": bootstrap_servers}
        self.producer = Producer(conf)
        self.logger = logging.getLogger(__name__)

    @log
    def __ack(self, err, msg):
        if err is not None:
            raise Exception(
                f"Failed to deliver message: {str(msg)} with error {str(err)}"
            )
        else:
            self.logger.info(f"Message produced: {str(msg)}")

    @log
    def send_message(self, topic, message, key="default_key", *args, **kwargs):
        self.producer.produce(topic, key=key, value=message, callback=self.__ack)
        self.producer.poll(1)
