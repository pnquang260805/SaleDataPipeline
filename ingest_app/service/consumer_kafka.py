import logging
from datetime import datetime

from confluent_kafka import Consumer
import requests

from utils.logger import log
from interface.consumer import ConsumerMessage


class KafkaConsumer(ConsumerMessage):
    def __init__(self, bootstrap_servers):
        conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "foo",
            "enable.auto.commit": "false",
            "auto.offset.reset": "earliest",
        }
        self.consumer = Consumer(conf)
        self.logger = logging.getLogger(__name__)

    def __get_today(self):
        today = datetime.now().strftime("%Y-%m-%d")
        return today

    @log
    def consume_message(self, topic: list[str]):
        running = True
        self.consumer.subscribe(topic)
        while running:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                self.logger.error(msg.error())
            else:
                self.logger.info("Set key to redis")
                try:
                    request = requests.post(
                        f"http://redis-api:80/api/redis/set-key?key=last_log&value={self.__get_today()}"
                    )
                    if request.status_code == 200:
                        self.logger.info(
                            f"Set key last_log success with value {self.__get_today()}"
                        )
                    else:
                        self.logger.error(f"Set key last_log failed")
                except:
                    self.logger.error(
                        f"Can't request to http://redis-api:80/api/redis/set-key?key=last_log&value={self.__get_today()}"
                    )
                self.logger.info("Finished")
