from abc import ABC, abstractmethod


class ProducerMessage(ABC):
    @abstractmethod
    def send_message(self, *args, **kwargs):
        pass
