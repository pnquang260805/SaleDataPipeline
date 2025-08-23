from abc import ABC, abstractmethod


class ConsumerMessage(ABC):
    @abstractmethod
    def consume_message(self, *args, **kwargs):
        pass
