from abc import ABC, abstractmethod


class Transform(ABC):
    @abstractmethod
    def transform_raw(*args, **kwargs):
        pass

    @abstractmethod
    def transform_bronze(*args, **kwargs):
        pass

    @abstractmethod
    def transform_silver(*args, **kwargs):
        pass
