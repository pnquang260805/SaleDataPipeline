from abc import ABC, abstractmethod
from dataclasses import dataclass

from services.spark_service import SparkService


@dataclass
class TransformSilverService(ABC):
    spark_service: SparkService

    def __post_init__(self):
        self.spark = self.spark_service.get_spark()

    @abstractmethod
    def transform(self, *args, **kwargs):
        pass
