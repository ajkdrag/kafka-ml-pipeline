from abc import ABC, abstractmethod
from dataclasses import dataclass
from pyspark.sql import SparkSession
from utils.config import Config


@dataclass
class BaseSparkJob(ABC):
    spark: SparkSession
    config: Config

    @abstractmethod
    def setup(self):
        print("==="*10, f"Setting up Job: {self.__class__.__name__}", "==="*10)

    @abstractmethod
    def run(self):
        print("==="*10, f"Running Job: {self.__class__.__name__}", "==="*10)