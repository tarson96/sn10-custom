from abc import ABC, abstractmethod
from common.data import (
    CompressedMinerIndex,
    DataEntity,
    DataEntityBucketId,
)
from typing import List
import datetime as dt


class MinerStorage(ABC):
    """An abstract class which defines the contract that all implementations of MinerStorage must fulfill."""

    @abstractmethod
    def store_data_entities(self, data_entities: List[DataEntity]):
        """Stores any number of DataEntities, making space if necessary."""
        raise NotImplemented

  


