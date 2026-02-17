"""Storage abstraction layer."""

from abc import ABC, abstractmethod
from typing import List, Dict
import asyncio


class BaseStorage(ABC):
    @abstractmethod
    async def store(self, records: List[Dict], source_config: Dict) -> None:
        ...

    async def query(self, sql: str, params: tuple = ()) -> List[Dict]:
        """Optional: run a read query. Not all backends need this."""
        raise NotImplementedError


class MultiStorage(BaseStorage):
    """Fan-out storage: writes to multiple backends in parallel."""

    def __init__(self, storages: List[BaseStorage]):
        self.storages = storages

    async def store(self, records: List[Dict], source_config: Dict) -> None:
        tasks = [s.store(records, source_config) for s in self.storages]
        await asyncio.gather(*tasks)
