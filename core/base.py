"""Abstract base for every data source."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class DataSource(ABC):
    def __init__(self, config: Dict, global_config: Dict,
                 rate_limiter=None, session_pool=None):
        self.config = config
        self.global_config = global_config
        self.name: str = config["name"]
        self.enabled: bool = config.get("enabled", True)
        self.schedule: Optional[str] = config.get("schedule")

    @abstractmethod
    async def fetch(self) -> Any:
        ...

    @abstractmethod
    async def parse(self, raw_data: Any) -> List[Dict]:
        ...

    @abstractmethod
    async def transform(self, records: List[Dict]) -> List[Dict]:
        ...

    @abstractmethod
    async def store(self, records: List[Dict]) -> None:
        ...

    async def run(self):
        """Fetch → parse → transform → store pipeline."""
        if not self.enabled:
            return
        raw = await self.fetch()
        if raw is None:
            logger.warning("Source %s: fetch returned None, skipping", self.name)
            return
        parsed = await self.parse(raw)
        if not parsed:
            logger.warning("Source %s: no records after parse", self.name)
            return
        transformed = await self.transform(parsed)
        if not transformed:
            logger.warning("Source %s: no records after transform", self.name)
            return
        await self.store(transformed)
        logger.info("Source %s: pipeline complete (%d records)", self.name, len(transformed))
