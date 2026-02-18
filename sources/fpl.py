"""
FPL (Fantasy Premier League) data source.

Handles config-driven fetching for any FPL API endpoint:
  - bootstrap-static
  - league standings (with pagination + multiple leagues)
  - manager profiles & history
  - manager picks & transfers
"""

import logging
from typing import List, Dict, Any

from core.base import DataSource
from core.fetcher import HttpFetcher
from core.parser import JsonParser
from core.transformer import Transformer
from storage.sqlite_storage import SQLiteStorage
from storage.file_storage import FileStorage

logger = logging.getLogger(__name__)


class FPLSource(DataSource):
    """Config-driven source that fetches from any HTTP endpoint."""

    def __init__(self, config: dict, global_config: dict, rate_limiter, session_pool):
        super().__init__(config, global_config, rate_limiter, session_pool)

        fetcher_cfg = config.get("fetcher", {})
        if not fetcher_cfg.get("url"):
            raise ValueError(f"Source {self.name}: fetcher.url is required")

        self.fetcher = HttpFetcher(
            fetcher_cfg,
            rate_limiter,
            session_pool,
            global_config["network"].get("proxies", []),
            global_config["network"].get("user_agents", []),
            cache_dir=global_config["network"].get("cache_dir", "./cache"),
            global_config=global_config,
            source_name=self.name,
        )

        parser_cfg = config.get("parser", {"type": "json"})
        self.parser = JsonParser(parser_cfg)
        self.transformer = Transformer(config.get("transformer", []))

        # Build storage items
        self.storage_items: list[tuple] = []
        storage_cfgs = config.get("storage", [])
        if not isinstance(storage_cfgs, list):
            storage_cfgs = [storage_cfgs]

        for st_cfg in storage_cfgs:
            if st_cfg is None:
                continue
            stype = st_cfg.get("type", "sqlite")
            if stype == "sqlite":
                storage = SQLiteStorage(global_config["default_storage"]["path"])
                self.storage_items.append((storage, st_cfg))
            elif stype == "file":
                storage = FileStorage(global_config["export"].get("csv_dir", "exports"))
                self.storage_items.append((storage, st_cfg))
            else:
                logger.warning("Unknown storage type %s in source %s", stype, self.name)

    async def fetch(self) -> Any:
        params = self.config.get("fetcher", {}).get("params", {})
        logger.info("Source %s: fetching with params %s", self.name, params)
        return await self.fetcher.fetch(params)

    async def parse(self, raw_data: Any) -> List[Dict]:
        logger.info("Source %s: parsing data", self.name)
        return self.parser.parse(raw_data)

    async def transform(self, records: List[Dict]) -> List[Dict]:
        logger.info("Source %s: transforming %d records", self.name, len(records))
        return self.transformer.transform(records)

    async def store(self, records: List[Dict]) -> None:
        logger.info(
            "Source %s: storing %d records in %d backend(s)",
            self.name, len(records), len(self.storage_items),
        )
        for storage, st_cfg in self.storage_items:
            try:
                await storage.store(records, st_cfg)
            except Exception as exc:
                logger.error(
                    "Source %s: storage error for table %s: %s",
                    self.name, st_cfg.get("table", "?"), exc,
                )

async def fetch_manager_history(manager_id: int) -> List[Dict]:
    """Получить историю менеджера (entry/{id}/history/)"""
    url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/history/"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get('current', [])
    return []

async def fetch_manager_picks(manager_id: int, event: int) -> List[Dict]:
    """Получить состав менеджера на игровую неделю"""
    url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/event/{event}/picks/"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get('picks', [])
    return []
