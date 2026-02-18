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

# ----------------------------------------------------------------------
# Добавленные функции для истории и составов (по запросу)
# ----------------------------------------------------------------------
import asyncio
import aiohttp

async def fetch_manager_full_history(manager_id: int) -> dict:
    url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/history/"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                return await resp.json()
            return {}

async def fetch_manager_picks(manager_id: int, event: int) -> list:
    url = f"https://fantasy.premierleague.com/api/entry/{manager_id}/event/{event}/picks/"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                picks = data.get('picks', [])
                for p in picks:
                    p['manager_id'] = manager_id
                    p['event'] = event
                return picks
            return []

async def collect_league_history(league_id: int):
    import sqlite3
    from storage.sqlite_storage import SQLiteStorage
    from core.config import get_config

    config = get_config()
    storage = SQLiteStorage(config["default_storage"]["path"])

    conn = sqlite3.connect(config["default_storage"]["path"])
    cursor = conn.cursor()
    cursor.execute(f"SELECT DISTINCT manager_id FROM league_standings_{league_id}")
    managers = [row[0] for row in cursor.fetchall()]
    conn.close()

    print(f"Сбор истории для {len(managers)} менеджеров")
    history = []
    picks = []

    for mid in managers:
        hist = await fetch_manager_full_history(mid)
        if hist:
            for gw in hist.get('current', []):
                gw['manager_id'] = mid
                history.append(gw)
            for chip in hist.get('chips', []):
                chip['manager_id'] = mid
                chip['chip_name'] = chip.get('name')
                history.append(chip)
            for gw in hist.get('current', []):
                event = gw['event']
                p = await fetch_manager_picks(mid, event)
                picks.extend(p)
        await asyncio.sleep(0.5)

    if history:
        await storage.store(history, {"table": "raw_manager_history", "mode": "append"})
    if picks:
        await storage.store(picks, {"table": "raw_manager_picks", "mode": "append"})
    print(f"Сохранено: история {len(history)}, составы {len(picks)}")
