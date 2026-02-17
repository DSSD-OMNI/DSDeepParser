"""
FCI (Fantasy Community Index) — extended player-level gameweek stats.

Reads from a CSV file (local or remote URL).
Stores into ``raw_fci_stats`` table.

Config example::

    - name: fci_player_stats
      type: fci
      enabled: true
      schedule: "0 8 * * *"
      params:
        csv_path: "player_gameweek_stats.csv"
        # or csv_url: "https://example.com/player_gameweek_stats.csv"
      storage:
        - table: raw_fci_stats
          type: sqlite
          mode: overwrite
"""

import aiohttp
import logging
import os
from typing import List, Dict, Any
from datetime import datetime, timezone

from core.base import DataSource
from storage.sqlite_storage import SQLiteStorage

logger = logging.getLogger(__name__)


class FCISource(DataSource):
    def __init__(self, config: dict, global_config: dict, rate_limiter=None, session_pool=None):
        super().__init__(config, global_config, rate_limiter, session_pool)
        params = config.get("params", config.get("fetcher", {}).get("params", {}))
        self.csv_path = params.get("csv_path", "player_gameweek_stats.csv")
        self.csv_url = params.get("csv_url")
        self.storage_items = self._build_storage(config, global_config)

    def _build_storage(self, config, global_config):
        items = []
        for st in config.get("storage", [{"table": "raw_fci_stats", "type": "sqlite", "mode": "overwrite"}]):
            if st and st.get("type") == "sqlite":
                items.append((SQLiteStorage(global_config["default_storage"]["path"]), st))
        if not items:
            items.append((
                SQLiteStorage(global_config["default_storage"]["path"]),
                {"table": "raw_fci_stats", "type": "sqlite", "mode": "overwrite"},
            ))
        return items

    async def fetch(self) -> Any:
        # Try remote URL first
        if self.csv_url:
            logger.info("FCI: downloading from %s", self.csv_url)
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.csv_url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                        if resp.status == 200:
                            return await resp.text()
                        logger.warning("FCI URL returned %d", resp.status)
            except Exception as exc:
                logger.error("FCI download failed: %s", exc)

        # Fallback to local file
        if os.path.exists(self.csv_path):
            logger.info("FCI: reading local file %s", self.csv_path)
            with open(self.csv_path, "r", encoding="utf-8") as f:
                return f.read()

        logger.warning("FCI: no data source available (no URL or local file)")
        return None

    async def parse(self, raw_data: Any) -> List[Dict]:
        if not raw_data:
            return []
        import csv
        from io import StringIO
        reader = csv.DictReader(StringIO(raw_data))
        records = []
        for row in reader:
            row["fetched_at"] = datetime.now(timezone.utc).isoformat()
            records.append(row)
        logger.info("FCI: parsed %d player-gameweek records", len(records))
        return records

    async def transform(self, records: List[Dict]) -> List[Dict]:
        # Ensure numeric columns
        numeric_fields = [
            "minutes", "goals_scored", "assists", "clean_sheets",
            "goals_conceded", "bonus", "bps", "influence", "creativity",
            "threat", "ict_index", "total_points", "xg", "xa", "xgi",
        ]
        for rec in records:
            for f in numeric_fields:
                if f in rec and rec[f] is not None:
                    try:
                        rec[f] = float(rec[f])
                    except (ValueError, TypeError):
                        pass
        return records

    async def store(self, records: List[Dict]) -> None:
        for storage, st_cfg in self.storage_items:
            await storage.store(records, st_cfg)
