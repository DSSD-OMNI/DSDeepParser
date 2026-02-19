"""
Calendar source — international breaks and match schedule data.

Provides ``days_since_last_match`` calculations and international break flags
for the ML feature pipeline.

Config example::

    - name: calendar_breaks
      type: calendar
      enabled: true
      schedule: "0 0 * * 1"  # weekly
      storage:
        - table: raw_calendar
          type: sqlite
          mode: overwrite
"""

import aiohttp
import logging
from typing import List, Dict, Any
from datetime import datetime, timezone

from core.base import DataSource
from storage.sqlite_storage import SQLiteStorage

logger = logging.getLogger(__name__)

# 2024-25 EPL season international breaks
INTERNATIONAL_BREAKS_2024_25 = [
    {"start": "2024-09-02", "end": "2024-09-10", "name": "September 2024 break"},
    {"start": "2024-10-07", "end": "2024-10-15", "name": "October 2024 break"},
    {"start": "2024-11-11", "end": "2024-11-19", "name": "November 2024 break"},
    {"start": "2025-03-17", "end": "2025-03-25", "name": "March 2025 break"},
]

# 2025-26 season (tentative)
INTERNATIONAL_BREAKS_2025_26 = [
    {"start": "2025-09-01", "end": "2025-09-09", "name": "September 2025 break"},
    {"start": "2025-10-06", "end": "2025-10-14", "name": "October 2025 break"},
    {"start": "2025-11-10", "end": "2025-11-18", "name": "November 2025 break"},
    {"start": "2026-03-23", "end": "2026-03-31", "name": "March 2026 break"},
]


class CalendarSource(DataSource):
    def __init__(self, config: dict, global_config: dict, rate_limiter=None, session_pool=None):
        super().__init__(config, global_config, rate_limiter, session_pool)
        self.storage_items = self._build_storage(config, global_config)
        self.fpl_fixtures_url = "https://fantasy.premierleague.com/api/fixtures/"

    def _build_storage(self, config, global_config):
        items = []
        for st in config.get("storage", [{"table": "raw_calendar", "type": "sqlite", "mode": "overwrite"}]):
            if st and st.get("type") == "sqlite":
                items.append((SQLiteStorage(global_config["default_storage"]["path"]), st))
        if not items:
            items.append((
                SQLiteStorage(global_config["default_storage"]["path"]),
                {"table": "raw_calendar", "type": "sqlite", "mode": "overwrite"},
            ))
        return items

    async def fetch(self) -> Any:
        """Fetch FPL fixtures for match scheduling data."""
        logger.info("Calendar: fetching FPL fixtures")
        fixtures = None
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.fpl_fixtures_url,
                    timeout=aiohttp.ClientTimeout(total=30),
                    headers={"Accept": "application/json"},
                ) as resp:
                    if resp.status == 200:
                        fixtures = await resp.json()
        except Exception as exc:
            logger.warning("Calendar: fixtures fetch failed: %s", exc)
        return fixtures

    async def parse(self, raw_data: Any) -> List[Dict]:
        records = []
        now = datetime.now(timezone.utc)

        # International breaks
        all_breaks = INTERNATIONAL_BREAKS_2024_25 + INTERNATIONAL_BREAKS_2025_26
        for brk in all_breaks:
            records.append({
                "record_type": "international_break",
                "name": brk["name"],
                "start_date": brk["start"],
                "end_date": brk["end"],
                "is_active": brk["start"] <= now.strftime("%Y-%m-%d") <= brk["end"],
                "fetched_at": now.isoformat(),
            })

        # Fixtures (if available)
        if raw_data and isinstance(raw_data, list):
            for fix in raw_data:
                kickoff = fix.get("kickoff_time")
                records.append({
                    "record_type": "fixture",
                    "fixture_id": fix.get("id"),
                    "event": fix.get("event"),  # gameweek
                    "home_team_id": fix.get("team_h"),
                    "away_team_id": fix.get("team_a"),
                    "home_score": fix.get("team_h_score"),
                    "away_score": fix.get("team_a_score"),
                    "kickoff_time": kickoff,
                    "finished": fix.get("finished", False),
                    "fetched_at": now.isoformat(),
                })

        logger.info("Calendar: %d records (breaks + fixtures)", len(records))
        return records

    async def transform(self, records: List[Dict]) -> List[Dict]:
        return records

    async def store(self, records: List[Dict]) -> None:
        breaks = [r for r in records if r.get("record_type") == "international_break"]
        fixtures = [r for r in records if r.get("record_type") == "fixture"]

        for storage, st_cfg in self.storage_items:
            table = st_cfg.get("table", "raw_calendar")
            if breaks:
                await storage.store(breaks, {**st_cfg, "table": table})
            if fixtures:
                await storage.store(fixtures, {**st_cfg, "table": f"{table}_fixtures"})
