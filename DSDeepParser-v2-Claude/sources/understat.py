"""
Understat xG source — scrapes understat.com for EPL match-level xG data.

Understat embeds JSON data in <script> tags.  We parse the inline JS to
extract team xG and match results.

Stores into ``raw_understat_teams`` (team-level aggregates) and
``raw_understat_matches`` (per-match xG).

Config example::

    - name: understat_epl
      type: understat
      enabled: true
      schedule: "0 7 * * *"
      params:
        league: EPL
        season: "2024"
      storage:
        - table: raw_understat_teams
          type: sqlite
          mode: overwrite
"""

import aiohttp
import asyncio
import json
import re
import logging
from typing import List, Dict, Any
from datetime import datetime, timezone

from core.base import DataSource
from storage.sqlite_storage import SQLiteStorage

logger = logging.getLogger(__name__)


class UnderstatSource(DataSource):
    """Scrape xG data from understat.com."""

    BASE_URL = "https://understat.com/league"

    def __init__(self, config: dict, global_config: dict, rate_limiter=None, session_pool=None):
        super().__init__(config, global_config, rate_limiter, session_pool)
        params = config.get("params", config.get("fetcher", {}).get("params", {}))
        self.league = params.get("league", "EPL")
        self.season = params.get("season", "2024")
        self.storage_items = self._build_storage(config, global_config)

    def _build_storage(self, config, global_config):
        items = []
        for st in config.get("storage", [{"table": "raw_understat_teams", "type": "sqlite", "mode": "overwrite"}]):
            if st and st.get("type") == "sqlite":
                items.append((SQLiteStorage(global_config["default_storage"]["path"]), st))
        if not items:
            items.append((
                SQLiteStorage(global_config["default_storage"]["path"]),
                {"table": "raw_understat_teams", "type": "sqlite", "mode": "overwrite"},
            ))
        return items

    async def fetch(self) -> Any:
        url = f"{self.BASE_URL}/{self.league}/{self.season}"
        logger.info("Understat: fetching %s", url)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        return await resp.text()
                    logger.warning("Understat returned %d for %s", resp.status, url)
        except Exception as exc:
            logger.error("Understat fetch failed: %s", exc)
        return None

    async def parse(self, raw_data: Any) -> List[Dict]:
        if not raw_data or not isinstance(raw_data, str):
            return []
        return self._parse_teams(raw_data) + self._parse_matches(raw_data)

    def _extract_json_var(self, html: str, var_name: str) -> Any:
        """Extract JSON data from an inline JS variable like ``var teamsData = JSON.parse('...')``."""
        pattern = rf"var\s+{var_name}\s*=\s*JSON\.parse\('(.+?)'\)"
        match = re.search(pattern, html)
        if not match:
            return None
        encoded = match.group(1)
        # Understat double-encodes: unicode escapes like \\x27
        decoded = encoded.encode("utf-8").decode("unicode_escape")
        try:
            return json.loads(decoded)
        except json.JSONDecodeError as exc:
            logger.warning("Failed to parse %s JSON: %s", var_name, exc)
            return None

    def _parse_teams(self, html: str) -> List[Dict]:
        data = self._extract_json_var(html, "teamsData")
        if not data:
            logger.warning("Understat: teamsData not found")
            return []

        teams = []
        for team_id, info in data.items():
            title = info.get("title", "Unknown")
            history = info.get("history", [])
            if not history:
                continue

            total_xg = sum(float(m.get("xG", 0)) for m in history)
            total_xga = sum(float(m.get("xGA", 0)) for m in history)
            total_goals = sum(int(m.get("scored", 0)) for m in history)
            total_conceded = sum(int(m.get("missed", 0)) for m in history)
            matches = len(history)

            teams.append({
                "record_type": "team",
                "team": title,
                "team_id": team_id,
                "matches_played": matches,
                "xg_total": round(total_xg, 2),
                "xga_total": round(total_xga, 2),
                "xg_per_match": round(total_xg / matches, 2) if matches else 0,
                "xga_per_match": round(total_xga / matches, 2) if matches else 0,
                "goals": total_goals,
                "conceded": total_conceded,
                "xg_diff": round(total_xg - total_xga, 2),
                "season": self.season,
                "league": self.league,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            })

        logger.info("Understat: parsed %d teams", len(teams))
        return teams

    def _parse_matches(self, html: str) -> List[Dict]:
        data = self._extract_json_var(html, "datesData")
        if not data:
            return []

        matches = []
        for match in data:
            try:
                matches.append({
                    "record_type": "match",
                    "match_id": match.get("id"),
                    "home_team": match.get("h", {}).get("title", ""),
                    "away_team": match.get("a", {}).get("title", ""),
                    "home_goals": match.get("goals", {}).get("h"),
                    "away_goals": match.get("goals", {}).get("a"),
                    "home_xg": match.get("xG", {}).get("h"),
                    "away_xg": match.get("xG", {}).get("a"),
                    "datetime": match.get("datetime", ""),
                    "is_result": match.get("isResult", False),
                    "season": self.season,
                    "league": self.league,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                })
            except Exception:
                continue

        logger.info("Understat: parsed %d matches", len(matches))
        return matches

    async def transform(self, records: List[Dict]) -> List[Dict]:
        return records

    async def store(self, records: List[Dict]) -> None:
        # Split by record_type for different tables
        team_records = [r for r in records if r.get("record_type") == "team"]
        match_records = [r for r in records if r.get("record_type") == "match"]

        for storage, st_cfg in self.storage_items:
            table = st_cfg.get("table", "raw_understat_teams")
            if team_records:
                await storage.store(team_records, {**st_cfg, "table": table})
            if match_records:
                match_table = table.replace("teams", "matches") if "teams" in table else f"{table}_matches"
                await storage.store(match_records, {**st_cfg, "table": match_table})
