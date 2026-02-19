"""
Elo ratings source — scrapes http://clubelo.com for English Premier League teams.

Stores team name, Elo rating, and rank into ``raw_team_elo`` table.
Runs on a daily schedule (configurable).

Config example in config.yaml::

    - name: elo_epl
      type: elo
      enabled: true
      schedule: "0 6 * * *"
      storage:
        - table: raw_team_elo
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

# Map clubelo team names → FPL-compatible names
ELO_TO_FPL = {
    "Man City": "Man City",
    "Arsenal": "Arsenal",
    "Liverpool": "Liverpool",
    "Chelsea": "Chelsea",
    "Man United": "Man Utd",
    "Newcastle": "Newcastle",
    "Tottenham": "Spurs",
    "Brighton": "Brighton",
    "Aston Villa": "Aston Villa",
    "West Ham": "West Ham",
    "Bournemouth": "Bournemouth",
    "Crystal Palace": "Crystal Palace",
    "Wolves": "Wolves",
    "Fulham": "Fulham",
    "Everton": "Everton",
    "Brentford": "Brentford",
    "Nottingham": "Nott'm Forest",
    "Ipswich": "Ipswich",
    "Leicester": "Leicester",
    "Southampton": "Southampton",
}


class EloSource(DataSource):
    """Fetch Elo ratings from clubelo.com."""

    def __init__(self, config: dict, global_config: dict, rate_limiter=None, session_pool=None):
        super().__init__(config, global_config, rate_limiter, session_pool)
        self.url = config.get("fetcher", {}).get("url", "http://api.clubelo.com/")
        self.storage_items = self._build_storage(config, global_config)

    def _build_storage(self, config, global_config):
        items = []
        for st in config.get("storage", [{"table": "raw_team_elo", "type": "sqlite", "mode": "overwrite"}]):
            if st and st.get("type") == "sqlite":
                items.append((SQLiteStorage(global_config["default_storage"]["path"]), st))
        if not items:
            items.append((
                SQLiteStorage(global_config["default_storage"]["path"]),
                {"table": "raw_team_elo", "type": "sqlite", "mode": "overwrite"},
            ))
        return items

    async def fetch(self) -> Any:
        """Fetch Elo data via the clubelo.com CSV API."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        url = f"{self.url.rstrip('/')}/{today}"
        logger.info("Elo: fetching %s", url)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        return await resp.text()
                    logger.warning("Elo API returned %d", resp.status)
        except Exception as exc:
            logger.error("Elo fetch failed: %s", exc)
        # Fallback: try HTML scraping
        return await self._fetch_html()

    async def _fetch_html(self) -> Any:
        """Fallback: scrape the HTML table."""
        url = "http://clubelo.com/ENG"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        return ("html", await resp.text())
        except Exception as exc:
            logger.error("Elo HTML fetch failed: %s", exc)
        return None

    async def parse(self, raw_data: Any) -> List[Dict]:
        if raw_data is None:
            return []

        # CSV API response
        if isinstance(raw_data, str):
            return self._parse_csv(raw_data)

        # HTML fallback
        if isinstance(raw_data, tuple) and raw_data[0] == "html":
            return self._parse_html(raw_data[1])

        return []

    def _parse_csv(self, text: str) -> List[Dict]:
        """Parse the clubelo CSV format: Rank,Club,Country,Level,Elo,From,To"""
        teams = []
        lines = text.strip().split("\n")
        for line in lines:
            parts = line.split(",")
            if len(parts) < 5:
                continue
            club = parts[1].strip()
            country = parts[2].strip()
            if country != "ENG":
                continue
            try:
                elo = round(float(parts[4].strip()))
            except (ValueError, IndexError):
                continue
            fpl_name = ELO_TO_FPL.get(club, club)
            teams.append({
                "team": club,
                "team_fpl": fpl_name,
                "elo": elo,
                "rank": len(teams) + 1,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            })
        logger.info("Elo: parsed %d English teams from CSV", len(teams))
        return teams

    def _parse_html(self, html: str) -> List[Dict]:
        """Fallback HTML parsing."""
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            logger.error("beautifulsoup4 required for Elo HTML parsing")
            return []

        teams = []
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if not table:
            return []
        rows = table.find_all("tr")[1:21]
        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 4:
                rank = cols[0].get_text(strip=True)
                team = cols[1].get_text(strip=True)
                elo_text = cols[2].get_text(strip=True) if len(cols) > 2 else "0"
                try:
                    elo = int(float(elo_text))
                    rank_int = int(rank) if rank.isdigit() else len(teams) + 1
                except ValueError:
                    continue
                fpl_name = ELO_TO_FPL.get(team, team)
                teams.append({
                    "team": team,
                    "team_fpl": fpl_name,
                    "elo": elo,
                    "rank": rank_int,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                })
        logger.info("Elo: parsed %d teams from HTML", len(teams))
        return teams

    async def transform(self, records: List[Dict]) -> List[Dict]:
        # Re-rank by Elo descending
        records.sort(key=lambda r: r.get("elo", 0), reverse=True)
        for i, r in enumerate(records, 1):
            r["rank"] = i
        return records

    async def store(self, records: List[Dict]) -> None:
        for storage, st_cfg in self.storage_items:
            await storage.store(records, st_cfg)
