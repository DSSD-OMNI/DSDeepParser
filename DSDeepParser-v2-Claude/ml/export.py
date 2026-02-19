"""Scheduled data exporter — produces CSV / JSONL / Parquet snapshots."""

import logging
import asyncio
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class Exporter:
    def __init__(self, config: dict):
        self.config = config
        self.csv_dir = Path(config.get("csv_dir", "exports"))
        self.csv_dir.mkdir(exist_ok=True)

    async def export_all(self) -> None:
        """Export features + LRI to configured formats."""
        logger.info("Running scheduled export…")
        try:
            import sqlite3
            import pandas as pd

            db_path = self.config.get("db_path", "/app/data/fpl_data.db")
            if not Path(db_path).exists():
                logger.warning("DB not found at %s, skipping export", db_path)
                return

            conn = sqlite3.connect(db_path)

            for table in ("features", "lri_scores", "league_standings"):
                try:
                    df = pd.read_sql_query(f'SELECT * FROM "{table}"', conn)
                    if df.empty:
                        continue
                    ts = datetime.now().strftime("%Y%m%d")
                    csv_path = self.csv_dir / f"{table}_{ts}.csv"
                    df.to_csv(csv_path, index=False)
                    logger.info("Exported %s → %s (%d rows)", table, csv_path, len(df))
                except Exception as exc:
                    logger.debug("Export of %s skipped: %s", table, exc)

            conn.close()
        except ImportError:
            logger.warning("pandas not available, skipping export")
        except Exception as exc:
            logger.error("Export failed: %s", exc)

    async def scheduled_export(self, hour: int) -> None:
        while True:
            now = datetime.now()
            target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
            if target <= now:
                from datetime import timedelta
                target += timedelta(days=1)
            wait = (target - now).total_seconds()
            await asyncio.sleep(wait)
            await self.export_all()
