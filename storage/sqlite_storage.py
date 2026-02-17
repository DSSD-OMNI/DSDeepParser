"""
SQLite storage backend.

Fixes:
- No longer skips table creation when first record is empty — skips empty records instead.
- Auto-creates indexes on unique_columns.
- INSERT OR REPLACE with proper unique constraints.
- Thread-safe with connection-per-call pattern.
"""

import sqlite3
import logging
import os
from typing import List, Dict
from core.storage import BaseStorage

logger = logging.getLogger(__name__)


class SQLiteStorage(BaseStorage):
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    async def store(self, records: List[Dict], source_config: Dict) -> None:
        table = source_config.get("table")
        if not table:
            logger.error("No 'table' in storage config, skipping store")
            return
        if not records:
            logger.info("No records for table %s", table)
            return

        # Filter out empty records
        records = [r for r in records if r and isinstance(r, dict) and len(r) > 0]
        if not records:
            logger.warning("All records empty for table %s after filtering", table)
            return

        unique_columns = source_config.get("unique_columns", [])
        mode = source_config.get("mode", "insert")

        conn = self._connect()
        cursor = conn.cursor()

        try:
            # Ensure table exists
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            table_exists = cursor.fetchone() is not None

            # Collect all field names across all records
            all_fields: set[str] = set()
            for rec in records:
                all_fields.update(rec.keys())

            if not table_exists:
                columns = sorted(all_fields)
                col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
                cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')

                # Create unique index if specified
                if unique_columns:
                    idx_cols = ", ".join(f'"{c}"' for c in unique_columns)
                    idx_name = f"idx_{table}_{'_'.join(unique_columns)}"
                    cursor.execute(
                        f'CREATE UNIQUE INDEX IF NOT EXISTS "{idx_name}" ON "{table}" ({idx_cols})'
                    )

                conn.commit()
                existing_columns = set(columns)
            else:
                cursor.execute(f'PRAGMA table_info("{table}")')
                existing_columns = {row[1] for row in cursor.fetchall()}

                # Add missing columns
                new_cols = all_fields - existing_columns
                for col in sorted(new_cols):
                    try:
                        cursor.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" TEXT')
                        logger.info("Added column %s to %s", col, table)
                    except sqlite3.OperationalError:
                        pass
                existing_columns.update(new_cols)

                # Ensure unique index exists
                if unique_columns:
                    idx_cols = ", ".join(f'"{c}"' for c in unique_columns)
                    idx_name = f"idx_{table}_{'_'.join(unique_columns)}"
                    try:
                        cursor.execute(
                            f'CREATE UNIQUE INDEX IF NOT EXISTS "{idx_name}" ON "{table}" ({idx_cols})'
                        )
                    except sqlite3.OperationalError:
                        pass

            # Overwrite mode: delete all first
            if mode == "overwrite":
                cursor.execute(f'DELETE FROM "{table}"')

            # Insert records in batches
            columns = sorted(all_fields)
            col_names = ", ".join(f'"{c}"' for c in columns)
            placeholders = ", ".join(["?"] * len(columns))

            if unique_columns:
                sql = f'INSERT OR REPLACE INTO "{table}" ({col_names}) VALUES ({placeholders})'
            else:
                sql = f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders})'

            batch = [
                tuple(str(rec.get(c, "")) if rec.get(c) is not None else None for c in columns)
                for rec in records
            ]

            cursor.executemany(sql, batch)
            conn.commit()
            logger.info("Stored %d records in %s", len(batch), table)

        except Exception as exc:
            logger.error("SQLite store error for %s: %s", table, exc, exc_info=True)
            conn.rollback()
            raise
        finally:
            conn.close()

    async def query(self, sql: str, params: tuple = ()) -> List[Dict]:
        """Run a read query and return list of dicts."""
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def query_sync(self, sql: str, params: tuple = ()) -> List[Dict]:
        """Synchronous query for use in non-async contexts."""
        conn = self._connect()
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()
