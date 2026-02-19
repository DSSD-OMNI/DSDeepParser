"""File-based storage (CSV, JSONL, Parquet)."""

import os
import logging
from typing import List, Dict
from core.storage import BaseStorage

logger = logging.getLogger(__name__)


class FileStorage(BaseStorage):
    def __init__(self, base_path: str):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)

    async def store(self, records: List[Dict], source_config: Dict) -> None:
        import pandas as pd

        fmt = source_config.get("format", "jsonl")
        table = source_config.get("table", "data")
        path = source_config.get("path", os.path.join(self.base_path, f"{table}.{fmt}"))
        df = pd.DataFrame(records)

        if fmt == "csv":
            df.to_csv(path, index=False)
        elif fmt == "jsonl":
            df.to_json(path, orient="records", lines=True)
        elif fmt == "parquet":
            df.to_parquet(path, index=False)
        else:
            logger.warning("Unknown file format %s, using jsonl", fmt)
            df.to_json(path + ".jsonl", orient="records", lines=True)

        logger.info("Exported %d records to %s", len(records), path)
