"""
Parsers for raw API responses.

JsonParser correctly handles:
- Single response dict
- List of paginated response dicts (extracts from each and combines)
- Already-flat list of records
"""

from abc import ABC, abstractmethod
from typing import Any, List, Dict
import json
import csv
from io import StringIO
import logging

logger = logging.getLogger(__name__)


class BaseParser(ABC):
    @abstractmethod
    def parse(self, raw: Any) -> List[Dict]:
        ...


class JsonParser(BaseParser):
    def __init__(self, config: Dict):
        self.extract_path = config.get("extract")

    def parse(self, raw: Any) -> List[Dict]:
        if isinstance(raw, str):
            data = json.loads(raw)
        else:
            data = raw

        if self.extract_path:
            return self._extract(data)

        if isinstance(data, list):
            return data
        return [data]

    def _extract(self, data: Any) -> List[Dict]:
        """
        Apply JSONPath-like extraction.

        If *data* is a list (e.g. multiple paginated responses), we extract
        from **each** item and combine the results — this is the fix for the
        ``league_standings`` bug where pagination produced a list of full
        response dicts.
        """
        if isinstance(data, list):
            # Check if the list already contains flat records (no extraction needed)
            if data and not isinstance(data[0], dict):
                return data
            # If items look like full API responses, extract from each
            if data and isinstance(data[0], dict) and self._path_applicable(data[0]):
                combined = []
                for item in data:
                    combined.extend(self._extract_one(item))
                return combined
            # Already flat dicts — return as-is
            return data

        return self._extract_one(data)

    def _path_applicable(self, obj: dict) -> bool:
        """Check whether the extract_path root key exists in the object."""
        if not self.extract_path:
            return False
        first_key = self.extract_path.strip("$.").split(".")[0].rstrip("[*]")
        return first_key in obj

    def _extract_one(self, data: Any) -> List[Dict]:
        """Navigate an extract path like ``$.standings.results[*]``."""
        parts = self.extract_path.strip("$.").split(".")
        current = data
        for i, part in enumerate(parts):
            if part.endswith("[*]"):
                key = part[:-3]
                if isinstance(current, dict):
                    current = current.get(key, [])
                else:
                    return []
                if i == len(parts) - 1:
                    break
            else:
                if isinstance(current, dict):
                    current = current.get(part, {})
                else:
                    return []
        if isinstance(current, list):
            return current
        if current:
            return [current]
        return []


class HtmlParser(BaseParser):
    def __init__(self, config: Dict):
        self.selector = config.get("selector")
        self.attribute = config.get("attribute")

    def parse(self, raw: str) -> List[Dict]:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(raw, "html.parser")
        elements = soup.select(self.selector) if self.selector else []
        result = []
        for el in elements:
            if self.attribute:
                value = el.get(self.attribute)
            else:
                value = el.get_text(strip=True)
            result.append({"value": value})
        return result


class CsvParser(BaseParser):
    def __init__(self, config: Dict):
        self.delimiter = config.get("delimiter", ",")

    def parse(self, raw: str) -> List[Dict]:
        reader = csv.DictReader(StringIO(raw), delimiter=self.delimiter)
        return list(reader)
