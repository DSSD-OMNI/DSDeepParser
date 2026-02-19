"""Unit tests for DSDeepParser core modules."""

import pytest
import asyncio
import os
import tempfile
import json

# ── Parser Tests ─────────────────────────────────────────────────────

class TestJsonParser:
    def _make_parser(self, extract=None):
        from core.parser import JsonParser
        return JsonParser({"extract": extract})

    def test_parse_flat_dict(self):
        p = self._make_parser()
        result = p.parse({"id": 1, "name": "test"})
        assert result == [{"id": 1, "name": "test"}]

    def test_parse_flat_list(self):
        p = self._make_parser()
        result = p.parse([{"id": 1}, {"id": 2}])
        assert len(result) == 2

    def test_parse_with_extract_path(self):
        p = self._make_parser(extract="$.standings.results[*]")
        data = {
            "standings": {
                "has_next": False,
                "results": [{"entry": 1}, {"entry": 2}],
            }
        }
        result = p.parse(data)
        assert len(result) == 2
        assert result[0]["entry"] == 1

    def test_parse_paginated_list(self):
        """The key bug fix: list of paginated response dicts."""
        p = self._make_parser(extract="$.standings.results[*]")
        data = [
            {"standings": {"has_next": True, "results": [{"entry": 1}, {"entry": 2}]}},
            {"standings": {"has_next": False, "results": [{"entry": 3}]}},
        ]
        result = p.parse(data)
        assert len(result) == 3
        assert result[2]["entry"] == 3

    def test_parse_json_string(self):
        p = self._make_parser()
        result = p.parse('{"key": "value"}')
        assert result == [{"key": "value"}]

    def test_parse_empty_extract(self):
        p = self._make_parser(extract="$.nonexistent[*]")
        result = p.parse({"other": "data"})
        assert result == []


# ── URL Template Tests ───────────────────────────────────────────────

class TestUrlTemplate:
    def test_render_url(self):
        from core.fetcher import HttpFetcher
        url, remaining = HttpFetcher._render_url(
            "https://api.com/{league_id}/standings/",
            {"league_id": 314, "page": 1},
        )
        assert url == "https://api.com/314/standings/"
        assert remaining == {"page": 1}
        assert "league_id" not in remaining

    def test_render_url_no_placeholders(self):
        from core.fetcher import HttpFetcher
        url, remaining = HttpFetcher._render_url(
            "https://api.com/data/",
            {"page": 1},
        )
        assert url == "https://api.com/data/"
        assert remaining == {"page": 1}

    def test_expand_params(self):
        from core.fetcher import HttpFetcher
        result = HttpFetcher._expand_params({"league_id": [1, 2], "other": "x"})
        assert len(result) == 2
        assert result[0]["league_id"] == 1
        assert result[1]["league_id"] == 2


# ── Transformer Tests ────────────────────────────────────────────────

class TestTransformer:
    def _make_transformer(self, steps):
        from core.transformer import Transformer
        return Transformer(steps, custom_functions_path="/nonexistent")

    def test_rename(self):
        t = self._make_transformer([
            {"operation": "rename", "mapping": {"old": "new"}}
        ])
        result = t.transform([{"old": "value", "keep": "x"}])
        assert result[0]["new"] == "value"
        assert result[0]["keep"] == "x"
        assert "old" not in result[0]

    def test_flatten(self):
        t = self._make_transformer([{"operation": "flatten"}])
        result = t.transform([{"a": {"b": 1, "c": 2}, "d": 3}])
        assert result[0] == {"a_b": 1, "a_c": 2, "d": 3}

    def test_filter(self):
        t = self._make_transformer([{"operation": "filter", "condition": "score > 50"}])
        result = t.transform([{"score": 60}, {"score": 40}, {"score": 70}])
        assert len(result) == 2

    def test_compute(self):
        t = self._make_transformer([{"operation": "compute", "expression": "total = a + b"}])
        result = t.transform([{"a": 10, "b": 20}])
        assert result[0]["total"] == 30

    def test_add_field(self):
        t = self._make_transformer([{"operation": "add_field", "field": "source", "value": "test"}])
        result = t.transform([{"id": 1}])
        assert result[0]["source"] == "test"


# ── SQLiteStorage Tests ──────────────────────────────────────────────

class TestSQLiteStorage:
    @pytest.fixture
    def db_path(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = f.name
        yield path
        os.unlink(path)

    @pytest.mark.asyncio
    async def test_store_and_query(self, db_path):
        from storage.sqlite_storage import SQLiteStorage
        s = SQLiteStorage(db_path)
        records = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
        await s.store(records, {"table": "users"})

        rows = await s.query("SELECT * FROM users")
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_store_skips_empty_records(self, db_path):
        from storage.sqlite_storage import SQLiteStorage
        s = SQLiteStorage(db_path)
        records = [{}, {"id": "1"}, {}, {"id": "2"}]
        await s.store(records, {"table": "data"})

        rows = await s.query("SELECT * FROM data")
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_store_overwrite_mode(self, db_path):
        from storage.sqlite_storage import SQLiteStorage
        s = SQLiteStorage(db_path)
        await s.store([{"val": "a"}], {"table": "t"})
        await s.store([{"val": "b"}], {"table": "t", "mode": "overwrite"})

        rows = await s.query("SELECT * FROM t")
        assert len(rows) == 1
        assert rows[0]["val"] == "b"

    @pytest.mark.asyncio
    async def test_upsert_with_unique_columns(self, db_path):
        from storage.sqlite_storage import SQLiteStorage
        s = SQLiteStorage(db_path)
        cfg = {"table": "standings", "unique_columns": ["manager_id"]}
        await s.store([{"manager_id": "1", "points": "50"}], cfg)
        await s.store([{"manager_id": "1", "points": "60"}], cfg)

        rows = await s.query("SELECT * FROM standings")
        assert len(rows) == 1
        assert rows[0]["points"] == "60"


# ── Rate Limiter Tests ───────────────────────────────────────────────

class TestRateLimiter:
    def test_backoff_on_errors(self):
        from rate_limiter import AdaptiveRateLimiter
        rl = AdaptiveRateLimiter({"base_delay": 1.0, "backoff_factor": 2.0, "max_delay": 10.0})
        initial = rl.current_delay
        rl.update_delay(success=False)
        assert rl.current_delay > initial

    def test_speedup_on_success(self):
        from rate_limiter import AdaptiveRateLimiter
        cfg = {"base_delay": 5.0, "min_delay": 0.1, "success_window": 5}
        rl = AdaptiveRateLimiter(cfg)
        rl.current_delay = 5.0
        for _ in range(10):
            rl.update_delay(success=True)
        assert rl.current_delay < 5.0


# ── Config Tests ─────────────────────────────────────────────────────

class TestConfig:
    def test_load_config(self):
        from utils import load_config
        config_path = os.path.join(os.path.dirname(__file__), "..", "config.yaml")
        if os.path.exists(config_path):
            cfg = load_config(config_path)
            assert "sources" in cfg
            assert "global" in cfg

    def test_resolve_env_vars(self):
        from utils import resolve_env_vars
        os.environ["TEST_VAR"] = "hello"
        cfg = {"key": "${TEST_VAR}", "default": "${MISSING:fallback}"}
        resolve_env_vars(cfg)
        assert cfg["key"] == "hello"
        assert cfg["default"] == "fallback"
        del os.environ["TEST_VAR"]
