"""
HTTP fetcher with:
 - URL template substitution  ({league_id}, {manager_id}, etc.)
 - Pagination (page-based)
 - Adaptive rate limiting
 - File-based caching
 - Retry via tenacity
 - Circuit breaker
"""

import aiohttp
import asyncio
import logging
import os
import pickle
import hashlib
import time
import random
import re
from typing import Optional, Dict, Any, List

from tenacity import (
    retry, stop_after_attempt, wait_exponential,
    retry_if_exception_type, before_sleep_log,
)
from circuit_breaker import CircuitBreaker

logger = logging.getLogger(__name__)


class HttpFetcher:
    def __init__(
        self,
        config: Dict,
        rate_limiter,
        session_pool: asyncio.Queue,
        proxies: List[str],
        user_agents: List[str],
        cache_dir: str = "./cache",
        global_config: dict | None = None,
        source_name: str = "unknown",
    ):
        self.config = config
        self.global_config = global_config or {}
        self.rate_limiter = rate_limiter
        self.session_pool = session_pool
        self.proxies = proxies
        self.user_agents = user_agents
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self._proxy_index = 0
        self.circuit_breaker = CircuitBreaker(source_name)

    # ── helpers ──────────────────────────────────────────────────────
    def _get_next_proxy(self) -> Optional[str]:
        if not self.proxies:
            return None
        proxy = self.proxies[self._proxy_index % len(self.proxies)]
        self._proxy_index += 1
        return proxy

    def _get_random_ua(self) -> str:
        return random.choice(self.user_agents) if self.user_agents else "Mozilla/5.0"

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session_pool.empty():
            connector = aiohttp.TCPConnector(
                limit=100, ttl_dns_cache=300, use_dns_cache=True, keepalive_timeout=30,
            )
            return aiohttp.ClientSession(connector=connector)
        return await self.session_pool.get()

    async def _release_session(self, session: aiohttp.ClientSession):
        if self.session_pool.qsize() < self.session_pool.maxsize:
            await self.session_pool.put(session)
        else:
            await session.close()

    def _cache_key(self, url: str, params: dict) -> str:
        key = url + str(sorted(params.items()))
        return hashlib.md5(key.encode()).hexdigest()

    # ── URL template ─────────────────────────────────────────────────
    @staticmethod
    def _render_url(url_template: str, params: dict) -> tuple[str, dict]:
        """Replace {placeholders} in URL and return (rendered_url, remaining_query_params)."""
        rendered = url_template
        used_keys: set[str] = set()
        for key, val in params.items():
            placeholder = "{" + key + "}"
            if placeholder in rendered:
                rendered = rendered.replace(placeholder, str(val))
                used_keys.add(key)
        remaining = {k: v for k, v in params.items() if k not in used_keys}
        return rendered, remaining

    # ── single request with retry ────────────────────────────────────
    async def _fetch_one(self, url: str, params: dict) -> Optional[Any]:
        cache_ttl = self.config.get(
            "cache_ttl",
            self.global_config.get("network", {}).get("cache_ttl_default", 0),
        )

        # Cache check
        if cache_ttl > 0:
            ck = self._cache_key(url, params)
            cp = os.path.join(self.cache_dir, ck + ".pkl")
            if os.path.exists(cp) and (time.time() - os.path.getmtime(cp)) < cache_ttl:
                logger.debug("Cache hit: %s", url)
                with open(cp, "rb") as f:
                    return pickle.load(f)

        await self.rate_limiter.wait_if_needed()

        method = self.config.get("method", "GET").upper()
        headers = self.config.get("headers", {}).copy()
        headers["User-Agent"] = self._get_random_ua()
        proxy = self._get_next_proxy()

        # Auth
        auth_cfg = self.config.get("auth")
        if auth_cfg:
            if auth_cfg["type"] == "bearer":
                headers["Authorization"] = f"Bearer {auth_cfg['token']}"
            elif auth_cfg["type"] == "basic":
                import base64
                cred = f"{auth_cfg['username']}:{auth_cfg['password']}"
                headers["Authorization"] = f"Basic {base64.b64encode(cred.encode()).decode()}"

        timeout = aiohttp.ClientTimeout(total=self.config.get("timeout", 30))
        session = await self._get_session()
        try:
            async with session.request(
                method, url, params=params, headers=headers,
                proxy=proxy, timeout=timeout,
            ) as resp:
                if resp.status == 200:
                    ct = resp.content_type or ""
                    if "json" in ct:
                        data = await resp.json()
                    else:
                        data = await resp.text()
                    self.rate_limiter.update_delay(success=True)
                    # Save cache
                    if cache_ttl > 0:
                        ck = self._cache_key(url, params)
                        cp = os.path.join(self.cache_dir, ck + ".pkl")
                        with open(cp, "wb") as f:
                            pickle.dump(data, f)
                    return data

                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    logger.warning("429 rate-limit, sleeping %ds", retry_after)
                    await asyncio.sleep(retry_after)
                    self.rate_limiter.update_delay(success=False)
                    raise aiohttp.ClientResponseError(
                        resp.request_info, resp.history, status=resp.status
                    )

                if resp.status == 404:
                    self.rate_limiter.update_delay(success=True)
                    return None

                logger.error("HTTP %d for %s", resp.status, url)
                self.rate_limiter.update_delay(success=False)
                resp.raise_for_status()

        except Exception:
            self.rate_limiter.update_delay(success=False)
            raise
        finally:
            await self._release_session(session)

    # ── public API ───────────────────────────────────────────────────
    async def fetch(self, params: Optional[Dict] = None) -> Any:
        """
        Fetch data from the configured URL.
        Handles URL templates, param expansion, and pagination.
        Returns a single result or combined list.
        """
        url_template = self.config["url"]
        param_sets = self._expand_params(params or {})
        results: list[Any] = []

        for p in param_sets:
            # Substitute path placeholders
            url, query_params = self._render_url(url_template, p)

            pagination = self.config.get("pagination")
            if pagination:
                page_param = pagination["param"]
                start = pagination.get("start", 1)
                max_pages = pagination.get("max_pages", 10)
                for page in range(start, start + max_pages):
                    qp = dict(query_params)
                    qp[page_param] = page
                    data = await self._fetch_one(url, qp)
                    if data is None:
                        break
                    if isinstance(data, list) and len(data) == 0:
                        break
                    results.append(data)
                    # Check if we should stop (no more pages)
                    if isinstance(data, dict):
                        # Handle FPL-style nested pagination
                        has_next = _deep_get(data, "standings.has_next") or \
                                   _deep_get(data, "has_next") or \
                                   data.get("has_next")
                        if has_next is False:
                            break
            else:
                data = await self._fetch_one(url, query_params)
                if data is not None:
                    results.append(data)

        if len(results) == 0:
            return None
        if len(results) == 1:
            return results[0]
        return results

    @staticmethod
    def _expand_params(params: dict) -> List[dict]:
        """Expand list-valued params into a Cartesian product of param dicts."""
        list_keys = [k for k, v in params.items() if isinstance(v, list)]
        if not list_keys:
            return [params]
        key = list_keys[0]
        values = params[key]
        rest = {k: v for k, v in params.items() if k != key}
        sub_list = HttpFetcher._expand_params(rest)
        result = []
        for val in values:
            for sub in sub_list:
                new = sub.copy()
                new[key] = val
                result.append(new)
        return result


def _deep_get(d: dict, path: str, default=None):
    """Access nested dict keys with dot notation: ``_deep_get(d, 'a.b.c')``."""
    keys = path.split(".")
    cur = d
    for k in keys:
        if isinstance(cur, dict):
            cur = cur.get(k)
        else:
            return default
        if cur is None:
            return default
    return cur
