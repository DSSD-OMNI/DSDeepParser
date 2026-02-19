#!/usr/bin/env python3
"""
DSDeepParser Universal Edition — Production-ready data collection engine
for the DSSD (Deep Sports Data) analytics platform.

Collects FPL, Elo, Understat, FCI and calendar data, runs ETL pipelines,
and exposes Prometheus metrics.  Designed for 24/7 operation on Railway.
"""

import asyncio
import os
import logging
import signal
import os
import sys
from datetime import datetime

from core.scheduler import Scheduler
from rate_limiter import AdaptiveRateLimiter
from notifier import TelegramNotifier
from utils import load_config, setup_logging, resolve_env_vars
from sources.fpl import FPLSource
from sources.elo import EloSource
from sources.understat import UnderstatSource
from sources.fci import FCISource
from sources.calendar_source import CalendarSource
from ml.features import MLFeatureEngine
from ml.export import Exporter
import metrics

logger = logging.getLogger(__name__)

# ── source registry ──────────────────────────────────────────────────
SOURCE_REGISTRY: dict[str, type] = {
    "fpl": FPLSource,
    "elo": EloSource,
    "understat": UnderstatSource,
    "fci": FCISource,
    "calendar": CalendarSource,
}


def _detect_source_type(src_cfg: dict) -> str:
    """Detect source type from config (by explicit ``type`` or name prefix)."""
    if "type" in src_cfg:
        return src_cfg["type"]
    name = src_cfg.get("name", "")
    for prefix in SOURCE_REGISTRY:
        if name.startswith(prefix):
            return prefix
    return "fpl"  # default — backward-compat with existing config-driven sources


# ── main class ───────────────────────────────────────────────────────
class DSDeepParser:
    def __init__(self, config_path: str = "config.yaml"):
        self.config = load_config(config_path)
        resolve_env_vars(self.config)
        setup_logging(self.config["global"])
        self.running = True
        self.stop_event = asyncio.Event()

        self.rate_limiter = AdaptiveRateLimiter(self.config["rate_limiter"])
        pool_size = self.config["network"].get("session_pool_size", 10)
        self.session_pool: asyncio.Queue = asyncio.Queue(maxsize=pool_size)

        # ── notifier ─────────────────────────────────────────────────
        tg_cfg = self.config["notifications"]["telegram"]
        if tg_cfg["enabled"] and tg_cfg.get("token", "YOUR_TOKEN") != "YOUR_TOKEN":
            self.notifier = TelegramNotifier(tg_cfg["token"], tg_cfg["chat_id"])
        else:
            self.notifier = None

        # ── Prometheus ───────────────────────────────────────────────
        if self.config["metrics"]["enabled"]:
            metrics.start_metrics_server(self.config["metrics"]["port"])
            logger.info("Metrics server on port %s", self.config["metrics"]["port"])

        # ── build sources ────────────────────────────────────────────
        self.sources = []
        for src_cfg in self.config.get("sources", []):
            if not src_cfg.get("enabled", True):
                continue
            try:
                stype = _detect_source_type(src_cfg)
                cls = SOURCE_REGISTRY.get(stype, FPLSource)
                source = cls(src_cfg, self.config, self.rate_limiter, self.session_pool)
                self.sources.append(source)
                logger.info("Source registered: %s (type=%s)", source.name, stype)
            except Exception as exc:
                logger.error("Failed to init source %s: %s", src_cfg.get("name", "?"), exc, exc_info=True)

        # ── scheduler ────────────────────────────────────────────────
        self.scheduler = Scheduler()
        for src in self.sources:
            if src.schedule:
                self.scheduler.add_job(
                    self._run_source_safe(src),
                    trigger=src.schedule,
                    id=src.name,
                    replace_existing=True,
                )

        # ── ML / ETL engine ──────────────────────────────────────────
        if self.config["ml"]["enabled"]:
            from storage.sqlite_storage import SQLiteStorage
            db_path = self.config["default_storage"]["path"]
            self.ml_engine = MLFeatureEngine(SQLiteStorage(db_path), self.config)
        else:
            self.ml_engine = None

        self.exporter = Exporter(self.config["export"])
        self.stats = {"start_time": datetime.now(), "runs": 0, "errors": 0}

    # ── safe wrapper for scheduled runs ──────────────────────────────
    def _run_source_safe(self, source):
        async def _wrapper():
            try:
                metrics.active_tasks.inc()
                await source.run()
                metrics.tasks_completed.labels(source=source.name).inc()
                self.stats["runs"] += 1
            except Exception as exc:
                self.stats["errors"] += 1
                logger.error("Source %s failed: %s", source.name, exc, exc_info=True)
                metrics.errors_total.labels(source=source.name, type=type(exc).__name__).inc()
                if self.notifier:
                    await self.notifier.send_message(
                        f"⚠️ <b>{source.name}</b> error:\n<code>{exc}</code>"
                    )
            finally:
                metrics.active_tasks.dec()
        return _wrapper

    # ── main loop ────────────────────────────────────────────────────
    async def run_24_7(self):
        logger.info("🚀 Starting DSDeepParser Universal 24/7")
        if self.notifier:
            await self.notifier.send_message("🚀 <b>DSDeepParser</b> started.")

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_shutdown)
            except NotImplementedError:
                pass  # Windows

        self.scheduler.start()

        # Run each source once at startup
        startup = [asyncio.create_task(self._run_source_safe(s)()) for s in self.sources]
        await asyncio.gather(*startup, return_exceptions=True)

        # Background tasks
        bg = []
        if self.ml_engine:
            bg.append(asyncio.create_task(
                self.ml_engine.periodic_recalc(self.config["ml"].get("recalculate_interval_hours", 6))
            ))
        bg.append(asyncio.create_task(
            self.exporter.scheduled_export(self.config["export"].get("schedule_hour", 0))
        ))

        await self.stop_event.wait()

        for t in bg:
            t.cancel()
        await asyncio.gather(*bg, return_exceptions=True)
        await self.shutdown()

    def request_shutdown(self):
        logger.info("Shutdown signal received")
        self.stop_event.set()

    async def shutdown(self):
        logger.info("Graceful shutdown in progress…")
        if self.notifier:
            await self.notifier.send_message("🛑 <b>DSDeepParser</b> stopped.")
        await self.scheduler.shutdown()
        while not self.session_pool.empty():
            try:
                s = self.session_pool.get_nowait()
                await s.close()
            except Exception:
                pass
        logger.info("Parser stopped.")


async def main():
    parser = DSDeepParser()
    await parser.run_24_7()


if __name__ == "__main__":
    asyncio.run(main())

# Принудительно читаем порт из переменной окружения Railway
PORT = int(os.getenv('PORT', 8000))

