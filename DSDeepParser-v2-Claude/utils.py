"""Utility helpers: config loading, logging setup, env var resolution."""

import yaml
import logging
import os
import re
import sys
from logging.handlers import TimedRotatingFileHandler


def load_config(path: str) -> dict:
    """Load YAML config with basic validation."""
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Ensure required top-level keys exist with defaults
    defaults = {
        "global": {"log_level": "INFO", "log_rotation": "midnight", "log_backup_count": 7, "timezone": "UTC"},
        "network": {"session_pool_size": 10, "proxies": [], "user_agents": [], "cache_dir": "./cache", "cache_ttl_default": 300},
        "rate_limiter": {"base_delay": 0.5, "min_delay": 0.2, "max_delay": 10.0, "backoff_factor": 2.0, "success_window": 50, "error_threshold": 0.1},
        "notifications": {"telegram": {"enabled": False, "token": "YOUR_TOKEN", "chat_id": "YOUR_CHAT_ID"}},
        "metrics": {"enabled": True, "port": 8000},
        "default_storage": {"type": "sqlite", "path": "/app/data/fpl_data.db"},
        "export": {"csv_dir": "exports", "schedule_hour": 0},
        "ml": {"enabled": False, "recalculate_interval_hours": 6},
        "sources": [],
    }

    for key, default in defaults.items():
        if key not in config:
            config[key] = default
        elif isinstance(default, dict):
            for k, v in default.items():
                config[key].setdefault(k, v)

    return config


def resolve_env_vars(config: dict) -> None:
    """
    Recursively replace ``${ENV_VAR}`` or ``${ENV_VAR:default}`` patterns
    with environment variable values.
    """
    pattern = re.compile(r"\$\{(\w+)(?::([^}]*))?\}")

    def _resolve(obj):
        if isinstance(obj, str):
            def _replacer(m):
                var, default = m.group(1), m.group(2)
                return os.environ.get(var, default if default is not None else m.group(0))
            return pattern.sub(_replacer, obj)
        elif isinstance(obj, dict):
            return {k: _resolve(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [_resolve(i) for i in obj]
        return obj

    for key in list(config.keys()):
        config[key] = _resolve(config[key])


def setup_logging(config: dict) -> None:
    """Configure root logger with console + rotating file handler."""
    level = getattr(logging, config.get("log_level", "INFO").upper(), logging.INFO)
    fmt = "%(asctime)s | %(name)-25s | %(levelname)-7s | %(message)s"
    formatter = logging.Formatter(fmt)

    root = logging.getLogger()
    root.setLevel(level)

    # Avoid duplicate handlers on reload
    if root.handlers:
        return

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    root.addHandler(console)

    rotation = config.get("log_rotation", "midnight")
    backup = config.get("log_backup_count", 7)
    try:
        fh = TimedRotatingFileHandler("parser.log", when=rotation, backupCount=backup)
        fh.setFormatter(formatter)
        root.addHandler(fh)
    except (PermissionError, OSError):
        pass  # Railway: filesystem may be read-only outside /app/data

    # Quiet noisy loggers
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
