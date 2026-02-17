"""Global config access helper."""

import os
import yaml

_config = None


def get_config() -> dict:
    global _config
    if _config is None:
        path = os.path.join(os.path.dirname(__file__), "..", "config.yaml")
        with open(path, "r", encoding="utf-8") as f:
            _config = yaml.safe_load(f)
    return _config
