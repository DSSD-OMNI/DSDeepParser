"""Record transformations: rename, compute, filter, flatten, template, custom."""

import logging
import re
import os
import importlib.util
from typing import List, Dict
from jinja2 import Template

logger = logging.getLogger(__name__)


class Transformer:
    def __init__(self, steps: List[Dict], custom_functions_path: str = "transformations.py"):
        self.steps = steps or []
        self.custom_functions = self._load_custom_functions(custom_functions_path)

    def _load_custom_functions(self, path: str) -> dict:
        if not os.path.exists(path):
            return {}
        try:
            spec = importlib.util.spec_from_file_location("custom_transforms", path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return {
                name: getattr(module, name)
                for name in dir(module)
                if callable(getattr(module, name)) and not name.startswith("_")
            }
        except Exception as exc:
            logger.warning("Failed to load custom transforms from %s: %s", path, exc)
            return {}

    def transform(self, records: List[Dict]) -> List[Dict]:
        for step in self.steps:
            op = step.get("operation")
            try:
                if op == "rename":
                    records = [self._rename(r, step["mapping"]) for r in records]
                elif op == "compute":
                    records = [self._compute(r, step["expression"]) for r in records]
                elif op == "filter":
                    records = [r for r in records if self._evaluate(r, step["condition"])]
                elif op == "flatten":
                    records = [self._flatten(r) for r in records]
                elif op == "template":
                    records = [self._apply_template(r, step["template"], step["target_field"]) for r in records]
                elif op == "custom":
                    fn = step["function"]
                    if fn in self.custom_functions:
                        records = [self.custom_functions[fn](r) for r in records]
                    else:
                        logger.warning("Custom function %s not found", fn)
                elif op == "add_field":
                    # Add a static or computed field
                    key, val = step["field"], step["value"]
                    records = [{**r, key: val} for r in records]
                else:
                    logger.warning("Unknown transform operation: %s", op)
            except Exception as exc:
                logger.error("Transform step %s failed: %s", op, exc)
        return records

    @staticmethod
    def _rename(record: Dict, mapping: Dict) -> Dict:
        return {mapping.get(k, k): v for k, v in record.items()}

    @staticmethod
    def _compute(record: Dict, expr: str) -> Dict:
        m = re.match(r"(\w+)\s*=\s*(.+)", expr)
        if m:
            field, formula = m.groups()
            ctx = record.copy()
            ctx.update({"len": len, "sum": sum, "min": min, "max": max, "abs": abs, "round": round})
            try:
                record[field] = eval(formula, {"__builtins__": {}}, ctx)
            except Exception as exc:
                logger.debug("Compute '%s' skipped for record: %s", formula, exc)
        return record

    @staticmethod
    def _evaluate(record: Dict, condition: str) -> bool:
        try:
            return bool(eval(condition, {"__builtins__": {}}, record))
        except Exception:
            return False

    @staticmethod
    def _flatten(record: Dict, parent_key: str = "", sep: str = "_") -> Dict:
        items = []
        for k, v in record.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(Transformer._flatten(v, new_key, sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    @staticmethod
    def _apply_template(record: Dict, template_str: str, target: str) -> Dict:
        record[target] = Template(template_str).render(**record)
        return record
