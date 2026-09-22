"""Load and validate YAML configuration."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


def _load_yaml(name: str) -> dict:
    with open(CONFIG_DIR / name, "r") as f:
        return yaml.safe_load(f)


@dataclass(frozen=True)
class Settings:
    raw: dict
    quality: dict
    bronze_schema: dict

    @property
    def env(self) -> str:
        # env var wins over the yaml default
        return os.getenv("ADMETRICS_ENV", self.raw.get("env", "local"))

    def table(self, layer: str, name: str) -> str:
        """Fully-qualified table name on Databricks, or a path locally."""
        if self.env == "databricks":
            schema = self.raw["schemas"][layer]
            return f"{self.raw['catalog']}.{schema}.{name}"
        return str(Path(self.raw["paths"][layer]) / name)


def load_settings() -> Settings:
    return Settings(
        raw=_load_yaml("settings.yaml"),
        quality=_load_yaml("quality_rules.yaml"),
        bronze_schema=_load_yaml("schema_bronze.yaml"),
    )