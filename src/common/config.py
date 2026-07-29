"""Configuration and path resolution."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class Settings:
    root: Path
    data_root: Path
    landing: Path
    lakehouse: Path
    quality: Path
    config: dict

    def table_path(self, layer: str, table: str) -> str:
        return str(self.lakehouse / self.config["tables"][layer][table])


def load_settings() -> Settings:
    root = Path(__file__).resolve().parents[2]
    with (root / "config" / "pipeline.yml").open(encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    data_root = Path(os.getenv("DATA_ROOT", str(root / "data"))).resolve()
    return Settings(
        root=root,
        data_root=data_root,
        landing=data_root / "landing",
        lakehouse=data_root / "lakehouse",
        quality=data_root / "quality",
        config=config,
    )


def ensure_directories(settings: Settings) -> None:
    for path in (settings.landing, settings.lakehouse, settings.quality):
        path.mkdir(parents=True, exist_ok=True)
