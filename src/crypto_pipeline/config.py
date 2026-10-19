"""Pipeline configuration.

Loads conf/<PIPELINE_ENV>.yml into a frozen dataclass. Environment variables override
connection settings (KAFKA_*, POSTGRES_*) so secrets never live in YAML. PIPELINE_CONF_DIR
overrides the conf directory location (needed on Databricks, where the repo is not at /app).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONF_DIR = Path(__file__).resolve().parents[2] / "conf"
LAYERS = ("bronze", "silver", "gold")


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str | None
    topic: str
    starting_offsets: str
    max_offsets_per_trigger: int


@dataclass(frozen=True)
class TriggerConfig:
    processing_time: str | None
    available_now: bool


@dataclass(frozen=True)
class PostgresConfig:
    jdbc_url: str | None
    user: str | None
    schema: str
    password: str | None = field(default=None, repr=False)


@dataclass(frozen=True)
class PipelineConfig:
    env: str
    platform: str
    use_tables: bool
    catalog: str | None
    lakehouse_root: str | None
    checkpoint_root: str
    contract_path: str
    kafka: KafkaConfig
    trigger: TriggerConfig
    postgres: PostgresConfig


def _env(name: str, default: Any) -> Any:
    """Return the env var `name` if set and non-empty, else `default`."""
    value = os.environ.get(name)
    return value if value else default


def load_config(env: str | None = None, conf_dir: str | Path | None = None) -> PipelineConfig:
    """Load the config for `env` (default: $PIPELINE_ENV or 'local'), applying env var overrides."""
    env = env or os.environ.get("PIPELINE_ENV", "local")
    conf_dir = Path(conf_dir or os.environ.get("PIPELINE_CONF_DIR") or DEFAULT_CONF_DIR)
    raw = yaml.safe_load((conf_dir / f"{env}.yml").read_text())

    contract_path = Path(_env("CONTRACT_PATH", raw["contract_path"]))
    if not contract_path.is_absolute():
        contract_path = conf_dir.parent / contract_path
    kafka, trigger, postgres, storage = raw["kafka"], raw["trigger"], raw["postgres"], raw["storage"]
    return PipelineConfig(
        env=env,
        platform=raw["platform"],
        use_tables=bool(raw["use_tables"]),
        catalog=raw.get("catalog"),
        lakehouse_root=storage.get("lakehouse_root"),
        checkpoint_root=storage["checkpoint_root"],
        contract_path=str(contract_path),
        kafka=KafkaConfig(
            bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", kafka.get("bootstrap_servers")),
            topic=_env("KAFKA_TOPIC", kafka["topic"]),
            starting_offsets=kafka["starting_offsets"],
            max_offsets_per_trigger=int(kafka["max_offsets_per_trigger"]),
        ),
        trigger=TriggerConfig(
            processing_time=trigger.get("processing_time"),
            available_now=bool(trigger.get("available_now", False)),
        ),
        postgres=PostgresConfig(
            jdbc_url=_env("POSTGRES_JDBC_URL", postgres.get("jdbc_url")),
            user=_env("POSTGRES_USER", postgres.get("user")),
            schema=postgres["schema"],
            password=_env("POSTGRES_PASSWORD", None),
        ),
    )
