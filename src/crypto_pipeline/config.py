"""Pipeline configuration.

Loads conf/<PIPELINE_ENV>.yml into a frozen dataclass. Environment variables override
connection settings (KAFKA_*, POSTGRES_*, DATABASE_URL) so secrets never live in YAML. PIPELINE_CONF_DIR
overrides the conf directory location (needed on Databricks, where the repo is not at /app).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, unquote, urlparse

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


def postgres_from_url(url: str) -> tuple[str, str | None, str | None]:
    """Split a libpq URL (postgresql://user:pass@host[:port]/db?sslmode=...) into JDBC URL, user, password.

    Hosted Postgres providers (Neon, Supabase) hand out this form. Only `sslmode` is carried into
    the JDBC URL; other libpq-only parameters such as channel_binding are dropped.
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("postgres", "postgresql") or not parsed.hostname:
        raise ValueError("DATABASE_URL must look like postgresql://user:password@host/dbname")
    port = f":{parsed.port}" if parsed.port else ""
    sslmode = dict(parse_qsl(parsed.query)).get("sslmode")
    query = f"?sslmode={sslmode}" if sslmode else ""
    jdbc_url = f"jdbc:postgresql://{parsed.hostname}{port}{parsed.path}{query}"
    user = unquote(parsed.username) if parsed.username else None
    password = unquote(parsed.password) if parsed.password else None
    return jdbc_url, user, password


def load_config(env: str | None = None, conf_dir: str | Path | None = None) -> PipelineConfig:
    """Load the config for `env` (default: $PIPELINE_ENV or 'local'), applying env var overrides."""
    env = env or os.environ.get("PIPELINE_ENV", "local")
    conf_dir = Path(conf_dir or os.environ.get("PIPELINE_CONF_DIR") or DEFAULT_CONF_DIR)
    raw = yaml.safe_load((conf_dir / f"{env}.yml").read_text())

    contract_path = Path(_env("CONTRACT_PATH", raw["contract_path"]))
    if not contract_path.is_absolute():
        contract_path = conf_dir.parent / contract_path
    kafka, trigger, postgres, storage = raw["kafka"], raw["trigger"], raw["postgres"], raw["storage"]
    jdbc_url = _env("POSTGRES_JDBC_URL", postgres.get("jdbc_url"))
    pg_user = _env("POSTGRES_USER", postgres.get("user"))
    pg_password = _env("POSTGRES_PASSWORD", None)
    # A full connection string (e.g. a hosted Neon database) takes precedence over the POSTGRES_* parts.
    if database_url := _env("DATABASE_URL", None):
        jdbc_url, pg_user, pg_password = postgres_from_url(database_url)
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
            jdbc_url=jdbc_url,
            user=pg_user,
            schema=postgres["schema"],
            password=pg_password,
        ),
    )
