"""Unit tests for config loading and DATABASE_URL parsing."""

from __future__ import annotations

from pathlib import Path

import pytest

from crypto_pipeline.config import load_config, postgres_from_url

CONF_DIR = Path(__file__).resolve().parents[1] / "conf"


def test_postgres_from_url_parses_neon_style_url() -> None:
    url = "postgresql://neondb_owner:s3cr%40t@ep-cool-1234.eu-west-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

    jdbc_url, user, password = postgres_from_url(url)

    assert jdbc_url == "jdbc:postgresql://ep-cool-1234.eu-west-2.aws.neon.tech/neondb?sslmode=require"
    assert user == "neondb_owner"
    assert password == "s3cr@t"


def test_postgres_from_url_keeps_explicit_port_and_omits_empty_query() -> None:
    jdbc_url, _, _ = postgres_from_url("postgres://crypto:crypto@postgres:5432/crypto")

    assert jdbc_url == "jdbc:postgresql://postgres:5432/crypto"


def test_postgres_from_url_rejects_other_schemes() -> None:
    with pytest.raises(ValueError):
        postgres_from_url("mysql://user:pass@host/db")


def test_database_url_overrides_postgres_settings(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_USER", "crypto")
    monkeypatch.setenv("POSTGRES_PASSWORD", "crypto")
    monkeypatch.setenv("DATABASE_URL", "postgresql://neon:pw@db.neon.tech/neondb?sslmode=require")

    cfg = load_config("local", CONF_DIR)

    assert cfg.postgres.jdbc_url == "jdbc:postgresql://db.neon.tech/neondb?sslmode=require"
    assert (cfg.postgres.user, cfg.postgres.password) == ("neon", "pw")


def test_empty_database_url_keeps_local_postgres(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "")
    monkeypatch.delenv("POSTGRES_JDBC_URL", raising=False)

    cfg = load_config("local", CONF_DIR)

    assert cfg.postgres.jdbc_url == "jdbc:postgresql://postgres:5432/crypto"
