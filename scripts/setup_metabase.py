#!/usr/bin/env python3
"""Dashboard-as-code: idempotently configure Metabase for the crypto pipeline.

Completes first-run setup if needed, registers the Postgres `crypto` database (gold schema only),
and creates/updates the "Crypto Market - Live" dashboard with native SQL questions. Safe to re-run:
everything is looked up by name and updated in place.

Reads .env (or the environment): METABASE_URL, MB_ADMIN_EMAIL, MB_ADMIN_PASSWORD, POSTGRES_*.
Stdlib only, so it runs on the host: python3 scripts/setup_metabase.py
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

COLLECTION = "Crypto Market"
DASHBOARD = "Crypto Market - Live"
DB_NAME = "Crypto"
REPORT_TIMEZONE = "Europe/London"
# gold `minute` is a UTC timestamp without time zone; Metabase would relabel it as report-timezone
# time instead of converting it, so cards select it as timestamptz and Metabase converts for display.
MINUTE = "minute AT TIME ZONE 'UTC' AS minute"
LOOKBACK = "(SELECT max(minute) FROM gold.ohlc_1m) - interval '3 hours'"


ENV_FILE = Path(__file__).resolve().parents[1] / ".env"


def load_env(path: Path = ENV_FILE) -> dict[str, str]:
    """Environment variables overlaid on the key=value pairs from .env."""
    values: dict[str, str] = {}
    if path.exists():
        for line in path.read_text().splitlines():
            if line.strip() and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip()
    overrides = {k: v for k, v in os.environ.items() if k in values or k.startswith(("MB_", "METABASE"))}
    return {**values, **overrides}


class Metabase:
    """Minimal Metabase REST client."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session: str | None = None

    def call(self, method: str, path: str, body: dict | None = None) -> dict | list:
        headers = {"Content-Type": "application/json"}
        if self.session:
            headers["X-Metabase-Session"] = self.session
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(f"{self.base_url}{path}", data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                payload = response.read()
        except urllib.error.HTTPError as err:
            sys.exit(f"{method} {path} -> HTTP {err.code}: {err.read().decode()[:500]}")
        return json.loads(payload) if payload else {}

    def wait_until_healthy(self, seconds: int = 120) -> None:
        deadline = time.time() + seconds
        while time.time() < deadline:
            try:
                if self.call("GET", "/api/health").get("status") == "ok":
                    return
            except (urllib.error.URLError, ConnectionError):
                pass
            time.sleep(3)
        sys.exit("Metabase did not become healthy")


def ensure_admin_and_login(mb: Metabase, env: dict[str, str]) -> None:
    """Run first-run setup if Metabase has no admin yet, then log in."""
    email, password = env["MB_ADMIN_EMAIL"], env["MB_ADMIN_PASSWORD"]
    token = mb.call("GET", "/api/session/properties").get("setup-token")
    if token:
        mb.call(
            "POST",
            "/api/setup",
            {
                "token": token,
                "user": {"first_name": "Admin", "last_name": "User", "email": email, "password": password},
                "prefs": {"site_name": "Crypto Market Data", "site_locale": "en", "allow_tracking": False},
            },
        )
        print("setup: created admin user")
    mb.session = mb.call("POST", "/api/session", {"username": email, "password": password})["id"]


def ensure_database(mb: Metabase, env: dict[str, str]) -> int:
    """Register the Postgres serving database (gold schema only) and wait for its first sync."""
    details = {
        "host": "postgres",
        "port": 5432,
        "dbname": env.get("POSTGRES_DB", "crypto"),
        "user": env.get("POSTGRES_USER", "crypto"),
        "password": env.get("POSTGRES_PASSWORD", "crypto"),
        "ssl": False,
        "schema-filters-type": "inclusion",
        "schema-filters-patterns": "gold",
    }
    body = {"engine": "postgres", "name": DB_NAME, "details": details, "is_full_sync": True}
    existing = next((d for d in mb.call("GET", "/api/database")["data"] if d["name"] == DB_NAME), None)
    if existing:
        db_id = existing["id"]
        mb.call("PUT", f"/api/database/{db_id}", body)
    else:
        db_id = mb.call("POST", "/api/database", body)["id"]
    mb.call("POST", f"/api/database/{db_id}/sync_schema")
    for _ in range(30):
        tables = mb.call("GET", f"/api/database/{db_id}/metadata").get("tables", [])
        if {"ohlc_1m", "volatility_1m"} <= {t["name"] for t in tables}:
            break
        time.sleep(2)
    else:
        print("warning: gold tables not synced yet; run serve first, then re-run this script")
    return db_id


def ensure_collection(mb: Metabase) -> int:
    existing = next((c for c in mb.call("GET", "/api/collection") if c["name"] == COLLECTION), None)
    return existing["id"] if existing else mb.call("POST", "/api/collection", {"name": COLLECTION})["id"]


def two_decimals(*columns: str) -> dict:
    """Metabase column_settings that format the given result columns with 2 decimals."""
    return {json.dumps(["name", c]): {"number_style": "decimal", "decimals": 2} for c in columns}


def question_specs() -> list[dict]:
    """The dashboard's questions: native SQL, display type, visualization settings and grid position."""
    # unpin_from_zero keeps the y-axis around the data; prices move by <1% so a zero baseline is flat.
    price_axis = {"graph.y_axis.auto_range": True, "graph.y_axis.unpin_from_zero": True}
    price_viz = {
        "graph.dimensions": ["minute"],
        "graph.metrics": ["close"],
        "column_settings": two_decimals("close"),
        **price_axis,
    }
    price_sql = (
        f"SELECT {MINUTE}, close FROM gold.ohlc_1m "
        "WHERE symbol = '{s}' AND minute >= " + LOOKBACK + " ORDER BY minute"
    )
    range_pct_sql = (
        f"SELECT {MINUTE}, symbol, ROUND(((high - low) / NULLIF(open, 0) * 100)::numeric, 4) AS range_pct "
        f"FROM gold.ohlc_1m WHERE minute >= {LOOKBACK} ORDER BY minute"
    )
    return [
        {
            "name": "BTCUSDT 1-min close price",
            "sql": price_sql.format(s="BTCUSDT"),
            "display": "line",
            "viz": price_viz,
            "pos": (0, 0, 12, 6),
        },
        {
            "name": "ETHUSDT 1-min close price",
            "sql": price_sql.format(s="ETHUSDT"),
            "display": "line",
            "viz": price_viz,
            "pos": (0, 12, 12, 6),
        },
        {
            "name": "Quote volume (USDT) per minute",
            "sql": (
                f"SELECT {MINUTE}, symbol, quote_volume FROM gold.ohlc_1m "
                f"WHERE minute >= {LOOKBACK} ORDER BY minute"
            ),
            "display": "bar",
            "viz": {
                "graph.dimensions": ["minute", "symbol"],
                "graph.metrics": ["quote_volume"],
                "stackable.stack_type": "stacked",
            },
            "pos": (6, 0, 12, 6),
        },
        {
            "name": "Price range % per minute",
            "sql": range_pct_sql,
            "display": "line",
            "viz": {
                "graph.dimensions": ["minute", "symbol"],
                "graph.metrics": ["range_pct"],
                "graph.y_axis.auto_range": True,
            },
            "pos": (6, 12, 12, 6),
        },
        {
            "name": "Latest 20 candles",
            "sql": (
                f"SELECT {MINUTE}, symbol, open, high, low, close, volume, quote_volume, trade_count, vwap "
                "FROM gold.ohlc_1m ORDER BY minute DESC, symbol LIMIT 20"
            ),
            "display": "table",
            "viz": {
                "column_settings": two_decimals("open", "high", "low", "close", "vwap", "quote_volume"),
            },
            "pos": (12, 0, 24, 8),
        },
    ]


def archive_stale_cards(mb: Metabase, existing: dict[str, int], specs: list[dict]) -> None:
    """Archive collection cards no longer in the specs (e.g. after a rename) so none are orphaned."""
    wanted = {spec["name"] for spec in specs}
    for name, card_id in existing.items():
        if name not in wanted:
            mb.call("PUT", f"/api/card/{card_id}", {"archived": True})
            print(f"archived stale card: {name}")


def set_report_timezone(mb: Metabase, timezone: str = REPORT_TIMEZONE) -> None:
    mb.call("PUT", "/api/setting/report-timezone", {"value": timezone})


def ensure_card(mb: Metabase, db_id: int, collection_id: int, spec: dict, existing: dict[str, int]) -> int:
    body = {
        "name": spec["name"],
        "type": "question",
        "display": spec["display"],
        "collection_id": collection_id,
        "dataset_query": {"type": "native", "database": db_id, "native": {"query": spec["sql"]}},
        "visualization_settings": spec["viz"],
    }
    if spec["name"] in existing:
        card_id = existing[spec["name"]]
        mb.call("PUT", f"/api/card/{card_id}", body)
        return card_id
    return mb.call("POST", "/api/card", body)["id"]


def ensure_dashboard(mb: Metabase, collection_id: int, card_ids: list[tuple[int, tuple]]) -> int:
    """Create or reuse the dashboard and replace its cards with the current layout."""
    items = mb.call("GET", f"/api/collection/{collection_id}/items?models=dashboard")["data"]
    found = next((i for i in items if i["name"] == DASHBOARD), None)
    if found:
        dash_id = found["id"]
    else:
        dash_id = mb.call("POST", "/api/dashboard", {"name": DASHBOARD, "collection_id": collection_id})["id"]
    dashcards = [
        {
            "id": -(index + 1),
            "card_id": card_id,
            "row": row,
            "col": col,
            "size_x": width,
            "size_y": height,
            "parameter_mappings": [],
            "visualization_settings": {},
        }
        for index, (card_id, (row, col, width, height)) in enumerate(card_ids)
    ]
    mb.call("PUT", f"/api/dashboard/{dash_id}", {"dashcards": dashcards})
    return dash_id


def main() -> None:
    env = load_env()
    missing = [k for k in ("MB_ADMIN_EMAIL", "MB_ADMIN_PASSWORD") if not env.get(k)]
    if missing:
        sys.exit(f"Set {', '.join(missing)} in .env")
    mb = Metabase(env.get("METABASE_URL", "http://localhost:3000"))
    mb.wait_until_healthy()
    ensure_admin_and_login(mb, env)
    set_report_timezone(mb)
    db_id = ensure_database(mb, env)
    collection_id = ensure_collection(mb)

    items = mb.call("GET", f"/api/collection/{collection_id}/items?models=card")["data"]
    existing = {item["name"]: item["id"] for item in items}
    specs = question_specs()
    placed = [(ensure_card(mb, db_id, collection_id, spec, existing), spec["pos"]) for spec in specs]
    archive_stale_cards(mb, existing, specs)
    dash_id = ensure_dashboard(mb, collection_id, placed)
    print(f"dashboard: {mb.base_url}/dashboard/{dash_id}#refresh=60")


if __name__ == "__main__":
    main()
