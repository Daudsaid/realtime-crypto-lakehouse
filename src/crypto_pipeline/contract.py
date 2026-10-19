"""Data contract loader and rule compiler.

Reads contracts/crypto_trades.yaml and compiles its `quality` rules into Spark column
expressions. Each check is a (reason, violation) pair; `reason_column` returns the reason of the
first violated check in contract order, or NULL when the row is valid. Checks are null-safe:
a NULL operand never counts as a violation of a value rule (the required-field rule catches it).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import Column
from pyspark.sql import functions as F

from crypto_pipeline.schemas import CORRUPT_FIELD, PARSED_COL

Check = tuple[str, Column]


@dataclass(frozen=True)
class Contract:
    """The parts of a data contract the pipeline enforces."""

    id: str
    version: str
    required_fields: tuple[str, ...]
    rules: tuple[dict[str, Any], ...]


def load_contract(path: str | Path) -> Contract:
    """Parse a contract YAML file."""
    raw = yaml.safe_load(Path(path).read_text())
    properties = [prop for table in raw["schema"] for prop in table["properties"]]
    return Contract(
        id=raw["id"],
        version=str(raw["version"]),
        required_fields=tuple(p["name"] for p in properties if p.get("required")),
        rules=tuple(raw["quality"]),
    )


def _valid_json(rule: dict[str, Any], contract: Contract) -> list[Check]:
    parsed = F.col(PARSED_COL)
    return [(rule["name"], parsed.isNull() | parsed[CORRUPT_FIELD].isNotNull())]


def _required_fields(rule: dict[str, Any], contract: Contract) -> list[Check]:
    return [(f"{rule['name']}:{col}", F.col(col).isNull()) for col in contract.required_fields]


def _positive(rule: dict[str, Any], contract: Contract) -> list[Check]:
    return [(rule["name"], F.col(rule["column"]) <= 0)]


def _allowed_values(rule: dict[str, Any], contract: Contract) -> list[Check]:
    col = F.col(rule["column"])
    return [(rule["name"], col.isNotNull() & ~col.isin(*rule["values"]))]


def _max_future_minutes(rule: dict[str, Any], contract: Contract) -> list[Check]:
    limit = F.current_timestamp() + F.expr(f"INTERVAL {int(rule['minutes'])} MINUTES")
    return [(rule["name"], F.col(rule["column"]) > limit)]


RULES: dict[str, Callable[[dict[str, Any], Contract], list[Check]]] = {
    "valid_json": _valid_json,
    "required_fields": _required_fields,
    "positive": _positive,
    "allowed_values": _allowed_values,
    "max_future_minutes": _max_future_minutes,
}


def build_checks(contract: Contract) -> list[Check]:
    """Compile the contract's quality rules, in contract order."""
    checks: list[Check] = []
    for rule in contract.rules:
        checks.extend(RULES[rule["rule"]](rule, contract))
    return checks


def reason_column(contract: Contract) -> Column:
    """Reason of the first violated check, or NULL if the row satisfies the contract."""
    return F.coalesce(*[F.when(violation, F.lit(reason)) for reason, violation in build_checks(contract)])
