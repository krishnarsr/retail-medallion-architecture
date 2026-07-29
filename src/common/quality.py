"""Reusable data-quality checks and reporting."""
from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class CheckResult:
    name: str
    passed: bool
    actual: int | float | str
    expectation: str


def not_null(df: DataFrame, column: str) -> CheckResult:
    failures = df.filter(F.col(column).isNull()).count()
    return CheckResult(f"{column}_not_null", failures == 0, failures, "0 null rows")


def unique(df: DataFrame, column: str) -> CheckResult:
    duplicates = (
        df.groupBy(column).count().filter(F.col("count") > 1).count()
    )
    return CheckResult(f"{column}_unique", duplicates == 0, duplicates, "0 duplicate keys")


def non_negative(df: DataFrame, column: str) -> CheckResult:
    failures = df.filter(F.col(column) < 0).count()
    return CheckResult(f"{column}_non_negative", failures == 0, failures, "0 negative rows")


def positive(df: DataFrame, column: str) -> CheckResult:
    failures = df.filter(F.col(column) <= 0).count()
    return CheckResult(f"{column}_positive", failures == 0, failures, "0 zero/negative rows")


def write_report(path: Path, stage: str, results: Iterable[CheckResult]) -> None:
    items = list(results)
    payload = {
        "stage": stage,
        "generated_at": datetime.now(UTC).isoformat(),
        "passed": all(item.passed for item in items),
        "checks": [asdict(item) for item in items],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    if not payload["passed"]:
        failures = [item.name for item in items if not item.passed]
        raise ValueError(f"Data quality failed at {stage}: {', '.join(failures)}")
