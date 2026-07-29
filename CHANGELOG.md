# Changelog

## 1.0.2 — Clean Python quality validation

- Updated UTC timestamps to the Python 3.11 `datetime.UTC` alias
- Moved `Iterable` to `collections.abc`
- Normalized PySpark and quality-helper import ordering
- Resolves all ten Ruff findings reported by the v1.0.1 validation

## 1.0.1 — Docker first-run correction

- Pinned Python base image to Debian Bookworm so OpenJDK 17 remains installable
- Added `.dockerignore` for a clean, reproducible build context
- Hardened PowerShell runner to stop after Docker/build/pipeline failures

## 1.0.0 — Complete portfolio baseline

- Local Docker/PySpark/Delta medallion architecture
- Retail CSV/JSON source simulator with controlled data defects
- Idempotent Bronze ingestion and lineage
- Silver validation, quarantine, SCD2 and conformed sales
- Gold star schema and analytical marts
- Quality gates, tests, CI, SQL examples and full documentation
