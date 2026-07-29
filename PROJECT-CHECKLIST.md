# Project acceptance checklist

## Repository

- [x] Clear business scenario and architecture
- [x] Pinned Python dependencies
- [x] Dockerfile and Docker Compose
- [x] Windows PowerShell runner
- [x] Environment template and centralized YAML configuration
- [x] Git ignore, licence, CI workflow and tests

## Data engineering

- [x] Multiple source formats: CSV and newline-delimited JSON
- [x] Date-partitioned source deliveries
- [x] Explicit ingestion schemas
- [x] Bronze lineage and deterministic record identity
- [x] Idempotent Delta merge
- [x] Silver normalization, deduplication and foreign-key checks
- [x] Customer SCD Type 2
- [x] Quarantine with original JSON payload and reason
- [x] Order financial derivations
- [x] Gold dimensions and fact
- [x] Daily, customer and product marts
- [x] Structured logs and JSON data-quality evidence

## Run before publishing

```powershell
Copy-Item .env.example .env
docker compose build
docker compose run --rm pipeline run-all --batch-date 2026-01-15
docker compose run --rm pipeline run-all --batch-date 2026-01-15
docker compose run --rm --entrypoint pytest pipeline -q
docker compose run --rm --entrypoint ruff pipeline check src tests
```

Confirm:

- [ ] Both quality report files say `"passed": true`
- [ ] Replaying the date does not increase trusted fact count
- [ ] Invalid customer and order records appear in Silver quarantine
- [ ] Gold dimensions have unique keys
- [ ] Gold fact order IDs are unique
- [ ] Revenue and margin formulas match the data dictionary
