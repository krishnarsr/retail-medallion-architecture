# Operations and troubleshooting

## Expected first run

```powershell
Copy-Item .env.example .env
.\scripts\run.ps1
```

Expected generated locations:

```text
data/landing/{customers,products,orders}/batch_date=YYYY-MM-DD/
data/lakehouse/{bronze,silver,gold}/
data/quality/{silver_quality.json,gold_quality.json}
```

The Dockerfile deliberately pins `python:3.11-slim-bookworm`. Do not replace it
with the floating `python:3.11-slim` tag: that tag can resolve to Debian Trixie,
whose repositories do not contain `openjdk-17-jre-headless`.

## Incremental demonstration

Run day one and day two:

```powershell
.\scripts\run.ps1 -BatchDate 2026-01-15
.\scripts\run.ps1 -BatchDate 2026-01-16
```

Then replay day two. Bronze sees the same file/content identity, Silver merges
natural keys and Gold remains stable.

## Common problems

| Symptom | Resolution |
|---|---|
| Docker command unavailable | Start Docker Desktop and confirm Linux containers |
| Java gateway exits locally | Use Docker, or install Java 17 and set `JAVA_HOME` |
| Delta package resolution fails | Confirm internet access during the first image build |
| Old generated schema conflicts | Run `python scripts/clean_generated.py`, then rerun |
| Pipeline stops after Silver | Read `data/quality/silver_quality.json` |
| Laptop is slow | Reduce record counts in `.env` and set fewer Spark threads |

## Safe cleanup

`python scripts/clean_generated.py` deletes only generated content below the
three data directories and preserves tracked placeholders/source code.

## Definition of done

- tests and lint pass;
- Docker image builds;
- pipeline completes;
- Silver and Gold quality reports pass;
- repeated batch does not inflate trusted order count;
- quarantine contains the deliberately invalid source records;
- Gold grains and business formulas match the dictionary.
