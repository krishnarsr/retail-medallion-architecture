# Portfolio and interview guide

## Suggested GitHub description

> Local retail lakehouse built with PySpark, Delta Lake and Docker. Implements
> idempotent Bronze ingestion, Silver quality/quarantine and SCD2, Gold star
> schema and customer/product marts, automated tests and CI.

## Resume bullet

> Engineered a Dockerized PySpark/Delta Lake medallion pipeline for omnichannel
> retail data, implementing idempotent ingestion, data lineage, SCD Type 2,
> quarantine rules, dimensional modelling, quality gates and CI-tested marts
> for revenue, margin and customer lifetime value.

## Interview explanation

**Why three layers?** Bronze preserves replayable evidence, Silver creates
trusted reusable entities, and Gold fixes business grain/metrics for consumers.

**How is retry safety achieved?** Bronze merges a deterministic file-and-row
identity, Silver upserts natural keys and customer history, and Gold rebuilds
from conformed state.

**Why SCD2?** Analysts may need the customer segment/location that was valid
over time. The Silver history retains change intervals while Gold currently
publishes the latest profile.

**Why quarantine instead of deleting invalid data?** Engineering retains the
original evidence for correction, monitoring and source-owner feedback without
allowing it into trusted facts.

**What would change in cloud production?** Local paths become object storage;
the Spark job moves to a managed runtime; orchestration, secrets, catalogue,
monitoring and access controls become managed services. Transformation logic
and table contracts remain largely portable.

## Five-minute demonstration

1. Explain the architecture diagram and source contracts.
2. Show a malformed customer and negative-quantity order.
3. Run `docker compose run --rm pipeline run-all`.
4. Open quality reports and quarantine.
5. Explain SCD2 and Delta merge idempotency.
6. Show Gold grains and answer one SQL business question.
7. Show tests and GitHub Actions workflow.
