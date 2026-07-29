# Data dictionary

## Sources and Bronze

All source fields are retained. Bronze adds:

| Column | Meaning |
|---|---|
| `_source_system` | Logical source name |
| `_source_file` | Exact input file URI |
| `_batch_id` | Pipeline/delivery identity |
| `_ingested_at` | UTC processing timestamp |
| `_ingestion_date` | Physical partition date |
| `_record_hash` | SHA-256 of the source-shaped row |
| `_bronze_record_id` | SHA-256 of source file + row hash |

## Silver customers_scd2

| Column | Type | Rule |
|---|---|---|
| `customer_id` | string | Natural key, uppercase, non-null |
| `full_name` | string | Trimmed and title-cased |
| `email` | string | Lowercase and basic format validation |
| `city`, `country` | string | Conformed location |
| `segment` | string | Consumer/Corporate/Small Business |
| `signup_date` | date | Parsed source date |
| `attribute_hash` | string | Change detection across descriptive values |
| `valid_from`, `valid_to` | timestamp | SCD2 validity interval |
| `is_current` | boolean | Exactly one current version per customer |

## Silver sales

| Column | Type | Definition |
|---|---|---|
| `order_id` | string | Unique natural key |
| `customer_id`, `product_id` | string | Valid Silver references |
| `ordered_at`, `order_date` | timestamp/date | Transaction time and partition date |
| `quantity` | integer | Greater than zero |
| `unit_price` | double | Non-negative source selling price |
| `discount_pct` | double | Fraction between zero and one in generated data |
| `gross_amount` | double | quantity × unit_price |
| `discount_amount` | double | gross − net |
| `net_amount` | double | gross × (1 − discount_pct) |
| `channel` | string | WEB, MOBILE, STORE or MARKETPLACE |
| `status` | string | COMPLETED, RETURNED or CANCELLED |

## Gold fact_sales

Grain: one trusted order record.

| Measure | Formula |
|---|---|
| `gross_amount` | quantity × selling price |
| `discount_amount` | gross amount − net amount |
| `net_amount` | revenue after discount |
| `cost_amount` | quantity × product unit cost |
| `margin_amount` | net amount − cost amount |

Cancelled/returned rows are retained because status is an analytical dimension.
A finance-specific mart could apply signed revenue or exclusion rules.

## Gold marts

- `daily_sales`: date × channel aggregates.
- `customer_360`: lifetime orders/value, last order, average order value,
  distinct products, recency and value band.
- `product_performance`: orders, units, revenue, margin and category rank.
