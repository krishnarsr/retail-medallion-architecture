"""Silver: validated, deduplicated and conformed business entities."""
from __future__ import annotations

from datetime import UTC, datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from src.common.config import Settings
from src.common.quality import (
    CheckResult,
    non_negative,
    not_null,
    positive,
    unique,
    write_report,
)


def _latest(df: DataFrame, key: str) -> DataFrame:
    window = Window.partitionBy(key).orderBy(
        F.col("_ingested_at").desc(), F.col("_record_hash").desc()
    )
    return df.withColumn("_rank", F.row_number().over(window)).filter("_rank = 1").drop("_rank")


def _write_quarantine(df: DataFrame, target: str) -> int:
    count = df.count()
    if count:
        df.write.format("delta").mode("append").partitionBy("_quarantine_date").save(target)
    return count


def _quarantine_projection(df: DataFrame, entity: str, reason_column) -> DataFrame:
    payload_columns = [
        column for column in df.columns
        if not column.startswith("_") or column in ("_source_file", "_batch_id")
    ]
    return df.select(
        F.lit(entity).alias("_entity"),
        reason_column.alias("_quarantine_reason"),
        F.current_date().alias("_quarantine_date"),
        F.col("_source_file"),
        F.to_json(F.struct(*[F.col(column) for column in payload_columns])).alias("_payload"),
    )


def _customers(spark: SparkSession, settings: Settings) -> tuple[DataFrame, int]:
    bronze = spark.read.format("delta").load(settings.table_path("bronze", "customers"))
    normalized = (
        bronze.withColumn("customer_id", F.upper(F.trim("customer_id")))
        .withColumn("full_name", F.initcap(F.trim("full_name")))
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("city", F.initcap(F.trim("city")))
        .withColumn("signup_date", F.to_date("signup_date"))
        .withColumn("source_updated_at", F.to_timestamp("updated_at"))
    )
    invalid = normalized.filter(
        F.col("customer_id").isNull()
        | (F.col("customer_id") == "")
        | ~F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    ).withColumn("_quarantine_reason", F.lit("INVALID_CUSTOMER_KEY_OR_EMAIL"))
    invalid = invalid.withColumn("_quarantine_date", F.current_date())
    _write_quarantine(
        _quarantine_projection(
            invalid, "customer", F.lit("INVALID_CUSTOMER_KEY_OR_EMAIL")
        ),
        settings.table_path("silver", "quarantine"),
    )
    valid = _latest(normalized.subtract(invalid.drop("_quarantine_reason", "_quarantine_date")), "customer_id")
    valid = valid.withColumn(
        "attribute_hash",
        F.sha2(F.concat_ws("||", "full_name", "email", "city", "country", "segment"), 256),
    )
    target = settings.table_path("silver", "customers")
    now = datetime.now(UTC)
    incoming = valid.select(
        "customer_id", "full_name", "email", "city", "country", "segment", "signup_date",
        "source_updated_at", "attribute_hash"
    )
    if not DeltaTable.isDeltaTable(spark, target):
        (
            incoming.withColumn("valid_from", F.lit(now).cast("timestamp"))
            .withColumn("valid_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .write.format("delta").mode("overwrite").save(target)
        )
    else:
        current = spark.read.format("delta").load(target).filter("is_current = true")
        changed = (
            incoming.alias("i")
            .join(current.alias("c"), "customer_id", "left")
            .filter(F.col("c.customer_id").isNull() | (F.col("i.attribute_hash") != F.col("c.attribute_hash")))
            .select("i.*")
        )
        changed_ids = changed.select("customer_id")
        if changed_ids.limit(1).count():
            delta = DeltaTable.forPath(spark, target)
            (
                delta.alias("t")
                .merge(changed_ids.alias("s"), "t.customer_id = s.customer_id AND t.is_current = true")
                .whenMatchedUpdate(set={"is_current": "false", "valid_to": "current_timestamp()"})
                .execute()
            )
            (
                changed.withColumn("valid_from", F.lit(now).cast("timestamp"))
                .withColumn("valid_to", F.lit(None).cast("timestamp"))
                .withColumn("is_current", F.lit(True))
                .write.format("delta").mode("append").save(target)
            )
    return spark.read.format("delta").load(target), invalid.count()


def _products(spark: SparkSession, settings: Settings) -> DataFrame:
    bronze = spark.read.format("delta").load(settings.table_path("bronze", "products"))
    valid = _latest(
        bronze.filter(
            F.col("product_id").isNotNull()
            & (F.col("unit_cost") >= 0)
            & (F.col("list_price") >= 0)
        )
        .withColumn("product_id", F.upper(F.trim("product_id")))
        .withColumn("source_updated_at", F.to_timestamp("updated_at")),
        "product_id",
    ).select(
        "product_id", "product_name", "category", "unit_cost", "list_price", "active", "source_updated_at"
    )
    target = settings.table_path("silver", "products")
    if DeltaTable.isDeltaTable(spark, target):
        (
            DeltaTable.forPath(spark, target)
            .alias("t")
            .merge(valid.alias("s"), "t.product_id = s.product_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        valid.write.format("delta").mode("overwrite").save(target)
    return spark.read.format("delta").load(target)


def _orders(spark: SparkSession, settings: Settings, customers: DataFrame, products: DataFrame) -> tuple[DataFrame, int]:
    bronze = spark.read.format("delta").load(settings.table_path("bronze", "orders"))
    latest = _latest(bronze, "order_id")
    conformed = (
        latest.withColumn("ordered_at", F.to_timestamp("ordered_at"))
        .withColumn("order_date", F.to_date("ordered_at"))
        .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn(
            "net_amount",
            F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct")), 2),
        )
        .withColumn("discount_amount", F.round(F.col("gross_amount") - F.col("net_amount"), 2))
    )
    current_customers = customers.filter("is_current = true").select("customer_id")
    product_keys = products.select("product_id")
    checked = (
        conformed.alias("o")
        .join(current_customers.withColumn("_customer_exists", F.lit(True)), "customer_id", "left")
        .join(product_keys.withColumn("_product_exists", F.lit(True)), "product_id", "left")
    )
    invalid_condition = (
        F.col("order_id").isNull()
        | (F.col("order_id") == "")
        | (F.col("quantity") <= 0)
        | (F.col("unit_price") < 0)
        | F.col("_customer_exists").isNull()
        | F.col("_product_exists").isNull()
    )
    invalid = (
        checked.filter(invalid_condition)
        .withColumn(
            "_quarantine_reason",
            F.when(F.col("quantity") <= 0, "NON_POSITIVE_QUANTITY")
            .when(F.col("unit_price") < 0, "NEGATIVE_PRICE")
            .when(F.col("_customer_exists").isNull(), "UNKNOWN_CUSTOMER")
            .when(F.col("_product_exists").isNull(), "UNKNOWN_PRODUCT")
            .otherwise("INVALID_ORDER_KEY"),
        )
        .withColumn("_quarantine_date", F.current_date())
    )
    _write_quarantine(
        _quarantine_projection(invalid, "order", F.col("_quarantine_reason")),
        settings.table_path("silver", "quarantine"),
    )
    valid = checked.filter(~invalid_condition).drop("_customer_exists", "_product_exists").select(
        "order_id", "customer_id", "product_id", "ordered_at", "order_date", "quantity",
        "unit_price", "discount_pct", "gross_amount", "discount_amount", "net_amount",
        "channel", "status", "currency", "_batch_id", "_ingested_at"
    )
    target = settings.table_path("silver", "sales")
    if DeltaTable.isDeltaTable(spark, target):
        (
            DeltaTable.forPath(spark, target)
            .alias("t")
            .merge(valid.alias("s"), "t.order_id = s.order_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        valid.write.format("delta").mode("overwrite").partitionBy("order_date").save(target)
    return spark.read.format("delta").load(target), invalid.count()


def run(spark: SparkSession, settings: Settings) -> dict:
    customers, customer_bad = _customers(spark, settings)
    products = _products(spark, settings)
    sales, order_bad = _orders(spark, settings, customers, products)

    results: list[CheckResult] = [
        not_null(sales, "order_id"),
        unique(sales, "order_id"),
        positive(sales, "quantity"),
        non_negative(sales, "unit_price"),
        not_null(customers.filter("is_current = true"), "customer_id"),
        unique(customers.filter("is_current = true"), "customer_id"),
        unique(products, "product_id"),
    ]
    write_report(settings.quality / "silver_quality.json", "silver", results)
    return {
        "silver_customer_versions": customers.count(),
        "silver_products": products.count(),
        "silver_sales": sales.count(),
        "quarantined_customers": customer_bad,
        "quarantined_orders": order_bad,
    }
