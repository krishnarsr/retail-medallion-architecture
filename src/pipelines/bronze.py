"""Bronze: idempotent raw ingestion with lineage metadata."""
from __future__ import annotations

from datetime import UTC, datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.common.config import Settings


CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType()),
        StructField("full_name", StringType()),
        StructField("email", StringType()),
        StructField("city", StringType()),
        StructField("country", StringType()),
        StructField("segment", StringType()),
        StructField("signup_date", StringType()),
        StructField("updated_at", StringType()),
    ]
)
PRODUCT_SCHEMA = StructType(
    [
        StructField("product_id", StringType()),
        StructField("product_name", StringType()),
        StructField("category", StringType()),
        StructField("unit_cost", DoubleType()),
        StructField("list_price", DoubleType()),
        StructField("active", BooleanType()),
        StructField("updated_at", StringType()),
    ]
)
ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("product_id", StringType()),
        StructField("ordered_at", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DoubleType()),
        StructField("discount_pct", DoubleType()),
        StructField("channel", StringType()),
        StructField("status", StringType()),
        StructField("currency", StringType()),
    ]
)


def _with_metadata(df: DataFrame, source_name: str, batch_id: str) -> DataFrame:
    raw_columns = df.columns
    return (
        df.withColumn("_source_system", F.lit(source_name))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingestion_date", F.current_date())
        .withColumn(
            "_record_hash",
            F.sha2(F.to_json(F.struct(*[F.col(c) for c in raw_columns])), 256),
        )
        .withColumn(
            "_bronze_record_id",
            F.sha2(F.concat_ws("||", "_source_file", "_record_hash"), 256),
        )
    )


def _merge_append(spark: SparkSession, df: DataFrame, target: str) -> int:
    count = df.count()
    if DeltaTable.isDeltaTable(spark, target):
        (
            DeltaTable.forPath(spark, target)
            .alias("target")
            .merge(df.alias("source"), "target._bronze_record_id = source._bronze_record_id")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").partitionBy("_ingestion_date").save(target)
    return count


def run(spark: SparkSession, settings: Settings, batch_id: str | None = None) -> dict:
    batch = batch_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    customers = spark.read.option("header", True).schema(CUSTOMER_SCHEMA).csv(
        str(settings.landing / "customers" / "batch_date=*" / "*.csv")
    )
    products = spark.read.schema(PRODUCT_SCHEMA).json(
        str(settings.landing / "products" / "batch_date=*" / "*.json")
    )
    orders = spark.read.option("header", True).schema(ORDER_SCHEMA).csv(
        str(settings.landing / "orders" / "batch_date=*" / "*.csv")
    )
    return {
        "customers_seen": _merge_append(
            spark,
            _with_metadata(customers, "crm_csv", batch),
            settings.table_path("bronze", "customers"),
        ),
        "products_seen": _merge_append(
            spark,
            _with_metadata(products, "product_json", batch),
            settings.table_path("bronze", "products"),
        ),
        "orders_seen": _merge_append(
            spark,
            _with_metadata(orders, "commerce_csv", batch),
            settings.table_path("bronze", "orders"),
        ),
    }
