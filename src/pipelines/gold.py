"""Gold: dimensional model, aggregates and customer/product marts."""
from __future__ import annotations

from pyspark.sql import SparkSession, Window, functions as F

from src.common.config import Settings
from src.common.quality import not_null, non_negative, unique, write_report


def _overwrite(df, path: str, partitions: list[str] | None = None) -> None:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.save(path)


def run(spark: SparkSession, settings: Settings) -> dict:
    customers = spark.read.format("delta").load(settings.table_path("silver", "customers"))
    products = spark.read.format("delta").load(settings.table_path("silver", "products"))
    sales = spark.read.format("delta").load(settings.table_path("silver", "sales"))

    dim_customer = (
        customers.filter("is_current = true")
        .withColumn("customer_key", F.abs(F.xxhash64("customer_id")))
        .select(
            "customer_key", "customer_id", "full_name", "email", "city", "country",
            "segment", "signup_date", "valid_from"
        )
    )
    dim_product = (
        products.withColumn("product_key", F.abs(F.xxhash64("product_id")))
        .select(
            "product_key", "product_id", "product_name", "category", "unit_cost",
            "list_price", "active"
        )
    )
    fact_sales = (
        sales.join(dim_customer.select("customer_id", "customer_key"), "customer_id")
        .join(dim_product.select("product_id", "product_key", "unit_cost"), "product_id")
        .withColumn("sales_key", F.abs(F.xxhash64("order_id")))
        .withColumn("cost_amount", F.round(F.col("quantity") * F.col("unit_cost"), 2))
        .withColumn("margin_amount", F.round(F.col("net_amount") - F.col("cost_amount"), 2))
        .select(
            "sales_key", "order_id", "customer_key", "product_key", "order_date",
            "ordered_at", "quantity", "gross_amount", "discount_amount", "net_amount",
            "cost_amount", "margin_amount", "channel", "status", "currency"
        )
    )
    daily_sales = (
        fact_sales.groupBy("order_date", "channel")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units"),
            F.round(F.sum("gross_amount"), 2).alias("gross_revenue"),
            F.round(F.sum("discount_amount"), 2).alias("discounts"),
            F.round(F.sum("net_amount"), 2).alias("net_revenue"),
            F.round(F.sum("margin_amount"), 2).alias("gross_margin"),
            F.round(F.avg("net_amount"), 2).alias("average_order_value"),
        )
    )
    customer_360 = (
        fact_sales.groupBy("customer_key")
        .agg(
            F.countDistinct("order_id").alias("lifetime_orders"),
            F.round(F.sum("net_amount"), 2).alias("lifetime_value"),
            F.max("ordered_at").alias("last_order_at"),
            F.round(F.avg("net_amount"), 2).alias("average_order_value"),
            F.countDistinct("product_key").alias("distinct_products"),
        )
        .join(dim_customer, "customer_key")
        .withColumn("days_since_last_order", F.datediff(F.current_date(), F.to_date("last_order_at")))
        .withColumn(
            "value_band",
            F.when(F.col("lifetime_value") >= 1000, "HIGH")
            .when(F.col("lifetime_value") >= 300, "MEDIUM")
            .otherwise("LOW"),
        )
    )
    product_performance = (
        fact_sales.groupBy("product_key")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("net_amount"), 2).alias("net_revenue"),
            F.round(F.sum("margin_amount"), 2).alias("gross_margin"),
        )
        .join(dim_product, "product_key")
        .withColumn(
            "revenue_rank_in_category",
            F.dense_rank().over(Window.partitionBy("category").orderBy(F.col("net_revenue").desc())),
        )
    )

    _overwrite(dim_customer, settings.table_path("gold", "dim_customer"))
    _overwrite(dim_product, settings.table_path("gold", "dim_product"))
    _overwrite(fact_sales, settings.table_path("gold", "fact_sales"), ["order_date"])
    _overwrite(daily_sales, settings.table_path("gold", "daily_sales"), ["order_date"])
    _overwrite(customer_360, settings.table_path("gold", "customer_360"))
    _overwrite(product_performance, settings.table_path("gold", "product_performance"))

    write_report(
        settings.quality / "gold_quality.json",
        "gold",
        [
            not_null(fact_sales, "sales_key"),
            unique(fact_sales, "order_id"),
            non_negative(fact_sales, "net_amount"),
            unique(dim_customer, "customer_key"),
            unique(dim_product, "product_key"),
        ],
    )
    return {
        "dim_customers": dim_customer.count(),
        "dim_products": dim_product.count(),
        "fact_sales": fact_sales.count(),
        "daily_sales_rows": daily_sales.count(),
        "customer_360_rows": customer_360.count(),
        "product_performance_rows": product_performance.count(),
    }
