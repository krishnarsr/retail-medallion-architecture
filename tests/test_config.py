from src.common.config import load_settings


def test_all_medallion_layers_are_configured():
    settings = load_settings()
    assert set(settings.config["tables"]) == {"bronze", "silver", "gold"}
    assert {"customers", "products", "orders"} <= set(settings.config["tables"]["bronze"])
    assert {"dim_customer", "dim_product", "fact_sales"} <= set(
        settings.config["tables"]["gold"]
    )


def test_table_paths_stay_under_lakehouse():
    settings = load_settings()
    path = settings.table_path("gold", "fact_sales")
    assert str(settings.lakehouse) in path
