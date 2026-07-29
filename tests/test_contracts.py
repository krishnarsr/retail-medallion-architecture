from src.pipelines.bronze import CUSTOMER_SCHEMA, ORDER_SCHEMA, PRODUCT_SCHEMA


def test_source_contract_keys():
    assert "customer_id" in CUSTOMER_SCHEMA.fieldNames()
    assert "product_id" in PRODUCT_SCHEMA.fieldNames()
    assert {"order_id", "customer_id", "product_id"} <= set(ORDER_SCHEMA.fieldNames())
