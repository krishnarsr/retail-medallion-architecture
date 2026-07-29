import csv
import json

from src.common.config import ensure_directories, load_settings
from src.generate_data import generate


def test_generator_is_deterministic_and_writes_three_sources(tmp_path, monkeypatch):
    settings = load_settings()
    settings = type(settings)(
        root=settings.root,
        data_root=tmp_path,
        landing=tmp_path / "landing",
        lakehouse=tmp_path / "lakehouse",
        quality=tmp_path / "quality",
        config=settings.config,
    )
    ensure_directories(settings)
    monkeypatch.setenv("CUSTOMER_RECORDS", "5")
    monkeypatch.setenv("PRODUCT_RECORDS", "4")
    monkeypatch.setenv("ORDER_RECORDS", "10")
    generate(settings, "2026-01-15")
    batch = "batch_date=2026-01-15"
    customer_file = settings.landing / "customers" / batch / "customers.csv"
    product_file = settings.landing / "products" / batch / "products.json"
    order_file = settings.landing / "orders" / batch / "orders.csv"
    assert customer_file.exists() and product_file.exists() and order_file.exists()
    with customer_file.open(encoding="utf-8") as handle:
        assert len(list(csv.DictReader(handle))) == 7
    first_product = json.loads(product_file.read_text(encoding="utf-8").splitlines()[0])
    assert first_product["product_id"] == "P00001"
