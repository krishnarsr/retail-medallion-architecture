"""Generate deterministic, intentionally imperfect retail source data."""
from __future__ import annotations

import csv
import json
import os
import random
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from src.common.config import Settings


CHANNELS = ("WEB", "MOBILE", "STORE", "MARKETPLACE")
STATUSES = ("COMPLETED", "COMPLETED", "COMPLETED", "RETURNED", "CANCELLED")
CATEGORIES = ("Electronics", "Home", "Beauty", "Sports", "Books", "Clothing")
FIRST_NAMES = ("Aarav", "Amelia", "Arjun", "Charlotte", "Harry", "Isha", "Jack", "Maya", "Noah", "Sofia")
LAST_NAMES = ("Brown", "Choudhary", "Davies", "Evans", "Khan", "Patel", "Shah", "Singh", "Smith", "Wilson")
CITIES = ("Birmingham", "Bristol", "Cardiff", "Leeds", "London", "Manchester", "Oxford", "Southampton")
PRODUCT_WORDS = ("Aurora", "Cedar", "Flux", "Halo", "Nimbus", "Orbit", "Pulse", "Summit")


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def generate(settings: Settings, batch_date: str | None = None) -> str:
    seed = int(os.getenv("DATA_SEED", "42"))
    random.seed(seed)
    day = batch_date or date.today().isoformat()
    reference_day = date.fromisoformat(day)
    batch_dir = f"batch_date={day}"

    customer_count = int(os.getenv("CUSTOMER_RECORDS", "500"))
    product_count = int(os.getenv("PRODUCT_RECORDS", "100"))
    order_count = int(os.getenv("ORDER_RECORDS", "2500"))

    customers = []
    for index in range(1, customer_count + 1):
        signup = reference_day - timedelta(days=random.randint(1, 4 * 365))
        first, last = random.choice(FIRST_NAMES), random.choice(LAST_NAMES)
        customers.append(
            {
                "customer_id": f"C{index:06d}",
                "full_name": f"{first} {last}",
                "email": f"{first}.{last}.{index}@example.test".lower(),
                "city": random.choice(CITIES),
                "country": "United Kingdom",
                "segment": random.choice(("Consumer", "Corporate", "Small Business")),
                "signup_date": signup.isoformat(),
                "updated_at": f"{day}T08:00:00Z",
            }
        )
    # Duplicate and malformed records demonstrate Silver controls.
    customers.append(customers[0].copy())
    customers.append({**customers[1], "customer_id": "", "email": "not-an-email"})
    _write_csv(settings.landing / "customers" / batch_dir / "customers.csv", customers)

    products = []
    for index in range(1, product_count + 1):
        cost = round(random.uniform(2, 500), 2)
        products.append(
            {
                "product_id": f"P{index:05d}",
                "product_name": f"{random.choice(PRODUCT_WORDS)} {random.choice(('Pro', 'Plus', 'Essential', 'Classic'))}",
                "category": random.choice(CATEGORIES),
                "unit_cost": cost,
                "list_price": round(cost * random.uniform(1.2, 2.5), 2),
                "active": True,
                "updated_at": f"{day}T08:05:00Z",
            }
        )
    (settings.landing / "products" / batch_dir).mkdir(parents=True, exist_ok=True)
    (settings.landing / "products" / batch_dir / "products.json").write_text(
        "\n".join(json.dumps(row) for row in products), encoding="utf-8"
    )

    orders = []
    start = datetime.fromisoformat(day).replace(tzinfo=UTC) - timedelta(days=89)
    for index in range(1, order_count + 1):
        product = random.choice(products)
        quantity = random.randint(1, 5)
        discount = random.choice((0, 0, 0.05, 0.10, 0.15, 0.20))
        ordered_at = start + timedelta(
            seconds=random.randint(0, 90 * 24 * 3600 - 1)
        )
        orders.append(
            {
                "order_id": f"O{day.replace('-', '')}{index:07d}",
                "customer_id": random.choice(customers[:-2])["customer_id"],
                "product_id": product["product_id"],
                "ordered_at": ordered_at.isoformat(),
                "quantity": quantity,
                "unit_price": product["list_price"],
                "discount_pct": discount,
                "channel": random.choice(CHANNELS),
                "status": random.choice(STATUSES),
                "currency": "GBP",
            }
        )
    orders.append({**orders[0], "order_id": orders[1]["order_id"]})
    orders.append({**orders[2], "order_id": "BAD-ORDER", "quantity": -2})
    _write_csv(settings.landing / "orders" / batch_dir / "orders.csv", orders)
    return day
