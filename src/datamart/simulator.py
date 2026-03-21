from __future__ import annotations

import random
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta

COUNTRIES = ["US", "DE", "CH", "PL", "UK", "FR"]
SEGMENTS = ["consumer", "small_business", "enterprise"]
CATEGORIES = ["software", "hardware", "accessories", "services"]
BRANDS = ["nova", "atlas", "orbit", "pixel"]
CHANNELS = ["web", "partner", "sales"]
SHIPMENT_MODES = ["standard", "express", "pickup"]
PAYMENT_METHODS = ["card", "bank_transfer", "paypal"]
TICKET_TYPES = ["delivery_issue", "billing_issue", "product_question", "return_request"]
PRIORITIES = ["low", "medium", "high"]


@dataclass
class SimulationConfig:
    start_date: date = date(2025, 1, 1)
    days: int = 90
    seed: int = 7


class DatamartSimulator:
    def __init__(self, connection: sqlite3.Connection, config: SimulationConfig) -> None:
        self.connection = connection
        self.config = config
        self.random = random.Random(config.seed)  # nosec B311

    def run(self) -> None:
        self._bootstrap_products()
        for offset in range(self.config.days):
            business_date = self.config.start_date + timedelta(days=offset)
            self._simulate_day(business_date)
        self.connection.commit()

    def _bootstrap_products(self) -> None:
        initial_products = 20
        for index in range(initial_products):
            created_date = self.config.start_date.isoformat()
            category = self.random.choice(CATEGORIES)
            brand = self.random.choice(BRANDS)
            base_price = round(self.random.uniform(25.0, 900.0), 2)
            self.connection.execute(
                """
                INSERT INTO products (
                    created_date, product_name, category, brand, base_price, product_status
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    created_date,
                    f"{brand}_{category}_{index + 1}",
                    category,
                    brand,
                    base_price,
                    "active",
                ),
            )

    def _simulate_day(self, business_date: date) -> None:
        self._maybe_launch_new_products(business_date)
        for _ in range(self.random.randint(4, 12)):
            self._insert_customer(business_date)
        all_customer_ids = self._get_ids("customers", "customer_id")
        all_product_ids = self._get_ids("products", "product_id")

        self._update_customer_statuses(all_customer_ids)
        order_ids = self._create_orders(business_date, all_customer_ids, all_product_ids)
        self._create_support_tickets(business_date, all_customer_ids, order_ids)

    def _maybe_launch_new_products(self, business_date: date) -> None:
        if self.random.random() < 0.25:
            launches = self.random.randint(1, 3)
            for index in range(launches):
                category = self.random.choice(CATEGORIES)
                brand = self.random.choice(BRANDS)
                base_price = round(self.random.uniform(30.0, 1200.0), 2)
                self.connection.execute(
                    """
                    INSERT INTO products (
                        created_date, product_name, category, brand, base_price, product_status
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        business_date.isoformat(),
                        f"{brand}_{category}_{business_date.isoformat()}_{index + 1}",
                        category,
                        brand,
                        base_price,
                        "active",
                    ),
                )

    def _insert_customer(self, business_date: date) -> int:
        cursor = self.connection.execute(
            """
            INSERT INTO customers (signup_date, country, segment, status)
            VALUES (?, ?, ?, ?)
            """,
            (
                business_date.isoformat(),
                self.random.choice(COUNTRIES),
                self.random.choices(SEGMENTS, weights=[0.6, 0.25, 0.15], k=1)[0],
                "active",
            ),
        )
        if cursor.lastrowid is None:
            raise RuntimeError("Customer insert did not return a row id.")
        return int(cursor.lastrowid)

    def _update_customer_statuses(self, customer_ids: list[int]) -> None:
        if not customer_ids:
            return
        sample = self.random.sample(
            customer_ids, k=min(max(1, len(customer_ids) // 15), len(customer_ids))
        )
        for customer_id in sample:
            status = self.random.choices(["active", "inactive"], weights=[0.88, 0.12], k=1)[0]
            self.connection.execute(
                "UPDATE customers SET status = ? WHERE customer_id = ?",
                (status, customer_id),
            )

    def _create_orders(
        self, business_date: date, customer_ids: list[int], product_ids: list[int]
    ) -> list[int]:
        if not customer_ids or not product_ids:
            return []
        buyers = self.random.sample(
            customer_ids,
            k=min(self.random.randint(6, max(7, len(customer_ids) // 3)), len(customer_ids)),
        )
        created_order_ids: list[int] = []
        for customer_id in buyers:
            for _ in range(self.random.randint(1, 2)):
                order_status = self.random.choices(
                    ["completed", "cancelled", "returned"],
                    weights=[0.9, 0.05, 0.05],
                    k=1,
                )[0]
                cursor = self.connection.execute(
                    """
                    INSERT INTO orders (
                        customer_id, order_date, channel, shipment_mode, order_status
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        customer_id,
                        business_date.isoformat(),
                        self.random.choice(CHANNELS),
                        self.random.choice(SHIPMENT_MODES),
                        order_status,
                    ),
                )
                if cursor.lastrowid is None:
                    raise RuntimeError("Order insert did not return a row id.")
                order_id = int(cursor.lastrowid)
                created_order_ids.append(order_id)
                order_total = self._create_order_items(order_id, product_ids)
                self._create_payment(order_id, business_date, order_total, order_status)
        return created_order_ids

    def _create_order_items(self, order_id: int, product_ids: list[int]) -> float:
        line_count = self.random.randint(1, 4)
        total = 0.0
        for _ in range(line_count):
            product_id = self.random.choice(product_ids)
            product = self.connection.execute(
                """
                SELECT base_price FROM products WHERE product_id = ?
                """,
                (product_id,),
            ).fetchone()
            quantity = self.random.randint(1, 5)
            discount_rate = round(self.random.choice([0.0, 0.05, 0.10, 0.15]), 2)
            unit_price = float(product["base_price"]) * self.random.uniform(0.9, 1.1)
            unit_price = round(unit_price, 2)
            self.connection.execute(
                """
                INSERT INTO order_items (
                    order_id, product_id, quantity, unit_price, discount_rate
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (order_id, product_id, quantity, unit_price, discount_rate),
            )
            total += quantity * unit_price * (1 - discount_rate)
        return round(total, 2)

    def _create_payment(
        self, order_id: int, business_date: date, order_total: float, order_status: str
    ) -> None:
        payment_status = "paid"
        if order_status == "cancelled":
            payment_status = "failed"
        elif order_status == "returned":
            payment_status = "refunded"
        amount = 0.0 if payment_status == "failed" else order_total
        self.connection.execute(
            """
            INSERT INTO payments (
                order_id, payment_date, payment_method, payment_amount, payment_status
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                order_id,
                business_date.isoformat(),
                self.random.choice(PAYMENT_METHODS),
                amount,
                payment_status,
            ),
        )

    def _create_support_tickets(
        self,
        business_date: date,
        customer_ids: list[int],
        order_ids: list[int],
    ) -> None:
        ticket_count = self.random.randint(1, max(2, len(order_ids) // 8 if order_ids else 2))
        for _ in range(ticket_count):
            customer_id = self.random.choice(customer_ids)
            related_order_id = (
                self.random.choice(order_ids) if order_ids and self.random.random() < 0.8 else None
            )
            ticket_status = self.random.choices(
                ["open", "resolved", "closed"],
                weights=[0.25, 0.55, 0.20],
                k=1,
            )[0]
            self.connection.execute(
                """
                INSERT INTO support_tickets (
                    customer_id, order_id, created_date, ticket_type, priority, ticket_status
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    customer_id,
                    related_order_id,
                    business_date.isoformat(),
                    self.random.choice(TICKET_TYPES),
                    self.random.choices(PRIORITIES, weights=[0.35, 0.45, 0.20], k=1)[0],
                    ticket_status,
                ),
            )

    def _get_ids(self, table_name: str, column_name: str) -> list[int]:
        queries = {
            ("customers", "customer_id"): "SELECT customer_id FROM customers",
            ("products", "product_id"): "SELECT product_id FROM products",
        }
        rows = self.connection.execute(queries[(table_name, column_name)]).fetchall()
        return [int(row[column_name]) for row in rows]
