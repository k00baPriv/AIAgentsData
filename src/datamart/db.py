from __future__ import annotations

import sqlite3
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
DB_PATH = DATA_DIR / "datamart.db"


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def initialize_database(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            signup_date TEXT NOT NULL,
            country TEXT NOT NULL,
            segment TEXT NOT NULL,
            status TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            created_date TEXT NOT NULL,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            brand TEXT NOT NULL,
            base_price REAL NOT NULL,
            product_status TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            order_date TEXT NOT NULL,
            channel TEXT NOT NULL,
            shipment_mode TEXT NOT NULL,
            order_status TEXT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        );

        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            discount_rate REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );

        CREATE TABLE IF NOT EXISTS payments (
            payment_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            payment_date TEXT NOT NULL,
            payment_method TEXT NOT NULL,
            payment_amount REAL NOT NULL,
            payment_status TEXT NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id)
        );

        CREATE TABLE IF NOT EXISTS support_tickets (
            ticket_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            order_id INTEGER,
            created_date TEXT NOT NULL,
            ticket_type TEXT NOT NULL,
            priority TEXT NOT NULL,
            ticket_status TEXT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY (order_id) REFERENCES orders(order_id)
        );

        CREATE TABLE IF NOT EXISTS dim_date (
            date_key INTEGER PRIMARY KEY,
            full_date TEXT NOT NULL UNIQUE,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS dim_country (
            country_key INTEGER PRIMARY KEY,
            country_code TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS dim_segment (
            segment_key INTEGER PRIMARY KEY,
            segment_name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_key INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL UNIQUE,
            signup_date_key INTEGER NOT NULL,
            country_key INTEGER NOT NULL,
            segment_key INTEGER NOT NULL,
            customer_status TEXT NOT NULL,
            FOREIGN KEY (signup_date_key) REFERENCES dim_date(date_key),
            FOREIGN KEY (country_key) REFERENCES dim_country(country_key),
            FOREIGN KEY (segment_key) REFERENCES dim_segment(segment_key)
        );

        CREATE TABLE IF NOT EXISTS dim_category (
            category_key INTEGER PRIMARY KEY,
            category_name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS dim_brand (
            brand_key INTEGER PRIMARY KEY,
            brand_name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS dim_product (
            product_key INTEGER PRIMARY KEY,
            product_id INTEGER NOT NULL UNIQUE,
            created_date_key INTEGER NOT NULL,
            category_key INTEGER NOT NULL,
            brand_key INTEGER NOT NULL,
            product_name TEXT NOT NULL,
            base_price REAL NOT NULL,
            product_status TEXT NOT NULL,
            FOREIGN KEY (created_date_key) REFERENCES dim_date(date_key),
            FOREIGN KEY (category_key) REFERENCES dim_category(category_key),
            FOREIGN KEY (brand_key) REFERENCES dim_brand(brand_key)
        );

        CREATE TABLE IF NOT EXISTS dim_channel (
            channel_key INTEGER PRIMARY KEY,
            channel_name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS dim_shipment_mode (
            shipment_mode_key INTEGER PRIMARY KEY,
            shipment_mode_name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS dim_payment_method (
            payment_method_key INTEGER PRIMARY KEY,
            payment_method_name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS dim_ticket_type (
            ticket_type_key INTEGER PRIMARY KEY,
            ticket_type_name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS dim_priority (
            priority_key INTEGER PRIMARY KEY,
            priority_name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS dim_kpi (
            kpi_key INTEGER PRIMARY KEY,
            kpi_name TEXT NOT NULL UNIQUE,
            description TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS fact_order_items (
            order_item_fact_key INTEGER PRIMARY KEY,
            order_item_id INTEGER NOT NULL UNIQUE,
            order_id INTEGER NOT NULL,
            order_date_key INTEGER NOT NULL,
            customer_key INTEGER NOT NULL,
            product_key INTEGER NOT NULL,
            country_key INTEGER NOT NULL,
            segment_key INTEGER NOT NULL,
            category_key INTEGER NOT NULL,
            brand_key INTEGER NOT NULL,
            channel_key INTEGER NOT NULL,
            shipment_mode_key INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            discount_rate REAL NOT NULL,
            gross_revenue REAL NOT NULL,
            net_revenue REAL NOT NULL,
            order_status TEXT NOT NULL,
            FOREIGN KEY (order_date_key) REFERENCES dim_date(date_key),
            FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
            FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
            FOREIGN KEY (country_key) REFERENCES dim_country(country_key),
            FOREIGN KEY (segment_key) REFERENCES dim_segment(segment_key),
            FOREIGN KEY (category_key) REFERENCES dim_category(category_key),
            FOREIGN KEY (brand_key) REFERENCES dim_brand(brand_key),
            FOREIGN KEY (channel_key) REFERENCES dim_channel(channel_key),
            FOREIGN KEY (shipment_mode_key) REFERENCES dim_shipment_mode(shipment_mode_key)
        );

        CREATE TABLE IF NOT EXISTS fact_payments (
            payment_fact_key INTEGER PRIMARY KEY,
            payment_id INTEGER NOT NULL UNIQUE,
            order_id INTEGER NOT NULL,
            payment_date_key INTEGER NOT NULL,
            customer_key INTEGER NOT NULL,
            payment_method_key INTEGER NOT NULL,
            payment_amount REAL NOT NULL,
            payment_status TEXT NOT NULL,
            FOREIGN KEY (payment_date_key) REFERENCES dim_date(date_key),
            FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
            FOREIGN KEY (payment_method_key) REFERENCES dim_payment_method(payment_method_key)
        );

        CREATE TABLE IF NOT EXISTS fact_support_tickets (
            ticket_fact_key INTEGER PRIMARY KEY,
            ticket_id INTEGER NOT NULL UNIQUE,
            created_date_key INTEGER NOT NULL,
            customer_key INTEGER NOT NULL,
            order_id INTEGER,
            country_key INTEGER NOT NULL,
            segment_key INTEGER NOT NULL,
            ticket_type_key INTEGER NOT NULL,
            priority_key INTEGER NOT NULL,
            ticket_status TEXT NOT NULL,
            FOREIGN KEY (created_date_key) REFERENCES dim_date(date_key),
            FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
            FOREIGN KEY (country_key) REFERENCES dim_country(country_key),
            FOREIGN KEY (segment_key) REFERENCES dim_segment(segment_key),
            FOREIGN KEY (ticket_type_key) REFERENCES dim_ticket_type(ticket_type_key),
            FOREIGN KEY (priority_key) REFERENCES dim_priority(priority_key)
        );

        CREATE TABLE IF NOT EXISTS fact_daily_kpis (
            kpi_fact_key INTEGER PRIMARY KEY,
            date_key INTEGER NOT NULL,
            kpi_key INTEGER NOT NULL,
            country_key INTEGER,
            segment_key INTEGER,
            category_key INTEGER,
            channel_key INTEGER,
            payment_method_key INTEGER,
            ticket_type_key INTEGER,
            kpi_value REAL NOT NULL,
            FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
            FOREIGN KEY (kpi_key) REFERENCES dim_kpi(kpi_key),
            FOREIGN KEY (country_key) REFERENCES dim_country(country_key),
            FOREIGN KEY (segment_key) REFERENCES dim_segment(segment_key),
            FOREIGN KEY (category_key) REFERENCES dim_category(category_key),
            FOREIGN KEY (channel_key) REFERENCES dim_channel(channel_key),
            FOREIGN KEY (payment_method_key) REFERENCES dim_payment_method(payment_method_key),
            FOREIGN KEY (ticket_type_key) REFERENCES dim_ticket_type(ticket_type_key)
        );

        CREATE TABLE IF NOT EXISTS fact_weekly_kpis (
            kpi_fact_key INTEGER PRIMARY KEY,
            week_start_date_key INTEGER NOT NULL,
            week_end_date_key INTEGER NOT NULL,
            kpi_key INTEGER NOT NULL,
            country_key INTEGER,
            segment_key INTEGER,
            category_key INTEGER,
            channel_key INTEGER,
            payment_method_key INTEGER,
            ticket_type_key INTEGER,
            kpi_value REAL NOT NULL,
            FOREIGN KEY (week_start_date_key) REFERENCES dim_date(date_key),
            FOREIGN KEY (week_end_date_key) REFERENCES dim_date(date_key),
            FOREIGN KEY (kpi_key) REFERENCES dim_kpi(kpi_key),
            FOREIGN KEY (country_key) REFERENCES dim_country(country_key),
            FOREIGN KEY (segment_key) REFERENCES dim_segment(segment_key),
            FOREIGN KEY (category_key) REFERENCES dim_category(category_key),
            FOREIGN KEY (channel_key) REFERENCES dim_channel(channel_key),
            FOREIGN KEY (payment_method_key) REFERENCES dim_payment_method(payment_method_key),
            FOREIGN KEY (ticket_type_key) REFERENCES dim_ticket_type(ticket_type_key)
        );

        CREATE TABLE IF NOT EXISTS fact_monthly_kpis (
            kpi_fact_key INTEGER PRIMARY KEY,
            month_start_date_key INTEGER NOT NULL,
            month_end_date_key INTEGER NOT NULL,
            kpi_key INTEGER NOT NULL,
            country_key INTEGER,
            segment_key INTEGER,
            category_key INTEGER,
            channel_key INTEGER,
            payment_method_key INTEGER,
            ticket_type_key INTEGER,
            kpi_value REAL NOT NULL,
            FOREIGN KEY (month_start_date_key) REFERENCES dim_date(date_key),
            FOREIGN KEY (month_end_date_key) REFERENCES dim_date(date_key),
            FOREIGN KEY (kpi_key) REFERENCES dim_kpi(kpi_key),
            FOREIGN KEY (country_key) REFERENCES dim_country(country_key),
            FOREIGN KEY (segment_key) REFERENCES dim_segment(segment_key),
            FOREIGN KEY (category_key) REFERENCES dim_category(category_key),
            FOREIGN KEY (channel_key) REFERENCES dim_channel(channel_key),
            FOREIGN KEY (payment_method_key) REFERENCES dim_payment_method(payment_method_key),
            FOREIGN KEY (ticket_type_key) REFERENCES dim_ticket_type(ticket_type_key)
        );

        CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date);
        CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
        CREATE INDEX IF NOT EXISTS idx_payments_order_id ON payments(order_id);
        CREATE INDEX IF NOT EXISTS idx_support_tickets_customer_id ON support_tickets(customer_id);
        CREATE INDEX IF NOT EXISTS idx_fact_order_items_date ON fact_order_items(order_date_key);
        CREATE INDEX IF NOT EXISTS idx_fact_payments_date ON fact_payments(payment_date_key);
        CREATE INDEX IF NOT EXISTS idx_fact_support_tickets_date
            ON fact_support_tickets(created_date_key);
        CREATE INDEX IF NOT EXISTS idx_fact_daily_kpis_lookup ON fact_daily_kpis(date_key, kpi_key);
        CREATE INDEX IF NOT EXISTS idx_fact_weekly_kpis_lookup
            ON fact_weekly_kpis(week_start_date_key, kpi_key);
        CREATE INDEX IF NOT EXISTS idx_fact_monthly_kpis_lookup
            ON fact_monthly_kpis(month_start_date_key, kpi_key);
        """
    )
    connection.commit()


def reset_database(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA foreign_keys = OFF")
    connection.executescript(
        """
        DROP TABLE IF EXISTS fact_daily_kpis;
        DROP TABLE IF EXISTS fact_monthly_kpis;
        DROP TABLE IF EXISTS fact_weekly_kpis;
        DROP TABLE IF EXISTS fact_support_tickets;
        DROP TABLE IF EXISTS fact_payments;
        DROP TABLE IF EXISTS fact_order_items;
        DROP TABLE IF EXISTS dim_kpi;
        DROP TABLE IF EXISTS dim_priority;
        DROP TABLE IF EXISTS dim_ticket_type;
        DROP TABLE IF EXISTS dim_payment_method;
        DROP TABLE IF EXISTS dim_shipment_mode;
        DROP TABLE IF EXISTS dim_channel;
        DROP TABLE IF EXISTS dim_product;
        DROP TABLE IF EXISTS dim_brand;
        DROP TABLE IF EXISTS dim_category;
        DROP TABLE IF EXISTS dim_customer;
        DROP TABLE IF EXISTS dim_segment;
        DROP TABLE IF EXISTS dim_country;
        DROP TABLE IF EXISTS dim_date;
        DROP TABLE IF EXISTS support_tickets;
        DROP TABLE IF EXISTS payments;
        DROP TABLE IF EXISTS order_items;
        DROP TABLE IF EXISTS orders;
        DROP TABLE IF EXISTS products;
        DROP TABLE IF EXISTS customers;
        """
    )
    connection.commit()
    connection.execute("PRAGMA foreign_keys = ON")
    initialize_database(connection)
