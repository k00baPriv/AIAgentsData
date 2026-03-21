from __future__ import annotations

import sqlite3
from datetime import date, timedelta

KPI_DEFINITIONS = {
    "new_customers": "Customers created in the period.",
    "new_products": "Products launched in the period.",
    "gross_revenue": "Gross revenue from completed order items in the period.",
    "net_revenue": "Net revenue from completed order items in the period.",
    "items_sold": "Units sold on completed order items in the period.",
    "completed_orders": "Completed orders in the period.",
    "avg_order_value": "Average net order value for completed orders in the period.",
    "payments_collected": "Successful paid amount in the period.",
    "refund_amount": "Refunded amount in the period.",
    "ticket_count": "Support tickets created in the period.",
    "open_ticket_count": "Open support tickets created in the period.",
    "revenue_by_segment": "Net revenue grouped by segment.",
    "revenue_by_country": "Net revenue grouped by country.",
    "revenue_by_category": "Net revenue grouped by category.",
    "revenue_by_channel": "Net revenue grouped by channel.",
    "payments_by_method": "Paid amount grouped by payment method.",
    "tickets_by_type": "Ticket count grouped by ticket type.",
}

DEFAULT_DIMENSION_INSERTS = {
    ("dim_country", "country_key", "country_code"): (
        "INSERT OR IGNORE INTO dim_country (country_key, country_code) VALUES (1, 'all')"
    ),
    ("dim_segment", "segment_key", "segment_name"): (
        "INSERT OR IGNORE INTO dim_segment (segment_key, segment_name) VALUES (1, 'all')"
    ),
    ("dim_category", "category_key", "category_name"): (
        "INSERT OR IGNORE INTO dim_category (category_key, category_name) VALUES (1, 'all')"
    ),
    ("dim_channel", "channel_key", "channel_name"): (
        "INSERT OR IGNORE INTO dim_channel (channel_key, channel_name) VALUES (1, 'all')"
    ),
    ("dim_payment_method", "payment_method_key", "payment_method_name"): (
        "INSERT OR IGNORE INTO dim_payment_method (payment_method_key, payment_method_name) "
        "VALUES (1, 'all')"
    ),
    ("dim_ticket_type", "ticket_type_key", "ticket_type_name"): (
        "INSERT OR IGNORE INTO dim_ticket_type (ticket_type_key, ticket_type_name) "
        "VALUES (1, 'all')"
    ),
}

DISTINCT_VALUE_QUERIES = {
    ("dim_country", "country_code", "customers", "country"): (
        "SELECT DISTINCT country AS value FROM customers ORDER BY 1"
    ),
    ("dim_segment", "segment_name", "customers", "segment"): (
        "SELECT DISTINCT segment AS value FROM customers ORDER BY 1"
    ),
    ("dim_category", "category_name", "products", "category"): (
        "SELECT DISTINCT category AS value FROM products ORDER BY 1"
    ),
    ("dim_brand", "brand_name", "products", "brand"): (
        "SELECT DISTINCT brand AS value FROM products ORDER BY 1"
    ),
    ("dim_channel", "channel_name", "orders", "channel"): (
        "SELECT DISTINCT channel AS value FROM orders ORDER BY 1"
    ),
    ("dim_shipment_mode", "shipment_mode_name", "orders", "shipment_mode"): (
        "SELECT DISTINCT shipment_mode AS value FROM orders ORDER BY 1"
    ),
    ("dim_payment_method", "payment_method_name", "payments", "payment_method"): (
        "SELECT DISTINCT payment_method AS value FROM payments ORDER BY 1"
    ),
    ("dim_ticket_type", "ticket_type_name", "support_tickets", "ticket_type"): (
        "SELECT DISTINCT ticket_type AS value FROM support_tickets ORDER BY 1"
    ),
    ("dim_priority", "priority_name", "support_tickets", "priority"): (
        "SELECT DISTINCT priority AS value FROM support_tickets ORDER BY 1"
    ),
}

DIMENSION_INSERTS = {
    "dim_country": "INSERT OR IGNORE INTO dim_country (country_code) VALUES (?)",
    "dim_segment": "INSERT OR IGNORE INTO dim_segment (segment_name) VALUES (?)",
    "dim_category": "INSERT OR IGNORE INTO dim_category (category_name) VALUES (?)",
    "dim_brand": "INSERT OR IGNORE INTO dim_brand (brand_name) VALUES (?)",
    "dim_channel": "INSERT OR IGNORE INTO dim_channel (channel_name) VALUES (?)",
    "dim_shipment_mode": "INSERT OR IGNORE INTO dim_shipment_mode (shipment_mode_name) VALUES (?)",
    "dim_payment_method": (
        "INSERT OR IGNORE INTO dim_payment_method (payment_method_name) VALUES (?)"
    ),
    "dim_ticket_type": "INSERT OR IGNORE INTO dim_ticket_type (ticket_type_name) VALUES (?)",
    "dim_priority": "INSERT OR IGNORE INTO dim_priority (priority_name) VALUES (?)",
}

REVENUE_DIMENSION_QUERIES = {
    "segment_key": """
        SELECT segment_key AS dimension_key, COALESCE(SUM(net_revenue), 0) AS value
        FROM fact_order_items
        WHERE order_date_key BETWEEN ? AND ?
          AND order_status = 'completed'
        GROUP BY segment_key
    """,
    "country_key": """
        SELECT country_key AS dimension_key, COALESCE(SUM(net_revenue), 0) AS value
        FROM fact_order_items
        WHERE order_date_key BETWEEN ? AND ?
          AND order_status = 'completed'
        GROUP BY country_key
    """,
    "category_key": """
        SELECT category_key AS dimension_key, COALESCE(SUM(net_revenue), 0) AS value
        FROM fact_order_items
        WHERE order_date_key BETWEEN ? AND ?
          AND order_status = 'completed'
        GROUP BY category_key
    """,
    "channel_key": """
        SELECT channel_key AS dimension_key, COALESCE(SUM(net_revenue), 0) AS value
        FROM fact_order_items
        WHERE order_date_key BETWEEN ? AND ?
          AND order_status = 'completed'
        GROUP BY channel_key
    """,
}


def build_star_schema(connection: sqlite3.Connection, start_date: date, days: int) -> None:
    _load_dim_dates(connection, start_date, days)
    _load_single_value_dimension(connection, "dim_country", "country_key", "country_code")
    _load_single_value_dimension(connection, "dim_segment", "segment_key", "segment_name")
    _load_single_value_dimension(connection, "dim_category", "category_key", "category_name")
    _load_single_value_dimension(connection, "dim_channel", "channel_key", "channel_name")
    _load_single_value_dimension(
        connection, "dim_payment_method", "payment_method_key", "payment_method_name"
    )
    _load_single_value_dimension(
        connection, "dim_ticket_type", "ticket_type_key", "ticket_type_name"
    )
    _load_distinct_values(connection, "dim_country", "country_code", "customers", "country")
    _load_distinct_values(connection, "dim_segment", "segment_name", "customers", "segment")
    _load_distinct_values(connection, "dim_category", "category_name", "products", "category")
    _load_distinct_values(connection, "dim_brand", "brand_name", "products", "brand")
    _load_distinct_values(connection, "dim_channel", "channel_name", "orders", "channel")
    _load_distinct_values(
        connection, "dim_shipment_mode", "shipment_mode_name", "orders", "shipment_mode"
    )
    _load_distinct_values(
        connection, "dim_payment_method", "payment_method_name", "payments", "payment_method"
    )
    _load_distinct_values(
        connection, "dim_ticket_type", "ticket_type_name", "support_tickets", "ticket_type"
    )
    _load_distinct_values(
        connection, "dim_priority", "priority_name", "support_tickets", "priority"
    )
    _load_dim_kpis(connection)
    _load_dim_customers(connection)
    _load_dim_products(connection)
    _load_fact_order_items(connection)
    _load_fact_payments(connection)
    _load_fact_support_tickets(connection)
    _load_periodic_kpis(connection, start_date, days)
    connection.commit()


def _load_dim_dates(connection: sqlite3.Connection, start_date: date, days: int) -> None:
    for offset in range(days):
        business_date = start_date + timedelta(days=offset)
        connection.execute(
            """
            INSERT OR IGNORE INTO dim_date (date_key, full_date, year, month, day)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                _date_key(business_date.isoformat()),
                business_date.isoformat(),
                business_date.year,
                business_date.month,
                business_date.day,
            ),
        )


def _load_single_value_dimension(
    connection: sqlite3.Connection,
    table_name: str,
    key_name: str,
    value_name: str,
) -> None:
    connection.execute(DEFAULT_DIMENSION_INSERTS[(table_name, key_name, value_name)])


def _load_distinct_values(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
    source_table: str,
    source_column: str,
) -> None:
    rows = connection.execute(
        DISTINCT_VALUE_QUERIES[(table_name, column_name, source_table, source_column)]
    ).fetchall()
    for row in rows:
        connection.execute(DIMENSION_INSERTS[table_name], (row["value"],))


def _load_dim_kpis(connection: sqlite3.Connection) -> None:
    for kpi_name, description in KPI_DEFINITIONS.items():
        connection.execute(
            "INSERT OR IGNORE INTO dim_kpi (kpi_name, description) VALUES (?, ?)",
            (kpi_name, description),
        )


def _load_dim_customers(connection: sqlite3.Connection) -> None:
    connection.execute("DELETE FROM fact_support_tickets")
    connection.execute("DELETE FROM fact_payments")
    connection.execute("DELETE FROM fact_order_items")
    connection.execute("DELETE FROM dim_customer")
    connection.execute(
        """
        INSERT INTO dim_customer (
            customer_id, signup_date_key, country_key, segment_key, customer_status
        )
        SELECT
            c.customer_id,
            d.date_key,
            co.country_key,
            s.segment_key,
            c.status
        FROM customers c
        JOIN dim_date d ON d.full_date = c.signup_date
        JOIN dim_country co ON co.country_code = c.country
        JOIN dim_segment s ON s.segment_name = c.segment
        ORDER BY c.customer_id
        """
    )


def _load_dim_products(connection: sqlite3.Connection) -> None:
    connection.execute("DELETE FROM dim_product")
    connection.execute(
        """
        INSERT INTO dim_product (
            product_id,
            created_date_key,
            category_key,
            brand_key,
            product_name,
            base_price,
            product_status
        )
        SELECT
            p.product_id,
            d.date_key,
            c.category_key,
            b.brand_key,
            p.product_name,
            p.base_price,
            p.product_status
        FROM products p
        JOIN dim_date d ON d.full_date = p.created_date
        JOIN dim_category c ON c.category_name = p.category
        JOIN dim_brand b ON b.brand_name = p.brand
        ORDER BY p.product_id
        """
    )


def _load_fact_order_items(connection: sqlite3.Connection) -> None:
    connection.execute("DELETE FROM fact_order_items")
    connection.execute(
        """
        INSERT INTO fact_order_items (
            order_item_id, order_id, order_date_key, customer_key, product_key, country_key,
            segment_key, category_key, brand_key, channel_key, shipment_mode_key, quantity,
            unit_price, discount_rate, gross_revenue, net_revenue, order_status
        )
        SELECT
            oi.order_item_id,
            o.order_id,
            d.date_key,
            cu.customer_key,
            p.product_key,
            cu.country_key,
            cu.segment_key,
            p.category_key,
            p.brand_key,
            ch.channel_key,
            sm.shipment_mode_key,
            oi.quantity,
            oi.unit_price,
            oi.discount_rate,
            oi.quantity * oi.unit_price,
            oi.quantity * oi.unit_price * (1 - oi.discount_rate),
            o.order_status
        FROM order_items oi
        JOIN orders o ON o.order_id = oi.order_id
        JOIN dim_date d ON d.full_date = o.order_date
        JOIN dim_customer cu ON cu.customer_id = o.customer_id
        JOIN dim_product p ON p.product_id = oi.product_id
        JOIN dim_channel ch ON ch.channel_name = o.channel
        JOIN dim_shipment_mode sm ON sm.shipment_mode_name = o.shipment_mode
        ORDER BY oi.order_item_id
        """
    )


def _load_fact_payments(connection: sqlite3.Connection) -> None:
    connection.execute("DELETE FROM fact_payments")
    connection.execute(
        """
        INSERT INTO fact_payments (
            payment_id,
            order_id,
            payment_date_key,
            customer_key,
            payment_method_key,
            payment_amount,
            payment_status
        )
        SELECT
            p.payment_id,
            p.order_id,
            d.date_key,
            cu.customer_key,
            pm.payment_method_key,
            p.payment_amount,
            p.payment_status
        FROM payments p
        JOIN orders o ON o.order_id = p.order_id
        JOIN dim_date d ON d.full_date = p.payment_date
        JOIN dim_customer cu ON cu.customer_id = o.customer_id
        JOIN dim_payment_method pm ON pm.payment_method_name = p.payment_method
        ORDER BY p.payment_id
        """
    )


def _load_fact_support_tickets(connection: sqlite3.Connection) -> None:
    connection.execute("DELETE FROM fact_support_tickets")
    connection.execute(
        """
        INSERT INTO fact_support_tickets (
            ticket_id, created_date_key, customer_key, order_id, country_key, segment_key,
            ticket_type_key, priority_key, ticket_status
        )
        SELECT
            t.ticket_id,
            d.date_key,
            cu.customer_key,
            t.order_id,
            cu.country_key,
            cu.segment_key,
            tt.ticket_type_key,
            pr.priority_key,
            t.ticket_status
        FROM support_tickets t
        JOIN dim_date d ON d.full_date = t.created_date
        JOIN dim_customer cu ON cu.customer_id = t.customer_id
        JOIN dim_ticket_type tt ON tt.ticket_type_name = t.ticket_type
        JOIN dim_priority pr ON pr.priority_name = t.priority
        ORDER BY t.ticket_id
        """
    )


def _load_periodic_kpis(connection: sqlite3.Connection, start_date: date, days: int) -> None:
    connection.execute("DELETE FROM fact_daily_kpis")
    connection.execute("DELETE FROM fact_weekly_kpis")
    connection.execute("DELETE FROM fact_monthly_kpis")

    all_dates = [start_date + timedelta(days=offset) for offset in range(days)]
    for business_date in all_dates:
        _insert_period_kpis(connection, "day", business_date, business_date)

    seen_weeks: set[date] = set()
    for business_date in all_dates:
        raw_week_start = business_date - timedelta(days=business_date.weekday())
        week_start = max(raw_week_start, all_dates[0])
        if raw_week_start in seen_weeks:
            continue
        seen_weeks.add(raw_week_start)
        raw_week_end = raw_week_start + timedelta(days=6)
        week_end = min(raw_week_end, all_dates[-1])
        _insert_period_kpis(connection, "week", week_start, week_end)

    seen_months: set[tuple[int, int]] = set()
    for business_date in all_dates:
        month_key = (business_date.year, business_date.month)
        if month_key in seen_months:
            continue
        seen_months.add(month_key)
        month_start = max(business_date.replace(day=1), all_dates[0])
        month_end = min(_month_end(month_start), all_dates[-1])
        _insert_period_kpis(connection, "month", month_start, month_end)


def _insert_period_kpis(
    connection: sqlite3.Connection,
    grain: str,
    period_start: date,
    period_end: date,
) -> None:
    start_key = _date_key(period_start.isoformat())
    end_key = _date_key(period_end.isoformat())
    metrics = _scalar_metrics(connection, start_key, end_key)
    for kpi_name, value in metrics.items():
        _insert_kpi_fact(connection, grain, start_key, end_key, kpi_name, float(value))

    _insert_dimension_metric(
        connection, grain, start_key, end_key, "revenue_by_segment", "segment_key"
    )
    _insert_dimension_metric(
        connection, grain, start_key, end_key, "revenue_by_country", "country_key"
    )
    _insert_dimension_metric(
        connection, grain, start_key, end_key, "revenue_by_category", "category_key"
    )
    _insert_dimension_metric(
        connection, grain, start_key, end_key, "revenue_by_channel", "channel_key"
    )
    _insert_payment_metric(connection, grain, start_key, end_key)
    _insert_ticket_metric(connection, grain, start_key, end_key)


def _scalar_metrics(
    connection: sqlite3.Connection, start_key: int, end_key: int
) -> dict[str, float]:
    order_value = connection.execute(
        """
        WITH order_totals AS (
            SELECT order_id, SUM(net_revenue) AS order_value
            FROM fact_order_items
            WHERE order_date_key BETWEEN ? AND ?
              AND order_status = 'completed'
            GROUP BY order_id
        )
        SELECT COALESCE(AVG(order_value), 0) AS value
        FROM order_totals
        """,
        (start_key, end_key),
    ).fetchone()["value"]

    return {
        "new_customers": connection.execute(
            "SELECT COUNT(*) AS value FROM dim_customer WHERE signup_date_key BETWEEN ? AND ?",
            (start_key, end_key),
        ).fetchone()["value"],
        "new_products": connection.execute(
            "SELECT COUNT(*) AS value FROM dim_product WHERE created_date_key BETWEEN ? AND ?",
            (start_key, end_key),
        ).fetchone()["value"],
        "gross_revenue": connection.execute(
            """
            SELECT COALESCE(SUM(gross_revenue), 0) AS value
            FROM fact_order_items
            WHERE order_date_key BETWEEN ? AND ?
              AND order_status = 'completed'
            """,
            (start_key, end_key),
        ).fetchone()["value"],
        "net_revenue": connection.execute(
            """
            SELECT COALESCE(SUM(net_revenue), 0) AS value
            FROM fact_order_items
            WHERE order_date_key BETWEEN ? AND ?
              AND order_status = 'completed'
            """,
            (start_key, end_key),
        ).fetchone()["value"],
        "items_sold": connection.execute(
            """
            SELECT COALESCE(SUM(quantity), 0) AS value
            FROM fact_order_items
            WHERE order_date_key BETWEEN ? AND ?
              AND order_status = 'completed'
            """,
            (start_key, end_key),
        ).fetchone()["value"],
        "completed_orders": connection.execute(
            """
            SELECT COUNT(DISTINCT order_id) AS value
            FROM fact_order_items
            WHERE order_date_key BETWEEN ? AND ?
              AND order_status = 'completed'
            """,
            (start_key, end_key),
        ).fetchone()["value"],
        "avg_order_value": order_value,
        "payments_collected": connection.execute(
            """
            SELECT COALESCE(SUM(payment_amount), 0) AS value
            FROM fact_payments
            WHERE payment_date_key BETWEEN ? AND ?
              AND payment_status = 'paid'
            """,
            (start_key, end_key),
        ).fetchone()["value"],
        "refund_amount": connection.execute(
            """
            SELECT COALESCE(SUM(payment_amount), 0) AS value
            FROM fact_payments
            WHERE payment_date_key BETWEEN ? AND ?
              AND payment_status = 'refunded'
            """,
            (start_key, end_key),
        ).fetchone()["value"],
        "ticket_count": connection.execute(
            """
            SELECT COUNT(*) AS value
            FROM fact_support_tickets
            WHERE created_date_key BETWEEN ? AND ?
            """,
            (start_key, end_key),
        ).fetchone()["value"],
        "open_ticket_count": connection.execute(
            """
            SELECT COUNT(*) AS value
            FROM fact_support_tickets
            WHERE created_date_key BETWEEN ? AND ?
              AND ticket_status = 'open'
            """,
            (start_key, end_key),
        ).fetchone()["value"],
    }


def _insert_dimension_metric(
    connection: sqlite3.Connection,
    grain: str,
    start_key: int,
    end_key: int,
    kpi_name: str,
    dimension_column: str,
) -> None:
    rows = connection.execute(
        REVENUE_DIMENSION_QUERIES[dimension_column],
        (start_key, end_key),
    ).fetchall()
    for row in rows:
        kwargs = {dimension_column: int(row["dimension_key"])}
        _insert_kpi_fact(
            connection, grain, start_key, end_key, kpi_name, float(row["value"]), **kwargs
        )


def _insert_payment_metric(
    connection: sqlite3.Connection, grain: str, start_key: int, end_key: int
) -> None:
    rows = connection.execute(
        """
        SELECT payment_method_key, COALESCE(SUM(payment_amount), 0) AS value
        FROM fact_payments
        WHERE payment_date_key BETWEEN ? AND ?
          AND payment_status = 'paid'
        GROUP BY payment_method_key
        """,
        (start_key, end_key),
    ).fetchall()
    for row in rows:
        _insert_kpi_fact(
            connection,
            grain,
            start_key,
            end_key,
            "payments_by_method",
            float(row["value"]),
            payment_method_key=int(row["payment_method_key"]),
        )


def _insert_ticket_metric(
    connection: sqlite3.Connection, grain: str, start_key: int, end_key: int
) -> None:
    rows = connection.execute(
        """
        SELECT ticket_type_key, COUNT(*) AS value
        FROM fact_support_tickets
        WHERE created_date_key BETWEEN ? AND ?
        GROUP BY ticket_type_key
        """,
        (start_key, end_key),
    ).fetchall()
    for row in rows:
        _insert_kpi_fact(
            connection,
            grain,
            start_key,
            end_key,
            "tickets_by_type",
            float(row["value"]),
            ticket_type_key=int(row["ticket_type_key"]),
        )


def _insert_kpi_fact(
    connection: sqlite3.Connection,
    grain: str,
    start_key: int,
    end_key: int,
    kpi_name: str,
    kpi_value: float,
    country_key: int = 1,
    segment_key: int = 1,
    category_key: int = 1,
    channel_key: int = 1,
    payment_method_key: int = 1,
    ticket_type_key: int = 1,
) -> None:
    kpi_key = connection.execute(
        "SELECT kpi_key FROM dim_kpi WHERE kpi_name = ?",
        (kpi_name,),
    ).fetchone()["kpi_key"]

    if grain == "day":
        connection.execute(
            """
            INSERT INTO fact_daily_kpis (
                date_key, kpi_key, country_key, segment_key, category_key, channel_key,
                payment_method_key, ticket_type_key, kpi_value
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                start_key,
                kpi_key,
                country_key,
                segment_key,
                category_key,
                channel_key,
                payment_method_key,
                ticket_type_key,
                kpi_value,
            ),
        )
        return

    if grain == "week":
        connection.execute(
            """
            INSERT INTO fact_weekly_kpis (
                week_start_date_key, week_end_date_key, kpi_key, country_key, segment_key,
                category_key, channel_key, payment_method_key, ticket_type_key, kpi_value
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                start_key,
                end_key,
                kpi_key,
                country_key,
                segment_key,
                category_key,
                channel_key,
                payment_method_key,
                ticket_type_key,
                kpi_value,
            ),
        )
        return

    connection.execute(
        """
        INSERT INTO fact_monthly_kpis (
            month_start_date_key, month_end_date_key, kpi_key, country_key, segment_key,
            category_key, channel_key, payment_method_key, ticket_type_key, kpi_value
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            start_key,
            end_key,
            kpi_key,
            country_key,
            segment_key,
            category_key,
            channel_key,
            payment_method_key,
            ticket_type_key,
            kpi_value,
        ),
    )


def _month_end(month_start: date) -> date:
    if month_start.month == 12:
        return date(month_start.year, 12, 31)
    next_month = date(month_start.year, month_start.month + 1, 1)
    return next_month - timedelta(days=1)


def _date_key(full_date: str) -> int:
    return int(full_date.replace("-", ""))
