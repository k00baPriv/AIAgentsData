from __future__ import annotations

import argparse
import sqlite3

from .db import get_connection, initialize_database, reset_database
from .qa import answer_question
from .simulator import DatamartSimulator, SimulationConfig
from .warehouse import build_star_schema


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SQLite multi-object star-schema datamart demo")
    subparsers = parser.add_subparsers(dest="command", required=True)

    simulate = subparsers.add_parser("simulate", help="Create demo data and warehouse facts")
    simulate.add_argument("--days", type=int, default=90)
    simulate.add_argument("--seed", type=int, default=7)
    simulate.add_argument("--reset", action="store_true")

    subparsers.add_parser("summary", help="Show operational counts and KPI snapshot")
    subparsers.add_parser("star", help="Show star schema table counts")

    ask = subparsers.add_parser("ask", help="Ask a predefined natural-language question")
    ask.add_argument("question", type=str)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    connection = get_connection()

    if args.command == "simulate":
        if args.reset:
            reset_database(connection)
        else:
            initialize_database(connection)
        config = SimulationConfig(days=args.days, seed=args.seed)
        DatamartSimulator(connection, config).run()
        build_star_schema(connection, config.start_date, config.days)
        print(f"Simulation complete for {args.days} days.")
        return

    initialize_database(connection)

    if args.command == "summary":
        _print_summary(connection)
        return

    if args.command == "star":
        _print_star_summary(connection)
        return

    if args.command == "ask":
        print(answer_question(connection, args.question))


def _print_summary(connection: sqlite3.Connection) -> None:
    table_names = ["customers", "products", "orders", "order_items", "payments", "support_tickets"]
    count_queries = {
        "customers": "SELECT COUNT(*) AS count FROM customers",
        "products": "SELECT COUNT(*) AS count FROM products",
        "orders": "SELECT COUNT(*) AS count FROM orders",
        "order_items": "SELECT COUNT(*) AS count FROM order_items",
        "payments": "SELECT COUNT(*) AS count FROM payments",
        "support_tickets": "SELECT COUNT(*) AS count FROM support_tickets",
    }
    for table_name in table_names:
        count = connection.execute(count_queries[table_name]).fetchone()["count"]
        print(f"{table_name}={count}")

    latest_kpis = connection.execute(
        """
        SELECT d.full_date AS kpi_date, k.kpi_name, ROUND(f.kpi_value, 2) AS kpi_value
        FROM fact_daily_kpis f
        JOIN dim_date d ON d.date_key = f.date_key
        JOIN dim_kpi k ON k.kpi_key = f.kpi_key
        JOIN dim_country co ON co.country_key = f.country_key
        JOIN dim_segment s ON s.segment_key = f.segment_key
        JOIN dim_category c ON c.category_key = f.category_key
        JOIN dim_channel ch ON ch.channel_key = f.channel_key
        JOIN dim_payment_method pm ON pm.payment_method_key = f.payment_method_key
        JOIN dim_ticket_type tt ON tt.ticket_type_key = f.ticket_type_key
        WHERE d.full_date = (
            SELECT MAX(d2.full_date)
            FROM fact_daily_kpis f2
            JOIN dim_date d2 ON d2.date_key = f2.date_key
        )
          AND co.country_code = 'all'
          AND s.segment_name = 'all'
          AND c.category_name = 'all'
          AND ch.channel_name = 'all'
          AND pm.payment_method_name = 'all'
          AND tt.ticket_type_name = 'all'
        ORDER BY k.kpi_name
        """
    ).fetchall()
    for row in latest_kpis:
        print(f"{row['kpi_date']} {row['kpi_name']}={row['kpi_value']}")

    weekly_period = connection.execute(
        """
        SELECT
            ds.full_date AS period_start,
            de.full_date AS period_end,
            k.kpi_name,
            ROUND(f.kpi_value, 2) AS kpi_value
        FROM fact_weekly_kpis f
        JOIN dim_date ds ON ds.date_key = f.week_start_date_key
        JOIN dim_date de ON de.date_key = f.week_end_date_key
        JOIN dim_kpi k ON k.kpi_key = f.kpi_key
        JOIN dim_country co ON co.country_key = f.country_key
        JOIN dim_segment s ON s.segment_key = f.segment_key
        JOIN dim_category c ON c.category_key = f.category_key
        JOIN dim_channel ch ON ch.channel_key = f.channel_key
        JOIN dim_payment_method pm ON pm.payment_method_key = f.payment_method_key
        JOIN dim_ticket_type tt ON tt.ticket_type_key = f.ticket_type_key
        WHERE f.week_start_date_key = (SELECT MAX(week_start_date_key) FROM fact_weekly_kpis)
          AND co.country_code = 'all'
          AND s.segment_name = 'all'
          AND c.category_name = 'all'
          AND ch.channel_name = 'all'
          AND pm.payment_method_name = 'all'
          AND tt.ticket_type_name = 'all'
          AND k.kpi_name IN ('net_revenue', 'payments_collected', 'ticket_count')
        ORDER BY k.kpi_name
        """
    ).fetchall()
    for row in weekly_period:
        print(
            f"week {row['period_start']}..{row['period_end']} {row['kpi_name']}={row['kpi_value']}"
        )

    monthly_period = connection.execute(
        """
        SELECT
            ds.full_date AS period_start,
            de.full_date AS period_end,
            k.kpi_name,
            ROUND(f.kpi_value, 2) AS kpi_value
        FROM fact_monthly_kpis f
        JOIN dim_date ds ON ds.date_key = f.month_start_date_key
        JOIN dim_date de ON de.date_key = f.month_end_date_key
        JOIN dim_kpi k ON k.kpi_key = f.kpi_key
        JOIN dim_country co ON co.country_key = f.country_key
        JOIN dim_segment s ON s.segment_key = f.segment_key
        JOIN dim_category c ON c.category_key = f.category_key
        JOIN dim_channel ch ON ch.channel_key = f.channel_key
        JOIN dim_payment_method pm ON pm.payment_method_key = f.payment_method_key
        JOIN dim_ticket_type tt ON tt.ticket_type_key = f.ticket_type_key
        WHERE f.month_start_date_key = (SELECT MAX(month_start_date_key) FROM fact_monthly_kpis)
          AND co.country_code = 'all'
          AND s.segment_name = 'all'
          AND c.category_name = 'all'
          AND ch.channel_name = 'all'
          AND pm.payment_method_name = 'all'
          AND tt.ticket_type_name = 'all'
          AND k.kpi_name IN ('net_revenue', 'payments_collected', 'ticket_count')
        ORDER BY k.kpi_name
        """
    ).fetchall()
    for row in monthly_period:
        print(
            f"month {row['period_start']}..{row['period_end']} {row['kpi_name']}={row['kpi_value']}"
        )


def _print_star_summary(connection: sqlite3.Connection) -> None:
    table_names = [
        "dim_date",
        "dim_country",
        "dim_segment",
        "dim_customer",
        "dim_category",
        "dim_brand",
        "dim_product",
        "dim_channel",
        "dim_shipment_mode",
        "dim_payment_method",
        "dim_ticket_type",
        "dim_priority",
        "dim_kpi",
        "fact_order_items",
        "fact_payments",
        "fact_support_tickets",
        "fact_daily_kpis",
        "fact_weekly_kpis",
        "fact_monthly_kpis",
    ]
    count_queries = {
        "dim_date": "SELECT COUNT(*) AS count FROM dim_date",
        "dim_country": "SELECT COUNT(*) AS count FROM dim_country",
        "dim_segment": "SELECT COUNT(*) AS count FROM dim_segment",
        "dim_customer": "SELECT COUNT(*) AS count FROM dim_customer",
        "dim_category": "SELECT COUNT(*) AS count FROM dim_category",
        "dim_brand": "SELECT COUNT(*) AS count FROM dim_brand",
        "dim_product": "SELECT COUNT(*) AS count FROM dim_product",
        "dim_channel": "SELECT COUNT(*) AS count FROM dim_channel",
        "dim_shipment_mode": "SELECT COUNT(*) AS count FROM dim_shipment_mode",
        "dim_payment_method": "SELECT COUNT(*) AS count FROM dim_payment_method",
        "dim_ticket_type": "SELECT COUNT(*) AS count FROM dim_ticket_type",
        "dim_priority": "SELECT COUNT(*) AS count FROM dim_priority",
        "dim_kpi": "SELECT COUNT(*) AS count FROM dim_kpi",
        "fact_order_items": "SELECT COUNT(*) AS count FROM fact_order_items",
        "fact_payments": "SELECT COUNT(*) AS count FROM fact_payments",
        "fact_support_tickets": "SELECT COUNT(*) AS count FROM fact_support_tickets",
        "fact_daily_kpis": "SELECT COUNT(*) AS count FROM fact_daily_kpis",
        "fact_weekly_kpis": "SELECT COUNT(*) AS count FROM fact_weekly_kpis",
        "fact_monthly_kpis": "SELECT COUNT(*) AS count FROM fact_monthly_kpis",
    }
    for table_name in table_names:
        count = connection.execute(count_queries[table_name]).fetchone()["count"]
        print(f"{table_name}={count}")


if __name__ == "__main__":
    main()
