from __future__ import annotations

import re
import sqlite3


def answer_question(connection: sqlite3.Connection, question: str) -> str:
    normalized = " ".join(question.lower().split())

    if "net revenue" in normalized and "last 7 days" in normalized:
        return _format_rows(
            connection.execute(
                """
                SELECT ROUND(COALESCE(SUM(fk.kpi_value), 0), 2) AS net_revenue
                FROM fact_daily_kpis fk
                JOIN dim_kpi k ON k.kpi_key = fk.kpi_key
                JOIN dim_date d ON d.date_key = fk.date_key
                JOIN dim_country co ON co.country_key = fk.country_key
                JOIN dim_segment s ON s.segment_key = fk.segment_key
                JOIN dim_category c ON c.category_key = fk.category_key
                JOIN dim_channel ch ON ch.channel_key = fk.channel_key
                JOIN dim_payment_method pm ON pm.payment_method_key = fk.payment_method_key
                JOIN dim_ticket_type tt ON tt.ticket_type_key = fk.ticket_type_key
                WHERE k.kpi_name = 'net_revenue'
                  AND co.country_code = 'all'
                  AND s.segment_name = 'all'
                  AND c.category_name = 'all'
                  AND ch.channel_name = 'all'
                  AND pm.payment_method_name = 'all'
                  AND tt.ticket_type_name = 'all'
                  AND d.full_date >= (
                      SELECT DATE(MAX(d2.full_date), '-6 day')
                      FROM fact_daily_kpis fk2
                      JOIN dim_date d2 ON d2.date_key = fk2.date_key
                  )
                """
            ).fetchall()
        )

    if "payments by method" in normalized and "yesterday" in normalized:
        return _format_rows(
            connection.execute(
                """
                SELECT pm.payment_method_name, ROUND(fk.kpi_value, 2) AS collected_amount
                FROM fact_daily_kpis fk
                JOIN dim_kpi k ON k.kpi_key = fk.kpi_key
                JOIN dim_date d ON d.date_key = fk.date_key
                JOIN dim_country co ON co.country_key = fk.country_key
                JOIN dim_segment s ON s.segment_key = fk.segment_key
                JOIN dim_category c ON c.category_key = fk.category_key
                JOIN dim_channel ch ON ch.channel_key = fk.channel_key
                JOIN dim_payment_method pm ON pm.payment_method_key = fk.payment_method_key
                JOIN dim_ticket_type tt ON tt.ticket_type_key = fk.ticket_type_key
                WHERE k.kpi_name = 'payments_by_method'
                  AND co.country_code = 'all'
                  AND s.segment_name = 'all'
                  AND c.category_name = 'all'
                  AND ch.channel_name = 'all'
                  AND tt.ticket_type_name = 'all'
                  AND d.full_date = (
                      SELECT DATE(MAX(d2.full_date), '-1 day')
                      FROM fact_daily_kpis fk2
                      JOIN dim_date d2 ON d2.date_key = fk2.date_key
                  )
                ORDER BY fk.kpi_value DESC
                """
            ).fetchall()
        )

    if "tickets by type" in normalized and "yesterday" in normalized:
        return _format_rows(
            connection.execute(
                """
                SELECT tt.ticket_type_name, CAST(fk.kpi_value AS INTEGER) AS ticket_count
                FROM fact_daily_kpis fk
                JOIN dim_kpi k ON k.kpi_key = fk.kpi_key
                JOIN dim_date d ON d.date_key = fk.date_key
                JOIN dim_country co ON co.country_key = fk.country_key
                JOIN dim_segment s ON s.segment_key = fk.segment_key
                JOIN dim_category c ON c.category_key = fk.category_key
                JOIN dim_channel ch ON ch.channel_key = fk.channel_key
                JOIN dim_payment_method pm ON pm.payment_method_key = fk.payment_method_key
                JOIN dim_ticket_type tt ON tt.ticket_type_key = fk.ticket_type_key
                WHERE k.kpi_name = 'tickets_by_type'
                  AND co.country_code = 'all'
                  AND s.segment_name = 'all'
                  AND c.category_name = 'all'
                  AND ch.channel_name = 'all'
                  AND pm.payment_method_name = 'all'
                  AND d.full_date = (
                      SELECT DATE(MAX(d2.full_date), '-1 day')
                      FROM fact_daily_kpis fk2
                      JOIN dim_date d2 ON d2.date_key = fk2.date_key
                  )
                ORDER BY fk.kpi_value DESC
                """
            ).fetchall()
        )

    trend_match = re.search(r"daily (\w+(?: \w+)*) for the last (\d+) days", normalized)
    if trend_match:
        metric_name = _map_metric(trend_match.group(1))
        days = int(trend_match.group(2))
        if metric_name:
            return _format_rows(
                connection.execute(
                    """
                    SELECT d.full_date AS kpi_date, ROUND(fk.kpi_value, 2) AS value
                    FROM fact_daily_kpis fk
                    JOIN dim_kpi k ON k.kpi_key = fk.kpi_key
                    JOIN dim_date d ON d.date_key = fk.date_key
                    JOIN dim_country co ON co.country_key = fk.country_key
                    JOIN dim_segment s ON s.segment_key = fk.segment_key
                    JOIN dim_category c ON c.category_key = fk.category_key
                    JOIN dim_channel ch ON ch.channel_key = fk.channel_key
                    JOIN dim_payment_method pm ON pm.payment_method_key = fk.payment_method_key
                    JOIN dim_ticket_type tt ON tt.ticket_type_key = fk.ticket_type_key
                    WHERE k.kpi_name = ?
                      AND co.country_code = 'all'
                      AND s.segment_name = 'all'
                      AND c.category_name = 'all'
                      AND ch.channel_name = 'all'
                      AND pm.payment_method_name = 'all'
                      AND tt.ticket_type_name = 'all'
                      AND d.full_date >= (
                          SELECT DATE(MAX(d2.full_date), ?)
                          FROM fact_daily_kpis fk2
                          JOIN dim_date d2 ON d2.date_key = fk2.date_key
                      )
                    ORDER BY d.full_date
                    """,
                    (metric_name, f"-{days - 1} day"),
                ).fetchall()
            )

    return (
        "Question not recognized. Try one of:\n"
        "- What was net revenue in the last 7 days?\n"
        "- Show daily new customers for the last 14 days\n"
        "- Show payments by method yesterday\n"
        "- Show tickets by type yesterday"
    )


def _map_metric(metric_phrase: str) -> str | None:
    mapping = {
        "new customers": "new_customers",
        "new products": "new_products",
        "gross revenue": "gross_revenue",
        "net revenue": "net_revenue",
        "items sold": "items_sold",
        "completed orders": "completed_orders",
        "avg order value": "avg_order_value",
        "payments collected": "payments_collected",
        "refund amount": "refund_amount",
        "ticket count": "ticket_count",
        "open ticket count": "open_ticket_count",
    }
    return mapping.get(metric_phrase.strip())


def _format_rows(rows: list[sqlite3.Row]) -> str:
    if not rows:
        return "No data found."
    return "\n".join(", ".join(f"{key}={row[key]}" for key in row.keys()) for row in rows)
