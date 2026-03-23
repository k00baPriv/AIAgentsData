from __future__ import annotations

import re
import sqlite3

from .semantic import (
    SemanticQuery,
    SemanticQueryDependencyError,
    SemanticQueryError,
    run_semantic_query,
)


def answer_question(connection: sqlite3.Connection, question: str) -> str:
    normalized = " ".join(question.lower().split())

    try:
        semantic_query = _semantic_query_for_question(normalized)
        if semantic_query is not None:
            return _format_semantic_rows(
                run_semantic_query(connection, semantic_query),
                semantic_query,
            )
    except (SemanticQueryDependencyError, SemanticQueryError):
        return _answer_question_sql_fallback(connection, normalized)

    return _answer_question_sql_fallback(connection, normalized)


def _answer_question_sql_fallback(connection: sqlite3.Connection, normalized: str) -> str:
    handlers = [
        _fallback_week_vs_week_revenue,
        _fallback_best_selling_products,
        _fallback_net_revenue_last_7_days,
        _fallback_payments_by_method_yesterday,
        _fallback_tickets_by_type_yesterday,
        _fallback_daily_metric_trend,
    ]
    for handler in handlers:
        result = handler(connection, normalized)
        if result is not None:
            return result

    return (
        "Question not recognized. Try one of:\n"
        "- What was net revenue in the last 7 days?\n"
        "- Show week vs week revenue\n"
        "- Show daily new customers for the last 14 days\n"
        "- Show payments by method yesterday\n"
        "- Show tickets by type yesterday"
    )


def _semantic_query_for_question(normalized: str) -> SemanticQuery | None:
    builders = [
        _semantic_week_vs_week_revenue,
        _semantic_best_selling_products,
        _semantic_net_revenue_last_7_days,
        _semantic_payments_by_method_yesterday,
        _semantic_tickets_by_type_yesterday,
        _semantic_daily_metric_trend,
    ]
    for builder in builders:
        query = builder(normalized)
        if query is not None:
            return query
    return None


def _fallback_week_vs_week_revenue(connection: sqlite3.Connection, normalized: str) -> str | None:
    if "week vs week revenue" not in normalized and "week over week revenue" not in normalized:
        return None

    rows = connection.execute(
        """
        SELECT ds.full_date AS period_start, ROUND(f.kpi_value, 2) AS value
        FROM fact_weekly_kpis f
        JOIN dim_kpi k ON k.kpi_key = f.kpi_key
        JOIN dim_date ds ON ds.date_key = f.week_start_date_key
        JOIN dim_country co ON co.country_key = f.country_key
        JOIN dim_segment s ON s.segment_key = f.segment_key
        JOIN dim_category c ON c.category_key = f.category_key
        JOIN dim_channel ch ON ch.channel_key = f.channel_key
        JOIN dim_payment_method pm ON pm.payment_method_key = f.payment_method_key
        JOIN dim_ticket_type tt ON tt.ticket_type_key = f.ticket_type_key
        WHERE k.kpi_name = 'net_revenue'
          AND co.country_code = 'all'
          AND s.segment_name = 'all'
          AND c.category_name = 'all'
          AND ch.channel_name = 'all'
          AND pm.payment_method_name = 'all'
          AND tt.ticket_type_name = 'all'
        ORDER BY ds.full_date DESC
        LIMIT 2
        """
    ).fetchall()
    if len(rows) < 2:
        return "No data found."

    current_value = float(rows[0]["value"])
    previous_value = float(rows[1]["value"])
    delta_value = round(current_value - previous_value, 2)
    delta_pct = round((delta_value / previous_value) * 100, 2) if previous_value != 0 else None
    return (
        f"current_period_start={rows[0]['period_start']}, "
        f"previous_period_start={rows[1]['period_start']}, "
        f"current_value={current_value}, previous_value={previous_value}, "
        f"delta_value={delta_value}, delta_pct={delta_pct}"
    )


def _fallback_best_selling_products(connection: sqlite3.Connection, normalized: str) -> str | None:
    best_selling_match = re.search(
        r"best selling products(?: top (\d+))?(?: last (\d+) days)?",
        normalized,
    )
    if best_selling_match is None:
        return None

    top_n = int(best_selling_match.group(1) or 10)
    trailing_days = int(best_selling_match.group(2) or 30)
    return _format_rows(
        connection.execute(
            """
            SELECT p.product_name, CAST(COALESCE(SUM(foi.quantity), 0) AS INTEGER) AS items_sold
            FROM fact_order_items foi
            JOIN dim_product p ON p.product_key = foi.product_key
            JOIN dim_date d ON d.date_key = foi.order_date_key
            WHERE foi.order_status = 'completed'
              AND d.full_date >= (
                  SELECT DATE(MAX(d2.full_date), ?)
                  FROM fact_order_items foi2
                  JOIN dim_date d2 ON d2.date_key = foi2.order_date_key
              )
            GROUP BY p.product_name
            ORDER BY items_sold DESC, p.product_name
            LIMIT ?
            """,
            (f"-{trailing_days - 1} day", top_n),
        ).fetchall()
    )


def _fallback_net_revenue_last_7_days(
    connection: sqlite3.Connection, normalized: str
) -> str | None:
    if "net revenue" not in normalized or "last 7 days" not in normalized:
        return None
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


def _fallback_payments_by_method_yesterday(
    connection: sqlite3.Connection, normalized: str
) -> str | None:
    if "payments by method" not in normalized or "yesterday" not in normalized:
        return None
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


def _fallback_tickets_by_type_yesterday(
    connection: sqlite3.Connection, normalized: str
) -> str | None:
    if "tickets by type" not in normalized or "yesterday" not in normalized:
        return None
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


def _fallback_daily_metric_trend(connection: sqlite3.Connection, normalized: str) -> str | None:
    trend_match = re.search(r"daily (\w+(?: \w+)*) for the last (\d+) days", normalized)
    if trend_match is None:
        return None

    metric_name = _map_metric(trend_match.group(1))
    if metric_name is None:
        return None

    days = int(trend_match.group(2))
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


def _semantic_week_vs_week_revenue(normalized: str) -> SemanticQuery | None:
    if "week vs week revenue" not in normalized and "week over week revenue" not in normalized:
        return None
    return SemanticQuery(
        metric="net_revenue",
        grain="week",
        compare_to="previous_period",
    )


def _semantic_best_selling_products(normalized: str) -> SemanticQuery | None:
    best_selling_match = re.search(
        r"best selling products(?: top (\d+))?(?: last (\d+) days)?",
        normalized,
    )
    if best_selling_match is None:
        return None

    top_n = int(best_selling_match.group(1) or 10)
    trailing_days = int(best_selling_match.group(2) or 30)
    return SemanticQuery(
        metric="items_sold",
        dimensions=("product",),
        trailing_days=trailing_days,
        aggregate_over_time=True,
        order_by_metric="items_sold",
        order_direction="desc",
        limit=top_n,
    )


def _semantic_net_revenue_last_7_days(normalized: str) -> SemanticQuery | None:
    if "net revenue" not in normalized or "last 7 days" not in normalized:
        return None
    return SemanticQuery(metric="net_revenue", trailing_days=7, aggregate_over_time=True)


def _semantic_payments_by_method_yesterday(normalized: str) -> SemanticQuery | None:
    if "payments by method" not in normalized or "yesterday" not in normalized:
        return None
    return SemanticQuery(
        metric="payments_collected",
        dimensions=("payment_method",),
        day_offset=1,
        aggregate_over_time=True,
    )


def _semantic_tickets_by_type_yesterday(normalized: str) -> SemanticQuery | None:
    if "tickets by type" not in normalized or "yesterday" not in normalized:
        return None
    return SemanticQuery(
        metric="ticket_count",
        dimensions=("ticket_type",),
        day_offset=1,
        aggregate_over_time=True,
    )


def _semantic_daily_metric_trend(normalized: str) -> SemanticQuery | None:
    trend_match = re.search(r"daily (\w+(?: \w+)*) for the last (\d+) days", normalized)
    if trend_match is None:
        return None

    metric_name = _map_metric(trend_match.group(1))
    if metric_name is None:
        return None
    return SemanticQuery(metric=metric_name, trailing_days=int(trend_match.group(2)))


def _format_semantic_rows(rows: list[dict[str, object]], query: SemanticQuery) -> str:
    if not rows:
        return "No data found."
    formatted_rows: list[str] = []
    for row in rows:
        formatted_items: list[str] = []
        for key, value in row.items():
            output_key = key
            if key == "value" and query.aggregate_over_time and "kpi_date" not in row:
                output_key = query.metric
            formatted_items.append(f"{output_key}={value}")
        formatted_rows.append(", ".join(formatted_items))
    return "\n".join(formatted_rows)


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
