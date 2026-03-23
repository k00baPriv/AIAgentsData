from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, cast

ROOT_DIR = Path(__file__).resolve().parents[2]
SEMANTIC_MODEL_PATH = ROOT_DIR / "semantic_model.yaml"

try:
    import ibis  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover - exercised when optional deps are absent.
    ibis = None

try:
    import yaml  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover - exercised when optional deps are absent.
    yaml = None

DIMENSION_LABEL_FIELDS = {
    "country": "country_code",
    "segment": "segment_name",
    "category": "category_name",
    "brand": "brand_name",
    "product": "product_name",
    "channel": "channel_name",
    "shipment_mode": "shipment_mode_name",
    "payment_method": "payment_method_name",
    "ticket_type": "ticket_type_name",
    "priority": "priority_name",
}

KPI_DIMENSIONAL_METRICS = {
    ("net_revenue", "segment"): "revenue_by_segment",
    ("net_revenue", "country"): "revenue_by_country",
    ("net_revenue", "category"): "revenue_by_category",
    ("net_revenue", "channel"): "revenue_by_channel",
    ("payments_collected", "payment_method"): "payments_by_method",
    ("ticket_count", "ticket_type"): "tickets_by_type",
}


class SemanticQueryError(RuntimeError):
    pass


class SemanticQueryDependencyError(SemanticQueryError):
    pass


@dataclass(frozen=True)
class SemanticQuery:
    metric: str
    grain: str = "day"
    dimensions: tuple[str, ...] = ()
    trailing_days: int | None = None
    day_offset: int | None = None
    aggregate_over_time: bool = False
    compare_to: str | None = None
    order_by_metric: str | None = None
    order_direction: str = "desc"
    limit: int | None = None


def load_semantic_model(model_path: Path | None = None) -> dict[str, Any]:
    if yaml is None:
        raise SemanticQueryDependencyError(
            "Semantic querying requires PyYAML. Install project dependencies first."
        )
    path = model_path or SEMANTIC_MODEL_PATH
    return yaml.safe_load(path.read_text())


def run_semantic_query(
    connection: sqlite3.Connection,
    query: SemanticQuery,
    model_path: Path | None = None,
) -> list[dict[str, object]]:
    if ibis is None:
        raise SemanticQueryDependencyError(
            "Semantic querying requires Ibis. Install project dependencies first."
        )

    model = load_semantic_model(model_path)
    metric_model = _get_metric_model(model, query.metric)
    _validate_query(metric_model, query)
    reference_date = _max_available_date(connection)

    backend = ibis.sqlite.connect(_sqlite_db_path(connection))
    if query.compare_to is not None:
        return _run_period_comparison_query(backend, model, query)
    if _should_use_kpi_facts(model, query):
        expr = _build_kpi_expression(backend, model, query, reference_date)
    else:
        expr = _build_base_fact_expression(backend, model, metric_model, query, reference_date)

    result = expr.execute()
    return _normalize_result(result)


def _get_metric_model(model: dict[str, Any], metric: str) -> dict[str, Any]:
    metrics = model.get("metrics", {})
    if metric not in metrics:
        raise SemanticQueryError(f"Metric '{metric}' is not defined in semantic_model.yaml.")
    return metrics[metric]


def _validate_query(metric_model: dict[str, Any], query: SemanticQuery) -> None:
    _validate_grain(metric_model, query)
    _validate_dimensions(metric_model, query)
    _validate_time_window(query)
    _validate_comparison(query)
    _validate_ranking(query)


def _validate_grain(metric_model: dict[str, Any], query: SemanticQuery) -> None:
    allowed_grains = set(metric_model.get("allowed_grains", []))
    if query.grain not in allowed_grains:
        raise SemanticQueryError(f"Metric '{query.metric}' does not allow grain '{query.grain}'.")


def _validate_dimensions(metric_model: dict[str, Any], query: SemanticQuery) -> None:
    allowed_dimensions = set(metric_model.get("allowed_dimensions", []))
    unsupported_dimensions = [dim for dim in query.dimensions if dim not in allowed_dimensions]
    if unsupported_dimensions:
        dimension_list = ", ".join(unsupported_dimensions)
        raise SemanticQueryError(
            f"Metric '{query.metric}' does not allow dimensions: {dimension_list}."
        )


def _validate_time_window(query: SemanticQuery) -> None:
    if query.trailing_days is not None and query.trailing_days <= 0:
        raise SemanticQueryError("trailing_days must be greater than zero.")

    if query.day_offset is not None and query.day_offset < 0:
        raise SemanticQueryError("day_offset must be zero or greater.")


def _validate_comparison(query: SemanticQuery) -> None:
    if query.compare_to is None:
        return
    if query.compare_to != "previous_period":
        raise SemanticQueryError("compare_to must be 'previous_period' when provided.")
    if query.dimensions:
        raise SemanticQueryError("Period comparisons do not support grouped dimensions yet.")
    if query.trailing_days is not None or query.day_offset is not None:
        raise SemanticQueryError("Period comparisons cannot be combined with time filters yet.")


def _validate_ranking(query: SemanticQuery) -> None:
    if query.order_direction not in {"asc", "desc"}:
        raise SemanticQueryError("order_direction must be either 'asc' or 'desc'.")

    if query.limit is not None and query.limit <= 0:
        raise SemanticQueryError("limit must be greater than zero.")

    if query.order_by_metric is not None and query.order_by_metric != query.metric:
        raise SemanticQueryError("order_by_metric must match metric for single-metric queries.")
    if query.compare_to is not None and (
        query.order_by_metric is not None or query.limit is not None
    ):
        raise SemanticQueryError("Period comparisons do not support ranking or limits.")


def _should_use_kpi_facts(model: dict[str, Any], query: SemanticQuery) -> bool:
    if query.compare_to is not None:
        return True
    policies = model.get("query_policies", {})
    if not policies.get("prefer_preaggregated_kpi_facts", False):
        return False
    if len(query.dimensions) > 1:
        return False
    if query.grain not in {"day", "week", "month"}:
        return False
    if query.dimensions and (query.metric, query.dimensions[0]) not in KPI_DIMENSIONAL_METRICS:
        return False
    return True


def _build_kpi_expression(
    backend: Any, model: dict[str, Any], query: SemanticQuery, reference_date: date
) -> Any:
    fact_name = _preferred_kpi_fact_name(model, query)
    fact = backend.table(fact_name)
    dim_kpi = backend.table("dim_kpi")
    date_dim = backend.table("dim_date")

    kpi_name = _resolve_kpi_name(query)
    expr = fact.join(dim_kpi, fact.kpi_key == dim_kpi.kpi_key)

    date_key_column = _period_date_key_column(query.grain)
    expr = expr.join(date_dim, expr[date_key_column] == date_dim.date_key)
    expr = expr.filter(dim_kpi.kpi_name == kpi_name)
    expr = _apply_time_filter(expr, query, reference_date)
    expr = _apply_default_kpi_dimension_filters(backend, model, expr, query)

    selected_dimensions = list(query.dimensions)
    if query.grain == "day" and not query.aggregate_over_time:
        selected_dimensions.insert(0, "date")

    group_columns = {name: _group_column(name, model, expr) for name in selected_dimensions}

    if group_columns:
        expr = expr.group_by(list(group_columns.values())).aggregate(value=expr.kpi_value.sum())
        projection = {
            _output_column_name(name): column.name(_output_column_name(name))
            for name, column in group_columns.items()
        }
        projection["value"] = expr.value.round(2).name("value")
        expr = expr.select(**projection)
    else:
        aggregation = expr.aggregate(value=expr.kpi_value.sum())
        expr = aggregation.select(value=aggregation.value.round(2).name("value"))

    order_columns = [_output_column_name(name) for name in selected_dimensions]
    return _apply_sort_and_limit(expr, query, order_columns)


def _build_base_fact_expression(
    backend: Any,
    model: dict[str, Any],
    metric_model: dict[str, Any],
    query: SemanticQuery,
    reference_date: date,
) -> Any:
    fact_name = metric_model["default_source_fact"]
    expr = backend.table(fact_name)

    date_dimension = backend.table("dim_date")
    date_key_column = _base_fact_date_key_column(fact_name)
    expr = expr.join(date_dimension, expr[date_key_column] == date_dimension.date_key)
    expr = _apply_time_filter(expr, query, reference_date)
    expr = _apply_base_filters(expr, metric_model.get("base_filters", []))
    expr = _join_dimensions(backend, model, expr, query.dimensions)

    selected_dimensions = list(query.dimensions)
    if query.grain == "day" and not query.aggregate_over_time:
        selected_dimensions.insert(0, "date")

    group_columns: dict[str, Any] = {}
    for dimension_name in selected_dimensions:
        column = _group_column(dimension_name, model, expr)
        group_columns[dimension_name] = column

    measure = _build_measure(expr, metric_model["sql_measure"])
    if group_columns:
        expr = expr.group_by(list(group_columns.values())).aggregate(value=measure)
        projection = {
            _output_column_name(name): column.name(_output_column_name(name))
            for name, column in group_columns.items()
        }
        projection["value"] = expr.value.round(2).name("value")
        expr = expr.select(**projection)
        order_columns = [_output_column_name(name) for name in selected_dimensions]
        return _apply_sort_and_limit(expr, query, order_columns)

    aggregation = expr.aggregate(value=measure)
    return aggregation.select(value=aggregation.value.round(2).name("value"))


def _preferred_kpi_fact_name(model: dict[str, Any], query: SemanticQuery) -> str:
    metric_model = model["metrics"][query.metric]
    preferred_facts = metric_model.get("preferred_grain_facts", {})
    fact_name = preferred_facts.get(query.grain)
    if fact_name is None:
        raise SemanticQueryError(
            f"Metric '{query.metric}' does not declare a preferred fact for grain '{query.grain}'."
        )
    return fact_name


def _resolve_kpi_name(query: SemanticQuery) -> str:
    if not query.dimensions:
        return query.metric
    if len(query.dimensions) != 1:
        raise SemanticQueryError(
            "KPI fact queries currently support at most one semantic dimension."
        )
    key = (query.metric, query.dimensions[0])
    if key not in KPI_DIMENSIONAL_METRICS:
        dimension_name = query.dimensions[0]
        raise SemanticQueryError(
            f"No pre-aggregated KPI mapping exists for metric "
            f"'{query.metric}' by '{dimension_name}'."
        )
    return KPI_DIMENSIONAL_METRICS[key]


def _run_period_comparison_query(
    backend: Any, model: dict[str, Any], query: SemanticQuery
) -> list[dict[str, object]]:
    fact_name = _preferred_kpi_fact_name(model, query)
    fact = backend.table(fact_name)
    dim_kpi = backend.table("dim_kpi")
    date_dim = backend.table("dim_date")

    period_key = _period_date_key_column(query.grain)
    expr = fact.join(dim_kpi, fact.kpi_key == dim_kpi.kpi_key)
    expr = expr.join(date_dim, expr[period_key] == date_dim.date_key)
    expr = expr.filter(dim_kpi.kpi_name == query.metric)
    expr = _apply_default_kpi_dimension_filters(backend, model, expr, query)
    expr = (
        expr.select(
            period_start=date_dim.full_date.name("period_start"),
            value=expr.kpi_value.round(2).name("value"),
        )
        .order_by(ibis.desc("period_start"))
        .limit(2)
    )

    rows = _normalize_result(expr.execute())
    if len(rows) < 2:
        raise SemanticQueryError("Not enough periods are available for comparison.")
    return [_build_period_comparison_result(rows[0], rows[1])]


def _build_period_comparison_result(
    current_row: dict[str, object], previous_row: dict[str, object]
) -> dict[str, object]:
    current_value = float(cast(float, current_row["value"]))
    previous_value = float(cast(float, previous_row["value"]))
    delta_value = round(current_value - previous_value, 2)
    delta_pct: float | None = None
    if previous_value != 0:
        delta_pct = round((delta_value / previous_value) * 100, 2)

    return {
        "current_period_start": current_row["period_start"],
        "previous_period_start": previous_row["period_start"],
        "current_value": current_value,
        "previous_value": previous_value,
        "delta_value": delta_value,
        "delta_pct": delta_pct,
    }


def _apply_time_filter(expr: Any, query: SemanticQuery, reference_date: date) -> Any:
    if query.trailing_days is not None:
        start = reference_date - timedelta(days=query.trailing_days - 1)
        return expr.filter(expr.full_date >= start.isoformat())
    if query.day_offset is not None:
        target = reference_date - timedelta(days=query.day_offset)
        return expr.filter(expr.full_date == target.isoformat())
    return expr


def _apply_default_kpi_dimension_filters(
    backend: Any,
    model: dict[str, Any],
    expr: Any,
    query: SemanticQuery,
) -> Any:
    selected_dimensions = set(query.dimensions)

    for dimension_name, dimension_model in model.get("dimensions", {}).items():
        if dimension_name == "date":
            continue
        key_name = dimension_model["key"]
        if key_name not in expr.columns:
            continue

        dim_table = backend.table(dimension_model["table"])
        label_field = dimension_model.get("label_field")
        if label_field is None:
            continue

        expr = expr.join(dim_table, expr[key_name] == dim_table[key_name])
        label_column = dim_table[label_field]
        if dimension_name in selected_dimensions:
            expr = expr.filter(label_column != "all")
        else:
            expr = expr.filter(label_column == "all")

    return expr


def _apply_base_filters(expr: Any, filters: list[str]) -> Any:
    for raw_filter in filters:
        column_name, expected_value = _parse_base_filter(raw_filter)
        expr = expr.filter(expr[column_name] == expected_value)
    return expr


def _parse_base_filter(raw_filter: str) -> tuple[str, str]:
    parts = raw_filter.split("=", maxsplit=1)
    if len(parts) != 2:
        raise SemanticQueryError(f"Unsupported base filter '{raw_filter}'.")
    column_name = parts[0].strip()
    expected_value = parts[1].strip().strip("'").strip('"')
    return column_name, expected_value


def _build_measure(expr: Any, sql_measure: str) -> Any:
    normalized = sql_measure.strip().upper()
    if normalized == "COUNT(*)":
        return expr.count()
    if normalized.startswith("SUM(") and normalized.endswith(")"):
        column_name = sql_measure[4:-1].strip()
        return expr[column_name].sum()
    if normalized.startswith("AVG(") and normalized.endswith(")"):
        column_name = sql_measure[4:-1].strip()
        return expr[column_name].mean()
    if normalized.startswith("COUNT(DISTINCT ") and normalized.endswith(")"):
        column_name = sql_measure[15:-1].strip()
        return expr[column_name].nunique()
    raise SemanticQueryError(f"Unsupported sql_measure '{sql_measure}'.")


def _join_dimensions(
    backend: Any, model: dict[str, Any], expr: Any, dimensions: tuple[str, ...]
) -> Any:
    for dimension_name in dimensions:
        dimension_model = model["dimensions"][dimension_name]
        key_name = dimension_model["key"]
        label_field = dimension_model.get("label_field")
        if label_field is None or label_field in expr.columns or key_name not in expr.columns:
            continue
        dim_table = backend.table(dimension_model["table"])
        expr = expr.join(dim_table, expr[key_name] == dim_table[key_name])
    return expr


def _group_column(dimension_name: str, model: dict[str, Any], expr: Any) -> Any:
    if dimension_name == "date":
        return expr.full_date

    dimension_model = model["dimensions"][dimension_name]
    label_field = dimension_model.get("label_field")
    if label_field is None:
        raise SemanticQueryError(f"Dimension '{dimension_name}' does not expose a label field.")

    if label_field in expr.columns:
        return expr[label_field]
    raise SemanticQueryError(f"Dimension '{dimension_name}' was not joined into the query.")


def _period_date_key_column(grain: str) -> str:
    mapping = {
        "day": "date_key",
        "week": "week_start_date_key",
        "month": "month_start_date_key",
    }
    return mapping[grain]


def _base_fact_date_key_column(fact_name: str) -> str:
    mapping = {
        "fact_order_items": "order_date_key",
        "fact_payments": "payment_date_key",
        "fact_support_tickets": "created_date_key",
        "dim_customer": "signup_date_key",
        "dim_product": "created_date_key",
    }
    if fact_name not in mapping:
        raise SemanticQueryError(f"Unsupported default source fact '{fact_name}'.")
    return mapping[fact_name]


def _output_column_name(dimension_name: str) -> str:
    if dimension_name == "date":
        return "kpi_date"
    return DIMENSION_LABEL_FIELDS.get(dimension_name, dimension_name)


def _apply_sort_and_limit(expr: Any, query: SemanticQuery, fallback_order: list[str]) -> Any:
    if query.order_by_metric is not None or query.limit is not None:
        descending = query.order_direction == "desc"
        expr = expr.order_by(ibis.desc("value") if descending else ibis.asc("value"))
    elif fallback_order:
        expr = expr.order_by(fallback_order)

    if query.limit is not None:
        expr = expr.limit(query.limit)
    return expr


def _normalize_result(result: Any) -> list[dict[str, object]]:
    if hasattr(result, "to_dict"):
        rows = result.to_dict(orient="records")
        return [dict(row) for row in rows]
    if isinstance(result, list):
        return [dict(row) for row in result]
    if isinstance(result, dict):
        return [result]
    return [{"value": result}]


def _sqlite_db_path(connection: sqlite3.Connection) -> str:
    rows = connection.execute("PRAGMA database_list").fetchall()
    for row in rows:
        if row[1] == "main" and row[2]:
            return str(row[2])
    raise SemanticQueryError(
        "Semantic Ibis queries require a file-backed SQLite database connection."
    )


def _max_available_date(connection: sqlite3.Connection) -> date:
    row = connection.execute("SELECT MAX(full_date) AS full_date FROM dim_date").fetchone()
    if row is None or row["full_date"] is None:
        raise SemanticQueryError("No semantic dates are available. Build the warehouse first.")
    return date.fromisoformat(str(row["full_date"]))
