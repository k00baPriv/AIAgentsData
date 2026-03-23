# SQLite Multi-Object Star Schema Demo

This project now simulates 6 operational business objects:

- `customers`
- `products`
- `orders`
- `order_items`
- `payments`
- `support_tickets`

It then loads a star schema with multiple dimensions and multiple fact tables.

## Project layout

- `src/datamart/db.py`: operational schema plus warehouse schema
- `src/datamart/simulator.py`: daily generation of the 6 business objects
- `src/datamart/warehouse.py`: ETL into dimensions, facts, and historical KPIs
- `src/datamart/qa.py`: example natural-language analytics queries
- `src/datamart/main.py`: CLI

## Quick start

```bash
./.venv/bin/pip install -e .
./.venv/bin/python -m src.datamart.main simulate --days 90 --reset
./.venv/bin/python -m src.datamart.main summary
./.venv/bin/python -m src.datamart.main star
```

Example questions:

```bash
./.venv/bin/python -m src.datamart.main ask "What was net revenue in the last 7 days?"
./.venv/bin/python -m src.datamart.main ask "Show payments by method yesterday"
./.venv/bin/python -m src.datamart.main ask "Show tickets by type yesterday"
```

The `ask` command now resolves supported questions through an Ibis semantic layer backed by
[`semantic_model.yaml`](/Users/kuba/PycharmProjects/PythonProject/AIAgentsData/semantic_model.yaml)
instead of hand-written SQL tied directly to the warehouse tables.

You can also query the semantic layer directly from Python:

```python
from datamart import SemanticQuery, run_semantic_query
from datamart.db import get_connection

connection = get_connection()
rows = run_semantic_query(
    connection,
    SemanticQuery(metric="payments_collected", dimensions=("payment_method",), day_offset=1),
)
print(rows)
```

## Warehouse design

Dimensions:

- `dim_date`
- `dim_country`
- `dim_segment`
- `dim_customer`
- `dim_category`
- `dim_brand`
- `dim_product`
- `dim_channel`
- `dim_shipment_mode`
- `dim_payment_method`
- `dim_ticket_type`
- `dim_priority`
- `dim_kpi`

Facts:

- `fact_order_items`
- `fact_payments`
- `fact_support_tickets`
- `fact_daily_kpis`
- `fact_weekly_kpis`
- `fact_monthly_kpis`

## Aggregation grains

KPI history is now stored at three grains:

- daily
- weekly
- monthly

This is intentional so you can study grain directly in the physical model instead of hiding it in one mixed table.

## Study path

1. Read `simulator.py` to understand the operational events.
2. Read `db.py` to understand the logical warehouse model.
3. Read `warehouse.py` to see how the dimensions and facts are populated.
4. Read `AGENT_SQL_GUIDE.md` to see how an AI agent should query the schema.
