# AI Agent SQL Semantic Guide

Use this guide together with [semantic_model.yaml](/Users/kuba/PycharmProjects/PythonProject/AIAgentsData/semantic_model.yaml).

The purpose of this guide is to make the agent think in business terms first, then translate those terms into SQL over the warehouse.

## Core idea

The agent should not start from physical tables.

It should start from:

1. the business metric
2. the requested grain
3. the requested dimensions
4. the comparison period
5. the preferred semantic source

Only after that should it generate SQL.

## Semantic workflow

For each question, the agent should follow this sequence:

1. Identify the metric from `semantic_model.yaml`.
2. Identify the time grain: `day`, `week`, or `month`.
3. Identify whether the user wants:
   - scalar value
   - trend
   - breakdown
   - comparison
4. Identify allowed dimensions for that metric.
5. Choose the preferred fact table:
   - `fact_daily_kpis` for daily KPI questions
   - `fact_weekly_kpis` for weekly KPI questions
   - `fact_monthly_kpis` for monthly KPI questions
   - transaction facts only when the semantic model says they are needed
6. Apply semantic constraints.
7. Generate SQLite `SELECT` SQL only.

## How the agent should choose data sources

### Use KPI facts when

- the question is KPI-oriented
- the question is about trends over day/week/month
- the question compares week vs week or month vs month
- the question is a standard business metric from the semantic model

Examples:

- net revenue this week
- payments collected last month
- new customers by day for 14 days
- ticket count this month vs last month

### Use transaction facts when

- the user asks for detail not stored in KPI facts
- the user asks for a dimension not covered by KPI facts
- the user asks for raw transactional exploration

Examples:

- revenue by brand
- revenue by shipment mode
- top 10 customers by order revenue
- average discount by category

## Grain rules

The agent must not mix grains casually.

Rules:

- Use `fact_daily_kpis` for daily questions.
- Use `fact_weekly_kpis` for weekly questions.
- Use `fact_monthly_kpis` for monthly questions.
- Do not aggregate daily `avg_order_value` into weekly or monthly averages.
- Do not rebuild weekly KPI results from daily KPI rows if weekly facts already exist.

## Dimensional scope rules for KPI facts

For whole-business KPI rows, unrelated dimensions must be filtered to `'all'`.

This applies to:

- `fact_daily_kpis`
- `fact_weekly_kpis`
- `fact_monthly_kpis`

Whole-business filter pattern:

```sql
co.country_code = 'all'
AND s.segment_name = 'all'
AND c.category_name = 'all'
AND ch.channel_name = 'all'
AND pm.payment_method_name = 'all'
AND tt.ticket_type_name = 'all'
```

If the KPI is sliced by one dimension, keep the other semantic dimensions at `'all'`.

Example:

- for `payments_by_method`, show `payment_method`, keep country/segment/category/channel/ticket_type at `'all'`
- for `revenue_by_category`, show `category`, keep country/segment/channel/payment_method/ticket_type at `'all'`

## Metric-to-source guidance

### KPI-friendly metrics

These should normally come from KPI facts:

- `new_customers`
- `new_products`
- `gross_revenue`
- `net_revenue`
- `items_sold`
- `completed_orders`
- `avg_order_value`
- `payments_collected`
- `refund_amount`
- `ticket_count`
- `open_ticket_count`

### Dimension-specific KPI patterns

These also come from KPI facts:

- `revenue_by_segment`
- `revenue_by_country`
- `revenue_by_category`
- `revenue_by_channel`
- `payments_by_method`
- `tickets_by_type`

### Transaction-fact-first patterns

Use transaction facts for:

- revenue by brand
- shipment mode analysis
- customer-level order ranking
- raw order-item drilldown
- raw payment row inspection
- raw ticket row inspection

## Comparison logic

The semantic model supports these comparison patterns:

- day vs previous day
- week vs previous week
- month vs previous month
- trailing 7 days
- trailing 30 days

The agent should prefer the matching fact grain.

Examples:

- “compare this week to last week” -> `fact_weekly_kpis`
- “compare this month to last month” -> `fact_monthly_kpis`
- “last 7 days” -> usually `fact_daily_kpis`

## SQL prompt template

Use this template with the AI agent.

```text
Use semantic_model.yaml and AGENT_SQL_SEMANTIC_GUIDE.md as the source of truth.

Your task is to generate one SQLite SELECT query.

Process:
1. Resolve the business metric using semantic_model.yaml.
2. Resolve the requested grain.
3. Resolve any dimensions or comparisons.
4. Choose the preferred fact table from the semantic model.
5. Generate SQL using the warehouse schema only.

Rules:
- Return SQL only
- Use SQLite syntax only
- Use the semantic model, not guessed business logic
- Prefer KPI fact tables for KPI questions
- Prefer transaction facts only when the semantic model requires them
- Respect fact grain
- Do not generate INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, PRAGMA, ATTACH, DETACH, BEGIN, COMMIT, or VACUUM
- Do not invent columns, tables, joins, metrics, or dimensions
- For KPI facts, apply the required 'all' dimensional filters

Question:
{{USER_QUESTION}}
```

## Example semantic interpretation

Question:

`How did this week’s net revenue compare with last week?`

Semantic interpretation:

1. Metric: `net_revenue`
2. Grain: `week`
3. Shape: comparison
4. Preferred source: `fact_weekly_kpis`
5. Scope: whole business, so all non-time dimensions should be `'all'`

Question:

`Which brand generated the most revenue this month?`

Semantic interpretation:

1. Metric: `net_revenue`
2. Grain: `month`
3. Dimension: `brand`
4. KPI fact does not provide brand-level revenue
5. Use `fact_order_items` joined to `dim_brand` and `dim_date`

## Manual review checklist

Before executing SQL produced from the semantic layer, check:

1. Did the agent choose the correct business metric?
2. Did it choose the correct grain?
3. Did it use the preferred KPI fact when possible?
4. If it used a transaction fact, was that justified by the question?
5. Are requested dimensions allowed for that metric?
6. Are the `'all'` filters correct for KPI facts?
7. Is the comparison period aligned with the chosen grain?
8. Is it `SELECT` only?

## Red flags

Reject the SQL if:

- it answers a weekly question from `fact_daily_kpis`
- it answers a month comparison by summing daily averages
- it uses `fact_payments` to answer category or brand revenue
- it uses `fact_support_tickets` to answer payment questions
- it ignores semantic constraints from `semantic_model.yaml`
- it invents unsupported year-over-year logic
