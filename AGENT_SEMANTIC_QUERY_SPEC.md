# Agent Semantic Query Spec

Use this file when translating natural-language analytics requests into semantic query objects.

Do not generate raw SQL unless explicitly asked.
Do not generate executable Python wrappers unless explicitly asked.

## Goal

Translate business questions into semantic queries against the semantic layer, not against the raw database structure.

The source of truth is:

- [`semantic_model.yaml`](semantic_model.yaml)

This Markdown file is the complete API contract for the agent.
Do not assume access to application source code.

## Output Format

Default output format:

```python
SemanticQuery(
    metric="new_customers",
    grain="day",
    dimensions=(),
    trailing_days=14,
    day_offset=None,
    aggregate_over_time=False,
    compare_to=None,
    order_by_metric=None,
    order_direction="desc",
    limit=None,
)
```

Do not wrap it in:

- `run_semantic_query(...)`
- `rows = ...`
- import statements
- explanations

Unless the user explicitly asks for another format, return only the `SemanticQuery(...)` object snippet.

Alternative machine-friendly format if requested:

```yaml
metric: new_customers
grain: day
dimensions: []
trailing_days: 14
day_offset: null
aggregate_over_time: false
compare_to: null
order_by_metric: null
order_direction: desc
limit: null
```

## SemanticQuery Contract

The agent must assume the query object has exactly these supported fields:

- `metric: str`
- `grain: str = "day"`
- `dimensions: tuple[str, ...] = ()`
- `trailing_days: int | None = None`
- `day_offset: int | None = None`
- `aggregate_over_time: bool = False`
- `compare_to: str | None = None`
- `order_by_metric: str | None = None`
- `order_direction: str = "desc"`
- `limit: int | None = None`

Field meaning:

- `metric`: business metric from the semantic model.
- `grain`: one of `day`, `week`, `month`, if allowed by the metric.
- `dimensions`: semantic grouping dimensions, not physical column names.
- `trailing_days`: rolling window ending on the latest available date.
- `day_offset`: relative day filter from the latest available date.
  - `day_offset=1` means yesterday.
- `aggregate_over_time`:
  - `False`: keep the time grain in the output when applicable.
  - `True`: collapse the selected time window into one aggregate result per dimension set.
- `compare_to`: period comparison mode. Currently only `previous_period` is supported.
- `order_by_metric`: metric used for sorting grouped results. For the current API, this must
  match `metric` when present.
- `order_direction`: `asc` or `desc`.
- `limit`: maximum number of rows to return after sorting.

The agent must not invent extra fields such as:

- `trailing_weeks`
- `trailing_months`
- `start_date`
- `end_date`
- `filters`
- `order_by`

Comparison constraints:

- `compare_to` may only be `previous_period`.
- comparisons do not currently support grouped dimensions
- comparisons do not currently support ranking or limits
- comparisons do not currently combine with `trailing_days` or `day_offset`

## Supported Metrics

Use these metric ids exactly:

- `net_revenue`
- `gross_revenue`
- `items_sold`
- `completed_orders`
- `avg_order_value`
- `payments_collected`
- `refund_amount`
- `ticket_count`
- `open_ticket_count`
- `new_customers`
- `new_products`

## Allowed Dimensions By Metric

- `net_revenue`: `country`, `segment`, `category`, `brand`, `product`, `channel`
- `gross_revenue`: `country`, `segment`, `category`, `brand`, `product`, `channel`
- `items_sold`: `country`, `segment`, `category`, `brand`, `product`, `channel`
- `completed_orders`: `country`, `segment`, `category`, `brand`, `product`, `channel`
- `avg_order_value`: none
- `payments_collected`: `payment_method`
- `refund_amount`: `payment_method`
- `ticket_count`: `country`, `segment`, `ticket_type`, `priority`
- `open_ticket_count`: `country`, `segment`, `ticket_type`, `priority`
- `new_customers`: none
- `new_products`: `category`, `brand`

Do not invent dimensions outside this list.

## Allowed Grains

All current metrics allow:

- `day`
- `week`
- `month`

Use `grain="day"` unless the user explicitly asks for weekly or monthly output.

## Time Interpretation Rules

Map common phrases like this:

- `last 7 days` -> `trailing_days=7`
- `last 14 days` -> `trailing_days=14`
- `yesterday` -> `day_offset=1`
- `today` -> `day_offset=0`

Current API limitation:

- There is no `trailing_weeks` or `trailing_months` field.
- For weekly requests like "last 3 weeks", approximate with `trailing_days=21`.
- For monthly requests like "last 2 months", the API does not yet support an explicit semantic month window.

## Output Shape Rules

Expected outputs:

- Scalar aggregate:

```python
{"value": 1234.56}
```

- Grouped aggregate:

```python
{"category_name": "Accessories", "value": 1234.56}
```

- Daily time series:

```python
{"kpi_date": "2026-03-10", "value": 8}
```

Important:

- `day` grain with `aggregate_over_time=False` typically includes `kpi_date`.
- Weekly and monthly queries currently do not expose a dedicated period label in the same way.

## Query Construction Rules

Always follow these rules:

1. Use `SemanticQuery`, not SQL.
2. Choose `metric` from the semantic model.
3. Use only dimensions allowed for that metric.
4. Use only allowed grains.
5. Prefer semantic business names, not raw table or column names.
6. If the user asks for something unsupported, say so clearly.
7. Do not invent filters, joins, dimensions, or metrics.
8. Do not sum `avg_order_value` across periods manually.

## Mapping Examples

Revenue by product category:

```python
SemanticQuery(
    metric="net_revenue",
    dimensions=("category",),
    aggregate_over_time=True,
)
```

Best selling products:

```python
SemanticQuery(
    metric="items_sold",
    dimensions=("product",),
    trailing_days=30,
    aggregate_over_time=True,
    order_by_metric="items_sold",
    order_direction="desc",
    limit=10,
)
```

Daily new customers last 14 days:

```python
SemanticQuery(
    metric="new_customers",
    grain="day",
    trailing_days=14,
)
```

Weekly new customers last 3 weeks:

```python
SemanticQuery(
    metric="new_customers",
    grain="week",
    trailing_days=21,
)
```

Week vs week revenue:

```python
SemanticQuery(
    metric="net_revenue",
    grain="week",
    compare_to="previous_period",
)
```

Payments by method yesterday:

```python
SemanticQuery(
    metric="payments_collected",
    dimensions=("payment_method",),
    day_offset=1,
    aggregate_over_time=True,
)
```

Tickets by type yesterday:

```python
SemanticQuery(
    metric="ticket_count",
    dimensions=("ticket_type",),
    day_offset=1,
    aggregate_over_time=True,
)
```

Revenue by country for the last 30 days:

```python
SemanticQuery(
    metric="net_revenue",
    dimensions=("country",),
    trailing_days=30,
    aggregate_over_time=True,
)
```

## Unsupported Or Ambiguous Requests

If the request needs unsupported semantics, do not guess.

Examples:

- "last 3 complete weeks" -> current API has no exact complete-week window control
- "year over year revenue" -> not supported cleanly in the semantic model
- "revenue by shipment mode" -> not allowed for `net_revenue` in the current semantic model
- "customer-level order inspection" -> this is raw exploration, not a semantic KPI query

Preferred response pattern:

```text
This cannot be expressed cleanly with the current SemanticQuery API because ...
```

## Recommended Agent Prompt

Use this instruction block for an AI agent:

```text
You must translate user analytics questions into SemanticQuery objects.

Use semantic_model.yaml and AGENT_SEMANTIC_QUERY_SPEC.md as the complete source of truth.

Rules:
- Do not generate SQL.
- Do not reference raw database tables unless explicitly asked.
- Use only supported metrics, dimensions, grains, and time filters.
- If the request is unsupported or ambiguous, explain that instead of guessing.
- Return only a SemanticQuery(...) snippet unless the user asks for another format.
```

## Minimal Agent Context

If the agent can only read two files, they should be:

- [`semantic_model.yaml`](semantic_model.yaml)
- [`AGENT_SEMANTIC_QUERY_SPEC.md`](AGENT_SEMANTIC_QUERY_SPEC.md)

That is sufficient context for generating semantic queries.
