# AI Agent SQL Guide

Use this file as the source of truth when an AI agent generates SQLite SQL for this project.

The database now models 6 operational objects:

- `customers`
- `products`
- `orders`
- `order_items`
- `payments`
- `support_tickets`

For analytics, the agent should use the warehouse layer.

## Use these tables for analytics

Facts:

- `fact_order_items`
- `fact_payments`
- `fact_support_tickets`
- `fact_daily_kpis`
- `fact_weekly_kpis`
- `fact_monthly_kpis`

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

Avoid the operational tables for normal BI questions unless explicitly requested.

## Fact grain

This is the first thing the agent must get right.

### `fact_order_items`

Grain:

- one row per order item

Measures:

- `quantity`
- `unit_price`
- `gross_revenue`
- `net_revenue`

Use for:

- sales analysis
- units sold
- revenue by customer/segment/country/category/brand/channel

### `fact_payments`

Grain:

- one row per payment

Measures:

- `payment_amount`

Use for:

- collections
- refunds
- payment method analysis

### `fact_support_tickets`

Grain:

- one row per support ticket

Use for:

- ticket counts
- ticket type analysis
- support load by segment/country/priority

### `fact_daily_kpis`

Grain:

- one row per day per KPI per slice

Measure:

- `kpi_value`

Use for:

- historical KPI trends
- fast agent responses
- precomputed business metrics

### `fact_weekly_kpis`

Grain:

- one row per week per KPI per slice

Use for:

- weekly business reporting
- week-over-week comparisons
- avoiding agent-generated re-aggregation from daily rows

### `fact_monthly_kpis`

Grain:

- one row per month per KPI per slice

Use for:

- monthly reporting
- month-over-month comparisons
- executive summary style analytics

## Dimensions

### `dim_date`

- `date_key`
- `full_date`
- `year`
- `month`
- `day`

### `dim_country`

- `country_key`
- `country_code`

### `dim_segment`

- `segment_key`
- `segment_name`

### `dim_customer`

- `customer_key`
- `customer_id`
- `signup_date_key`
- `country_key`
- `segment_key`
- `customer_status`

### `dim_category`

- `category_key`
- `category_name`

### `dim_brand`

- `brand_key`
- `brand_name`

### `dim_product`

- `product_key`
- `product_id`
- `created_date_key`
- `category_key`
- `brand_key`
- `product_name`
- `base_price`
- `product_status`

### `dim_channel`

- `channel_key`
- `channel_name`

### `dim_shipment_mode`

- `shipment_mode_key`
- `shipment_mode_name`

### `dim_payment_method`

- `payment_method_key`
- `payment_method_name`

### `dim_ticket_type`

- `ticket_type_key`
- `ticket_type_name`

### `dim_priority`

- `priority_key`
- `priority_name`

### `dim_kpi`

- `kpi_key`
- `kpi_name`
- `description`

Known `kpi_name` values:

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
- `revenue_by_segment`
- `revenue_by_country`
- `revenue_by_category`
- `revenue_by_channel`
- `payments_by_method`
- `tickets_by_type`

## KPI scope rules

For whole-business KPI rows from `fact_daily_kpis`, filter to:

```sql
co.country_code = 'all'
AND s.segment_name = 'all'
AND c.category_name = 'all'
AND ch.channel_name = 'all'
AND pm.payment_method_name = 'all'
AND tt.ticket_type_name = 'all'
```

If the KPI is sliced by one dimension, keep the non-relevant dimensions at `'all'`.

Examples:

- `revenue_by_segment`
  Display `s.segment_name`, keep country/category/channel/payment_method/ticket_type at `'all'`.
- `revenue_by_channel`
  Display `ch.channel_name`, keep country/segment/category/payment_method/ticket_type at `'all'`.
- `payments_by_method`
  Display `pm.payment_method_name`, keep country/segment/category/channel/ticket_type at `'all'`.
- `tickets_by_type`
  Display `tt.ticket_type_name`, keep country/segment/category/channel/payment_method at `'all'`.

## Allowed joins

### Sales analysis

```sql
FROM fact_order_items fo
JOIN dim_date d ON d.date_key = fo.order_date_key
JOIN dim_customer cu ON cu.customer_key = fo.customer_key
JOIN dim_country co ON co.country_key = fo.country_key
JOIN dim_segment s ON s.segment_key = fo.segment_key
JOIN dim_product p ON p.product_key = fo.product_key
JOIN dim_category c ON c.category_key = fo.category_key
JOIN dim_brand b ON b.brand_key = fo.brand_key
JOIN dim_channel ch ON ch.channel_key = fo.channel_key
JOIN dim_shipment_mode sm ON sm.shipment_mode_key = fo.shipment_mode_key
```

### Payment analysis

```sql
FROM fact_payments fp
JOIN dim_date d ON d.date_key = fp.payment_date_key
JOIN dim_customer cu ON cu.customer_key = fp.customer_key
JOIN dim_payment_method pm ON pm.payment_method_key = fp.payment_method_key
```

### Support analysis

```sql
FROM fact_support_tickets ft
JOIN dim_date d ON d.date_key = ft.created_date_key
JOIN dim_customer cu ON cu.customer_key = ft.customer_key
JOIN dim_country co ON co.country_key = ft.country_key
JOIN dim_segment s ON s.segment_key = ft.segment_key
JOIN dim_ticket_type tt ON tt.ticket_type_key = ft.ticket_type_key
JOIN dim_priority pr ON pr.priority_key = ft.priority_key
```

### KPI analysis

```sql
FROM fact_daily_kpis fk
JOIN dim_date d ON d.date_key = fk.date_key
JOIN dim_kpi k ON k.kpi_key = fk.kpi_key
LEFT JOIN dim_country co ON co.country_key = fk.country_key
LEFT JOIN dim_segment s ON s.segment_key = fk.segment_key
LEFT JOIN dim_category c ON c.category_key = fk.category_key
LEFT JOIN dim_channel ch ON ch.channel_key = fk.channel_key
LEFT JOIN dim_payment_method pm ON pm.payment_method_key = fk.payment_method_key
LEFT JOIN dim_ticket_type tt ON tt.ticket_type_key = fk.ticket_type_key
```

Weekly KPI joins follow the same pattern, but use:

```sql
FROM fact_weekly_kpis fk
JOIN dim_date ds ON ds.date_key = fk.week_start_date_key
JOIN dim_date de ON de.date_key = fk.week_end_date_key
JOIN dim_kpi k ON k.kpi_key = fk.kpi_key
LEFT JOIN dim_country co ON co.country_key = fk.country_key
LEFT JOIN dim_segment s ON s.segment_key = fk.segment_key
LEFT JOIN dim_category c ON c.category_key = fk.category_key
LEFT JOIN dim_channel ch ON ch.channel_key = fk.channel_key
LEFT JOIN dim_payment_method pm ON pm.payment_method_key = fk.payment_method_key
LEFT JOIN dim_ticket_type tt ON tt.ticket_type_key = fk.ticket_type_key
```

Monthly KPI joins follow the same pattern, but use:

```sql
FROM fact_monthly_kpis fk
JOIN dim_date ds ON ds.date_key = fk.month_start_date_key
JOIN dim_date de ON de.date_key = fk.month_end_date_key
JOIN dim_kpi k ON k.kpi_key = fk.kpi_key
LEFT JOIN dim_country co ON co.country_key = fk.country_key
LEFT JOIN dim_segment s ON s.segment_key = fk.segment_key
LEFT JOIN dim_category c ON c.category_key = fk.category_key
LEFT JOIN dim_channel ch ON ch.channel_key = fk.channel_key
LEFT JOIN dim_payment_method pm ON pm.payment_method_key = fk.payment_method_key
LEFT JOIN dim_ticket_type tt ON tt.ticket_type_key = fk.ticket_type_key
```

## Example questions and SQL patterns

### Net revenue in the last 7 days

```sql
SELECT ROUND(SUM(fk.kpi_value), 2) AS net_revenue
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
  );
```

### Payments by method yesterday

```sql
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
ORDER BY fk.kpi_value DESC;
```

### Ticket count by type yesterday

```sql
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
ORDER BY fk.kpi_value DESC;
```

### Revenue by category from transaction facts

```sql
SELECT c.category_name, ROUND(SUM(fo.net_revenue), 2) AS net_revenue
FROM fact_order_items fo
JOIN dim_date d ON d.date_key = fo.order_date_key
JOIN dim_category c ON c.category_key = fo.category_key
WHERE fo.order_status = 'completed'
GROUP BY c.category_name
ORDER BY net_revenue DESC;
```

## SQL generation prompt

Use this prompt with your AI agent.

```text
You are generating SQLite SQL for a star schema analytics database.

Return SQL only.

Rules:
1. Use warehouse tables for analytics.
2. Prefer fact_daily_kpis for historical KPI questions.
3. Prefer fact_weekly_kpis for explicitly weekly questions.
4. Prefer fact_monthly_kpis for explicitly monthly questions.
5. Prefer fact_order_items for transaction-level sales analysis.
6. Prefer fact_payments for payment analysis.
7. Prefer fact_support_tickets for support analysis.
8. Respect fact grain exactly.
9. Use only documented tables and columns.
10. Use only documented joins.
11. Use SQLite-compatible syntax only.
12. Generate SELECT statements only.
13. Do not generate INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, PRAGMA, ATTACH, DETACH, BEGIN, COMMIT, or VACUUM.
14. For whole-business KPI rows in daily, weekly, or monthly KPI facts, filter the unrelated dimensions to 'all'.

Warehouse schema:
- fact_order_items(order_item_fact_key, order_item_id, order_id, order_date_key, customer_key, product_key, country_key, segment_key, category_key, brand_key, channel_key, shipment_mode_key, quantity, unit_price, discount_rate, gross_revenue, net_revenue, order_status)
- fact_payments(payment_fact_key, payment_id, order_id, payment_date_key, customer_key, payment_method_key, payment_amount, payment_status)
- fact_support_tickets(ticket_fact_key, ticket_id, created_date_key, customer_key, order_id, country_key, segment_key, ticket_type_key, priority_key, ticket_status)
- fact_daily_kpis(kpi_fact_key, date_key, kpi_key, country_key, segment_key, category_key, channel_key, payment_method_key, ticket_type_key, kpi_value)
- fact_weekly_kpis(kpi_fact_key, week_start_date_key, week_end_date_key, kpi_key, country_key, segment_key, category_key, channel_key, payment_method_key, ticket_type_key, kpi_value)
- fact_monthly_kpis(kpi_fact_key, month_start_date_key, month_end_date_key, kpi_key, country_key, segment_key, category_key, channel_key, payment_method_key, ticket_type_key, kpi_value)
- dim_date(date_key, full_date, year, month, day)
- dim_country(country_key, country_code)
- dim_segment(segment_key, segment_name)
- dim_customer(customer_key, customer_id, signup_date_key, country_key, segment_key, customer_status)
- dim_category(category_key, category_name)
- dim_brand(brand_key, brand_name)
- dim_product(product_key, product_id, created_date_key, category_key, brand_key, product_name, base_price, product_status)
- dim_channel(channel_key, channel_name)
- dim_shipment_mode(shipment_mode_key, shipment_mode_name)
- dim_payment_method(payment_method_key, payment_method_name)
- dim_ticket_type(ticket_type_key, ticket_type_name)
- dim_priority(priority_key, priority_name)
- dim_kpi(kpi_key, kpi_name, description)

Known KPI names:
- new_customers
- new_products
- gross_revenue
- net_revenue
- items_sold
- completed_orders
- avg_order_value
- payments_collected
- refund_amount
- ticket_count
- open_ticket_count
- revenue_by_segment
- revenue_by_country
- revenue_by_category
- revenue_by_channel
- payments_by_method
- tickets_by_type

Question:
{{USER_QUESTION}}
```

## Manual review checklist

Before running agent-generated SQL, check:

1. Is it `SELECT` only?
2. Did it choose the right fact table for the question?
3. Are all joins valid for this schema?
4. If it uses `fact_daily_kpis`, did it apply the correct `'all'` filters?
5. Are KPI names valid?
6. Is the aggregation consistent with fact grain?
7. Is the date logic based on `dim_date.full_date`?
8. Did it avoid invented columns and invented dimensions?
9. Does the result shape match the question?
10. Would adding `ORDER BY` or `LIMIT` make the output safer?

## Common mistakes to reject

- summing `avg_order_value` over multiple days and calling it an average
- using operational tables for normal analytics
- mixing `fact_order_items` and `fact_daily_kpis` unnecessarily
- forgetting `'all'` filters in KPI queries
- grouping by a dimension not present in the chosen fact
- treating `fact_order_items` as one row per order
- using payment facts to answer product/category questions
