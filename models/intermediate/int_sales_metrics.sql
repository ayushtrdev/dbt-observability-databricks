{{ config(
    materialized='table',
    schema='silver',
    tags=['intermediate', 'metrics']
) }}

WITH order_items AS (
    SELECT * FROM {{ ref('int_order_items') }}
),

order_summary AS (
    SELECT
        order_id,
        customer_id,
        SUM(sale_amount) as total_order_amount,
        SUM(cost_amount) as total_cost,
        SUM(gross_profit) as total_profit,
        COUNT(sale_id) as items_count,
        AVG(profit_margin_pct) as avg_profit_margin,
        MIN(sale_date) as order_date
    FROM order_items
    GROUP BY order_id, customer_id
)

SELECT
    order_id,
    customer_id,
    total_order_amount,
    total_cost,
    total_profit,
    ROUND(total_profit / NULLIF(total_order_amount, 0) * 100, 2) as order_profit_margin_pct,
    items_count,
    avg_profit_margin,
    order_date,
    CURRENT_TIMESTAMP() as calculated_at
FROM order_summary
WHERE total_order_amount > 0
