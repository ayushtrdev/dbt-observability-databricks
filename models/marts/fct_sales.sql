{{ config(
    materialized='table',
    schema='gold',
    tags=['marts', 'facts'],
    cluster_by=['sale_date', 'customer_id']
) }}

WITH order_items AS (
    SELECT * FROM {{ ref('int_order_items') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_dates') }}
)

SELECT
    oi.sale_id,
    oi.order_id,
    oi.customer_id,
    oi.product_id,
    d.date_key as sale_date_key,
    oi.sale_date,
    oi.quantity,
    oi.unit_price,
    oi.discount_percent,
    oi.sale_amount,
    oi.cost_amount,
    oi.gross_profit,
    oi.profit_margin_pct,
    oi.category,
    oi.subcategory,
    oi.warehouse_id,
    oi.promotion_id,
    CASE
        WHEN oi.profit_margin_pct >= 30 THEN 'High Margin'
        WHEN oi.profit_margin_pct >= 15 THEN 'Medium Margin'
        ELSE 'Low Margin'
    END as margin_category,
    CURRENT_TIMESTAMP() as updated_at
FROM order_items oi
LEFT JOIN dates d ON oi.sale_date = d.date_key
