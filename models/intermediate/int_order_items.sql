{{ config(
    materialized='table',
    schema='silver',
    tags=['intermediate', 'orders']
) }}

WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    s.sale_id,
    s.order_id,
    s.customer_id,
    s.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    s.quantity,
    s.unit_price,
    s.discount_percent,
    s.sale_amount,
    s.cost_amount,
    ROUND(s.sale_amount - s.cost_amount, 2) as gross_profit,
    ROUND((s.sale_amount - s.cost_amount) / NULLIF(s.sale_amount, 0) * 100, 2) as profit_margin_pct,
    s.sale_date,
    s.warehouse_id,
    s.promotion_id
FROM sales s
LEFT JOIN products p ON s.product_id = p.product_id
WHERE s.sale_amount > 0
