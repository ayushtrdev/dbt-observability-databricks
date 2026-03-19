{{ config(
    materialized='table',
    schema='gold',
    tags=['marts', 'dimensions']
) }}

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

product_sales AS (
    SELECT
        product_id,
        COUNT(DISTINCT sale_id) as total_sales,
        SUM(quantity) as total_quantity_sold,
        SUM(sale_amount) as total_revenue,
        SUM(cost_amount) as total_cost,
        AVG(unit_price) as avg_selling_price,
        MIN(sale_date) as first_sale_date,
        MAX(sale_date) as last_sale_date
    FROM {{ ref('int_order_items') }}
    GROUP BY product_id
)

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.unit_cost,
    COALESCE(ps.total_sales, 0) as total_sales,
    COALESCE(ps.total_quantity_sold, 0) as total_quantity_sold,
    COALESCE(ps.total_revenue, 0) as total_revenue,
    COALESCE(ps.total_cost, 0) as total_cost,
    ROUND(COALESCE(ps.total_revenue, 0) - COALESCE(ps.total_cost, 0), 2) as total_profit,
    COALESCE(ps.avg_selling_price, 0) as avg_selling_price,
    ps.first_sale_date,
    ps.last_sale_date,
    CASE
        WHEN COALESCE(ps.total_sales, 0) > 1000 THEN 'High Volume'
        WHEN COALESCE(ps.total_sales, 0) > 100 THEN 'Medium Volume'
        WHEN COALESCE(ps.total_sales, 0) > 0 THEN 'Low Volume'
        ELSE 'No Sales'
    END as product_tier,
    CURRENT_TIMESTAMP() as updated_at
FROM products p
LEFT JOIN product_sales ps ON p.product_id = ps.product_id
