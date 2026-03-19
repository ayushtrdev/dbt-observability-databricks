{{ config(
    materialized='table',
    schema='silver',
    tags=['intermediate', 'customers']
) }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
)

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.country,
    c.state,
    c.customer_status,
    c.signup_date,
    COUNT(o.order_id) as total_orders,
    COALESCE(SUM(o.order_amount), 0) as total_spent,
    MAX(o.order_date) as last_order_date,
    MIN(o.order_date) as first_order_date,
    DATEDIFF(day, c.signup_date, CURRENT_DATE()) as days_since_signup
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.country,
    c.state,
    c.customer_status,
    c.signup_date
