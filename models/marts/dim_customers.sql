{{ config(
    materialized='table',
    schema='gold',
    tags=['marts', 'dimensions']
) }}

WITH customer_orders AS (
    SELECT * FROM {{ ref('int_customer_orders') }}
)

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    country,
    state,
    customer_status,
    signup_date,
    total_orders,
    total_spent,
    ROUND(total_spent / NULLIF(total_orders, 0), 2) as avg_order_value,
    last_order_date,
    first_order_date,
    days_since_signup,
    CASE
        WHEN customer_status = 'active' AND last_order_date >= DATE_ADD(CURRENT_DATE(), -90) THEN 'Active Recent'
        WHEN customer_status = 'active' THEN 'Active Dormant'
        WHEN customer_status = 'inactive' THEN 'Inactive'
        WHEN customer_status = 'churned' THEN 'Churned'
        ELSE 'Unknown'
    END as customer_segment,
    CURRENT_TIMESTAMP() as updated_at
FROM customer_orders
