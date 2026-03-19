{{ config(
    materialized='table',
    schema='bronze',
    tags=['staging', 'sales']
) }}

WITH source AS (
    SELECT
        sale_id,
        order_id,
        customer_id,
        product_id,
        quantity,
        unit_price,
        discount_percent,
        sale_amount,
        cost_amount,
        sale_date,
        sale_timestamp,
        warehouse_id,
        promotion_id,
        _ingestion_timestamp
    FROM {{ source('raw', 'sales') }}
)

SELECT
    sale_id,
    order_id,
    customer_id,
    product_id,
    COALESCE(quantity, 0) as quantity,
    COALESCE(unit_price, 0) as unit_price,
    COALESCE(discount_percent, 0) as discount_percent,
    COALESCE(sale_amount, 0) as sale_amount,
    COALESCE(cost_amount, 0) as cost_amount,
    sale_date,
    sale_timestamp,
    COALESCE(warehouse_id, 'Unknown') as warehouse_id,
    promotion_id,
    CURRENT_TIMESTAMP() as created_at,
    _ingestion_timestamp as source_timestamp
FROM source
WHERE sale_id IS NOT NULL
