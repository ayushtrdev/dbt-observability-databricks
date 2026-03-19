{{ config(
    materialized='table',
    schema='bronze',
    tags=['staging', 'orders']
) }}

WITH source AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        order_timestamp,
        order_amount,
        currency,
        order_status,
        shipping_address_state,
        payment_method,
        _ingestion_timestamp
    FROM {{ source('raw', 'orders') }}
)

SELECT
    order_id,
    customer_id,
    order_date,
    order_timestamp,
    COALESCE(order_amount, 0) as order_amount,
    COALESCE(currency, 'USD') as currency,
    COALESCE(order_status, 'unknown') as order_status,
    COALESCE(shipping_address_state, 'Unknown') as shipping_address_state,
    COALESCE(payment_method, 'unknown') as payment_method,
    CURRENT_TIMESTAMP() as created_at,
    _ingestion_timestamp as source_timestamp
FROM source
WHERE order_id IS NOT NULL
