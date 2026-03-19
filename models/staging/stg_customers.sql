{{ config(
    materialized='table',
    schema='bronze',
    tags=['staging', 'customers']
) }}

WITH source AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        country,
        state,
        customer_status,
        signup_date,
        _ingestion_timestamp
    FROM {{ source('raw', 'customers') }}
)

SELECT
    customer_id,
    COALESCE(first_name, 'Unknown') as first_name,
    COALESCE(last_name, 'Unknown') as last_name,
    LOWER(COALESCE(email, 'no_email@unknown.com')) as email,
    phone,
    COALESCE(country, 'Unknown') as country,
    COALESCE(state, 'Unknown') as state,
    COALESCE(customer_status, 'unknown') as customer_status,
    signup_date,
    CURRENT_TIMESTAMP() as created_at,
    _ingestion_timestamp as source_timestamp
FROM source
WHERE customer_id IS NOT NULL
