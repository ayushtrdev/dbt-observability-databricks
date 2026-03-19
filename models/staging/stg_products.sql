{{ config(
    materialized='table',
    schema='bronze',
    tags=['staging', 'products']
) }}

WITH source AS (
    SELECT
        product_id,
        product_name,
        category,
        subcategory,
        unit_cost,
        supplier_id,
        created_date,
        _ingestion_timestamp
    FROM {{ source('raw', 'products') }}
)

SELECT
    product_id,
    COALESCE(product_name, 'Unknown Product') as product_name,
    COALESCE(category, 'Uncategorized') as category,
    COALESCE(subcategory, 'Uncategorized') as subcategory,
    COALESCE(unit_cost, 0) as unit_cost,
    supplier_id,
    created_date,
    CURRENT_TIMESTAMP() as created_at,
    _ingestion_timestamp as source_timestamp
FROM source
WHERE product_id IS NOT NULL
