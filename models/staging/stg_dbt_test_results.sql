{{ config(
    materialized='table',
    schema='bronze',
    tags=['bronze', 'dbt_cloud']
) }}

WITH source_data AS (
    SELECT
        CAST(NULL AS STRING) as run_id,
        CAST(NULL AS STRING) as test_id,
        CAST(NULL AS STRING) as test_name,
        CAST(NULL AS STRING) as status,
        CAST(NULL AS INT) as num_failures,
        CAST(NULL AS INT) as num_passes,
        CAST(NULL AS TIMESTAMP) as executed_at,
        CAST(NULL AS TIMESTAMP) as extracted_at
    LIMIT 0
)

SELECT
    run_id,
    test_id,
    test_name,
    status,
    num_failures,
    num_passes,
    executed_at,
    extracted_at,
    CURRENT_TIMESTAMP() as loaded_at
FROM source_data
WHERE run_id IS NOT NULL
