{{ config(
    materialized='table',
    schema='bronze',
    tags=['bronze', 'dbt_cloud']
) }}

WITH source_data AS (
    SELECT
        CAST(NULL AS STRING) as run_id,
        CAST(NULL AS STRING) as job_id,
        CAST(NULL AS STRING) as account_id,
        CAST(NULL AS STRING) as project_id,
        CAST(NULL AS STRING) as status,
        CAST(NULL AS TIMESTAMP) as created_at,
        CAST(NULL AS TIMESTAMP) as started_at,
        CAST(NULL AS TIMESTAMP) as finished_at,
        CAST(NULL AS INT) as duration_seconds,
        CAST(NULL AS TIMESTAMP) as extracted_at
    LIMIT 0
)

SELECT
    run_id,
    job_id,
    account_id,
    project_id,
    status,
    created_at,
    started_at,
    finished_at,
    duration_seconds,
    extracted_at,
    CURRENT_TIMESTAMP() as loaded_at
FROM source_data
WHERE run_id IS NOT NULL
