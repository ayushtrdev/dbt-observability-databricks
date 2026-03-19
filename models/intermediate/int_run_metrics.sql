{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'metrics']
) }}

WITH run_data AS (
    SELECT * FROM {{ ref('stg_dbt_runs') }}
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
    CASE WHEN status = 'success' THEN 1 ELSE 0 END as is_success,
    CASE WHEN status = 'error' THEN 1 ELSE 0 END as is_failure,
    DATE(created_at) as run_date,
    HOUR(created_at) as run_hour,
    extracted_at,
    loaded_at
FROM run_data
WHERE run_id IS NOT NULL
