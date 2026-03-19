{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'facts'],
    cluster_by=['job_id']
) }}

WITH metrics AS (
    SELECT * FROM {{ ref('int_run_metrics') }}
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
    is_success,
    is_failure,
    run_date,
    run_hour,
    PERCENT_RANK() OVER (PARTITION BY job_id ORDER BY duration_seconds) as duration_percentile,
    AVG(duration_seconds) OVER (PARTITION BY job_id ORDER BY created_at ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as avg_duration_10_runs,
    loaded_at
FROM metrics
ORDER BY created_at DESC
