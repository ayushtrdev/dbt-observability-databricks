{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'dimensions']
) }}

WITH runs AS (
    SELECT
        job_id,
        account_id,
        project_id,
        COUNT(*) as total_runs,
        COUNT(CASE WHEN is_success = 1 THEN 1 END) as successful_runs,
        COUNT(CASE WHEN is_failure = 1 THEN 1 END) as failed_runs,
        MAX(created_at) as last_run_at,
        MIN(created_at) as first_run_at,
        AVG(duration_seconds) as avg_duration_seconds
    FROM {{ ref('int_run_metrics') }}
    GROUP BY job_id, account_id, project_id
)

SELECT
    job_id,
    account_id,
    project_id,
    total_runs,
    successful_runs,
    failed_runs,
    ROUND(100.0 * successful_runs / total_runs, 2) as success_rate_pct,
    ROUND(100.0 * failed_runs / total_runs, 2) as failure_rate_pct,
    ROUND(avg_duration_seconds, 2) as avg_duration_seconds,
    last_run_at,
    first_run_at,
    DATEDIFF(day, first_run_at, last_run_at) as days_active,
    CURRENT_TIMESTAMP() as updated_at
FROM runs
ORDER BY total_runs DESC
