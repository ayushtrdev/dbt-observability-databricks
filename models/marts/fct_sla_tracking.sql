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
    created_at,
    duration_seconds,
    CASE WHEN duration_seconds > 3600 THEN 'exceeded' ELSE 'within_sla' END as sla_status,
    CASE WHEN duration_seconds > 3600 THEN 1 ELSE 0 END as sla_violation_flag,
    CASE WHEN duration_seconds > 2880 THEN 'warning' WHEN duration_seconds > 3600 THEN 'critical' ELSE 'healthy' END as health_status,
    is_failure,
    is_success,
    loaded_at
FROM metrics
ORDER BY created_at DESC
