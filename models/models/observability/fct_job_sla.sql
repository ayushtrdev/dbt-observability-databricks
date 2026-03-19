{{ config(
    materialized='table',
    schema='gold',
    tags=['observability']
) }}

WITH runs AS (
    SELECT * FROM {{ ref('stg_dbt_job_runs') }}
)

SELECT
    job_id,
    run_id,
    status,
    created_at,
    duration_seconds,
    CASE WHEN duration_seconds > 5 THEN 'EXCEEDED' ELSE 'WITHIN_SLA' END as sla_status,
    CASE WHEN duration_seconds > 5 THEN 1 ELSE 0 END as sla_violation_flag,
    CASE 
        WHEN duration_seconds > 10 THEN 'SLOW'
        WHEN duration_seconds > 5 THEN 'MEDIUM'
        ELSE 'FAST'
    END as performance_tier
FROM runs
WHERE duration_seconds IS NOT NULL
ORDER BY created_at DESC
