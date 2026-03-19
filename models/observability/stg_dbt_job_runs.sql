{{ config(
    materialized='table',
    schema='bronze',
    tags=['observability']
) }}

SELECT
    run_id,
    job_id,
    account_id,
    status,
    CAST(created_at AS TIMESTAMP) as created_at,
    CAST(started_at AS TIMESTAMP) as started_at,
    CAST(finished_at AS TIMESTAMP) as finished_at,
    CAST(duration_seconds AS DECIMAL(10,2)) as duration_seconds,
    CAST(extracted_at AS TIMESTAMP) as extracted_at,
    CURRENT_TIMESTAMP() as loaded_at
FROM workspace.bronze.dbt_job_runs_raw
WHERE run_id IS NOT NULL
