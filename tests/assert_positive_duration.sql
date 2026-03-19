SELECT * FROM {{ ref('fct_job_runs') }}
WHERE duration_seconds < 0
