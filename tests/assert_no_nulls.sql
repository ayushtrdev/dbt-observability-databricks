SELECT run_id, job_id, status
FROM {{ ref('fct_job_runs') }}
WHERE run_id IS NULL OR job_id IS NULL OR status IS NULL
