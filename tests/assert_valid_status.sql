SELECT * FROM {{ ref('fct_job_runs') }}
WHERE status NOT IN ('success', 'error', 'running', 'cancelled')
