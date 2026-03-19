{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'metrics']
) }}

WITH test_data AS (
    SELECT * FROM {{ ref('stg_dbt_test_results') }}
)

SELECT
    run_id,
    test_id,
    test_name,
    status,
    num_failures,
    num_passes,
    num_failures + num_passes as total_tests,
    ROUND(100.0 * num_passes / (num_failures + num_passes), 2) as test_pass_rate,
    CASE WHEN status = 'pass' THEN 1 ELSE 0 END as is_passed,
    executed_at,
    extracted_at,
    loaded_at
FROM test_data
WHERE run_id IS NOT NULL
