{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'facts']
) }}

WITH test_metrics AS (
    SELECT * FROM {{ ref('int_test_metrics') }}
)

SELECT
    run_id,
    test_id,
    test_name,
    status,
    num_failures,
    num_passes,
    total_tests,
    test_pass_rate,
    is_passed,
    CASE WHEN test_pass_rate = 100 THEN 'healthy' WHEN test_pass_rate >= 90 THEN 'warning' ELSE 'critical' END as quality_status,
    executed_at,
    loaded_at
FROM test_metrics
ORDER BY executed_at DESC
