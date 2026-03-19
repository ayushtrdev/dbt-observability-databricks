{{ config(
    materialized='table',
    schema='gold',
    tags=['marts', 'dimensions']
) }}

SELECT
    DATE_ADD(DATE('2023-01-01'), INTERVAL CAST(n AS INT) DAY) as date_key,
    YEAR(DATE_ADD(DATE('2023-01-01'), INTERVAL CAST(n AS INT) DAY)) as year,
    MONTH(DATE_ADD(DATE('2023-01-01'), INTERVAL CAST(n AS INT) DAY)) as month,
    QUARTER(DATE_ADD(DATE('2023-01-01'), INTERVAL CAST(n AS INT) DAY)) as quarter,
    DAYOFWEEK(DATE_ADD(DATE('2023-01-01'), INTERVAL CAST(n AS INT) DAY)) as day_of_week,
    DAYNAME(DATE_ADD(DATE('2023-01-01'), INTERVAL CAST(n AS INT) DAY)) as day_name,
    MONTHNAME(DATE_ADD(DATE('2023-01-01'), INTERVAL CAST(n AS INT) DAY)) as month_name,
    WEEKOFYEAR(DATE_ADD(DATE('2023-01-01'), INTERVAL CAST(n AS INT) DAY)) as week_of_year,
    CASE WHEN DAYOFWEEK(DATE_ADD(DATE('2023-01-01'), INTERVAL CAST(n AS INT) DAY)) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END as weekend_flag,
    CURRENT_TIMESTAMP() as updated_at
FROM (
    SELECT EXPLODE(SEQUENCE(0, 729)) as n
)