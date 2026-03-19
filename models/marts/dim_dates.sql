{{ config(
    materialized='table',
    schema='gold',
    tags=['marts', 'dimensions']
) }}

WITH date_range AS (
    SELECT
        DATE_ADD(DATE('2023-01-01'), INTERVAL CAST(ROW_NUMBER() OVER (ORDER BY 1) - 1 AS INT) DAY) as date_key
    FROM RANGE(730)  -- 2 years of dates
)

SELECT
    date_key,
    CAST(YEAR(date_key) AS STRING) as year,
    CAST(MONTH(date_key) AS STRING) as month,
    CAST(QUARTER(date_key) AS STRING) as quarter,
    CAST(DAYOFWEEK(date_key) AS STRING) as day_of_week,
    DAYNAME(date_key) as day_name,
    MONTHNAME(date_key) as month_name,
    WEEKOFYEAR(date_key) as week_of_year,
    CASE WHEN DAYOFWEEK(date_key) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END as weekend_flag,
    CURRENT_TIMESTAMP() as updated_at
FROM date_range
