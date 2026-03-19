{{ config(
    materialized='table',
    schema='gold',
    tags=['marts', 'dimensions']
) }}

WITH date_series AS (
    SELECT EXPLODE(SEQUENCE(0, 729)) as days_offset
)

SELECT
    DATE_ADD(CURRENT_DATE(), -days_offset) as date_key,
    YEAR(DATE_ADD(CURRENT_DATE(), -days_offset)) as year,
    MONTH(DATE_ADD(CURRENT_DATE(), -days_offset)) as month,
    QUARTER(DATE_ADD(CURRENT_DATE(), -days_offset)) as quarter,
    DAYOFWEEK(DATE_ADD(CURRENT_DATE(), -days_offset)) as day_of_week,
    DAYNAME(DATE_ADD(CURRENT_DATE(), -days_offset)) as day_name,
    MONTHNAME(DATE_ADD(CURRENT_DATE(), -days_offset)) as month_name,
    WEEKOFYEAR(DATE_ADD(CURRENT_DATE(), -days_offset)) as week_of_year,
    CASE WHEN DAYOFWEEK(DATE_ADD(CURRENT_DATE(), -days_offset)) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END as weekend_flag,
    CURRENT_TIMESTAMP() as updated_at
FROM date_series