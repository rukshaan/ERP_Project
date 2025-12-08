{{ config(materialized='table') }}

WITH base AS (
    SELECT
        MAKE_DATE(2015, 1, 1) + CAST(x AS INTEGER) AS date
    FROM range(0, 6000) AS r(x)
),

dates AS (
    SELECT
        CAST(STRFTIME('%Y%m%d', date) AS INTEGER) AS date_key,
        date,
        EXTRACT(year FROM date) AS year,
        EXTRACT(month FROM date) AS month,
        EXTRACT(day FROM date) AS day,
        EXTRACT(quarter FROM date) AS quarter,
        STRFTIME('%B', date) AS month_name,
        EXTRACT(week FROM date) AS week,
        EXTRACT(dow FROM date) AS day_of_week,
        STRFTIME('%A', date) AS day_name
    FROM base
)

SELECT * FROM dates
