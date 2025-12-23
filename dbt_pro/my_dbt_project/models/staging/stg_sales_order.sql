{{ config(
    materialized='incremental',
    unique_key=['sales_order_id', 'item_code'],
    
) }}

WITH source_data AS (
    SELECT *
    FROM delta_scan('/opt/airflow/data/Silver/delta/SalesOrderItems')
),

max_loaded AS (
    {% if is_incremental() %}
    SELECT
        MAX(order_date::date) AS max_order_date
    FROM {{ this }}
    {% else %}
    SELECT
        NULL::date AS max_order_date
    {% endif %}
)

SELECT
    TRIM(CAST(sales_order_id AS VARCHAR)) AS sales_order_id,
    TRIM(CAST(item_code AS VARCHAR))      AS item_code,

    /*  NEW COLUMNS */
    COALESCE(TRIM(CAST(company AS VARCHAR)), 'N/A')        AS company,
    COALESCE(TRIM(CAST(item_name AS VARCHAR)), 'Unknown') AS item_name,
    COALESCE(TRIM(CAST(customer AS VARCHAR)), 'N/A') AS customer_name,

    CAST(order_date AS DATE)              AS order_date,
    CAST(delivery_date AS DATE)           AS delivery_date,

    CAST(qty AS DOUBLE)                  AS qty,
    CAST(rate AS DOUBLE)                  AS rate,
    CAST(amount AS DOUBLE)                AS amount,
    CAST(open_qty AS DOUBLE)             AS open_qty,
    CAST(open_amount AS DOUBLE)           AS open_amount,
    CAST(status AS VARCHAR)                AS status,
    /*  Safe Yes/No → 1/0 normalization */
    CASE
        WHEN LOWER(TRIM(is_fully_delivered)) IN ('yes', 'y', 'true', '1') THEN 1
        WHEN LOWER(TRIM(is_fully_delivered)) IN ('no', 'n', 'false', '0') THEN 0
        ELSE 0
    END AS is_fully_delivered,

    COALESCE(CAST(warehouse AS VARCHAR), 'N/A') AS warehouse,
    COALESCE(CAST(status AS VARCHAR), 'Open')   AS status,
    COALESCE(CAST(currency AS VARCHAR), 'N/A')  AS currency

FROM source_data

{% if is_incremental() %}
WHERE CAST(order_date AS DATE) >
      COALESCE(
          (SELECT max_order_date FROM max_loaded),
          DATE '1900-01-01'
      )
{% endif %}
