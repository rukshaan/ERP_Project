{{ config(
    materialized='incremental',
    unique_key='sales_order_id'
) }}

WITH source_data AS (
    SELECT *
    FROM delta_scan('/opt/airflow/data/Silver/delta/SalesOrderItems')
),

max_loaded AS (
    {% if is_incremental() %}
    SELECT MAX(order_date) AS max_order_date
    FROM {{ this }}
    {% else %}
    SELECT NULL AS max_order_date
    {% endif %}
)

SELECT
    TRIM(CAST(sales_order_id AS VARCHAR)) AS sales_order_id,
    TRIM(CAST(item_code AS VARCHAR)) AS item_code,
    order_date::date AS order_date,
    delivery_date::date AS delivery_date,
    qty,
    rate,
    amount,
    open_qty,
    open_amount,
    is_fully_delivered,
    warehouse,
    status,
    currency

FROM source_data

{% if is_incremental() %}
WHERE order_date::date > (SELECT max_order_date FROM max_loaded)
{% endif %}
