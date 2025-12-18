{{ config(materialized='table') }}

WITH source_data AS (

    SELECT
        *
    FROM {{ ref('stg_sales_order') }}

)

SELECT
    sales_order_id,
    item_code,
    order_date,
    delivery_date,
    qty,
    rate,
    amount,
    open_qty,
    open_amount,
    is_fully_delivered,
    warehouse
FROM source_data
