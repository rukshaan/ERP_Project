{{ config(materialized='table') }}

WITH source_data AS (
    SELECT *
    FROM {{ ref('stg_sales_order') }}
)

SELECT
    sd.sales_order_id,
    sd.customer AS customer_key,
    c.customer,
    sd.item_code,
    i.item_name,
    sd.order_date,
    sd.delivery_date,
    sd.qty,
    sd.rate,
    sd.amount,
    sd.open_qty,
    sd.open_amount,
    sd.is_fully_delivered,
    sd.warehouse,
    comp.company AS company
FROM source_data sd
LEFT JOIN {{ ref('dim_customer') }} c
    ON sd.customer = c.customer
LEFT JOIN {{ ref('dim_item') }} i
    ON sd.item_code = i.item_code
LEFT JOIN {{ ref('dim_company') }} comp
    ON sd.company = comp.company_key
