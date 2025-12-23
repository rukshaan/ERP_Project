{{ config(materialized='table') }}

WITH source_data AS (
    SELECT *
    FROM {{ ref('stg_sales_order') }}
)

SELECT
    sd.sales_order_id,
    sd.customer_name AS customer_key,
    COALESCE(c.customer_name, 'Unknown Customer') AS customer_name,
    sd.item_code,
    COALESCE(i.item_name, 'Unknown Item') AS item_name,
    sd.order_date,
    sd.delivery_date,
    COALESCE(sd.qty, 0) AS qty,
    COALESCE(sd.rate, 0) AS rate,
    COALESCE(sd.amount, 0) AS amount,
    COALESCE(sd.open_qty, 0) AS open_qty,
    COALESCE(sd.open_amount, 0) AS open_amount,
    COALESCE(sd.status, 'Status_Pending') AS order_status,

    COALESCE(sd.is_fully_delivered, false) AS is_fully_delivered,

    COALESCE(sd.warehouse, 'Unknown Warehouse') AS warehouse,
    
    COALESCE(comp.company, 'Fanta Co') AS company
FROM source_data sd
LEFT JOIN {{ ref('dim_customer') }} c
    ON sd.customer_name = c.customer_name
LEFT JOIN {{ ref('dim_item') }} i
    ON sd.item_code = i.item_code
LEFT JOIN {{ ref('dim_company') }} comp
    ON sd.company = comp.company_key
