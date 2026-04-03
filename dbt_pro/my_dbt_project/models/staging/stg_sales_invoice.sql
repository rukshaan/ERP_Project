{{
    config(
        materialized='incremental',
        
    )
}}

WITH source_data AS (

    SELECT *
    FROM delta_scan('/opt/airflow/data/Silver/delta/SalesInvoice')

),

casted AS (

    SELECT
        *,
        CAST(creationdate AS TIMESTAMP) AS creation_ts,
        CAST(posting_date AS DATE) AS posting_date_ts,
        CAST(due_date AS DATE) AS due_date_ts
    FROM source_data

),

deduped AS (

    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY sales_invoice_id
                   ORDER BY creation_ts DESC
               ) AS rn
        FROM casted
    ) t
    WHERE rn = 1
),

filtered AS (

    SELECT *
    FROM deduped

    {% if is_incremental() %}
    WHERE creation_ts > (
        SELECT COALESCE(MAX(creation_ts), TIMESTAMP '1900-01-01')
        FROM {{ this }}
    )
    {% endif %}

)

SELECT

    -- =========================
    -- IDENTIFIERS (UNCHANGED)
    -- =========================
    sales_invoice_id,
    customer,
    customer_name,
    customer_group,
    territory,
    company,

    -- items
    item_name,
    item_code,

    item_description,
    item_rate,
    item_qty as item_qty,
    item_amount,

     -- =========================
    -- dates
    posting_date,
    due_date,
    creation_ts,
    posting_date_ts,
    due_date_ts,

    -- status
    is_pos,
    is_return,
    docstatus,
    status,
    is_internal_customer,
    is_discounted,

    -- financials
    currency,
    conversion_rate,
    total_qty,
    net_total,
    grand_total,
    rounded_total,
    outstanding_amount,
    paid_amount,

    apply_discount_on,
    discount_amount,
    additional_discount_percentage,

    customer_address,
    shipping_address,

    remarks,

    -- payments
    payment_amount,

    -- metadata
    batchid,
    creationdate,
    md5

FROM filtered