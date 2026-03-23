-- STAGING LAYER: Sources from Silver (until snapshot is added)
-- Returns only new records for incremental processing

{{
    config(
        materialized='incremental'
        
    )
}}

WITH source_data AS (

    SELECT *
    FROM delta_scan('/opt/airflow/data/Silver/delta/Quotation')

),

max_loaded AS (

    {% if is_incremental() %}

    SELECT MAX(creation) AS max_record_date
    FROM {{ this }}

    {% else %}

    SELECT NULL AS max_record_date

    {% endif %}

)

SELECT
    -- Primary identifiers
    name AS quotation_id,
    customer_name,
    party_name,

    -- Dates
    creation::date AS creation,
    transaction_date::date AS transaction_date,
    valid_till::date AS valid_till,

    -- Company & status
    company,
    status,
    docstatus,

    -- Financials
    currency,
    conversion_rate,
    total_qty,
    net_total,

    -- Discounts
    discount_amount,
    additional_discount_percentage,

    -- Address info
    customer_address,
    shipping_address,

    -- Metadata
    territory,
    customer_group,
    order_type,

    -- Nested fields kept as-is (important for next layer)
    items,
    payments,
    payment_schedule,

    creationdate,
    batchid,
    md5

FROM source_data
CROSS JOIN max_loaded

{% if is_incremental() %}
WHERE creation::date > max_loaded.max_record_date
{% endif %}