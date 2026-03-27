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

),

-- 🔹 Explode items
items_exploded AS (
    SELECT
        sd.*,
        item.value AS item_json
    FROM source_data sd
    CROSS JOIN UNNEST(sd.items) AS item(value)
),

-- 🔹 Explode payment_schedule
payment_schedule_exploded AS (
    SELECT
        ie.*,
        ps.value AS payment_schedule_json
    FROM items_exploded ie
    CROSS JOIN UNNEST(ie.payment_schedule) AS ps(value)
),

-- 🔹 Explode payments
payments_exploded AS (
    SELECT
        pse.*,
        pay.value AS payment_json
    FROM payment_schedule_exploded pse
    CROSS JOIN UNNEST(pse.payments) AS pay(value)
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

    -- Nested fields preserved
    items,
    payments,
    payment_schedule,

    creationdate,
    batchid,
    md5,

    -- 🔹 Flattened item fields
    item_json->>'item_code' AS item_code,
    item_json->>'item_name' AS item_name,
    item_json->>'description' AS item_description,
    CAST(item_json->>'qty' AS DOUBLE) AS item_qty,
    CAST(item_json->>'rate' AS DOUBLE) AS item_rate,
    CAST(item_json->>'amount' AS DOUBLE) AS item_amount,

    -- 🔹 Flattened payment_schedule fields
    payment_schedule_json->>'due_date' AS payment_due_date,
    CAST(payment_schedule_json->>'payment_amount' AS DOUBLE) AS payment_amount,
    CAST(payment_schedule_json->>'invoice_portion' AS DOUBLE) AS invoice_portion,

    -- 🔹 Flattened payments fields
    payment_json->>'payment_method' AS payment_method,
    CAST(payment_json->>'paid_amount' AS DOUBLE) AS paid_amount,
    CAST(payment_json->>'outstanding' AS DOUBLE) AS outstanding_amount

FROM payments_exploded pse
CROSS JOIN max_loaded

{% if is_incremental() %}
WHERE creation::date > max_loaded.max_record_date
{% endif %}