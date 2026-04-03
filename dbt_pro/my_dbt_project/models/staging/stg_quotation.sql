{{
    config(
        materialized='incremental'
    )
}}

WITH source_data AS (

    SELECT *
    FROM delta_scan('/opt/airflow/data/Silver/delta/Quotation')

),

casted AS (

    SELECT
        *,
        CAST(creation AS TIMESTAMP) AS creation_ts,
        CAST(transaction_date AS DATE) AS transaction_date_ts,
        CAST(valid_till AS DATE) AS valid_till_ts
    FROM source_data

),

deduped AS (

    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY name
                   ORDER BY creation_ts DESC
               ) AS rn
        FROM casted
    ) t
    WHERE rn = 1
),

-- ✅ FIX: move MAX() OUTSIDE WHERE
max_loaded AS (

    {% if is_incremental() %}
        SELECT COALESCE(MAX(creation_ts), TIMESTAMP '1900-01-01') AS max_creation_ts
        FROM {{ this }}
    {% else %}
        SELECT TIMESTAMP '1900-01-01' AS max_creation_ts
    {% endif %}

),

filtered AS (

    SELECT d.*
    FROM deduped d
    CROSS JOIN max_loaded m
    WHERE d.creation_ts > m.max_creation_ts

)

SELECT

    -- identifiers
    name AS quotation_id,
    customer_name,
    party_name,

    -- dates
    creation,
    transaction_date,
    valid_till,

    creation_ts,
    transaction_date_ts,
    valid_till_ts,

    -- business
    company,
    status,
    docstatus,
    order_type,
    customer_group,
    territory,

    -- items
    item_code,
    item_name,
    item_description,

    -- financials
    currency,
    conversion_rate,
    total_qty,
    net_total,
    discount_amount,
    additional_discount_percentage,

    -- addresses
    customer_address,
    shipping_address,

    -- metadata
    batchid,
    creationdate,
    md5

FROM filtered