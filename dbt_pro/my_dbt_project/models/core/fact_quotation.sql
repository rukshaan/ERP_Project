-- FACT LAYER: Quotation Fact Table
-- Sources: stg_quotation + dim_customer + dim_company + dim_date
-- Materialized as incremental based on unique quotation

{{ config(
    materialized='table',
    
) }}

WITH source_data AS (
    SELECT *
    FROM {{ ref('stg_quotation') }}
),

-- Map to dimensions
dim_mapped AS (
    SELECT
        sq.*,
        c.customer_key,
        comp.company_key,
        dd.date_key AS creation_date_key,
        dd2.date_key AS transaction_date_key,
        dd3.date_key AS valid_till_date_key
    FROM source_data sq
    LEFT JOIN {{ ref('dim_customer') }} c
        ON LOWER(TRIM(sq.customer_name)) = LOWER(TRIM(c.customer_name))
    LEFT JOIN {{ ref('dim_company') }} comp
        ON LOWER(TRIM(sq.company)) = LOWER(TRIM(comp.company))
    LEFT JOIN {{ ref('dim_date') }} dd
        ON sq.creation::date = dd.date_value
    LEFT JOIN {{ ref('dim_date') }} dd2
        ON sq.transaction_date::date = dd2.date_value
    LEFT JOIN {{ ref('dim_date') }} dd3
        ON sq.valid_till::date = dd3.date_value
),

-- Incremental logic: only new fact rows
incremental AS (
    {% if is_incremental() %}
    SELECT dm.*
    FROM dim_mapped dm
    LEFT JOIN {{ this }} existing
      ON md5(dm.quotation_id) = existing.fact_quotation_key
    WHERE existing.fact_quotation_key IS NULL
    {% else %}
    SELECT * FROM dim_mapped
    {% endif %}
)

SELECT
    -- Fact surrogate key
    MD5(quotation_id) AS fact_quotation_key,

    -- Quotation identifiers
    quotation_id,

    -- Customer
    customer_key,
    COALESCE(customer_name, 'Unknown Customer') AS customer_name,
    territory,
    customer_type,
    customer_group,

    -- Company
    company_key,
    COALESCE(company, 'Unknown Company') AS company,

    -- Dates (fact-to-dim_date keys)
    creation_date_key,
    transaction_date_key,
    valid_till_date_key,

    -- Status & metadata
    COALESCE(status, 'Draft') AS status,
    docstatus,
    order_type,
    currency,
    COALESCE(conversion_rate, 1) AS conversion_rate,

    -- Financials
    COALESCE(total_qty, 0) AS total_qty,
    COALESCE(net_total, 0) AS net_total,
    COALESCE(discount_amount, 0) AS discount_amount,
    COALESCE(additional_discount_percentage, 0) AS additional_discount_percentage,

    -- Address info
    customer_address,
    shipping_address,

    -- Nested fields
    items,
    payments,
    payment_schedule,

    -- Metadata
    creationdate,
    batchid,
    md5

FROM incremental