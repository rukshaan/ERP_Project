-- FACT LAYER: Quotation Fact Table (with item-level mapping)
-- Sources: stg_quotation + dim_customer + dim_company + dim_date + dim_item
-- Materialized as table

{{ config(
    materialized='table'
) }}

WITH source_data AS (
    SELECT *
    FROM {{ ref('stg_quotation') }}
),

-- Explode items array for mapping to dim_item
items_exploded AS (
    SELECT
        sd.*,
        item.value AS item_json
    FROM source_data sd
    CROSS JOIN UNNEST(sd.items) AS item(value)
),

-- Map to item dimension
item_mapped AS (
    SELECT
        ie.*,
        di.item_key AS item_key,
        di.item_name AS dim_item_name,
        di.standard_rate,
        di.asset_category,
        di.country_of_origin,
        di.customer_code AS item_customer_code,
        di.owner AS item_owner,
        di.warranty_period,
        di.weight_uom AS item_weight_uom,
        di.weight_per_unit AS item_weight_per_unit
    FROM items_exploded ie
    LEFT JOIN {{ ref('dim_item') }} di
        ON ie.item_json->>'item_code' = di.item_code
),

-- Map to other dimensions
dim_mapped AS (
    SELECT
        im.*,
        c.customer_key,
        comp.company_key,
        dd.date_key AS creation_date_key,
        dd2.date_key AS transaction_date_key,
        dd3.date_key AS valid_till_date_key
    FROM item_mapped im
    LEFT JOIN {{ ref('dim_customer') }} c
        ON LOWER(TRIM(im.customer_name)) = LOWER(TRIM(c.customer_name))
    LEFT JOIN {{ ref('dim_company') }} comp
        ON LOWER(TRIM(im.company)) = LOWER(TRIM(comp.company))
    LEFT JOIN {{ ref('dim_date') }} dd
        ON im.creation::date = dd.date_value
    LEFT JOIN {{ ref('dim_date') }} dd2
        ON im.transaction_date::date = dd2.date_value
    LEFT JOIN {{ ref('dim_date') }} dd3
        ON im.valid_till::date = dd3.date_value
)

SELECT
    -- Fact surrogate key (unique per quotation + item)
    MD5(quotation_id || item_json->>'item_code') AS fact_quotation_item_key,

    -- Quotation identifiers
    quotation_id,

    -- Customer
    customer_key,
    COALESCE(customer_name, 'Unknown Customer') AS customer_name,
    territory,
    customer_group,

    -- Company
    company_key,
    COALESCE(company, 'Unknown Company') AS company,

    -- Dates
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

    -- Original nested fields
    items,
    payments,
    payment_schedule,

    -- Item-level flattened fields
    item_json->>'item_code' AS item_code,
    item_json->>'item_name' AS item_name,
    CAST(item_json->>'qty' AS DOUBLE) AS item_qty,
    CAST(item_json->>'rate' AS DOUBLE) AS item_rate,
    CAST(item_json->>'amount' AS DOUBLE) AS item_amount,
    item_key,
    dim_item_name,
    standard_rate,
    asset_category,
    country_of_origin,
    item_customer_code,
    item_owner,
    warranty_period,
    item_weight_uom,
    item_weight_per_unit,

    -- Metadata
    creationdate,
    batchid,
    md5

FROM dim_mapped