{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (

    SELECT *
    FROM {{ ref('stg_quotation') }}

),

item_mapped AS (

    SELECT
        sd.*,

        di.item_key,
        di.item_name AS dim_item_name,
        di.standard_rate,
        di.asset_category,
        di.country_of_origin,
        di.customer_code AS item_customer_code,
        di.owner AS item_owner,
        di.warranty_period,
        di.weight_uom AS item_weight_uom,
        di.weight_per_unit AS item_weight_per_unit

    FROM source_data sd

    LEFT JOIN {{ ref('dim_item') }} di
        ON sd.item_code = di.item_code

),

dim_mapped AS (

    SELECT
        im.*,

        c.customer_key,
        comp.company_key,
        d1.date_key AS creation_date_key,
        d2.date_key AS transaction_date_key,
        d3.date_key AS valid_till_date_key

    FROM item_mapped im

    LEFT JOIN {{ ref('dim_customer') }} c
        ON LOWER(TRIM(im.customer_name)) = LOWER(TRIM(c.customer_name))

    LEFT JOIN {{ ref('dim_company') }} comp
        ON LOWER(TRIM(im.company)) = LOWER(TRIM(comp.company))

    LEFT JOIN {{ ref('dim_date') }} d1
        ON im.creation::date = d1.date_value

    LEFT JOIN {{ ref('dim_date') }} d2
        ON im.transaction_date::date = d2.date_value

    LEFT JOIN {{ ref('dim_date') }} d3
        ON im.valid_till::date = d3.date_value
)

SELECT

    MD5(
        COALESCE(quotation_id, '') || '|' ||
        COALESCE(item_code, '')
    ) AS fact_quotation_item_key,

    quotation_id,
    customer_key,
    customer_name,
    territory,
    customer_group,

    company_key,
    company,

    creation_date_key,
    transaction_date_key,
    valid_till_date_key,

    status,
    docstatus,
    order_type,
    currency,
    conversion_rate,

    total_qty,
    net_total,
    discount_amount,
    additional_discount_percentage,

    customer_address,
    shipping_address,

    -- ITEM LEVEL (NOW FROM FLAT MODEL)
    item_code,
    item_name,
    item_description,

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

    creationdate,
    batchid,
    md5

FROM dim_mapped