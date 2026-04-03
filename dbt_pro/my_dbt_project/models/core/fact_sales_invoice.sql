{{
    config(
        materialized='table'
        
    )
}}

-- =====================================
-- 1. SOURCE
-- =====================================
WITH source_data AS (
    SELECT *
    FROM {{ ref('stg_sales_invoice') }}
),

-- =====================================
-- 2. DIMENSION MAPPING (KEEP RELATIONSHIPS)
-- =====================================
dim_mapped AS (
    SELECT
        si.*,

        -- Customer mapping
        c.customer_key,

        -- Company mapping
        comp.company_key

    FROM source_data si

    LEFT JOIN {{ ref('dim_customer') }} c
        ON LOWER(TRIM(si.customer_name)) = LOWER(TRIM(c.customer_name))
    LEFT JOIN {{ ref('dim_item') }} i
        ON LOWER(TRIM(si.item_code)) = LOWER(TRIM(i.item_code))
    LEFT JOIN {{ ref('dim_company') }} comp
        ON LOWER(TRIM(si.company)) = LOWER(TRIM(comp.company))
),

-- =====================================
-- 3. FINAL
-- =====================================
final AS (
    SELECT
        *,

        -- Surrogate Key (NO ITEM LEVEL NOW)
        MD5(CONCAT(
            sales_invoice_id,
            COALESCE(CAST(creation_ts AS VARCHAR), '0')
        )) AS sales_invoice_key

    FROM dim_mapped
)

-- =====================================
-- 4. OUTPUT (ALL COLUMNS KEPT)
-- =====================================
SELECT

    -- identifiers
    sales_invoice_id,

    -- relationships
    customer_key,
    company_key,
    item_name,
    item_code,
    customer,
    customer_name,
    customer_group,
    territory,
    company,
    customer_group,
    
    
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

    -- address
    customer_address,
    shipping_address,
        -- items
    item_name,
    item_code,
    item_description,
    item_rate,
    item_qty,
    item_amount,

    -- misc
    remarks,

    -- payments
    payment_amount,
    -- metadata
    batchid,
    creationdate,
    md5,

    -- key
    sales_invoice_key

FROM final

