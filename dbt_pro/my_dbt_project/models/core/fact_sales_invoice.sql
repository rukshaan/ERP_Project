{{
    config(
        materialized='incremental',
        unique_key='fact_sales_invoice_key'
    )
}}

WITH source_data AS (
    SELECT *
    FROM {{ ref('stg_sales_invoice') }}
),

-- Deduplicate
deduped AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY sales_invoice_id
                   ORDER BY creationdate DESC
               ) AS rn
        FROM source_data
    ) t
    WHERE rn = 1
),

-- Map to dimensions
dim_mapped AS (
    SELECT
        si.*,
        -- Fix COALESCE type casting
        COALESCE(CAST(c.customer_key AS STRING), '-1') AS customer_key,
        COALESCE(CAST(comp.company_key AS STRING), '-1') AS company_key,
        COALESCE(CAST(dd.date_key AS STRING), '-1') AS creation_date_key,
        COALESCE(CAST(dd2.date_key AS STRING), '-1') AS posting_date_key,
        COALESCE(CAST(dd3.date_key AS STRING), '-1') AS due_date_key
    FROM deduped si
    LEFT JOIN {{ ref('dim_customer') }} c
        ON LOWER(TRIM(si.customer_name)) = LOWER(TRIM(c.customer_name))
    LEFT JOIN {{ ref('dim_company') }} comp
        ON LOWER(TRIM(si.company)) = LOWER(TRIM(comp.company))
    LEFT JOIN {{ ref('dim_date') }} dd
        ON si.creationdate = dd.date_value
    LEFT JOIN {{ ref('dim_date') }} dd2
        ON si.posting_date = dd2.date_value
    LEFT JOIN {{ ref('dim_date') }} dd3
        ON si.due_date = dd3.date_value
),

-- Add surrogate key
with_keys AS (
    SELECT
        *,
        MD5(sales_invoice_id) AS fact_sales_invoice_key
    FROM dim_mapped
),

-- Incremental logic
final_incremental AS (
    {% if is_incremental() %}
    SELECT wk.*
    FROM with_keys wk
    LEFT JOIN {{ this }} existing
        ON wk.fact_sales_invoice_key = existing.fact_sales_invoice_key
    WHERE existing.fact_sales_invoice_key IS NULL
    {% else %}
    SELECT * FROM with_keys
    {% endif %}
)

SELECT
    fact_sales_invoice_key,
    sales_invoice_id,
    customer_key,
    COALESCE(customer_name, 'Unknown Customer') AS customer_name,
    territory,
    customer_group,
    is_internal_customer,
    company_key,
    COALESCE(company, 'Unknown Company') AS company,
    creation_date_key,
    posting_date_key,
    due_date_key,
    COALESCE(status, 'Draft') AS status,
    docstatus,
    is_return,
    is_pos,
    is_discounted,
    currency,
    COALESCE(CAST(conversion_rate AS DOUBLE), 1.0) AS conversion_rate,
    COALESCE(CAST(total_qty AS DOUBLE), 0.0) AS total_qty,
    COALESCE(CAST(net_total AS DOUBLE), 0.0) AS net_total,
    COALESCE(CAST(grand_total AS DOUBLE), 0.0) AS grand_total,
    COALESCE(CAST(rounded_total AS DOUBLE), 0.0) AS rounded_total,
    COALESCE(CAST(outstanding_amount AS DOUBLE), 0.0) AS outstanding_amount,
    COALESCE(CAST(paid_amount AS DOUBLE), 0.0) AS paid_amount,
    apply_discount_on,
    COALESCE(CAST(discount_amount AS DOUBLE), 0.0) AS discount_amount,
    COALESCE(CAST(additional_discount_percentage AS DOUBLE), 0.0) AS additional_discount_percentage,
    customer_address,
    shipping_address,
    remarks,
    creationdate,
    batchid,
    md5
FROM final_incremental