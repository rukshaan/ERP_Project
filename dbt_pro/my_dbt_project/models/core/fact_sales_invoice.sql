{{
    config(
        materialized='incremental',
        unique_key='sales_invoice_item_key'
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
-- 2. ITEMS (BASE GRAIN)
-- =====================================
items_exploded AS (
    SELECT
        si.*,
        item.value AS item_json
    FROM source_data si,
         UNNEST(si.items) AS item(value)
),

items_parsed AS (
    SELECT
        ie.sales_invoice_id,
        ie.creation_ts,
        ie.customer,
        ie.posting_date,
        ie.company,

        json_extract(item_json, '$.item_code') AS item_code,
        json_extract(item_json, '$.item_name') AS item_name,
        json_extract(item_json, '$.description') AS item_description,

        CAST(json_extract(item_json, '$.qty') AS DOUBLE) AS item_qty,
        CAST(json_extract(item_json, '$.rate') AS DOUBLE) AS item_rate,
        CAST(json_extract(item_json, '$.amount') AS DOUBLE) AS item_amount

    FROM items_exploded ie
),

-- =====================================
-- 3. SALES TEAM
-- =====================================
sales_team_exploded AS (
    SELECT
        si.sales_invoice_id,
        team.value AS team_json
    FROM source_data si,
         UNNEST(si.sales_team) AS team(value)
),

sales_team_parsed AS (
    SELECT
        sales_invoice_id,

        CAST(json_extract(team_json, '$.allocated_percentage') AS DOUBLE) AS allocated_percentage,
        CAST(json_extract(team_json, '$.allocated_amount') AS DOUBLE) AS team_allocated_amount

    FROM sales_team_exploded
),

-- =====================================
-- 4. PAYMENT SCHEDULE
-- =====================================
payment_exploded AS (
    SELECT
        si.sales_invoice_id,
        pay.value AS payment_json
    FROM source_data si,
         UNNEST(si.payment_schedule) AS pay(value)
),

payment_parsed AS (
    SELECT
        sales_invoice_id,

        json_extract(payment_json, '$.due_date') AS due_date,
        CAST(json_extract(payment_json, '$.payment_amount') AS DOUBLE) AS payment_amount,
        CAST(json_extract(payment_json, '$.outstanding') AS DOUBLE) AS outstanding_amount,
        CAST(json_extract(payment_json, '$.invoice_portion') AS DOUBLE) AS invoice_portion

    FROM payment_exploded
),

-- =====================================
-- 5. DIM ITEM MAPPING
-- =====================================
dim_mapped AS (
    SELECT
        ip.*,
        COALESCE(di.item_key, '-1') AS item_key
    FROM items_parsed ip
    LEFT JOIN {{ ref('dim_item') }} di
        ON LOWER(ip.item_code) = LOWER(di.item_code)
),

-- =====================================
-- 6. JOIN SALES TEAM
-- =====================================
with_sales_team AS (
    SELECT
        d.*,

        
        st.allocated_percentage,
        st.team_allocated_amount

    FROM dim_mapped d
    LEFT JOIN sales_team_parsed st
        ON d.sales_invoice_id = st.sales_invoice_id
),

-- =====================================
-- 7. JOIN PAYMENT SCHEDULE
-- =====================================
with_payments AS (
    SELECT
        w.*,

        p.due_date,
        p.payment_amount,
        p.outstanding_amount,
        p.invoice_portion

    FROM with_sales_team w
    LEFT JOIN payment_parsed p
        ON w.sales_invoice_id = p.sales_invoice_id
),

-- =====================================
-- 8. FINAL LOGIC
-- =====================================
final AS (
    SELECT
        *,

        MD5(CONCAT(
            sales_invoice_id,
            item_code,
            COALESCE(due_date, DATE '1900-01-01')
        )) AS sales_invoice_item_key

    FROM with_payments
),

-- =====================================
-- 9. INCREMENTAL FILTER
-- =====================================
filtered AS (
    SELECT *
    FROM final

    {% if is_incremental() %}
    WHERE creation_ts > (
        SELECT COALESCE(MAX(creation_ts), TIMESTAMP '1900-01-01')
        FROM {{ this }}
    )
    {% endif %}
)

-- =====================================
-- 10. FINAL OUTPUT (NO SELECT *)
-- =====================================
SELECT

    -- INVOICE LEVEL
    sales_invoice_id,
    creation_ts,
    customer,
    posting_date,
    company,

    -- ITEM LEVEL
    item_code,
    item_name,
    item_description,
    item_qty,
    item_rate,
    item_amount,
    item_key,

    -- SALES TEAM
    
    allocated_percentage,
    team_allocated_amount,

    -- PAYMENT SCHEDULE
    due_date,
    payment_amount,
    outstanding_amount,
    invoice_portion,

    -- SURROGATE KEY
    sales_invoice_item_key

FROM filtered