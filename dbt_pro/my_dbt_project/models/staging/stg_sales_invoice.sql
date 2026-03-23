{{
    config(
        materialized='incremental',
        unique_key='sales_invoice_id'
    )
}}

WITH source_data AS (
    SELECT *
    FROM delta_scan('/opt/airflow/data/Silver/delta/SalesInvoice')
),

-- ❗ Step 1: Fix creation alias
cleaned_raw AS (
    SELECT
        *,
        CAST(creationdate AS TIMESTAMP) AS creation,   -- fixed alias + type
        CAST(posting_date AS DATE) AS posting_date,
        CAST(due_date AS DATE) AS due_date
    FROM source_data
),

-- ❗ Step 2: Cleaned data with defaults
cleaned AS (
    SELECT
        TRIM(CAST(sales_invoice_id AS VARCHAR)) AS sales_invoice_id,
        COALESCE(TRIM(CAST(customer AS VARCHAR)), 'N/A') AS customer,
        COALESCE(TRIM(CAST(customer_name AS VARCHAR)), 'Unknown') AS customer_name,
        COALESCE(TRIM(CAST(customer_group AS VARCHAR)), 'N/A') AS customer_group,
        COALESCE(TRIM(CAST(territory AS VARCHAR)), 'N/A') AS territory,
        creation,
        posting_date,
        due_date,
        COALESCE(TRIM(CAST(company AS VARCHAR)), 'N/A') AS company,
        COALESCE(TRIM(CAST(status AS VARCHAR)), 'Draft') AS status,
        docstatus,
        is_return,
        is_pos,
        COALESCE(TRIM(CAST(currency AS VARCHAR)), 'N/A') AS currency,
        CAST(conversion_rate AS DOUBLE) AS conversion_rate,
        CAST(total_qty AS DOUBLE) AS total_qty,
        CAST(net_total AS DOUBLE) AS net_total,
        CAST(grand_total AS DOUBLE) AS grand_total,
        CAST(rounded_total AS DOUBLE) AS rounded_total,
        CAST(outstanding_amount AS DOUBLE) AS outstanding_amount,
        CAST(paid_amount AS DOUBLE) AS paid_amount,
        COALESCE(TRIM(CAST(apply_discount_on AS VARCHAR)), 'N/A') AS apply_discount_on,
        CAST(discount_amount AS DOUBLE) AS discount_amount,
        CAST(additional_discount_percentage AS DOUBLE) AS additional_discount_percentage,
        COALESCE(TRIM(CAST(customer_address AS VARCHAR)), 'N/A') AS customer_address,
        COALESCE(TRIM(CAST(shipping_address AS VARCHAR)), 'N/A') AS shipping_address,
        is_internal_customer,
        is_discounted,
        remarks,
        creationdate,
        batchid,
        md5,
        items,
        payments,
        payment_schedule,
        sales_team,
        taxes
    FROM cleaned_raw
),

-- ❗ Step 3: Deduplicate
deduped AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY sales_invoice_id
                   ORDER BY creationdate DESC
               ) AS rn
        FROM cleaned
    ) t
    WHERE rn = 1
),

-- ❗ Step 4: Max loaded for incremental
max_loaded AS (
    {% if is_incremental() %}
    SELECT MAX(CAST(creationdate AS TIMESTAMP)) AS max_record_date FROM {{ this }}
    {% else %}
    SELECT NULL AS max_record_date
    {% endif %}
)

-- ❗ Step 5: Final select with incremental filter
SELECT *
FROM deduped

{% if is_incremental() %}
WHERE COALESCE(CAST(creationdate AS TIMESTAMP), TIMESTAMP '1900-01-01 00:00:00') >
      COALESCE(CAST((SELECT max_record_date FROM max_loaded) AS TIMESTAMP),
               TIMESTAMP '1900-01-01 00:00:00')
{% endif %}