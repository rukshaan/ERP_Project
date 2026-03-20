-- DIMENSION LAYER: Sources from STAGING layer
-- Creates deduplicated dimension table with surrogate keys
-- Note: Historical tracking is in snapshot layer (SCD-2), this is current state only

{{ config(materialized='table') }}

WITH cte AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_key,  -- surrogate key based on natural key
        customer_id,
        customer_name,
        territory,
        customer_type,
        customer_group,
        customer_primary_address,
        sales_team_name,
        is_internal_customer,
        email_id,
        mobile_no,
        ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY customer_name) AS rank       -- deduplicate
    FROM {{ ref('stg_customer') }}
)

SELECT
    customer_key,
    customer_id,
    customer_name,
    COALESCE(territory, 'N/A') AS territory,
    customer_type,
    COALESCE(customer_group, 'N/A') AS customer_group,
    customer_primary_address,
    sales_team_name,
    is_internal_customer,
    email_id,
    mobile_no

FROM cte
WHERE rank = 1
ORDER BY customer_name
