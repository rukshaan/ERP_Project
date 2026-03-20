-- STAGING LAYER: Sources from SNAPSHOT layer (SCD-2 historical data)
-- Returns only current records (dbt_valid_to IS NULL) for incremental processing

{{
    config(
        materialized='incremental',
        unique_key='customer_id'
    )
}}

WITH source_data AS (
    -- TEMP: Direct from Silver until snapshot is created
    SELECT *
    FROM delta_scan('/opt/airflow/data/Silver/delta/Customer')
),
max_loaded AS (

    {% if is_incremental() %}

    SELECT MAX(creation) AS max_record_date

    FROM {{ this }}

    {% else %}

    SELECT NULL AS max_record_date

    {% endif %}

)
 

SELECT
    customer_id,
    customer AS customer_name,
    COALESCE(customer_group, 'N/A') AS customer_group,
    customer_type,
    COALESCE(territory, 'N/A') AS territory,
    customer_primary_address,
    -- salesteam, 
    sales_team_name, 
    is_internal_customer,
    email_id,
    mobile_no,
    creation::date AS creation,
    modified::date AS modified_date

FROM source_data
CROSS JOIN max_loaded

{% if is_incremental() %}
WHERE creation::date > max_loaded.max_record_date
{% endif %}
