{{
    config(
        materialized='incremental',
        unique_key='customer_id'
    )
}}

WITH source_data AS (
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
    TRIM(CAST(customer_id AS VARCHAR)) AS customer_id,
    COALESCE(customer, 'N/A') AS customer_name,
    COALESCE(customer_group, 'N/A') AS customer_group,
    COALESCE(customer_type, 'N/A') AS customer_type,
    COALESCE(territory, 'N/A') AS territory,  
    COALESCE(customer_primary_address, 'N/A') AS customer_primary_address,
    COALESCE(sales_team_sales_person, 'salesteam') AS salesteam, 
    COALESCE(sales_team_name, 'N/A') AS sales_team_name, 
    COALESCE(is_internal_customer, 1) AS is_internal_customer,
    COALESCE(email_id, 'N/A') AS email_id,
    COALESCE(mobile_no, 'N/A') AS mobile_no,
    creation::date AS creation,
    modified::date AS modified_date

FROM source_data
CROSS JOIN max_loaded

{% if is_incremental() %}
WHERE creation::date > max_loaded.max_record_date
{% endif %}
