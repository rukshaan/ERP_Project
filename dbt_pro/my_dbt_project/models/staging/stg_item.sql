-- STAGING LAYER: Sources from SNAPSHOT layer (SCD-2 historical data)
-- Returns only current records (dbt_valid_to IS NULL) for incremental processing

{{
    config(
        materialized='incremental',
        unique_key='item_code'
    )
}}

WITH source_data AS (
    -- TEMP: Direct from Silver until snapshot is created
    SELECT *
    FROM delta_scan('/opt/airflow/data/Silver/delta/Item')
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
    item_code,
    item_name,
    standard_rate,
    COALESCE(asset_category, 'N/A') AS asset_category,
    COALESCE(country_of_origin, 'N/A') AS country_of_origin,
    customer_code,
    owner,
    warranty_period,
    weight_uom,
    weight_per_unit,
    valid_from,
    valid_to,
    is_current,
    creationdate::date AS creation

FROM source_data
CROSS JOIN max_loaded

{% if is_incremental() %}
WHERE creationdate::date > max_loaded.max_record_date
{% endif %}
