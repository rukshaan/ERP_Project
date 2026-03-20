-- DIMENSION LAYER: Sources from STAGING layer
-- Creates deduplicated dimension table with surrogate keys
-- Note: Historical tracking is in snapshot layer (SCD-2), this is current state only

{{ config(materialized='table') }}

WITH cte AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['item_code']) }} AS item_key,  -- surrogate key

        item_code,
        item_name,
        standard_rate,
        asset_category,
        country_of_origin,
        customer_code,
        owner,
        warranty_period,
        weight_uom,
        weight_per_unit,

        ROW_NUMBER() OVER(
            PARTITION BY item_code 
            ORDER BY item_name
        ) AS rank   -- deduplication

    FROM {{ ref('stg_item') }}
)

SELECT
    item_key,
    item_code,
    item_name,
    standard_rate,
    COALESCE(asset_category, 'N/A') AS asset_category,
    COALESCE(country_of_origin, 'N/A') AS country_of_origin,
    customer_code,
    owner,
    warranty_period,
    weight_uom,
    weight_per_unit

FROM cte
WHERE rank = 1
ORDER BY item_name