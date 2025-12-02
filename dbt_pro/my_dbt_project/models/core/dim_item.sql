-- This is a staging model for Sales Order Items
{{ config(materialized='table') }}

WITH source_data AS (
    SELECT *
    FROM delta_scan('/opt/airflow/data/Silver/delta/SalesOrderItems')
   
)

select 
    item_key,
    item_id,
    item_code,
    item_name,qty,rate,amount,
    md5(item_id) as item_hashkey
from source_data 

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null

