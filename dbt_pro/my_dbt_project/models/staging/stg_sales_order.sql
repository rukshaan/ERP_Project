
{{ config(materialized='table') }}

WITH source_data AS (
    SELECT *
    FROM delta_scan('/opt/airflow/data/Silver/delta/SalesOrderItems')
   
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null

