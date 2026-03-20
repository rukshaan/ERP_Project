{{
    config(
        strategy='check',
        unique_key='company',
        check_cols=['company'],
        updated_at='modified'
    )
}}

SELECT DISTINCT
    TRIM(CAST(company AS VARCHAR)) AS company,
    MIN(modified)::timestamp AS modified
FROM delta_scan('/opt/airflow/data/Silver/delta/SalesOrderItems')
WHERE company IS NOT NULL
GROUP BY company
