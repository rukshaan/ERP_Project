{{
    config(
        strategy='check',
        unique_key='customer_id',
        check_cols=['customer_name', 'customer_group', 'customer_type', 'territory', 'customer_primary_address', 'sales_team_name', 'is_internal_customer', 'email_id', 'mobile_no'],
        updated_at='modified_date'
    )
}}

SELECT
    customer_id,
    customer_name,
    customer_group,
    customer_type,
    territory,
    customer_primary_address,
    sales_team_name,
    is_internal_customer,
    email_id,
    mobile_no,
    creation,
    modified_date
FROM {{ ref('stg_customer') }}
