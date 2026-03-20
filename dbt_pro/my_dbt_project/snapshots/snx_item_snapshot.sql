{% snapshot snap_item %}

{{
    config(
        target_schema='snapshot',
        unique_key='item_code',

        strategy='check',
        check_cols=[
            'item_name',
            'standard_rate',
            'asset_category',
            'country_of_origin',
            'customer_code',
            'owner',
            'warranty_period',
            'weight_uom',
            'weight_per_unit'
        ],

        invalidate_hard_deletes=True
    )
}}

SELECT
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
    creationdate

FROM delta_scan('/opt/airflow/data/Silver/delta/Item')

{% endsnapshot %}