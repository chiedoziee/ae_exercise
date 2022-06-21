{% snapshot snapshot_products %}

{{
    config(
      target_schema='snapshots',
      strategy='check',
      unique_key='product_id',
      check_cols=['product_category_name', 'product_name_lenght'],
    )
}}

    select * from {{ source('raw', 'products') }}

{% endsnapshot %}
