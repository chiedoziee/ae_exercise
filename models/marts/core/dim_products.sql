
  with
    product_sales as (
      select * from {{ ref('int_product_sales') }}
    )

  , final as (
    select
    product_sales.product_id
    , product_sales.product_category_name
    , product_sales.product_name_length
    , product_sales.product_description_length
    , product_sales.product_photos_count
    , product_sales.product_weight_kg
    , product_sales.product_length_cm
    , product_sales.product_height_cm
    , product_sales.product_width_cm

    /* Requested fields */
    , product_sales.product_volume_cubic_cm
    , product_sales.total_units_sold
    , product_sales.total_revenue
    , product_sales.is_top_10_product

    from product_sales
  )

select * from final
