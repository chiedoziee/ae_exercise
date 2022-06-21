
  with

    products as (
      select * from {{ ref('stg_products') }}
    )

    , order_items as (
      select * from {{ ref('stg_order_items') }}
    )

    , product_sales_details as (
      select
        products.product_id
        , coalesce(round((products.product_length_cm * products.product_height_cm * products.product_width_cm),2),0) as product_volume_cubic_cm
        , count (order_items.order_item_id) as total_units_sold
        , coalesce(round(sum (order_items.price),2),0) as total_revenue
      from products
      left join order_items on  products.product_id = order_items.product_id
      group by 1,2
    )

    , top_10_products_sold as (
      select distinct
        product_id
        , total_revenue
      from product_sales_details
      order by 2 desc
      limit 10
    )

  , final as (
    select
      products.product_id
      , products.product_category_name
      , products.product_name_length
      , products.product_description_length
      , products.product_photos_count
      , products.product_weight_kg
      , products.product_length_cm
      , products.product_height_cm
      , products.product_width_cm
      , product_sales_details.product_volume_cubic_cm
      , product_sales_details.total_units_sold
      , product_sales_details.total_revenue
      , cast ((product_sales_details.product_id = top_10_products_sold.product_id) as int64) as is_top_10_product

    from products
    left join product_sales_details on products.product_id = product_sales_details.product_id
    left join top_10_products_sold on product_sales_details.product_id = top_10_products_sold.product_id
  )

select * from final
