
  with

    products as (
      select * from {{ ref('stg_products') }}
    )

    , order_items as (
      select * from {{ ref('stg_order_items') }}
    )

    , product_sales_details as (
      /* Calculating product volume in cm3, total number of units sold per product and total revenue per product 
      (Unit was kept in cm at stg_products to maintain a uniform metric at stakeholder's request) */
      select distinct
        products.product_id
        , coalesce(round((products.product_length_cm * products.product_height_cm * products.product_width_cm),2),0) as product_volume_cubic_cm
        , count (order_items.order_item_id) as total_units_sold
        , coalesce(round(sum (order_items.price),2),0) as total_revenue
      from products
      left join order_items on  products.product_id = order_items.product_id
      group by 1,2
    )

    , top_10_products_sold as (
      /* Determine overall top 10 products sold based on number of units sold
      (Top 10 products per month or year would be interesting) */
      select distinct
        product_id
        , total_units_sold
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
      , case when product_sales_details.product_id = top_10_products_sold.product_id then true end as is_top_10_product

    from products
    left join product_sales_details on products.product_id = product_sales_details.product_id
    left join top_10_products_sold on product_sales_details.product_id = top_10_products_sold.product_id
  )

select * from final
where is_top_10_product
