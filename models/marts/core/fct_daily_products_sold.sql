
{{
  config(
    materialized='incremental'
    , unique_key = 'daily_product_id'
  )
}}


  with

    sales as (
        select * from {{ ref('int_sales') }}
    )

    , date_spine as (
      select * from {{ ref('dates') }}
    )

    , earliest_item_purchase_date as (
        /* Identify earliest purchased date */
      select
        min(cast(order_purchased_at_et as date)) as first_purchase_date
      from sales
    )

    , date_item_purchase_spine as (
      /* Join Spree stock item properties on date spine starting with the first stock item created date */
      select
        concat(date_spine.date_day,sales.product_id) as daily_product_id
        , date_spine.date_day
        , sales.product_id
      from date_spine
      inner join earliest_item_purchase_date as earliest_date on date_spine.date_day >= earliest_date.first_purchase_date
        and date_spine.date_day <= current_date()
      left join sales on date_spine.date_day >= cast(sales.order_purchased_at_et as date)
        and date_spine.date_day <= current_date()
      order by 1
    )

  , final as (
    select
      daily_product_id
      , date_item_purchase_spine.date_day
      , date_item_purchase_spine.product_id
      , count(sales.order_item_id) as daily_items_count
      , count(sales.order_id) as daily_orders_count
      , coalesce(round(sum(sales.item_price),2),0) as daily_revenue_total

    from date_item_purchase_spine
    left join sales on date_item_purchase_spine.product_id = sales.product_id
      and cast(date_item_purchase_spine.date_day as date) = cast(sales.order_purchased_at_et as date)
    group by 1,2,3
    order by 1
  )

select * from final

{% if is_incremental() %}

  where date_item_purchase_spine.date_day >= (select max(date_item_purchase_spine.date_day) from {{ this }})

{% endif %}
