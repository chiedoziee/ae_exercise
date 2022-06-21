
  with
    order_items as (
      select * from {{ ref('stg_order_items') }}
    )

    , orders as (
      select * from {{ ref('stg_orders') }}
    )

    , payments as (
      select * from {{ ref('stg_payments') }}
    )

    , order_value as (
      select distinct
        order_id
        , sum(price) as order_revenue_total
      from order_items
      group by 1
    )

    , payment_details as (
      select
        order_id
        , sum(payment_installments) as payment_installments_total
        , sum(payment_value) as payment_value_total
        from payments
        group by 1
    )

, final as (
    select
      order_items.order_id
      , order_items.order_item_id
      , order_items.product_id
      , order_items.seller_id
      , order_items.price as item_price
      , order_items.freight_value
      , orders.customer_id
      , orders.order_status
      , orders.order_purchased_at_et
      , orders.order_delivered_customer_at_et
      , order_value.order_revenue_total
      , coalesce(date_diff(orders.order_delivered_customer_at_et, orders.order_purchased_at_et, day),0) as days_to_deliver
      , payment_details.payment_installments_total
      , payment_details.payment_value_total

    from order_items
    left join orders on order_items.order_id = orders.order_id
    left join order_value on order_items.order_id = order_value.order_id
    left join payment_details on order_items.order_id = payment_details.order_id
)

select * from final
