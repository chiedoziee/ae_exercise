
  with
    sales as (
        select * from {{ ref('int_sales') }}
    )

    , customer_orders as (
        select * from {{ ref('int_customer_orders') }}
    )

, final as (
  select
    sales.order_item_id
    , sales.order_id
    , customer_orders.customer_id
    , sales.product_id
    , sales.seller_id
    , sales.item_price
    , sales.freight_value
    , sales.order_status
    , sales.order_purchased_at_et
    , sales.order_delivered_customer_at_et
    , sales.order_revenue_total
    , sales.payment_installments_total
    , sales.payment_value_total
    , customer_orders.customer_zip_code_prefix
    , customer_orders.customer_city
    , customer_orders.customer_state
    , customer_orders.customer_first_purchased_at_et
    , customer_orders.customer_most_recent_purchased_at_et

    /* Requested fields */
    , sales.days_to_deliver
    , customer_orders.new_or_repeat_customer

  from sales
  left join customer_orders on sales.customer_id = customer_orders.customer_id
)

select * from final
