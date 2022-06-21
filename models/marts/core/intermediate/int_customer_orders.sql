
  with
    orders as (
        select * from {{ ref('stg_orders') }}
    )

    , customers as (
        select * from {{ ref('stg_customers') }}
    )

    , customer_orders as (
        /* For retention, it would be important to have customer order sequence based on order_delivery_date */
        select distinct
        orders.customer_id
        , min(cast(orders.order_purchased_at_et as datetime)) as first_purchased_at_et
        , max(cast(orders.order_purchased_at_et as datetime)) as most_recent_purchased_at_et
        from orders
        group by 1
    )

, final as (
    select
    customers.customer_id
    , customers.customer_unique_id
    , customers.customer_zip_code_prefix
    , customers.customer_city
    , customers.customer_state
    , customer_orders.first_purchased_at_et as customer_first_purchased_at_et
    , customer_orders.most_recent_purchased_at_et as customer_most_recent_purchased_at_et

    /* New or Repeat Customer defined: New = less than 90 days, Repeat = Customer returned on a different date */
    , case when
        (coalesce(date_diff(current_date(), customer_orders.most_recent_purchased_at_et, day)) <=90) or (customer_orders.first_purchased_at_et != customer_orders.most_recent_purchased_at_et)
        then true
        end as new_or_repeat_customer

    from customers
    left join customer_orders on customers.customer_id = customer_orders.customer_id
)

select * from final