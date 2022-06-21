
  with
    customer_orders as (
      select * from {{ ref('int_customer_orders') }}
    )

    , sales as (
      select * from {{ ref('int_sales') }}
    )

    , order_value as (
      /* Most expensive order value for each customer_unique_id */
      select
        customer_orders.customer_unique_id
        , customer_orders.customer_id
        , sales.order_id
        , sales.order_revenue_total as most_expensive_order_value
      from sales
      left join customer_orders on sales.customer_id = customer_orders.customer_id

      qualify row_number() over(partition by customer_orders.customer_unique_id order by sales.order_revenue_total desc) = 1
    )

  , final as (
    /* Grouping by customer_unique_id makes it difficult to determine the correct zip_code or city or state in the scenario
    a customer_unique_id is associated to more than one customer_id */
    select
      customer_orders.customer_unique_id

      /* Requested fields */
      , order_value.most_expensive_order_value
      , min(customer_orders.customer_first_purchased_at_et) as unique_customer_first_purchased_at_et
      , min(customer_orders.customer_most_recent_purchased_at_et) as unique_customer_most_recent_purchased_at_et
      , count(distinct sales.order_id) as number_of_orders
      , sum(sales.order_revenue_total) as total_orders_value

    from customer_orders
    left join sales on customer_orders.customer_id = sales.customer_id
    left join order_value on customer_orders.customer_unique_id = order_value.customer_unique_id
    group by 1,2
  )

select * from final
