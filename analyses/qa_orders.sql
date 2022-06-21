
/* This could be modified into a singular test for fct_sales */
with orders as (
  select * from {{ ref('stg_orders') }}
)

, order_items as (
  select * from {{ ref('stg_order_items') }}
)

, missing_order_ids_in_order_items as (
  select distinct orders.order_id
from orders
where not orders.order_id in (select distinct order_items.order_id from order_items)
)

, missing_order_ids_in_orders as (
  select distinct order_items.order_id
from order_items
where not order_items.order_id in (select distinct orders.order_id from orders)
)

, order_dates_check as (
  select
    orders.order_id
    , case when orders.order_purchased_at_et is null and orders.order_delivered_customer_at_et is not null then true end as delivery_without_purchase_date
    , case when orders.order_purchased_at_et is null and orders.order_delivered_customer_at_et is null then true end as no_purchase
  from orders

)

select
  count (orders.order_id) as all_orders
  , count(missing_order_ids_in_order_items.order_id) as count_order_ids_missing_in_order_items
  , count(missing_order_ids_in_orders.order_id) as count_order_ids_missing_in_orders
  , count (order_dates_check.delivery_without_purchase_date) as count_delivery_without_purchase_date
  , count (order_dates_check.no_purchase) as count_no_purchase

from orders
left join missing_order_ids_in_order_items on orders.order_id = missing_order_ids_in_order_items.order_id
left join missing_order_ids_in_orders on orders.order_id = missing_order_ids_in_orders.order_id
left join order_dates_check on orders.order_id = order_dates_check.order_id
