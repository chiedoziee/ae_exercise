
with
  customers as (
    select * from {{ ref('stg_customers') }}
  )

, customer_unique_id_check as (
  select
    customer_id
    , count (customer_unique_id) as count_customer_with_multiple_unique_ids
  from customers
  group by 1
  having count (customer_unique_id) > 1
  order by 2
)

, customer_id_check as (
  select
    customer_unique_id
    , count (customer_id) as count_customer_with_multiple_ids
  from customers
  group by 1
  having count (customer_id) > 1
  order by 2
)

, duplicate_customer_information_check as (
  select
    customer_unique_id
    , count (distinct customer_zip_code_prefix)
    , count (distinct customer_city)
    , count (distinct customer_state)
from customers
group by 1
having count (distinct customer_zip_code_prefix) > 1 or count (distinct customer_city) > 1 or count (distinct customer_city) > 1

)


select * from customer_unique_id_check ;

/* Unhash these during QA */
-- select * from customer_id_check ;
-- select count (*) from duplicate_customer_information_check ;
