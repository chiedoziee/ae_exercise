
/* Order Items PK not unique */

with order_items as (
  select * from {{ ref('stg_order_items') }}
)

select distinct
  order_item_id
  , count (order_item_id) count_duplicate_item_id
from order_items
group by 1
having count (order_item_id) > 1
