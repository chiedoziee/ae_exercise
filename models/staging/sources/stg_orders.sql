select
  order_id
  , customer_id
  , order_status
  , {{convert_timezone ('order_purchase_timestamp') }} as order_purchased_at_et
  , {{convert_timezone ('order_approved_at') }} as order_approved_at_et
  , {{convert_timezone ('order_delivered_carrier_date') }} as order_delivered_carrier_at_et
  , {{convert_timezone ('order_delivered_customer_date') }} as order_delivered_customer_at_et
  , {{convert_timezone ('order_estimated_delivery_date') }} as order_estimated_delivery_at_et

from {{ source('raw','orders') }}
