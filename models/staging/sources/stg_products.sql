select
  product_id
  , product_category_name
  , product_name_lenght as product_name_length
  , product_description_lenght as product_description_length
  , product_photos_qty as product_photos_count
  , round(1.0 * product_weight_g / 1000, 2) as product_weight_kg
  , product_length_cm
  , product_height_cm
  , product_width_cm

from {{ source('raw','products') }}
