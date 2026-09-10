select
    product_id,
    product_name,
    category,
    sub_category
from {{ ref('stg_orders') }}
qualify row_number() over (partition by product_id order by order_date desc) = 1