select
    customer_id,
    customer_name,
    segment
from {{ ref('stg_orders') }}
group by 1, 2, 3