{{ config(materialized='table') }}

select
    row_id,
    order_id,
    order_date,
    ship_date,
    ship_mode,
    customer_id,
    product_id,
    region,
    sales,
    quantity,
    discount,
    profit
from {{ ref('stg_orders') }}