with source as (
    select * from {{ source('raw', 'superstore_orders') }}
),

renamed as (
    select
        row_id::integer as row_id,
        order_id as order_id,
        to_date(order_date, 'MM/DD/YYYY') as order_date,
        to_date(ship_date, 'MM/DD/YYYY') as ship_date,
        ship_mode,
        customer_id,
        customer_name,
        segment,
        country,
        city,
        state,
        postal_code,
        region,
        product_id,
        category,
        sub_category,
        product_name,
        sales::float as sales,
        quantity::integer as quantity,
        discount::float as discount,
        profit::float as profit
    from source
)

select * from renamed