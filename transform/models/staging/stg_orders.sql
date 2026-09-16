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
),

deduped as (
    -- Defensive dedup: makes this model idempotent even if RAW.SUPERSTORE_ORDERS
    -- ever ends up with duplicate rows again (e.g. a re-uploaded or reprocessed
    -- source file). Keeps exactly one row per row_id no matter how many times
    -- the same order shows up upstream.
    select *
    from renamed
    qualify row_number() over (partition by row_id order by row_id) = 1
)

select * from deduped
