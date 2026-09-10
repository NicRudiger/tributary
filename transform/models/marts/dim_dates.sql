with bounds as (
    select
        min(order_date) as min_date,
        max(ship_date) as max_date
    from {{ ref('stg_orders') }}
),

date_spine as (
    select dateadd(day, seq4(), (select min_date from bounds)) as date_day
    from table(generator(rowcount => 10000))
)

select
    date_day,
    year(date_day) as year,
    month(date_day) as month,
    dayofweek(date_day) as day_of_week,
    dayofweek(date_day) in (0, 6) as is_weekend
from date_spine
where date_day <= (select max_date from bounds)