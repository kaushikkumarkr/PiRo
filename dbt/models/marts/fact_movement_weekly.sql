with movement as (
    select * from {{ ref('int_movement_union') }}
),

calendar as (
    select * from {{ ref('dim_calendar') }}
)

select
    m.store_id,
    m.upc_id,
    m.week_id,
    c.start_date,
    m.category_id,
    m.sales_units,
    m.unit_price_raw as price,
    m.gross_profit_dollars,
    m.is_promo,
    -- Simple derived metrics
    (m.sales_units * m.unit_price_raw) as revenue
from movement m
left join calendar c on m.week_id = c.week_id
where m.sales_units >= 0 and m.unit_price_raw > 0
