with base as (
    select * from {{ ref('fact_movement_weekly') }}
),

windowed as (
    select
        store_id,
        upc_id,
        week_id,
        category_id,
        sales_units,
        price,
        revenue,
        is_promo,
        start_date,
        -- Price Transformations
        ln(price) as log_price,
        lag(price, 1) over (partition by store_id, upc_id order by week_id) as lag_price_1w,
        
        -- Reference Price (e.g., 4-week rolling avg)
        avg(price) over (partition by store_id, upc_id order by week_id rows between 3 preceding and current row) as avg_price_4w,
        
        -- Volume lags
        lag(sales_units, 1) over (partition by store_id, upc_id order by week_id) as lag_units_1w
        
    from base
)

select
    *,
    case when price < avg_price_4w then 1 else 0 end as is_below_avg_price,
    coalesce(lag_price_1w, price) as lag_price_1w_clean
from windowed
where price > 0
