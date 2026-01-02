-- Select high-velocity items to ensure stable elasticity estimates.
-- Filtering for Top 20 UPCs per category by total revenue to keep MCMC sampling fast for this sprint.
with top_upcs as (
    select 
        category_id, 
        upc_id,
        sum(sales_units * price) as total_rev
    from {{ ref('fact_movement_weekly') }}
    group by 1, 2
    order by 1, 3 desc
),

ranked as (
    select *,
        row_number() over (partition by category_id order by total_rev desc) as rnk
    from top_upcs
),

filtered_upcs as (
    select upc_id, category_id from ranked where rnk <= 20
),

filtered_fact as (
    select 
        f.store_id,
        f.upc_id,
        f.week_id,
        f.category_id,
        f.start_date,
        f.sales_units,
        f.price,
        f.is_promo
    from {{ ref('fact_movement_weekly') }} f
    inner join filtered_upcs u on f.upc_id = u.upc_id
    where f.sales_units > 0 
      and f.price > 0
),

features as (
    select
        *,
        ln(price) as log_price,
        ln(sales_units) as log_sales,
        lag(price, 1) over (partition by store_id, upc_id order by week_id) as lag_price_1w,
        -- Promo Features: Base Price Estimation (Max in last 8 weeks)
        max(price) over (partition by store_id, upc_id order by week_id rows between 8 preceding and current row) as max_price_8w
    from filtered_fact
)

select 
    store_id,
    upc_id,
    week_id,
    category_id,
    start_date,
    log_price,
    log_sales,
    is_promo::int as is_promo,
    coalesce(lag_price_1w, price) as lag_price_1w_clean,
    max_price_8w,
    case 
        when max_price_8w > 0 then (max_price_8w - price) / max_price_8w 
        else 0 
    end as promo_depth
from features
where log_price is not null
