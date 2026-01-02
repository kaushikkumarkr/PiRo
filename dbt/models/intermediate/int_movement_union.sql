with unioned as (
    select * from {{ source('piro_raw', 'raw_wsdr') }}
    union all
    select * from {{ source('piro_raw', 'raw_wcer') }}
    union all
    select * from {{ source('piro_raw', 'raw_wlnd') }}
    union all
    select * from {{ source('piro_raw', 'raw_wsna') }}
),

coalesced as (
    select
        store as store_id,
        upc as upc_id,
        week as week_id,
        category_id,
        move as sales_units,
        price as unit_price_raw, -- derived in raw file as price. sometimes price/qty is better. DFF specific: 'price' is actual retail price.
        qty as quantity_check, -- bundle quantity
        profit as gross_profit_dollars,
        ok,
        case when sale != '' then true else false end as is_promo
    from unioned
)

select * from coalesced
