with source as (
    select * from {{ source('piro_raw', 'raw_demo') }}
),

renamed as (
    select
        store as store_id,
        name as store_name,
        city,
        zip as zip_code,
        lat,
        long as lng,
        
        -- Demographics (DFF Dictionary assumptions)
        income as log_median_income,
        -- Convert to approx dollar if needed: exp(income) * 10?? DFF income is weird.
        -- Usually it's log income.
        
        age9 as pct_age_lt_9,
        age60 as pct_age_gt_60,
        ethnic as pct_minority,
        educ as pct_college,
        nocar as pct_no_car,
        hvalmean as mean_house_value,
        
        -- Store tiers
        -- tier as store_tier, -- Missing in raw
        zone as price_zone
    from source
)

select * from renamed
