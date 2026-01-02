{{ config(materialized='table') }}

-- Feature Store: Centralized definition of features for Training and Serving
-- Offline Store: This table
-- Online Store: A view on top of this (e.g., SELECT * FROM ... WHERE week_id = (SELECT MAX(week_id) ...))

WITH base AS (
    SELECT
        -- Keys
        store_id,
        upc_id,
        week_id,
        
        -- Temporal Field (Point-in-Time)
        start_date as event_timestamp,

        -- Target
        sales_units,
        log(sales_units + 1) as log_sales,

        -- Features: Pricing
        log_price as feat_log_price,
        avg_price_4w as feat_avg_price_4w,
        lag_price_1w as feat_lag_price_1w,
        
        -- Features: Competitor / Market (Placeholder for real data)
        -- In a real store, we'd join with competitor tables here
        1.0 as feat_competitor_price_ratio, 
        
        -- Features: Context
        -- Seasonality (Month)
        EXTRACT(MONTH FROM start_date) as feat_month_of_year

    FROM {{ ref('mart_weekly_pricing_features') }}
)

SELECT * FROM base
