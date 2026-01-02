-- Query 1: Top 20 UPCs by Promo Lift
SELECT 
    upc_id,
    avg_lift_pct,
    base_elasticity,
    avg_promo_depth
FROM promo_lift_estimates
WHERE category_id = 'sdr'
ORDER BY avg_lift_pct DESC
LIMIT 20;

-- Query 2: Elasticity vs Income Visualization (Data Prep)
-- Join Panel with Demographics and bucket by Income
WITH panel AS (
    SELECT * FROM elasticity_ready_panel
),
demos AS (
    SELECT 
        store_id, 
        log_median_income,
        NTILE(4) OVER (ORDER BY log_median_income) as income_quartile
    FROM dim_store_demographics
)
SELECT 
    p.week_id,
    d.income_quartile,
    AVG(p.log_price) as avg_log_price,
    AVG(p.log_sales) as avg_log_sales
FROM panel p
JOIN demos d ON p.store_id = d.store_id
GROUP BY 1, 2
ORDER BY 1, 2;
-- In Metabase: Scatter Plot of Price vs Sales, Series = Income Quartile.
-- Steeper slope for Low Income = Higher Elasticity.
