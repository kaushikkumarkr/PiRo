-- Pivot Data for Top 5 UPCs
WITH top_upcs AS (
    SELECT upc_id 
    FROM fact_movement_weekly 
    JOIN dim_upc USING(upc_id) 
    WHERE category_id = 'sdr' 
    GROUP BY 1 ORDER BY SUM(sales_units) DESC LIMIT 5
)
SELECT 
    week_id,
    store_id,
    MAX(CASE WHEN upc_id = (SELECT upc_id FROM top_upcs OFFSET 0 LIMIT 1) THEN log_price END) as ln_p_1,
    MAX(CASE WHEN upc_id = (SELECT upc_id FROM top_upcs OFFSET 1 LIMIT 1) THEN log_price END) as ln_p_2,
    MAX(CASE WHEN upc_id = (SELECT upc_id FROM top_upcs OFFSET 2 LIMIT 1) THEN log_price END) as ln_p_3,
    MAX(CASE WHEN upc_id = (SELECT upc_id FROM top_upcs OFFSET 3 LIMIT 1) THEN log_price END) as ln_p_4,
    MAX(CASE WHEN upc_id = (SELECT upc_id FROM top_upcs OFFSET 4 LIMIT 1) THEN log_price END) as ln_p_5,
    MAX(CASE WHEN upc_id = (SELECT upc_id FROM top_upcs OFFSET 0 LIMIT 1) THEN log_sales END) as ln_q_1,
    MAX(CASE WHEN upc_id = (SELECT upc_id FROM top_upcs OFFSET 1 LIMIT 1) THEN log_sales END) as ln_q_2,
    MAX(CASE WHEN upc_id = (SELECT upc_id FROM top_upcs OFFSET 2 LIMIT 1) THEN log_sales END) as ln_q_3,
    MAX(CASE WHEN upc_id = (SELECT upc_id FROM top_upcs OFFSET 3 LIMIT 1) THEN log_sales END) as ln_q_4,
    MAX(CASE WHEN upc_id = (SELECT upc_id FROM top_upcs OFFSET 4 LIMIT 1) THEN log_sales END) as ln_q_5
FROM pricing_panel
WHERE upc_id IN (SELECT upc_id FROM top_upcs)
GROUP BY 1, 2
HAVING COUNT(DISTINCT upc_id) = 5; -- Only complete weeks
