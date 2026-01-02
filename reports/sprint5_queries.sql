-- Query 1: Baseline Forecast (StatsForecast)
SELECT 
    unique_id,
    ds as forecast_date,
    y as predicted_units,
    category_id
FROM baseline_forecasts
WHERE ds > CURRENT_DATE
ORDER BY unique_id, ds;

-- Query 2: Scenario Comparison (Simulation)
-- Compare Current vs Simulated Revenue/Profit for different Price Points
-- Identify Optimal Price Point (Max Profit Index)
WITH best_case AS (
    SELECT 
        upc_id, 
        simulated_price as optimal_price,
        profit_index as max_profit_lift,
        price_change_pct as optimal_change_oct
    FROM (
        SELECT 
            *, 
            ROW_NUMBER() OVER (PARTITION BY upc_id ORDER BY profit_index DESC) as rn
        FROM scenario_results
    ) sub
    WHERE rn = 1
)
SELECT 
    b.upc_id,
    p.current_price,
    b.optimal_price,
    b.optimal_change_oct,
    (b.max_profit_lift - 1) * 100 as profit_lift_pct
FROM best_case b
JOIN (SELECT DISTINCT upc_id, current_price FROM scenario_results) p 
  ON b.upc_id = p.upc_id
ORDER BY profit_lift_pct DESC;
