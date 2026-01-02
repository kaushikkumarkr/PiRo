-- Query 1: Optimization Recommendations
-- Compare Current vs Recommended Price
SELECT 
    o.upc_id,
    u.description,
    o.current_price,
    o.recommended_price,
    o.price_change_pct,
    o.predicted_profit as projected_profit
FROM optimization_results o
LEFT JOIN dim_upc u ON o.upc_id = u.upc_id
WHERE o.category_id = 'sdr'
ORDER BY o.predicted_profit DESC;

-- Query 2: Category Impact Summary
SELECT 
    category_id,
    SUM(predicted_profit) as total_projected_profit,
    SUM(current_price * (predicted_profit/NULLIF(profit_index_implied,0))) as total_current_profit -- Approximation if needed
FROM optimization_results
GROUP BY 1;
