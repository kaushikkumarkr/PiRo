import pandas as pd
import numpy as np
import argparse
from sqlalchemy import create_engine
from pipelines.utils import get_db_engine

def run_simulation(category_id='sdr', min_change_pct=-0.20, max_change_pct=0.20, steps=20):
    print(f"Running Price Simulation for {category_id}...")
    engine = get_db_engine()
    
    # 1. Load Elasticity Model Coefficients
    query_cat = f"""
        select * from elasticity_catalog
        -- where category_id = '{category_id}' -- Assuming catalog has category_id or we filter by UPCs
    """
    elasticity_df = pd.read_sql(query_cat, engine)
    
    # 2. Load Current Prices and Costs (Using avg cost proxy = price * 0.7 for now if cost unknown)
    # We need a reference price. Let's take the latest price from elasticity_ready_panel
    query_prices = f"""
        select distinct on (upc_id) 
            upc_id, exp(log_price) as current_price
        from elasticity_ready_panel
        where category_id = '{category_id}'
        order by upc_id, week_id desc
    """
    prices_df = pd.read_sql(query_prices, engine)
    
    # Merge
    # Note: elasticity_catalog might be keyed by specific UPCs.
    # We need to map elasticity back to UPCs.
    # In Sprint 3, elasticity_catalog has: category_id, upc_id, mu_elasticity (if pooled) or beta_price.
    # Let's inspect catalog schema if column names are needed. 
    # Assuming catalog has 'upc_id' and 'mean_elasticity'.
    
    merged = prices_df.merge(elasticity_df, on='upc_id', how='inner')
    print(f"Simulating for {len(merged)} UPCs.")
    
    scenario_results = []
    
    for _, row in merged.iterrows():
        upc = row['upc_id']
        curr_price = row['current_price']
        elasticity = row['elasticity'] 
        
        # Calculate Reference Quantity (Q_curr) if needed, or use Relative Change
        # Ratio Q_new / Q_curr = (P_new / P_curr) ^ elasticity
        # This avoids needing the intercept (alpha).
        # Simplification: Linear approximation around current point using elasticity.
        # % Change in Qty = Elasticity * % Change in Price
        # Base Qty = exp(intercept + elasticity * log(curr_price)) ... roughly.
        
        # Better: Use the formula: Q_new = Q_curr * (P_new / P_curr) ^ Elasticity
        # We need Q_curr (Current Baseline Volume). 
        # Let's fetch average recent volume as baseline.
        
        # Generate Price Grid
        price_grid = np.linspace(curr_price * (1 + min_change_pct), curr_price * (1 + max_change_pct), steps)
        
        # Assume Q_curr is 100 (index) to just normalize, or fetch real avg sales.
        # Let's use relative revenue impact.
        # New Revenue Index = (P_new * Q_new) / (P_curr * Q_curr)
        #                   = (P_new / P_curr) * (Q_new / Q_curr)
        #                   = (P_new / P_curr) * (P_new / P_curr) ^ Elasticity
        #                   = (P_new / P_curr) ^ (1 + Elasticity)
        
        for p_sim in price_grid:
            pct_change_price = (p_sim - curr_price) / curr_price
            
            # Revenue Impact Factor
            revenue_index = (p_sim / curr_price) ** (1 + elasticity)
            
            # Profit Impact? Need Cost.
            # Assume constant margin % for baseline?? No, cost is fixed usually.
            # let Cost = 0.7 * curr_price
            cost = 0.7 * curr_price
            
            # Profit_curr = (curr_price - cost) * Q_curr
            # Profit_new = (p_sim - cost) * Q_new
            #            = (p_sim - cost) * Q_curr * (p_sim/curr_price)^elasticity
            
            # Profit Index = Profit_new / Profit_curr
            #              = ((p_sim - cost) / (curr_price - cost)) * (p_sim/curr_price)^elasticity
            
            base_margin = curr_price - cost
            sim_margin = p_sim - cost
            
            if base_margin <= 0: base_margin = 0.01 # Avoid div by zero
            
            profit_index = (sim_margin / base_margin) * ((p_sim / curr_price) ** elasticity)
            
            scenario_results.append({
                'category_id': category_id,
                'upc_id': upc,
                'current_price': curr_price,
                'simulated_price': p_sim,
                'price_change_pct': pct_change_price,
                'elasticity': elasticity,
                'revenue_index': revenue_index,
                'profit_index': profit_index
            })

    res_df = pd.DataFrame(scenario_results)
    
    # Save
    res_df.to_sql('scenario_results', engine, if_exists='replace', index=False)
    print("Scenario results saved to 'scenario_results'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--category", type=str, default="sdr")
    args = parser.parse_args()
    
    run_simulation(category_id=args.category)
