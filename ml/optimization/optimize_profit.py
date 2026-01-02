import pandas as pd
import numpy as np
from ortools.linear_solver import pywraplp
import argparse
from sqlalchemy import create_engine
from pipelines.utils import get_db_engine

def optimize_profit(category_id='sdr', min_revenue_pct=0.95):
    print(f"Running Profit Optimization for {category_id}...")
    engine = get_db_engine()
    
    # 1. Load Simulation Results (Candidate Prices)
    # We need: upc_id, price_change_pct, revenue_index, profit_index, current_price
    # We assume 'scenario_results' has these.
    # Note: profit_index is relative. To solve Max Profit in Dollars, we need absolute numbers.
    # Or we maximize Total Profit Index * scaling factor? 
    # Because UPCs have different volumes, maximizing Sum(Profit Index) treats small and big UPCs equally! 
    # ERROR in Logic: We need weighted profit.
    # We need Base Profit / Base Revenue to weight them.
    
    # Let's fetch base metrics from 'elasticity_ready_panel' (latest week or avg)
    query_base = f"""
        select upc_id, 
               exp(avg(log_sales)) as base_units,
               exp(avg(log_price)) as base_price
        from elasticity_ready_panel
        where category_id = '{category_id}'
        group by 1
    """
    base_df = pd.read_sql(query_base, engine)
    
    # Approximate base cost = 0.7 * price
    base_df['base_revenue'] = base_df['base_units'] * base_df['base_price']
    base_df['base_profit'] = (base_df['base_price'] * 0.3) * base_df['base_units'] # 30% margin
    
    # Load Scenarios
    query_scenarios = f"""
        select * from scenario_results 
        where category_id = '{category_id}'
        -- filter out extreme changes if needed
    """
    scenarios_df = pd.read_sql(query_scenarios, engine)
    
    # Join to get absolute values
    # scenarios_df has 'revenue_index' (Multiplier) and 'profit_index' (Multiplier)
    merged = scenarios_df.merge(base_df, on='upc_id', how='inner')
    
    merged['abs_revenue_sim'] = merged['revenue_index'] * merged['base_revenue']
    merged['abs_profit_sim'] = merged['profit_index'] * merged['base_profit']
    
    # 2. Setup Optimization Problem using OR-Tools (MIP)
    solver = pywraplp.Solver.CreateSolver('SCIP')
    if not solver:
        print("SCIP solver not found.")
        return

    # Variables: x[upc, scenario_idx] -> Binary
    # Unique UPCs
    upcs = merged['upc_id'].unique()
    
    variables = {}
    
    # For constraints
    total_base_revenue = base_df['base_revenue'].sum()
    print(f"Total Base Revenue: ${total_base_revenue:,.2f}")
    
    # Objective Terms
    obj_terms = []
    
    # Revenue constraint terms
    rev_terms = []
    
    print("Creating variables and constraints...")
    
    for upc in upcs:
        upc_data = merged[merged['upc_id'] == upc].reset_index(drop=True)
        
        # Constraint: Select exactly one price for this UPC
        # sum(x_i_k) = 1
        x_vars = []
        for idx, row in upc_data.iterrows():
            # Variable name: x_UPC_IDX
            var_name = f"x_{upc}_{idx}"
            x = solver.IntVar(0, 1, var_name)
            variables[var_name] = (x, row) # Store row for retrieval
            x_vars.append(x)
            
            # Add to objective (Maximize Profit)
            obj_terms.append(x * row['abs_profit_sim'])
            
            # Add to Revenue Constraint
            rev_terms.append(x * row['abs_revenue_sim'])
            
        solver.Add(solver.Sum(x_vars) == 1)
        
    # Global Constraint: Total Revenue >= min_revenue_pct * Total Base Revenue
    # sum(x_i_k * Rev_i_k) >= Threshold
    solver.Add(solver.Sum(rev_terms) >= (total_base_revenue * min_revenue_pct))
    
    # Objective: Maximize Total Profit
    solver.Maximize(solver.Sum(obj_terms))
    
    # 3. Solve
    print(f"Solving with Revenue Constraint >= {min_revenue_pct*100}%...")
    status = solver.Solve()
    
    results = []
    if status == pywraplp.Solver.OPTIMAL or status == pywraplp.Solver.FEASIBLE:
        print(f"Solution Found! Objective Value (Profit): ${solver.Objective().Value():,.2f}")
        
        total_rev_sim = 0
        
        for name, (var, row) in variables.items():
            if var.solution_value() > 0.5: # Selected
                results.append({
                    'category_id': category_id,
                    'upc_id': row['upc_id'],
                    'recommended_price': row['simulated_price'],
                    'current_price': row['current_price'],
                    'price_change_pct': row['price_change_pct'],
                    'predicted_revenue': row['abs_revenue_sim'],
                    'predicted_profit': row['abs_profit_sim']
                })
                total_rev_sim += row['abs_revenue_sim']
                
        print(f"Simulated Total Revenue: ${total_rev_sim:,.2f} ({(total_rev_sim/total_base_revenue - 1)*100:.2f}%)")
        
        # Save Recommendations
        rec_df = pd.DataFrame(results)
        rec_df.to_sql('optimization_results', engine, if_exists='replace', index=False)
        print("Optimization results saved to 'optimization_results'.")
        
    else:
        print("No solution found.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--category", type=str, default="sdr")
    args = parser.parse_args()
    
    optimize_profit(category_id=args.category)
