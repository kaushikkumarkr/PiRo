import pandas as pd
import numpy as np
import pymc as pm
import arviz as az
from sqlalchemy import create_engine
from pipelines.utils import get_db_engine

def train_cross_elasticity(category_id='sdr'):
    print(f"Training Cross-Elasticity Model for Category: {category_id}")
    engine = get_db_engine()
    
    # 1. Identify Top 5 UPCs
    print("Identifying Top 5 UPCs...")
    top_upc_query = f"""
        SELECT u.upc_id, u.description 
        FROM fact_movement_weekly m
        JOIN dim_upc u USING(upc_id) 
        WHERE u.category_id = '{category_id}' 
        GROUP BY 1, 2 
        ORDER BY SUM(sales_units) DESC 
        LIMIT 5
    """
    top_upcs = pd.read_sql(top_upc_query, engine)
    upc_list = top_upcs['upc_id'].tolist()
    upc_map = {i: u for i, u in enumerate(upc_list)}
    print(f"Top UPCs: {upc_list}")
    
    # 2. Construct Dynamic Pivot Query
    # We build this dynamically to ensure alignment with upc_list order
    select_clauses = []
    for i, upc in enumerate(upc_list):
        select_clauses.append(f"MAX(CASE WHEN upc_id = {upc} THEN log_price END) as ln_p_{i}")
        select_clauses.append(f"MAX(CASE WHEN upc_id = {upc} THEN log_sales END) as ln_q_{i}")
        
    query = f"""
        SELECT 
            week_id,
            store_id,
            {', '.join(select_clauses)}
        FROM elasticity_ready_panel
        WHERE upc_id IN ({','.join(map(str, upc_list))})
        GROUP BY 1, 2
        HAVING COUNT(DISTINCT upc_id) = {len(upc_list)} -- Ensure all products present
    """
    
    print("Fetching data...")
    df = pd.read_sql(query, engine)
    print(f"Data Shape: {df.shape}")
    
    if len(df) < 50:
        print("Not enough data points for reliable estimation.")
        return

    # 3. Prepare Data for PyMC
    # Y: Log Sales (N_samples x 5)
    # X: Log Prices (N_samples x 5)
    
    N_products = len(upc_list)
    Y_obs = df[[f'ln_q_{i}' for i in range(N_products)]].values
    X_obs = df[[f'ln_p_{i}' for i in range(N_products)]].values
    
    # Normalize X for better convergence
    X_mean = X_obs.mean(axis=0)
    X_centered = X_obs - X_mean
    
    print("Building PyMC Model...")
    with pm.Model() as model:
        # Priors
        # Alpha (Intercepts): 5 vector
        alpha = pm.Normal('alpha', mu=0, sigma=10, shape=N_products)
        
        # Beta Matrix (Elasticities): 5x5 matrix
        # We can model this as a flattened vector or matrix
        # Priors: Diagonal (Own) ~ Normal(-2, 1), Off-Diagonal (Cross) ~ Normal(0, 1)
        
        beta_list = []
        for i in range(N_products):
            row = []
            for j in range(N_products):
                if i == j:
                    # Own elasticity
                    b = pm.Normal(f'beta_{i}_{j}', mu=-2.0, sigma=1.0)
                else:
                    # Cross elasticity
                    b = pm.Normal(f'beta_{i}_{j}', mu=0.0, sigma=1.0)
                row.append(b)
            beta_list.append(row)
        
        # Convert list of lists to tensor
        B = pm.math.stack([pm.math.stack(row) for row in beta_list])
        
        # Expected Log Sales
        # mu = alpha + B @ X
        # Shape: (5,) + (5x5) @ (5,)
        # We process in batches: (N, 5)
        
        # mu = alpha + X @ B.T
        mu = alpha + pm.math.dot(X_centered, B.T)
        
        # Likelihood
        sigma = pm.HalfNormal('sigma', sigma=1, shape=N_products)
        
        # MvNormal is traditional for systems of demand, but independent Normal is often sufficient log-log
        # unless errors are correlated (e.g., category shock). Let's use independent for robustness speed
        
        Y_est = pm.Normal('Y_obs', mu=mu, sigma=sigma, observed=Y_obs)
        
        # Sampling
        print("Fitting ADVI...")
        with model:
            approx = pm.fit(n=5000, method='advi', progressbar=True)
            trace = approx.sample(draws=1000)
        
    # 4. Extract Results
    print("Extracting Matrix...")
    summary = az.summary(trace, var_names=['beta_.*'], filter_vars="regex")
    
    results = []
    for i in range(N_products):
        for j in range(N_products):
            var_name = f"beta_{i}_{j}"
            mean_val = summary.loc[var_name, 'mean']
            ci_lower = summary.loc[var_name, 'hdi_3%']
            ci_upper = summary.loc[var_name, 'hdi_97%']
            
            results.append({
                'category_id': category_id,
                'upc_id_target': upc_map[i],
                'upc_id_driver': upc_map[j],
                'elasticity': mean_val,
                'ci_lower': ci_lower,
                'ci_upper': ci_upper,
                'is_own_elasticity': (i == j)
            })
            
    # 5. Save to DB
    res_df = pd.DataFrame(results)
    res_df.to_sql('cross_elasticity_matrix', engine, if_exists='replace', index=False)
    
    print("Cross-Elasticity Matrix Saved:")
    print(res_df.pivot(index='upc_id_target', columns='upc_id_driver', values='elasticity'))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--category", type=str, default="sdr")
    args = parser.parse_args()
    
    train_cross_elasticity(category_id=args.category)
