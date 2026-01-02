import pandas as pd
import numpy as np
import statsmodels.api as sm
import argparse
from sqlalchemy import create_engine
from pipelines.utils import get_db_engine

def estimate_uplift(category_id='sdr'):
    print(f"Estimating Promo Uplift for Category: {category_id}")
    engine = get_db_engine()
    
    # Load Data
    query = f"""
        select * from elasticity_ready_panel 
        where category_id = '{category_id}'
    """
    df = pd.read_sql(query, engine)
    print(f"Loaded {len(df)} rows.")
    
    # Simple log-log model with promo features
    # log_sales ~ log_price + promo_depth + trend + seasonality(month)
    
    # Preprocessing
    # df['log_price'] = np.log(df['price'])
    # df['log_sales'] = np.log(df['sales_units']) # Already logged in dbt?
    # Check if log_sales is in columns, dbt sql says 'log_sales'
    
    if 'log_price' not in df.columns and 'price' in df.columns:
         df['log_price'] = np.log(df['price'])
         
    df['promo_depth'] = df['promo_depth'].fillna(0)
    
    # Run separate regression for each UPC? Or pooled?
    # Let's do pooled with UPC fixed effects (dummy variables)
    # Be careful with dummy variable trap, drop_first=True
    
    results = []
    
    # Iterate per UPC for granular lift
    for upc in df['upc_id'].unique():
        sub = df[df['upc_id'] == upc].copy()
        if len(sub) < 50:
            continue
            
        X = sub[['log_price', 'promo_depth']]
        X = sm.add_constant(X)
        y = sub['log_sales']
        
        try:
            model = sm.OLS(y, X).fit()
            
            # Extract coefficients
            elasticity = model.params.get('log_price', 0)
            lift_coef = model.params.get('promo_depth', 0) 
            # Note: if promo_depth is 0.2 (20% off), lift is exp(coef * 0.2)
            
            # Calculate Average Lift for this UPC
            # Avg Promo Depth when promo is active
            avg_depth = sub[sub['promo_depth'] > 0]['promo_depth'].mean()
            if pd.isna(avg_depth): avg_depth = 0
            
            # Expected % Volume Increase = exp(lift_coef * avg_depth) - 1
            expected_lift_pct = (np.exp(lift_coef * avg_depth) - 1) * 100
            
            results.append({
                'category_id': category_id,
                'upc_id': upc,
                'base_elasticity': elasticity,
                'promo_sensitivity': lift_coef,
                'avg_promo_depth': avg_depth,
                'avg_lift_pct': expected_lift_pct,
                'r2': model.rsquared
            })
        except Exception as e:
            print(f"Error modeling UPC {upc}: {e}")
            
    res_df = pd.DataFrame(results)
    print(res_df.head())
    
    # Save to DB
    res_df.to_sql('promo_lift_estimates', engine, if_exists='append', index=False)
    print("Saved lift estimates to 'promo_lift_estimates' table.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--category", type=str, default="sdr")
    args = parser.parse_args()
    
    estimate_uplift(category_id=args.category)
