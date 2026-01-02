import pandas as pd
import numpy as np
import statsmodels.api as sm
import argparse
from sqlalchemy import create_engine
from pipelines.utils import get_db_engine

def analyze_heterogeneity(category_id='sdr'):
    print(f"Analyzing Heterogeneity for Category: {category_id}")
    engine = get_db_engine()
    
    # 1. Load Data
    # Panel
    query_panel = f"""
        select * from elasticity_ready_panel 
        where category_id = '{category_id}'
    """
    df = pd.read_sql(query_panel, engine)
    
    # Demographics
    query_demo = "select * from dim_store_demographics"
    demo_df = pd.read_sql(query_demo, engine)
    
    # Join
    df = df.merge(demo_df, on='store_id', how='inner')
    print(f"Merged Data Shape: {df.shape}")
    
    # Preprocessing
    if 'log_price' not in df.columns and 'price' in df.columns:
         df['log_price'] = np.log(df['price'])
    # log_median_income is already logged in dim (assuming DFF standard)
    
    # 2. Model: log_sales ~ log_price * log_median_income + promo_depth + ...
    # We want to see if Price Elasticity (beta) depends on Income.
    # Interaction term: log_price * log_median_income
    
    df['interaction_price_income'] = df['log_price'] * df['log_median_income']
    # df['log_sales'] = np.log(df['sales_units'])
    if 'log_sales' not in df.columns and 'sales_units' in df.columns:
         df['log_sales'] = np.log(df['sales_units'])
    
    df['promo_depth'] = df['promo_depth'].fillna(0)
    
    # Drop NAs
    model_df = df[['log_sales', 'log_price', 'log_median_income', 'interaction_price_income', 'promo_depth']].dropna()
    
    # Add constant
    X = model_df[['log_price', 'log_median_income', 'interaction_price_income', 'promo_depth']]
    X = sm.add_constant(X)
    y = model_df['log_sales']
    
    # Fit OLS
    model = sm.OLS(y, X).fit()
    print(model.summary())
    
    # Interpretation
    base_elas = model.params['log_price']
    interaction = model.params['interaction_price_income']
    p_val = model.pvalues['interaction_price_income']
    
    print("\n--- Results ---")
    print(f"Base Elasticity: {base_elas:.4f}")
    print(f"Interaction (Price x Income): {interaction:.4f} (p={p_val:.4f})")
    
    if p_val < 0.05:
        print("SIGNIFICANT Heterogeneity detected.")
        if interaction > 0:
            print("Positive interaction: Higher income -> Less negative elasticity (Less price sensitive).")
        else:
            print("Negative interaction: Higher income -> More negative elasticity (More price sensitive).")
    else:
        print("No significant heterogeneity detected.")

    # Save summary results?
    # Maybe just print for now.

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--category", type=str, default="sdr")
    args = parser.parse_args()
    
    analyze_heterogeneity(category_id=args.category)
