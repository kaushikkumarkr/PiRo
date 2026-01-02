import os
import pandas as pd
import numpy as np
import pymc as pm
import arviz as az
import mlflow
import argparse
from sqlalchemy import create_engine
from pipelines.utils import get_db_engine

# Improve PyMC performance
import pytensor.tensor as pt

def train_elasticity_model(category_id='sdr', samples=50, tune=50):
    print(f"Starting Elasticity Training for Category: {category_id}")
    
    # MLflow Setup - Robustness
    use_mlflow = False
    
    # 1. Load Data
    engine = get_db_engine()
    query = f"""
        select * from elasticity_ready_panel 
        where category_id = '{category_id}'
    """
    print("Fetching data from Postgres...")
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print(f"No data found for category {category_id}")
        return

    print(f"Loaded {len(df)} rows. Unique UPCs: {df['upc_id'].nunique()}. Unique Stores: {df['store_id'].nunique()}")

    # 2. Preprocessing
    # Label Encode IDs for Indexing
    store_idx, store_labels = pd.factorize(df['store_id'])
    upc_idx, upc_labels = pd.factorize(df['upc_id'])
    
    n_stores = len(store_labels)
    n_upcs = len(upc_labels)
    
    coords = {
        "store": store_labels,
        "upc": upc_labels,
        "obs_id": np.arange(len(df))
    }

    # 3. PyMC Model Definition
    print("Building Hierarchical Model...")
    with pm.Model(coords=coords) as model:
        # Data Containers
        try:
             log_price = pm.Data("log_price", df['log_price'].values, dims="obs_id")
             is_promo = pm.Data("is_promo", df['is_promo'].values, dims="obs_id")
             log_sales_obs = pm.Data("log_sales_obs", df['log_sales'].values, dims="obs_id")
        except:
             log_price = pm.Data("log_price", df['log_price'].values)
             is_promo = pm.Data("is_promo", df['is_promo'].values)
             log_sales_obs = pm.Data("log_sales_obs", df['log_sales'].values)
        
        # Hyperpriors
        mu_elasticity = pm.Normal("mu_elasticity", mu=-2.0, sigma=1.0)
        sigma_elasticity = pm.HalfNormal("sigma_elasticity", sigma=1.0)
        
        # UPC-level Elasticity
        beta_price = pm.Normal("beta_price", mu=mu_elasticity, sigma=sigma_elasticity, dims="upc")
        
        # Store-level Intercepts
        sigma_alpha = pm.HalfNormal("sigma_alpha", sigma=1.0)
        alpha_store = pm.Normal("alpha_store", mu=0, sigma=sigma_alpha, dims="store")
        
        # Promo Effect
        beta_promo = pm.Normal("beta_promo", mu=0.5, sigma=0.5)
        
        # Model Mean
        mu = alpha_store[store_idx] + beta_price[upc_idx] * log_price + beta_promo * is_promo
        
        # Likelihood noise
        sigma_y = pm.HalfNormal("sigma_y", sigma=1.0)
        
        # Likelihood
        pm.Normal("g", mu=mu, sigma=sigma_y, observed=log_sales_obs, dims="obs_id")

    # 4. Sampling
    print(f"Sampling {samples} draws...")
    
    # Conditional MLflow Context
    if use_mlflow:
        try:
            run_context = mlflow.start_run()
        except:
            run_context = mlflow.start_run() # Let it crash if manual
    else:
        from contextlib import nullcontext
        run_context = nullcontext()

    with run_context:
        if use_mlflow:
            try:
                mlflow.log_param("category_id", category_id)
                mlflow.log_param("n_upcs", n_upcs)
            except: pass
        
        with model:
            trace = pm.sample(samples, tune=tune, target_accept=0.9, chains=2, cores=2)
            
        # 5. Diagnostics
        print("Calculating Diagnostics...")
        summary = az.summary(trace, var_names=["mu_elasticity", "beta_promo", "beta_price"])
        print(summary.head())
        
        if use_mlflow:
            try:
                mlflow.log_metric("mu_elasticity_mean", summary.loc['mu_elasticity', 'mean'])
            except: pass
        
        # 6. Extract Catalog & Save
        print("Generating Elasticity Catalog...")
        
        # Calculate HDI on original trace (with chain/draw dims)
        beta_price_hdi = az.hdi(trace.posterior["beta_price"], hdi_prob=0.95)
        
        # Calculate Means on stacked
        posterior_stacked = trace.posterior.stack(draws=("chain", "draw"))
        beta_price_means = posterior_stacked["beta_price"].mean(dim="draws").values
        beta_promo_mean = float(posterior_stacked["beta_promo"].mean(dim="draws").values)

        catalog_rows = []
        for i, label in enumerate(upc_labels):
            # HDI is a Dataset. We need the DataArray "beta_price"
            # .sel(hdi="lower") could be "lower" or "higher"
            # Use .item() to get scalar
            try:
                da = beta_price_hdi["beta_price"]
                lower = da.isel(upc=i).sel(hdi="lower").values.item()
                upper = da.isel(upc=i).sel(hdi="higher").values.item()
            except Exception as e:
                print(f"Error extracting HDI for {label}: {e}")
                lower, upper = 0.0, 0.0
            
            catalog_rows.append({
                "category_id": category_id,
                "upc_id": label,
                "elasticity": float(beta_price_means[i]),
                "ci_lower": lower,
                "ci_upper": upper,
                "promo_lift": beta_promo_mean
            })
            
        catalog_df = pd.DataFrame(catalog_rows)
        
        # Save to Postgres
        catalog_df.to_sql('elasticity_catalog', engine, if_exists='append', index=False)
        print("Catalog saved to Postgres.")
        
        # Save Artifact
        csv_path = f"/app/ml/elasticity/catalog_{category_id}.csv"
        catalog_df.to_csv(csv_path, index=False)
        if use_mlflow:
            try:
                mlflow.log_artifact(csv_path)
            except: pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--category", type=str, default="sdr", help="Category ID (sdr, cer, lnd, sna)")
    args = parser.parse_args()
    
    train_elasticity_model(category_id=args.category)
