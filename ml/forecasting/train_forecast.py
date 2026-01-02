import pandas as pd
import numpy as np
from statsforecast import StatsForecast
from statsforecast.models import AutoARIMA
from sqlalchemy import create_engine
from pipelines.utils import get_db_engine
import argparse
import os

def run_forecast(category_id='sdr', horizon=12):
    print(f"Starting Forecasting for Category: {category_id}")
    engine = get_db_engine()
    
    # 1. Load Panel Data
    query = f"""
        select 
            store_id, 
            upc_id, 
            start_date as ds, 
            exp(log_sales) as y,
            exp(log_price) as price,
            is_promo
        from elasticity_ready_panel 
        where category_id = '{category_id}'
        order by store_id, upc_id, start_date
    """
    df = pd.read_sql(query, engine)
    
    # 2. Prepare for StatsForecast
    # unique_id = store_id + '_' + upc_id
    df['unique_id'] = df['store_id'].astype(str) + '_' + df['upc_id'].astype(str)
    df['ds'] = pd.to_datetime(df['ds'])
    
    # StatsForecast expects: unique_id, ds, y
    # We can handle exogenous variables (price, is_promo) in AutoARIMA if we want, 
    # but for a pure baseline, univariate might be safer/faster first. 
    # Let's try univariate first to ensure stability, then add exog if time permits.
    # Actually, the plan mentions covariates. Let's try with Price as a regressor.
    
    # Check data sufficiency
    counts = df.groupby('unique_id').size()
    valid_ids = counts[counts > 20].index # Need some history
    df = df[df['unique_id'].isin(valid_ids)]
    
    print(f"Training on {len(valid_ids)} time series...")
    
    # 3. Define Models
    models = [
        AutoARIMA(season_length=52), 
    ]
    
    sf = StatsForecast(
        models=models,
        freq='W',
        n_jobs=-1
    )
    
    # 4. Fit & Forecast
    # For future exog, we need future values.
    # Scenario: Constant Price (Last observed), No Promo.
    
    # Create future frame
    # StatsForecast fit can take X_train. 
    # But dealing with exog in SF can be tricky with signatures.
    # Let's stick to Univariate Baseline for "Inventory Planning" scenario first.
    # If we want Price optimization, we use the Elasticity Model (PyMC), not ARIMA.
    # The ARIMA here is for "Base Demand" trend.
    
    sf.fit(df[['unique_id', 'ds', 'y']])
    forecast_df = sf.predict(h=horizon, level=[90])
    
    print("Forecast generated. Sample:")
    print(forecast_df.head())
    
    # 5. Save to DB
    forecast_df.reset_index(inplace=True)
    forecast_df['category_id'] = category_id
    forecast_df['created_at'] = pd.Timestamp.now()
    
    # Split unique_id back to store/upc
    # Assumes store_upc format
    forecast_df['store_id'] = forecast_df['unique_id'].apply(lambda x: x.split('_')[0])
    forecast_df['upc_id'] = forecast_df['unique_id'].apply(lambda x: x.split('_')[1])
    
    table_name = 'baseline_forecasts'
    forecast_df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"Saved forecasts to table '{table_name}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--category", type=str, default="sdr")
    args = parser.parse_args()
    
    run_forecast(category_id=args.category)
