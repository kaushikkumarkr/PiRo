import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from scipy.spatial.distance import jensenshannon
import argparse
from pipelines.utils import get_db_engine

def calculate_psi(expected_array, actual_array, buckets=10):
    """
    Calculate Population Stability Index (PSI) between two distributions.
    PSI < 0.1: No change
    PSI 0.1 - 0.2: Slight change
    PSI > 0.2: Significant shift (Drift)
    """
    def scale_range(input, min, max):
        input += (1e-6)  # Avoid zero division
        return (input - min) / (max - min)

    breakpoints = np.arange(0, buckets + 1) / (buckets) * 100
    
    # Define buckets based on expected distribution
    try:
        expected_percents = np.percentile(expected_array, breakpoints)
    except IndexError:
        return 0.0

    # Bucketing
    expected_percents[0] = -np.inf
    expected_percents[-1] = np.inf
    
    expected_counts = np.histogram(expected_array, expected_percents)[0]
    actual_counts = np.histogram(actual_array, expected_percents)[0]

    # Calculate proportions
    expected_prop = expected_counts / len(expected_array)
    actual_prop = actual_counts / len(actual_array)
    
    # Avoid zero division
    expected_prop = np.where(expected_prop == 0, 0.0001, expected_prop)
    actual_prop = np.where(actual_prop == 0, 0.0001, actual_prop)
    
    # PSI
    psi_values = (expected_prop - actual_prop) * np.log(expected_prop / actual_prop)
    psi = np.sum(psi_values)
    
    return psi

def check_drift(start_date_train, end_date_train, start_date_serve, end_date_serve):
    print(f"Checking Drift: Train({start_date_train} to {end_date_train}) vs Serving({start_date_serve} to {end_date_serve})")
    
    engine = get_db_engine()
    
    # 1. Fetch Data
    query = f"""
        SELECT 
            event_timestamp,
            feat_log_price,
            log_sales
        FROM mart_feature_store
        WHERE event_timestamp BETWEEN '{start_date_train}' AND '{end_date_serve}'
    """
    df = pd.read_sql(query, engine)
    
    # Convert timestamp
    df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
    
    # Split
    df_train = df[(df['event_timestamp'] >= start_date_train) & (df['event_timestamp'] <= end_date_train)]
    df_serve = df[(df['event_timestamp'] >= start_date_serve) & (df['event_timestamp'] <= end_date_serve)]
    
    if len(df_serve) == 0:
        print("No serving data found for period.")
        return
        
    features = ['feat_log_price', 'log_sales']
    drift_detected = False
    
    print("\n--- Feature Drift Report (PSI) ---")
    for feat in features:
        psi = calculate_psi(df_train[feat].dropna().values, df_serve[feat].dropna().values)
        status = "PASS" if psi < 0.2 else "FAIL (DRIFT DETECTED)"
        print(f"Feature: {feat:<20} | PSI: {psi:.4f} | Status: {status}")
        
        if psi > 0.2:
            drift_detected = True
            
    if drift_detected:
        print("\n[ALERT] Significant Drift Detected! Recommendation: Trigger Re-Training.")
    else:
        print("\n[OK] System Stable. No re-training needed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # Defaults just for demo
    parser.add_argument("--train_start", default="1990-01-01")
    parser.add_argument("--train_end", default="1992-06-01")
    parser.add_argument("--serve_start", default="1992-06-01")
    parser.add_argument("--serve_end", default="1993-01-01")
    
    args = parser.parse_args()
    
    check_drift(args.train_start, args.train_end, args.serve_start, args.serve_end)
