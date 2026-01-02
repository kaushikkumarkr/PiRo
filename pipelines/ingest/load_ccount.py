import pandas as pd
import os
from pipelines.utils import get_db_engine

def load_ccount():
    print("Loading ccount (Store Traffic)...")
    data_path = "/app/data/raw/ccount(stata).zip" # Path inside container
    # If using local dev without docker, adjust path
    if not os.path.exists(data_path):
         # Try local path relative to repo root if running locally
         data_path = "data/raw/ccount(stata).zip"

    if not os.path.exists(data_path):
        # Try unzipped .dta
        data_path_dta = "/app/data/raw/ccount.dta" 
        if os.path.exists(data_path_dta):
            data_path = data_path_dta
        elif os.path.exists("data/raw/ccount.dta"):
            data_path = "data/raw/ccount.dta"

    if not os.path.exists(data_path):
        print(f"Skipping ccount: File not found at {data_path}")
        return

    # Pandas can read directly from zip if it contains one file
    # We assume standard Stata zip from Kilts Center
    try:
        df = pd.read_stata(data_path)
    except Exception as e:
        print(f"Error reading Stata file: {e}")
        # Build robustness: try extracting first
        return

    # Clean columns
    df.columns = [c.lower() for c in df.columns]
    
    # Simple rename for clarity if needed, or keeping raw
    # raw_ccount: store, date, count
    
    # Store to DB
    engine = get_db_engine()
    df.to_sql('raw_ccount', engine, schema='public', if_exists='replace', index=False, chunksize=10000)
    print(f"Loaded {len(df)} rows into raw_ccount.")

if __name__ == "__main__":
    load_ccount()
